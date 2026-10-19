"""Tests for the post-migration Studio activation boundary."""

import asyncio
import logging
from dataclasses import dataclass
from types import SimpleNamespace
from collections.abc import Awaitable, Callable
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import FastAPI

from databricks_labs_dqx_app.backend.config import AppConfig
from databricks_labs_dqx_app.backend.setup.access import AudienceAccess
from databricks_labs_dqx_app.backend.setup.audience import resolve_audience
from databricks_labs_dqx_app.backend.runtime import Runtime
from databricks_labs_dqx_app.backend.setup.resources import (
    ActiveResources,
    BootstrapResources,
    LakebaseConnection,
    VolumeLocation,
)
from databricks_labs_dqx_app.backend.setup.models import SetupReport, SetupState, SetupStep, SetupStepId, StepState
from databricks_labs_dqx_app.backend.setup.runtime import setup_runtime
from databricks_labs_dqx_app.backend.setup.verification_memo import VerificationMemo
from databricks_labs_dqx_app.backend.startup import (
    StartupContext,
    activate_studio,
    deactivate_studio,
    start_studio_background,
)


@pytest.fixture
def resources() -> ActiveResources:
    return ActiveResources(
        volume=VolumeLocation("main", "studio", "wheels", "/Volumes/main/studio/wheels"),
        lakebase=LakebaseConnection(
            endpoint="projects/p/branches/b/endpoints/primary",
            host=None,
            port=5432,
            database="databricks_postgres",
            username=None,
            password=None,
            schema="studio",
        ),
        warehouse_id="warehouse-id",
        job_id="29",
        tmp_schema="studio_tmp",
        genie_schema="genie",
        demo_schema="studio_demo",
        audience=resolve_audience(["data-team"], "admins", allow_broad=False),
    )


def _hook(name: str, events: list[str]) -> Callable[[], Awaitable[None]]:
    async def run() -> None:
        events.append(name)

    return run


@pytest.mark.asyncio
async def test_activation_publishes_resources_and_starts_hooks_once(resources: ActiveResources) -> None:
    events: list[str] = []
    runtime = Runtime()
    oltp = object()

    def register(executor: object | None) -> None:
        events.append("register" if executor is oltp else "clear")

    context = StartupContext(
        resources=resources,
        runtime=runtime,
        oltp_executor=oltp,
        register_oltp=register,
        activation_hooks=(_hook("views_and_seeds", events),),
        background_hooks=(_hook("scheduler_and_ai", events),),
        shutdown_hooks=(_hook("stop_background", events),),
    )

    await activate_studio(context)
    await activate_studio(context)

    assert runtime.require_resources() is resources
    assert events == ["register", "views_and_seeds"]

    await start_studio_background(context)
    await start_studio_background(context)

    assert events == ["register", "views_and_seeds", "scheduler_and_ai"]


@pytest.mark.asyncio
async def test_deactivation_cleans_opened_resources_even_after_partial_activation(resources: ActiveResources) -> None:
    events: list[str] = []

    async def fail_after_open() -> None:
        events.append("start_failed")
        raise RuntimeError("startup failed")

    context = StartupContext(
        resources=resources,
        runtime=Runtime(),
        oltp_executor=object(),
        register_oltp=lambda executor: events.append("register" if executor is not None else "clear"),
        activation_hooks=(fail_after_open,),
        shutdown_hooks=(_hook("close_pool", events),),
    )

    with pytest.raises(RuntimeError, match="startup failed"):
        await activate_studio(context)
    await deactivate_studio(context)

    assert events == ["register", "start_failed", "close_pool", "clear"]


@pytest.mark.asyncio
async def test_deactivation_attempts_all_cleanup_after_shutdown_hook_failure(resources: ActiveResources) -> None:
    events: list[str] = []
    runtime = Runtime()
    runtime.activate(resources)

    async def fail_scheduler_stop() -> None:
        events.append("scheduler_stop_failed")
        raise RuntimeError("scheduler stop failed")

    context = StartupContext(
        resources=resources,
        runtime=runtime,
        oltp_executor=object(),
        register_oltp=lambda executor: events.append("register" if executor is not None else "clear"),
        shutdown_hooks=(
            fail_scheduler_stop,
            _hook("cancel_ai", events),
            _hook("close_pool", events),
        ),
        opened=True,
        active=True,
    )

    with pytest.raises(RuntimeError, match="DQX Studio shutdown cleanup failed") as error:
        await deactivate_studio(context)

    assert "scheduler stop failed" not in str(error.value)
    assert events == ["scheduler_stop_failed", "cancel_ai", "close_pool", "clear"]
    with pytest.raises(RuntimeError, match="resources are not ready"):
        runtime.require_resources()
    assert context.opened is False
    assert context.active is False


@pytest.mark.asyncio
async def test_fastapi_lifespan_yields_restricted_app_and_always_cleans_up(monkeypatch) -> None:
    from fastapi import FastAPI

    from databricks_labs_dqx_app.backend import app as app_module

    events: list[str] = []
    lifecycle = object()

    async def start(_app: FastAPI) -> object:
        events.append("start")
        setup_runtime.publish(
            SetupReport(
                state=SetupState.SETUP_REQUIRED,
                current_step=SetupStepId.TASK_RUNNER,
                steps=(
                    SetupStep(
                        id=SetupStepId.TASK_RUNNER,
                        state=StepState.ACTION_REQUIRED,
                        code="task_runner_run_as_missing",
                    ),
                ),
            )
        )
        return lifecycle

    async def stop(received: object | None) -> None:
        assert received is lifecycle
        events.append("stop")

    monkeypatch.setattr(app_module, "start_studio", start)
    monkeypatch.setattr(app_module, "stop_studio", stop)

    async with app_module.lifespan(FastAPI()):
        events.append("served")
        assert setup_runtime.report().state == SetupState.SETUP_REQUIRED

    assert events == ["start", "served", "stop"]


def _bootstrap(resources: ActiveResources) -> BootstrapResources:
    return BootstrapResources(resources.lakebase, resources.warehouse_id, resources.job_id)


def _patch_startup(
    monkeypatch: pytest.MonkeyPatch,
    resources: ActiveResources,
    *,
    workspace: MagicMock | None = None,
    delta_sql: MagicMock | None = None,
    pg_executor: MagicMock | None = None,
    orchestrator: MagicMock | None = None,
    patch_bootstrap: bool = True,
) -> dict[str, object]:
    """Patch startup collaborators and capture the orchestrator constructor arguments."""
    from databricks_labs_dqx_app.backend import startup

    captured: dict[str, object] = {}
    sp_workspace = workspace or MagicMock()
    compute = MagicMock()
    compute.sp_application_id.return_value = "app-sp"
    setup_orchestrator = orchestrator or MagicMock()
    if orchestrator is None:
        setup_orchestrator.reconcile = AsyncMock()

    async def get_workspace() -> MagicMock:
        return sp_workspace

    def capture(**kwargs: object) -> MagicMock:
        captured.update(kwargs)
        return setup_orchestrator

    if patch_bootstrap:
        monkeypatch.setattr(startup, "_resolve_bootstrap", lambda: _bootstrap(resources))
    monkeypatch.setattr(startup, "get_sp_ws", get_workspace)
    monkeypatch.setattr(startup, "SqlExecutor", lambda **_kwargs: delta_sql or MagicMock())
    monkeypatch.setattr(
        startup, "build_pg_executor_from_connection", lambda *_args, **_kwargs: pg_executor or MagicMock()
    )
    monkeypatch.setattr(startup, "AppSettingsService", lambda **_kwargs: MagicMock())
    monkeypatch.setattr(startup, "ComputeService", lambda **_kwargs: compute)
    monkeypatch.setattr(startup, "ResourceCheckers", lambda **_kwargs: MagicMock())
    monkeypatch.setattr(startup, "TaskRunnerJobManager", lambda *_args: MagicMock())
    monkeypatch.setattr(startup, "PgMigrationRunner", lambda *_args: MagicMock())
    monkeypatch.setattr(startup, "MigrationRunner", lambda *_args: MagicMock())
    monkeypatch.setattr(startup, "SetupOrchestrator", capture)
    return captured


@pytest.mark.asyncio
async def test_successful_startup_metadata_refresh_seeds_genie_cache(
    resources: ActiveResources, monkeypatch: pytest.MonkeyPatch
) -> None:
    from databricks_labs_dqx_app.backend import startup
    from databricks_labs_dqx_app.backend.routes.v1 import genie

    delta_sql = MagicMock()
    delta_sql.q.side_effect = lambda value: f"`{value}`"
    startup_metadata_dims = MagicMock()
    request_metadata_dims = MagicMock()
    captured = _patch_startup(monkeypatch, resources, delta_sql=delta_sql)
    monkeypatch.setattr(startup, "MetadataDimService", lambda **_kwargs: startup_metadata_dims)
    monkeypatch.setattr(startup, "_ensure_score_views", lambda *_args: None)
    monkeypatch.setattr(startup, "ensure_entitlement_objects", lambda *_args: None)
    monkeypatch.setattr(startup, "_ensure_genie_space", lambda *_args: None)
    monkeypatch.setattr(startup, "mark_tmp_schema_ready", lambda: None)
    monkeypatch.setattr(startup, "_stop_background_services", AsyncMock())

    lifecycle = await startup.start_studio(FastAPI())
    assert lifecycle is not None
    try:
        bound = await captured["binder"].bind(resources)
        await bound.activation.activate()
        await genie.refresh_metadata_dims(request_metadata_dims)
    finally:
        await startup.stop_studio(lifecycle)

    startup_metadata_dims.refresh.assert_called_once_with()
    request_metadata_dims.refresh.assert_not_called()
    grant_statements = [call.args[0] for call in delta_sql.execute_no_schema.call_args_list]
    assert not any("account users" in statement for statement in grant_statements)


@pytest.mark.asyncio
@pytest.mark.parametrize("failing_view", ["score", "entitlement"])
async def test_startup_does_not_activate_when_required_view_fails(
    resources: ActiveResources, monkeypatch: pytest.MonkeyPatch, failing_view: str
) -> None:
    """A failed view DDL must keep setup from reporting the app as ready."""
    from databricks_labs_dqx_app.backend import startup

    delta_sql = MagicMock()
    delta_sql.q.side_effect = lambda value: f"`{value}`"
    delta_sql.execute.side_effect = RuntimeError("SQLSTATE 42501")
    captured = _patch_startup(monkeypatch, resources, delta_sql=delta_sql)
    monkeypatch.setattr(startup, "_ensure_metadata_dims", AsyncMock())
    if failing_view == "score":
        monkeypatch.setattr(startup, "ensure_entitlement_objects", lambda *_args: None)
    else:
        monkeypatch.setattr(startup, "_ensure_score_views", lambda *_args: None)
    monkeypatch.setattr(startup, "_ensure_genie_space", lambda *_args: None)
    monkeypatch.setattr(startup, "mark_tmp_schema_ready", lambda: None)
    monkeypatch.setattr(startup, "_stop_background_services", AsyncMock())

    lifecycle = await startup.start_studio(FastAPI())
    assert lifecycle is not None
    try:
        bound = await captured["binder"].bind(resources)
        with pytest.raises(RuntimeError, match="Could not create required Studio views"):
            await bound.activation.activate()
    finally:
        await startup.stop_studio(lifecycle)


def _patch_metadata_activation(
    monkeypatch: pytest.MonkeyPatch, resources: ActiveResources, *, tables_exist: bool
) -> tuple[dict[str, object], MagicMock, MagicMock]:
    from databricks_labs_dqx_app.backend import startup

    workspace = MagicMock()
    workspace.tables.exists.return_value = SimpleNamespace(table_exists=tables_exist)
    metadata_dims = MagicMock()
    metadata_dims.refresh.side_effect = [RuntimeError("SQLSTATE 42501"), None]
    captured = _patch_startup(monkeypatch, resources, workspace=workspace)
    monkeypatch.setattr(startup, "MetadataDimService", lambda **_kwargs: metadata_dims)
    monkeypatch.setattr(startup, "_ensure_score_views", lambda *_args: None)
    monkeypatch.setattr(startup, "ensure_entitlement_objects", lambda *_args: None)
    monkeypatch.setattr(startup, "_ensure_genie_space", lambda *_args: None)
    monkeypatch.setattr(startup, "mark_tmp_schema_ready", lambda: None)
    monkeypatch.setattr(startup, "_stop_background_services", AsyncMock())
    return captured, metadata_dims, workspace


@pytest.mark.asyncio
async def test_first_metadata_dimension_failure_blocks_activation_until_retry(
    resources: ActiveResources, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Without the metadata tables the access step cannot grant them, so activation must wait."""
    from databricks_labs_dqx_app.backend import startup
    from databricks_labs_dqx_app.backend.setup.errors import RequiredViewSetupError

    captured, metadata_dims, _ = _patch_metadata_activation(monkeypatch, resources, tables_exist=False)

    lifecycle = await startup.start_studio(FastAPI())
    assert lifecycle is not None
    try:
        bound = await captured["binder"].bind(resources)
        with pytest.raises(RequiredViewSetupError):
            await bound.activation.activate()
        assert metadata_dims.refresh.call_count == 1

        await bound.activation.activate()
        assert metadata_dims.refresh.call_count == 2
        assert startup.application_runtime.require_resources() == resources
    finally:
        await startup.stop_studio(lifecycle)


@pytest.mark.asyncio
async def test_metadata_dimension_refresh_failure_keeps_activation_when_tables_exist(
    resources: ActiveResources, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A transient refresh failure after an earlier refresh must not take Studio down."""
    from databricks_labs_dqx_app.backend import startup

    captured, metadata_dims, workspace = _patch_metadata_activation(monkeypatch, resources, tables_exist=True)

    lifecycle = await startup.start_studio(FastAPI())
    assert lifecycle is not None
    try:
        bound = await captured["binder"].bind(resources)
        await bound.activation.activate()

        assert metadata_dims.refresh.call_count == 1
        checked = {call.args[0] for call in workspace.tables.exists.call_args_list}
        assert checked == {
            f"{resources.volume.catalog}.{resources.genie_schema}.dim_dq_rules",
            f"{resources.volume.catalog}.{resources.genie_schema}.dim_dq_monitored_tables",
        }
        assert startup.application_runtime.require_resources() == resources
    finally:
        await startup.stop_studio(lifecycle)


@pytest.mark.asyncio
@pytest.mark.parametrize("include_bundle_resources", [False, True])
async def test_startup_reconciles_studio_resource_tags(
    resources: ActiveResources, monkeypatch: pytest.MonkeyPatch, include_bundle_resources: bool
) -> None:
    from databricks_labs_dqx_app.backend import startup
    from databricks_labs_dqx_app.backend.services.resource_tagging_service import startup_tag_targets

    tagger = MagicMock()
    captured = _patch_startup(monkeypatch, resources)
    monkeypatch.setattr(startup, "ResourceTaggingService", lambda _workspace: tagger)
    monkeypatch.setattr(startup, "_ensure_score_views", lambda *_args: None)
    monkeypatch.setattr(startup, "_ensure_metadata_dims", AsyncMock())
    monkeypatch.setattr(startup, "ensure_entitlement_objects", lambda *_args: None)
    monkeypatch.setattr(startup, "_ensure_genie_space", lambda *_args: None)
    monkeypatch.setattr(startup, "mark_tmp_schema_ready", lambda: None)
    monkeypatch.setattr(startup, "_stop_background_services", AsyncMock())
    monkeypatch.setattr(startup.conf, "tag_bundle_owned_resources", include_bundle_resources)

    lifecycle = await startup.start_studio(FastAPI())
    assert lifecycle is not None
    try:
        bound = await captured["binder"].bind(resources)
        await bound.activation.activate()
        tagger.reconcile.assert_called_once_with(startup_tag_targets(resources, include_bundle_resources))
    finally:
        await startup.stop_studio(lifecycle)


@pytest.mark.asyncio
async def test_startup_exposes_orchestrator_for_setup_routes(resources: ActiveResources, monkeypatch) -> None:
    from databricks_labs_dqx_app.backend import startup

    app = FastAPI()
    orchestrator = MagicMock()
    orchestrator.reconcile = AsyncMock()
    _patch_startup(monkeypatch, resources, orchestrator=orchestrator)

    lifecycle = await startup.start_studio(app)

    assert lifecycle is not None
    assert app.state.setup_orchestrator is orchestrator
    await startup.stop_studio(lifecycle)


@pytest.mark.asyncio
async def test_startup_without_storage_configuration_opens_lakebase_and_waits_for_form(monkeypatch) -> None:
    from databricks_labs_dqx_app.backend import startup

    app = FastAPI()
    workspace = MagicMock()
    workspace.current_user.me.return_value = SimpleNamespace(user_name="app-sp", id=None)
    pg_executor = MagicMock()
    app_settings = MagicMock()
    app_settings.get_setting.return_value = None
    lakebase = LakebaseConnection(
        "projects/p/branches/b/endpoints/e", None, 5432, "databricks_postgres", None, None, "studio"
    )

    async def get_workspace() -> MagicMock:
        return workspace

    monkeypatch.delenv("DQX_CATALOG", raising=False)
    monkeypatch.setattr(startup, "_resolve_bootstrap", lambda: BootstrapResources(lakebase, "warehouse-id", None))
    monkeypatch.setattr(startup, "conf", AppConfig.model_validate({}))
    monkeypatch.setattr(startup, "get_sp_ws", get_workspace)
    monkeypatch.setattr(startup, "build_pg_executor_from_connection", lambda *_args, **_kwargs: pg_executor)
    monkeypatch.setattr(startup, "AppSettingsService", lambda **_kwargs: app_settings)
    monkeypatch.setattr(startup, "PgMigrationRunner", lambda *_args: MagicMock())
    monkeypatch.setattr(startup, "TaskRunnerJobManager", lambda *_args: MagicMock())
    sql_executor = MagicMock()
    monkeypatch.setattr(startup, "SqlExecutor", sql_executor)

    lifecycle = await startup.start_studio(app)

    report = setup_runtime.report()
    assert lifecycle is not None
    assert report.current_step == SetupStepId.CONFIGURATION
    assert report.step(SetupStepId.CONFIGURATION).code == "configuration_required"
    assert app.state.setup_orchestrator.bound is None
    await startup.stop_studio(lifecycle)
    pg_executor.close.assert_called_once()
    sql_executor.assert_not_called()
    workspace.statement_execution.execute_statement.assert_not_called()


@pytest.mark.asyncio
async def test_startup_reports_invalid_deployment_audience_on_configuration_step(monkeypatch) -> None:
    from databricks_labs_dqx_app.backend import startup

    app = FastAPI()
    workspace = MagicMock()
    workspace.current_user.me.return_value = SimpleNamespace(user_name="app-sp", id=None)
    pg_executor = MagicMock()
    app_settings = MagicMock()
    app_settings.get_setting.return_value = None
    lakebase = LakebaseConnection(
        "projects/p/branches/b/endpoints/e", None, 5432, "databricks_postgres", None, None, "studio"
    )

    async def get_workspace() -> MagicMock:
        return workspace

    monkeypatch.setattr(startup, "_resolve_bootstrap", lambda: BootstrapResources(lakebase, "warehouse-id", None))
    monkeypatch.setattr(startup, "conf", AppConfig.model_validate({"catalog": "main", "user_groups": []}))
    monkeypatch.setattr(startup, "get_sp_ws", get_workspace)
    monkeypatch.setattr(startup, "build_pg_executor_from_connection", lambda *_args, **_kwargs: pg_executor)
    monkeypatch.setattr(startup, "AppSettingsService", lambda **_kwargs: app_settings)
    monkeypatch.setattr(startup, "PgMigrationRunner", lambda *_args: MagicMock())
    monkeypatch.setattr(startup, "TaskRunnerJobManager", lambda *_args: MagicMock())

    lifecycle = await startup.start_studio(app)

    report = setup_runtime.report()
    assert lifecycle is not None
    assert report.state is SetupState.SETUP_REQUIRED
    assert report.current_step == SetupStepId.CONFIGURATION
    assert report.step(SetupStepId.CONFIGURATION).code == "deployment_configuration_invalid"
    assert app.state.setup_orchestrator.bound is None
    await startup.stop_studio(lifecycle)
    pg_executor.close.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("environment", "code"),
    [
        ({"DATABRICKS_WAREHOUSE_ID": "warehouse-id"}, "lakebase_binding_missing"),
        ({"PGHOST": "lakebase.example.com"}, "warehouse_binding_missing"),
    ],
)
async def test_startup_reports_missing_bootstrap_bindings(
    monkeypatch: pytest.MonkeyPatch, environment: dict[str, str], code: str
) -> None:
    from databricks_labs_dqx_app.backend import startup

    for name in ("PGHOST", "DATABRICKS_WAREHOUSE_ID", "DATABRICKS_SQL_WAREHOUSE_ID"):
        monkeypatch.delenv(name, raising=False)
    for name, value in environment.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(startup, "conf", AppConfig.model_validate({}))

    assert await startup.start_studio(FastAPI()) is None
    assert setup_runtime.report().steps[0].code == code


@pytest.mark.asyncio
async def test_startup_bootstrap_needs_only_lakebase_and_warehouse(
    resources: ActiveResources, monkeypatch: pytest.MonkeyPatch
) -> None:
    from databricks_labs_dqx_app.backend import startup

    for name in ("DATABRICKS_SQL_WAREHOUSE_ID", "PGPASSWORD", "PGUSER", "PGDATABASE", "PGPORT"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("PGHOST", "lakebase.example.com")
    monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "warehouse-id")
    captured = _patch_startup(monkeypatch, resources, patch_bootstrap=False)
    monkeypatch.setattr(startup, "conf", AppConfig.model_validate({"job_id": " 29 "}))

    lifecycle = await startup.start_studio(FastAPI())
    assert lifecycle is not None
    await startup.stop_studio(lifecycle)

    bootstrap = captured["bootstrap"]
    assert isinstance(bootstrap, BootstrapResources)
    assert bootstrap.warehouse_id == "warehouse-id"
    assert bootstrap.job_id == "29"
    assert bootstrap.lakebase.host == "lakebase.example.com"


@pytest.mark.asyncio
async def test_rebinding_deactivates_the_previous_context_and_stop_closes_lakebase_once(
    resources: ActiveResources, monkeypatch: pytest.MonkeyPatch
) -> None:
    import dataclasses

    from databricks_labs_dqx_app.backend import startup

    pg_executor = MagicMock()
    stop_background = AsyncMock()
    captured = _patch_startup(monkeypatch, resources, pg_executor=pg_executor)
    monkeypatch.setattr(startup, "_run_post_migration_startup", AsyncMock())
    monkeypatch.setattr(startup, "_stop_background_services", stop_background)
    other = dataclasses.replace(
        resources, volume=VolumeLocation("other", "studio", "wheels", "/Volumes/other/studio/wheels")
    )

    lifecycle = await startup.start_studio(FastAPI())
    assert lifecycle is not None
    first = await captured["binder"].bind(resources)
    await first.activation.activate()
    assert startup.application_runtime.require_resources() == resources

    second = await captured["binder"].bind(other)

    assert stop_background.await_count == 1
    with pytest.raises(RuntimeError, match="resources are not ready"):
        startup.application_runtime.require_resources()
    await second.activation.activate()
    assert startup.application_runtime.require_resources() == other
    pg_executor.close.assert_not_called()

    await startup.stop_studio(lifecycle)
    await startup.stop_studio(lifecycle)

    assert stop_background.await_count == 2
    pg_executor.close.assert_called_once_with()
    with pytest.raises(RuntimeError, match="resources are not ready"):
        startup.application_runtime.require_resources()


@pytest.mark.asyncio
async def test_startup_logs_lakebase_connection_failure(
    resources: ActiveResources, monkeypatch, caplog: pytest.LogCaptureFixture
) -> None:
    from databricks_labs_dqx_app.backend import startup

    app = FastAPI()

    async def get_workspace() -> MagicMock:
        return MagicMock()

    def fail_connection(*_args, **_kwargs) -> None:
        raise RuntimeError("endpoint resolution failed")

    monkeypatch.setattr(startup, "_resolve_bootstrap", lambda: _bootstrap(resources))
    monkeypatch.setattr(startup, "get_sp_ws", get_workspace)
    monkeypatch.setattr(startup, "build_pg_executor_from_connection", fail_connection)
    startup.logger.addHandler(caplog.handler)
    try:
        with caplog.at_level(logging.ERROR, logger=startup.logger.name):
            lifecycle = await startup.start_studio(app)
    finally:
        startup.logger.removeHandler(caplog.handler)

    assert lifecycle is None
    assert setup_runtime.report().steps[0].code == "lakebase_connection_unavailable"
    errors = [record for record in caplog.records if record.levelno == logging.ERROR]
    assert len(errors) == 1
    assert errors[0].exc_info is not None


@pytest.mark.asyncio
async def test_startup_cleans_open_context_when_reconciliation_is_cancelled(
    resources: ActiveResources, monkeypatch
) -> None:
    from databricks_labs_dqx_app.backend import startup

    pg_executor = MagicMock()
    orchestrator = MagicMock()
    orchestrator.reconcile = AsyncMock(side_effect=asyncio.CancelledError)
    _patch_startup(monkeypatch, resources, pg_executor=pg_executor, orchestrator=orchestrator)

    with pytest.raises(asyncio.CancelledError):
        await startup.start_studio(FastAPI())

    pg_executor.close.assert_called_once_with()


@pytest.mark.asyncio
async def test_startup_cleans_bound_context_when_reconciliation_is_cancelled(
    resources: ActiveResources, monkeypatch
) -> None:
    from databricks_labs_dqx_app.backend import startup

    pg_executor = MagicMock()
    stop_background = AsyncMock()
    orchestrator = MagicMock()
    captured = _patch_startup(monkeypatch, resources, pg_executor=pg_executor, orchestrator=orchestrator)

    async def bind_activate_and_cancel() -> None:
        bound = await captured["binder"].bind(resources)
        await bound.activation.activate()
        raise asyncio.CancelledError

    orchestrator.reconcile = AsyncMock(side_effect=bind_activate_and_cancel)
    monkeypatch.setattr(startup, "_run_post_migration_startup", AsyncMock())
    monkeypatch.setattr(startup, "_stop_background_services", stop_background)

    with pytest.raises(asyncio.CancelledError):
        await startup.start_studio(FastAPI())

    stop_background.assert_awaited_once()
    pg_executor.close.assert_called_once_with()
    with pytest.raises(RuntimeError, match="resources are not ready"):
        startup.application_runtime.require_resources()


@pytest.mark.asyncio
async def test_binder_builds_collaborators_for_bound_resources(
    resources: ActiveResources, monkeypatch: pytest.MonkeyPatch
) -> None:
    from databricks_labs_dqx_app.backend import startup

    checker_kwargs: dict[str, object] = {}
    sql_kwargs: dict[str, object] = {}

    def capture_checkers(**kwargs: object) -> MagicMock:
        checker_kwargs.update(kwargs)
        return MagicMock()

    def capture_sql(**kwargs: object) -> MagicMock:
        sql_kwargs.update(kwargs)
        return MagicMock()

    workspace = MagicMock()
    workspace.current_user.me.return_value = SimpleNamespace(user_name="app-sp-name", id=None)
    captured = _patch_startup(monkeypatch, resources, workspace=workspace)
    monkeypatch.setattr(startup, "ResourceCheckers", capture_checkers)
    monkeypatch.setattr(startup, "SqlExecutor", capture_sql)

    lifecycle = await startup.start_studio(FastAPI())
    assert lifecycle is not None
    try:
        assert captured["bootstrap"] == _bootstrap(resources)
        bound = await captured["binder"].bind(resources)
    finally:
        await startup.stop_studio(lifecycle)

    assert bound.resources == resources
    assert sql_kwargs == {
        "ws": workspace,
        "warehouse_id": resources.warehouse_id,
        "catalog": resources.volume.catalog,
        "schema": resources.volume.schema,
    }
    assert checker_kwargs["resources"] == resources
    assert checker_kwargs["app_sp_id"] == "app-sp-name"
    assert isinstance(checker_kwargs["verification_memo"], VerificationMemo)
    assert isinstance(bound.access, AudienceAccess)
    assert bound.access.check_app_sharing().id is SetupStepId.APP_SHARING


@pytest.mark.asyncio
async def test_configuration_resolver_reads_and_locks_saved_choices(
    resources: ActiveResources, monkeypatch: pytest.MonkeyPatch
) -> None:
    from databricks_labs_dqx_app.backend import startup
    from databricks_labs_dqx_app.backend.setup.configuration import ConfigurationSource

    saved = {"setup_catalog": "main", "setup_prefix": "studio", "setup_audience_group": "data-team"}
    app_settings = MagicMock()
    app_settings.get_setting.side_effect = saved.get
    captured = _patch_startup(monkeypatch, resources)
    monkeypatch.setattr(startup, "AppSettingsService", lambda **_kwargs: app_settings)
    monkeypatch.delenv("DQX_CATALOG", raising=False)
    monkeypatch.setattr(startup, "conf", AppConfig.model_validate({}))

    lifecycle = await startup.start_studio(FastAPI())
    assert lifecycle is not None
    try:
        resolved = captured["configuration"].resolve()
        captured["configuration"].lock(user_email="admin@example.com")
    finally:
        await startup.stop_studio(lifecycle)

    assert resolved.source is ConfigurationSource.SAVED
    assert resolved.storage is not None
    assert resolved.storage.catalog == "main"
    assert resolved.audience is not None
    assert resolved.audience.groups == ("data-team",)
    app_settings.save_setting.assert_called_once_with("setup_storage_locked", "true", user_email="admin@example.com")


_DEPLOYMENT_ENVIRONMENT = ("DQX_PREFIX", "DQX_SCHEMA", "DQX_TMP_SCHEMA", "DQX_GENIE_SCHEMA", "DQX_DEMO_SCHEMA")


@dataclass
class _DeploymentStartup:
    """Real orchestrator, resolver and binder with stubbed workspace-facing collaborators."""

    app: FastAPI
    config: AppConfig
    pg_executor: MagicMock
    stop_background: AsyncMock
    migration_failures: list[Exception]


def _deployment_startup(monkeypatch: pytest.MonkeyPatch, catalog: str) -> _DeploymentStartup:
    from databricks_labs_dqx_app.backend import startup

    for name in _DEPLOYMENT_ENVIRONMENT:
        monkeypatch.delenv(name, raising=False)
    workspace = MagicMock()
    workspace.current_user.me.return_value = SimpleNamespace(user_name="app-sp", id=None)
    pg_executor = MagicMock()
    app_settings = MagicMock()
    app_settings.get_setting.return_value = None
    lakebase = LakebaseConnection(
        "projects/p/branches/b/endpoints/e", None, 5432, "databricks_postgres", None, None, "studio"
    )
    checkers = MagicMock()
    checkers.check_unity_catalog.return_value = SetupStep(
        id=SetupStepId.UNITY_CATALOG, state=StepState.ACTION_REQUIRED, code="catalog_access_missing"
    )
    stop_background = AsyncMock()
    migration_failures: list[Exception] = []

    def build_migrations(*_args: object) -> MagicMock:
        if migration_failures:
            raise migration_failures.pop()
        return MagicMock()

    async def get_workspace() -> MagicMock:
        return workspace

    config = AppConfig.model_validate({"catalog": catalog, "user_groups": ["data-team"]})
    monkeypatch.setattr(startup, "_resolve_bootstrap", lambda: BootstrapResources(lakebase, "warehouse-id", None))
    monkeypatch.setattr(startup, "conf", config)
    monkeypatch.setattr(startup, "get_sp_ws", get_workspace)
    monkeypatch.setattr(startup, "build_pg_executor_from_connection", lambda *_args, **_kwargs: pg_executor)
    monkeypatch.setattr(startup, "AppSettingsService", lambda **_kwargs: app_settings)
    monkeypatch.setattr(startup, "PgMigrationRunner", lambda *_args: MagicMock())
    monkeypatch.setattr(startup, "TaskRunnerJobManager", lambda *_args: MagicMock())
    monkeypatch.setattr(startup, "SqlExecutor", lambda **_kwargs: MagicMock())
    monkeypatch.setattr(startup, "ResourceCheckers", lambda **_kwargs: checkers)
    monkeypatch.setattr(startup, "MigrationRunner", build_migrations)
    monkeypatch.setattr(startup, "_run_post_migration_startup", AsyncMock())
    monkeypatch.setattr(startup, "_stop_background_services", stop_background)
    return _DeploymentStartup(FastAPI(), config, pg_executor, stop_background, migration_failures)


@pytest.mark.asyncio
async def test_deployment_storage_binds_lazily_through_the_real_resolver_and_binder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from databricks_labs_dqx_app.backend import startup

    harness = _deployment_startup(monkeypatch, "main")

    lifecycle = await startup.start_studio(harness.app)

    report = setup_runtime.report()
    bound = harness.app.state.setup_orchestrator.bound
    assert lifecycle is not None
    assert report.step(SetupStepId.CONFIGURATION).state is StepState.PASSED
    assert report.current_step == SetupStepId.UNITY_CATALOG
    assert bound is not None
    assert bound.resources.volume.path == "/Volumes/main/dqx_studio/wheels"
    assert bound.resources.tmp_schema == "dqx_studio_tmp"
    assert bound.resources.audience.groups == ("data-team",)
    await startup.stop_studio(lifecycle)
    harness.pg_executor.close.assert_called_once_with()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["deactivation", "collaborators"])
async def test_failed_rebind_leaves_no_stale_binding_and_recovers(
    monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    from databricks_labs_dqx_app.backend import startup

    harness = _deployment_startup(monkeypatch, "main")
    lifecycle = await startup.start_studio(harness.app)
    assert lifecycle is not None
    orchestrator = harness.app.state.setup_orchestrator
    await orchestrator.bound.activation.activate()
    assert startup.application_runtime.require_resources().volume.catalog == "main"

    if failure == "deactivation":
        harness.stop_background.side_effect = [RuntimeError("scheduler stop failed"), None, None]
    else:
        harness.migration_failures.append(RuntimeError("collaborator construction failed"))
    monkeypatch.setattr(harness.config, "catalog", "other")

    report = await orchestrator.reconcile()

    assert report.step(SetupStepId.CONFIGURATION).code == "configuration_binding_failed"
    assert orchestrator.bound is None
    with pytest.raises(RuntimeError, match="resources are not ready"):
        startup.application_runtime.require_resources()

    await orchestrator.reconcile()
    bound = orchestrator.bound
    assert bound is not None
    assert bound.resources.volume.path == "/Volumes/other/dqx_studio/wheels"
    await bound.activation.activate()
    assert startup.application_runtime.require_resources().volume.catalog == "other"
    stops_before_shutdown = harness.stop_background.await_count

    await startup.stop_studio(lifecycle)

    assert harness.stop_background.await_count == stops_before_shutdown + 1
    with pytest.raises(RuntimeError, match="resources are not ready"):
        startup.application_runtime.require_resources()
    harness.pg_executor.close.assert_called_once_with()
