"""Tests for the post-migration Studio activation boundary."""

import asyncio
import logging
from types import SimpleNamespace
from collections.abc import Awaitable, Callable
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import FastAPI

from databricks_labs_dqx_app.backend.setup.access import AudienceAccess
from databricks_labs_dqx_app.backend.setup.audience import resolve_audience
from databricks_labs_dqx_app.backend.runtime import Runtime
from databricks_labs_dqx_app.backend.setup.resources import ActiveResources, LakebaseConnection, VolumeLocation
from databricks_labs_dqx_app.backend.setup.models import SetupReport, SetupState, SetupStep, SetupStepId, StepState
from databricks_labs_dqx_app.backend.setup.runtime import setup_runtime
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
async def test_fastapi_lifespan_yields_restricted_app_and_always_cleans_up(
    resources: ActiveResources, monkeypatch
) -> None:
    from fastapi import FastAPI

    from databricks_labs_dqx_app.backend import app as app_module

    events: list[str] = []
    context = StartupContext(
        resources=resources,
        runtime=Runtime(),
        oltp_executor=object(),
        register_oltp=lambda _executor: None,
    )

    async def start(_app: FastAPI) -> StartupContext:
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
        return context

    async def stop(received: StartupContext | None) -> None:
        assert received is context
        events.append("stop")

    monkeypatch.setattr(app_module, "start_studio", start)
    monkeypatch.setattr(app_module, "stop_studio", stop)

    async with app_module.lifespan(FastAPI()):
        events.append("served")
        assert setup_runtime.report().state == SetupState.SETUP_REQUIRED

    assert events == ["start", "served", "stop"]


@pytest.mark.asyncio
async def test_successful_startup_metadata_refresh_seeds_genie_cache(
    resources: ActiveResources, monkeypatch: pytest.MonkeyPatch
) -> None:
    from databricks_labs_dqx_app.backend import startup
    from databricks_labs_dqx_app.backend.routes.v1 import genie

    app = FastAPI()
    workspace = MagicMock()
    delta_sql = MagicMock()
    delta_sql.q.side_effect = lambda value: f"`{value}`"
    pg_executor = MagicMock()
    startup_metadata_dims = MagicMock()
    request_metadata_dims = MagicMock()
    app_settings = MagicMock()
    compute = MagicMock()
    compute.sp_application_id.return_value = "app-sp"
    orchestrator = MagicMock()
    orchestrator.reconcile = AsyncMock()

    async def get_workspace() -> MagicMock:
        return workspace

    monkeypatch.setattr(startup, "_resolve_resources", lambda: resources)
    monkeypatch.setattr(startup, "get_sp_ws", get_workspace)
    monkeypatch.setattr(startup, "SqlExecutor", lambda **_kwargs: delta_sql)
    monkeypatch.setattr(startup, "build_pg_executor_from_connection", lambda *_args, **_kwargs: pg_executor)
    monkeypatch.setattr(startup, "AppSettingsService", lambda **_kwargs: app_settings)
    monkeypatch.setattr(startup, "ComputeService", lambda **_kwargs: compute)
    monkeypatch.setattr(startup, "ResourceCheckers", lambda **_kwargs: MagicMock())
    monkeypatch.setattr(startup, "TaskRunnerJobManager", lambda *_args: MagicMock())
    monkeypatch.setattr(startup, "PgMigrationRunner", lambda *_args: MagicMock())
    monkeypatch.setattr(startup, "MigrationRunner", lambda *_args: MagicMock())
    monkeypatch.setattr(startup, "SetupOrchestrator", lambda **_kwargs: orchestrator)
    monkeypatch.setattr(startup, "MetadataDimService", lambda **_kwargs: startup_metadata_dims)
    monkeypatch.setattr(startup, "_ensure_score_views", lambda *_args: None)
    monkeypatch.setattr(startup, "ensure_entitlement_objects", lambda *_args: None)
    monkeypatch.setattr(startup, "_ensure_genie_space", lambda *_args: None)
    monkeypatch.setattr(startup, "mark_tmp_schema_ready", lambda: None)
    monkeypatch.setattr(startup, "_stop_background_services", AsyncMock())

    context = await startup.start_studio(app)
    assert context is not None
    try:
        await activate_studio(context)
        await genie.refresh_metadata_dims(request_metadata_dims)
    finally:
        await deactivate_studio(context)

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

    workspace = MagicMock()
    delta_sql = MagicMock()
    delta_sql.q.side_effect = lambda value: f"`{value}`"
    delta_sql.execute.side_effect = RuntimeError("SQLSTATE 42501")
    orchestrator = MagicMock()
    orchestrator.reconcile = AsyncMock()
    compute = MagicMock()
    compute.sp_application_id.return_value = "app-sp"

    async def get_workspace() -> MagicMock:
        return workspace

    monkeypatch.setattr(startup, "_resolve_resources", lambda: resources)
    monkeypatch.setattr(startup, "get_sp_ws", get_workspace)
    monkeypatch.setattr(startup, "SqlExecutor", lambda **_kwargs: delta_sql)
    monkeypatch.setattr(startup, "build_pg_executor_from_connection", lambda *_args, **_kwargs: MagicMock())
    monkeypatch.setattr(startup, "AppSettingsService", lambda **_kwargs: MagicMock())
    monkeypatch.setattr(startup, "ComputeService", lambda **_kwargs: compute)
    monkeypatch.setattr(startup, "ResourceCheckers", lambda **_kwargs: MagicMock())
    monkeypatch.setattr(startup, "TaskRunnerJobManager", lambda *_args: MagicMock())
    monkeypatch.setattr(startup, "PgMigrationRunner", lambda *_args: MagicMock())
    monkeypatch.setattr(startup, "MigrationRunner", lambda *_args: MagicMock())
    monkeypatch.setattr(startup, "SetupOrchestrator", lambda **_kwargs: orchestrator)
    monkeypatch.setattr(startup, "_ensure_metadata_dims", AsyncMock())
    if failing_view == "score":
        monkeypatch.setattr(startup, "ensure_entitlement_objects", lambda *_args: None)
    else:
        monkeypatch.setattr(startup, "_ensure_score_views", lambda *_args: None)
    monkeypatch.setattr(startup, "_ensure_genie_space", lambda *_args: None)
    monkeypatch.setattr(startup, "mark_tmp_schema_ready", lambda: None)
    monkeypatch.setattr(startup, "_stop_background_services", AsyncMock())

    context = await startup.start_studio(FastAPI())
    assert context is not None
    try:
        with pytest.raises(RuntimeError, match="Could not create required Studio views"):
            await activate_studio(context)
    finally:
        await deactivate_studio(context)


@pytest.mark.asyncio
async def test_metadata_dimension_failure_blocks_activation_until_retry(
    resources: ActiveResources, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed metadata-dimension refresh keeps Studio inactive so the next reconcile retries it."""
    from databricks_labs_dqx_app.backend import startup
    from databricks_labs_dqx_app.backend.setup.errors import RequiredViewSetupError

    compute = MagicMock()
    compute.sp_application_id.return_value = "app-sp"
    orchestrator = MagicMock()
    orchestrator.reconcile = AsyncMock()
    metadata_dims = MagicMock()
    metadata_dims.refresh.side_effect = [RuntimeError("SQLSTATE 42501"), None]

    async def get_workspace() -> MagicMock:
        return MagicMock()

    monkeypatch.setattr(startup, "_resolve_resources", lambda: resources)
    monkeypatch.setattr(startup, "get_sp_ws", get_workspace)
    monkeypatch.setattr(startup, "SqlExecutor", lambda **_kwargs: MagicMock())
    monkeypatch.setattr(startup, "build_pg_executor_from_connection", lambda *_args, **_kwargs: MagicMock())
    monkeypatch.setattr(startup, "AppSettingsService", lambda **_kwargs: MagicMock())
    monkeypatch.setattr(startup, "ComputeService", lambda **_kwargs: compute)
    monkeypatch.setattr(startup, "ResourceCheckers", lambda **_kwargs: MagicMock())
    monkeypatch.setattr(startup, "TaskRunnerJobManager", lambda *_args: MagicMock())
    monkeypatch.setattr(startup, "PgMigrationRunner", lambda *_args: MagicMock())
    monkeypatch.setattr(startup, "MigrationRunner", lambda *_args: MagicMock())
    monkeypatch.setattr(startup, "SetupOrchestrator", lambda **_kwargs: orchestrator)
    monkeypatch.setattr(startup, "MetadataDimService", lambda **_kwargs: metadata_dims)
    monkeypatch.setattr(startup, "_ensure_score_views", lambda *_args: None)
    monkeypatch.setattr(startup, "ensure_entitlement_objects", lambda *_args: None)
    monkeypatch.setattr(startup, "_ensure_genie_space", lambda *_args: None)
    monkeypatch.setattr(startup, "mark_tmp_schema_ready", lambda: None)
    monkeypatch.setattr(startup, "_stop_background_services", AsyncMock())

    context = await startup.start_studio(FastAPI())
    assert context is not None
    try:
        with pytest.raises(RequiredViewSetupError):
            await activate_studio(context)
        assert context.active is False

        await activate_studio(context)
        assert context.active is True
        assert metadata_dims.refresh.call_count == 2
    finally:
        await deactivate_studio(context)


@pytest.mark.asyncio
@pytest.mark.parametrize("include_bundle_resources", [False, True])
async def test_startup_reconciles_studio_resource_tags(
    resources: ActiveResources, monkeypatch: pytest.MonkeyPatch, include_bundle_resources: bool
) -> None:
    from databricks_labs_dqx_app.backend import startup
    from databricks_labs_dqx_app.backend.services.resource_tagging_service import startup_tag_targets

    app = FastAPI()
    workspace = MagicMock()
    delta_sql = MagicMock()
    pg_executor = MagicMock()
    app_settings = MagicMock()
    compute = MagicMock()
    compute.sp_application_id.return_value = "app-sp"
    orchestrator = MagicMock()
    orchestrator.reconcile = AsyncMock()
    tagger = MagicMock()

    async def get_workspace() -> MagicMock:
        return workspace

    monkeypatch.setattr(startup, "_resolve_resources", lambda: resources)
    monkeypatch.setattr(startup, "get_sp_ws", get_workspace)
    monkeypatch.setattr(startup, "SqlExecutor", lambda **_kwargs: delta_sql)
    monkeypatch.setattr(startup, "build_pg_executor_from_connection", lambda *_args, **_kwargs: pg_executor)
    monkeypatch.setattr(startup, "AppSettingsService", lambda **_kwargs: app_settings)
    monkeypatch.setattr(startup, "ComputeService", lambda **_kwargs: compute)
    monkeypatch.setattr(startup, "ResourceCheckers", lambda **_kwargs: MagicMock())
    monkeypatch.setattr(startup, "TaskRunnerJobManager", lambda *_args: MagicMock())
    monkeypatch.setattr(startup, "PgMigrationRunner", lambda *_args: MagicMock())
    monkeypatch.setattr(startup, "MigrationRunner", lambda *_args: MagicMock())
    monkeypatch.setattr(startup, "SetupOrchestrator", lambda **_kwargs: orchestrator)
    monkeypatch.setattr(startup, "ResourceTaggingService", lambda _workspace: tagger)
    monkeypatch.setattr(startup, "_ensure_score_views", lambda *_args: None)
    monkeypatch.setattr(startup, "_ensure_metadata_dims", AsyncMock())
    monkeypatch.setattr(startup, "ensure_entitlement_objects", lambda *_args: None)
    monkeypatch.setattr(startup, "_ensure_genie_space", lambda *_args: None)
    monkeypatch.setattr(startup, "mark_tmp_schema_ready", lambda: None)
    monkeypatch.setattr(startup, "_stop_background_services", AsyncMock())
    monkeypatch.setattr(startup.conf, "tag_bundle_owned_resources", include_bundle_resources)

    context = await startup.start_studio(app)
    assert context is not None
    try:
        await activate_studio(context)
        tagger.reconcile.assert_called_once_with(startup_tag_targets(resources, include_bundle_resources))
    finally:
        await deactivate_studio(context)


@pytest.mark.asyncio
async def test_startup_exposes_orchestrator_for_setup_routes(resources: ActiveResources, monkeypatch) -> None:
    from databricks_labs_dqx_app.backend import startup

    app = FastAPI()
    workspace = MagicMock()
    delta_sql = MagicMock()
    pg_executor = MagicMock()
    app_settings = MagicMock()
    compute = MagicMock()
    compute.sp_application_id.return_value = "app-sp"
    orchestrator = MagicMock()
    orchestrator.reconcile = AsyncMock()

    async def get_workspace() -> MagicMock:
        return workspace

    monkeypatch.setattr(startup, "_resolve_resources", lambda: resources)
    monkeypatch.setattr(startup, "get_sp_ws", get_workspace)
    monkeypatch.setattr(startup, "SqlExecutor", lambda **_kwargs: delta_sql)
    monkeypatch.setattr(startup, "build_pg_executor_from_connection", lambda *_args, **_kwargs: pg_executor)
    monkeypatch.setattr(startup, "AppSettingsService", lambda **_kwargs: app_settings)
    monkeypatch.setattr(startup, "ComputeService", lambda **_kwargs: compute)
    monkeypatch.setattr(startup, "ResourceCheckers", lambda **_kwargs: MagicMock())
    monkeypatch.setattr(startup, "TaskRunnerJobManager", lambda *_args: MagicMock())
    monkeypatch.setattr(startup, "PgMigrationRunner", lambda *_args: MagicMock())
    monkeypatch.setattr(startup, "MigrationRunner", lambda *_args: MagicMock())
    monkeypatch.setattr(startup, "SetupOrchestrator", lambda **_kwargs: orchestrator)

    context = await startup.start_studio(app)

    assert context is not None
    assert app.state.setup_orchestrator is orchestrator


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

    monkeypatch.setattr(startup, "_resolve_resources", lambda: resources)
    monkeypatch.setattr(startup, "get_sp_ws", get_workspace)
    monkeypatch.setattr(startup, "SqlExecutor", lambda **_kwargs: MagicMock())
    monkeypatch.setattr(startup, "build_pg_executor_from_connection", fail_connection)
    startup.logger.addHandler(caplog.handler)
    try:
        with caplog.at_level(logging.ERROR, logger=startup.logger.name):
            context = await startup.start_studio(app)
    finally:
        startup.logger.removeHandler(caplog.handler)

    assert context is None
    assert setup_runtime.report().steps[0].code == "lakebase_connection_unavailable"
    errors = [record for record in caplog.records if record.levelno == logging.ERROR]
    assert len(errors) == 1
    assert errors[0].exc_info is not None


@pytest.mark.asyncio
async def test_startup_cleans_open_context_when_reconciliation_is_cancelled(
    resources: ActiveResources, monkeypatch
) -> None:
    from databricks_labs_dqx_app.backend import startup

    app = FastAPI()
    workspace = MagicMock()
    delta_sql = MagicMock()
    pg_executor = MagicMock()
    app_settings = MagicMock()
    compute = MagicMock()
    compute.sp_application_id.return_value = "app-sp"
    orchestrator = MagicMock()
    orchestrator.reconcile = AsyncMock(side_effect=asyncio.CancelledError)

    async def get_workspace() -> MagicMock:
        return workspace

    monkeypatch.setattr(startup, "_resolve_resources", lambda: resources)
    monkeypatch.setattr(startup, "get_sp_ws", get_workspace)
    monkeypatch.setattr(startup, "SqlExecutor", lambda **_kwargs: delta_sql)
    monkeypatch.setattr(startup, "build_pg_executor_from_connection", lambda *_args, **_kwargs: pg_executor)
    monkeypatch.setattr(startup, "AppSettingsService", lambda **_kwargs: app_settings)
    monkeypatch.setattr(startup, "ComputeService", lambda **_kwargs: compute)
    monkeypatch.setattr(startup, "ResourceCheckers", lambda **_kwargs: MagicMock())
    monkeypatch.setattr(startup, "TaskRunnerJobManager", lambda *_args: MagicMock())
    monkeypatch.setattr(startup, "PgMigrationRunner", lambda *_args: MagicMock())
    monkeypatch.setattr(startup, "MigrationRunner", lambda *_args: MagicMock())
    monkeypatch.setattr(startup, "SetupOrchestrator", lambda **_kwargs: orchestrator)

    with pytest.raises(asyncio.CancelledError):
        await startup.start_studio(app)

    pg_executor.close.assert_called_once_with()


@pytest.mark.asyncio
async def test_startup_reports_invalid_audience_configuration_and_does_not_activate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from databricks_labs_dqx_app.backend import startup

    previous_report = setup_runtime.report()
    monkeypatch.setattr(startup.conf, "wheels_volume", "/Volumes/main/studio/wheels")
    monkeypatch.setattr(startup.conf, "lakebase_endpoint", "projects/p/branches/b/endpoints/e")
    monkeypatch.setattr(startup.conf, "user_groups", [])
    try:
        context = await startup.start_studio(FastAPI())
        report = setup_runtime.report()
    finally:
        setup_runtime.publish(previous_report)

    assert context is None
    assert report.state is SetupState.SETUP_REQUIRED
    assert report.current_step is SetupStepId.UNITY_CATALOG
    assert report.steps[0].code == "audience_configuration_invalid"
    assert report.steps[0].summary == "A valid Studio audience group is required."


@pytest.mark.asyncio
@pytest.mark.parametrize("volume_name", ["wheels", "studio_wheels"])
async def test_startup_configuration_reproduces_deployment_resources(
    resources: ActiveResources, monkeypatch: pytest.MonkeyPatch, volume_name: str
) -> None:
    """The interim deployment configuration must bind exactly the resources startup resolved."""
    import dataclasses

    from databricks_labs_dqx_app.backend import startup
    from databricks_labs_dqx_app.backend.setup.configuration import ConfigurationSource
    from databricks_labs_dqx_app.backend.setup.resources import build_active_resources

    deployment = dataclasses.replace(
        resources,
        volume=VolumeLocation("main", "studio", volume_name, f"/Volumes/main/studio/{volume_name}"),
    )
    captured: dict[str, MagicMock] = {}
    orchestrator = MagicMock()
    orchestrator.reconcile = AsyncMock()
    compute = MagicMock()
    compute.sp_application_id.return_value = "app-sp"

    checker_kwargs: dict[str, object] = {}

    def capture(**kwargs: MagicMock) -> MagicMock:
        captured.update(kwargs)
        return orchestrator

    def capture_checkers(**kwargs: object) -> MagicMock:
        checker_kwargs.update(kwargs)
        return MagicMock()

    workspace = MagicMock()
    workspace.current_user.me.return_value = SimpleNamespace(user_name="app-sp-name", id=None)

    async def get_workspace() -> MagicMock:
        return workspace

    monkeypatch.setattr(startup, "_resolve_resources", lambda: deployment)
    monkeypatch.setattr(startup, "get_sp_ws", get_workspace)
    monkeypatch.setattr(startup, "SqlExecutor", lambda **_kwargs: MagicMock())
    monkeypatch.setattr(startup, "build_pg_executor_from_connection", lambda *_args, **_kwargs: MagicMock())
    monkeypatch.setattr(startup, "AppSettingsService", lambda **_kwargs: MagicMock())
    monkeypatch.setattr(startup, "ComputeService", lambda **_kwargs: compute)
    monkeypatch.setattr(startup, "ResourceCheckers", capture_checkers)
    monkeypatch.setattr(startup, "TaskRunnerJobManager", lambda *_args: MagicMock())
    monkeypatch.setattr(startup, "PgMigrationRunner", lambda *_args: MagicMock())
    monkeypatch.setattr(startup, "MigrationRunner", lambda *_args: MagicMock())
    monkeypatch.setattr(startup, "SetupOrchestrator", capture)

    context = await startup.start_studio(FastAPI())
    assert context is not None

    resolved = captured["configuration"].resolve()
    assert resolved.source is ConfigurationSource.DEPLOYMENT
    assert resolved.locked is True
    assert resolved.error is None
    assert build_active_resources(captured["bootstrap"], resolved.storage, resolved.audience) == deployment

    bound = captured["binder"].bind(deployment)
    assert bound.resources == deployment
    assert checker_kwargs["app_sp_id"] == "app-sp-name"
    assert isinstance(bound.access, AudienceAccess)
    sharing = bound.access.check_app_sharing()
    assert sharing.state is StepState.PASSED
    assert sharing.summary == "Audience access is verified in a later setup step."
