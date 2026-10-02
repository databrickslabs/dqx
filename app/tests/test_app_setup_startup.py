"""Tests for the post-migration Studio activation boundary."""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from unittest.mock import AsyncMock, MagicMock, create_autospec

import pytest
from fastapi import FastAPI

from databricks_labs_dqx_app.backend.runtime import Runtime
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor
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


def test_user_view_grants_are_limited_to_approved_genie_objects(resources: ActiveResources) -> None:
    from databricks_labs_dqx_app.backend import startup

    delta_sql = create_autospec(SqlExecutor, instance=True)
    delta_sql.q.side_effect = lambda value: f"`{value}`"

    startup.grant_user_view_access(delta_sql, resources, audience_groups=("studio-authors",))

    statements = [call.args[0] for call in delta_sql.execute_no_schema.call_args_list]
    assert statements == [
        "GRANT USE SCHEMA ON SCHEMA `main`.`genie` TO `studio-authors`",
        "GRANT SELECT ON TABLE `main`.`genie`.`mv_dq_scores` TO `studio-authors`",
        "GRANT SELECT ON TABLE `main`.`genie`.`v_dq_check_results` TO `studio-authors`",
        "GRANT SELECT ON TABLE `main`.`genie`.`v_dq_check_results_asof` TO `studio-authors`",
        "GRANT SELECT ON TABLE `main`.`genie`.`v_dq_check_attribution` TO `studio-authors`",
        "GRANT SELECT ON TABLE `main`.`genie`.`v_dq_failing_rows` TO `studio-authors`",
        "GRANT SELECT ON TABLE `main`.`genie`.`dim_dq_rules` TO `studio-authors`",
        "GRANT SELECT ON TABLE `main`.`genie`.`dim_dq_monitored_tables` TO `studio-authors`",
    ]


def test_user_view_grant_failure_does_not_block_other_grants(resources: ActiveResources) -> None:
    from databricks_labs_dqx_app.backend import startup

    delta_sql = create_autospec(SqlExecutor, instance=True)
    delta_sql.q.side_effect = lambda value: f"`{value}`"
    delta_sql.execute_no_schema.side_effect = [
        RuntimeError("permission denied"),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    ]

    startup.grant_user_view_access(delta_sql, resources, audience_groups=("studio-authors",))

    assert delta_sql.execute_no_schema.call_args_list[-1].args == (
        "GRANT SELECT ON TABLE `main`.`genie`.`dim_dq_monitored_tables` TO `studio-authors`",
    )


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


def test_user_view_grants_default_to_administrator_managed(resources: ActiveResources) -> None:
    from databricks_labs_dqx_app.backend import startup

    delta_sql = create_autospec(SqlExecutor, instance=True)
    startup.grant_user_view_access(delta_sql, resources)
    delta_sql.execute_no_schema.assert_not_called()


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
