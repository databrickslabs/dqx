"""Ordered-transition tests for the DQX Studio setup orchestrator."""

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from unittest.mock import create_autospec

import pytest
from databricks.sdk import WorkspaceClient

from databricks_labs_dqx_app.backend.setup.job_manager import ResolvedJob
from databricks_labs_dqx_app.backend.setup.errors import RequiredViewSetupError
from databricks_labs_dqx_app.backend.setup.models import (
    SetupActionId,
    SetupState,
    SetupStep,
    SetupStepId,
    StepState,
)
from databricks_labs_dqx_app.backend.setup.orchestrator import SetupOrchestrator
from databricks_labs_dqx_app.backend.setup.resources import ActiveResources, LakebaseConnection, VolumeLocation
from databricks_labs_dqx_app.backend.setup.runtime import SetupRuntime
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor


def _passed(step_id: SetupStepId) -> SetupStep:
    return SetupStep(id=step_id, state=StepState.PASSED, summary=f"{step_id.value} ready")


def _action_required(step_id: SetupStepId, code: str) -> SetupStep:
    return SetupStep(
        id=step_id,
        state=StepState.ACTION_REQUIRED,
        code=code,
        summary=f"{step_id.value} needs administrator action",
        actions=(SetupActionId.VERIFY_AGAIN,),
    )


@dataclass
class FakeCheckers:
    events: list[str]
    results: dict[SetupStepId, SetupStep] = field(default_factory=dict)
    runner_reader: WorkspaceClient | None = None
    runner_sql: SqlExecutor | None = None
    output_result: SetupStep = field(default_factory=lambda: _passed(SetupStepId.TASK_RUNNER))

    def _result(self, step_id: SetupStepId) -> SetupStep:
        self.events.append(step_id.value)
        return self.results.get(step_id, _passed(step_id))

    def check_app_identity(self) -> SetupStep:
        return self._result(SetupStepId.IDENTITY)

    def check_volume(self) -> SetupStep:
        return self._result(SetupStepId.VOLUME)

    def check_unity_catalog(self) -> SetupStep:
        return self._result(SetupStepId.UNITY_CATALOG)

    def check_runner_access(
        self,
        job_id: int,
        reader_ws: WorkspaceClient | None = None,
        *,
        reader_sql: SqlExecutor | None = None,
        include_outputs: bool = False,
    ) -> SetupStep:
        self.events.append(f"runner_outputs:{job_id}" if include_outputs else f"runner_volume:{job_id}")
        self.runner_reader = reader_ws
        self.runner_sql = reader_sql
        if include_outputs:
            return self.output_result
        return self.results.get(SetupStepId.TASK_RUNNER, _passed(SetupStepId.TASK_RUNNER))

    def ensure_sibling_schemas(self) -> SetupStep:
        return self._result(SetupStepId.SCHEMAS)

    def check_lakebase(self) -> SetupStep:
        return self._result(SetupStepId.LAKEBASE)

    def ensure_lakebase_schema(self) -> SetupStep:
        self.events.append("lakebase_schema")
        return self.results.get(SetupStepId.LAKEBASE, _passed(SetupStepId.LAKEBASE))

    def check_warehouse(
        self,
        warehouse_id: str | None = None,
        reader_ws: WorkspaceClient | None = None,
    ) -> SetupStep:
        return self._result(SetupStepId.WAREHOUSE)


@dataclass
class FakeJobs:
    events: list[str]
    resolved: ResolvedJob = ResolvedJob(job_id=27, created=False)
    run_as_result: SetupStep = field(default_factory=lambda: _passed(SetupStepId.TASK_RUNNER))
    grant_failure: Exception | None = None

    def resolve(self, configured_job_id: str | None) -> ResolvedJob:
        self.events.append(f"resolve_job:{configured_job_id or 'discover'}")
        return self.resolved

    def grant_setup_admin(self, job_id: int, user_name: str) -> None:
        self.events.append(f"grant_admin:{job_id}:{user_name}")
        if self.grant_failure is not None:
            raise self.grant_failure

    def validate_run_as(self, job_id: int, app_sp_id: str) -> SetupStep:
        self.events.append(f"validate_run_as:{job_id}:{app_sp_id}")
        return self.run_as_result

    def configure(self, job_id: int, wheel_paths: list[str]) -> None:
        self.events.append(f"configure_job:{job_id}:{len(wheel_paths)}")


@dataclass
class FakeMigrationRunner:
    event: str
    events: list[str]
    failure: Exception | None = None

    def run_all(self) -> int:
        self.events.append(self.event)
        if self.failure is not None:
            raise self.failure
        return 0


@dataclass
class FakeAppSettings:
    events: list[str]

    def record_setup_completion(self, job_id: int, completed_at, user_name: str | None) -> None:
        assert completed_at.tzinfo is not None
        self.events.append(f"persist:{job_id}:{user_name or 'system'}")


@dataclass
class FakeActivation:
    events: list[str]
    wait_until: asyncio.Event | None = None
    runtime: SetupRuntime | None = None
    background_failure: Exception | None = None
    activation_failure: Exception | None = None

    async def activate(self) -> None:
        self.events.append("activate")
        if self.activation_failure is not None:
            raise self.activation_failure
        if self.wait_until is not None:
            await self.wait_until.wait()

    async def start_background(self) -> None:
        if self.runtime is None:
            raise RuntimeError("setup runtime is unavailable")
        if self.background_failure is not None:
            raise self.background_failure
        self.events.append(f"start_background:{self.runtime.report().state.value}")


@dataclass
class OrchestratorFixture:
    orchestrator: SetupOrchestrator
    runtime: SetupRuntime
    checkers: FakeCheckers
    jobs: FakeJobs
    events: list[str]


@pytest.fixture
def resources() -> ActiveResources:
    return ActiveResources(
        volume=VolumeLocation("main", "dqx_studio", "wheels", "/Volumes/main/dqx_studio/wheels"),
        lakebase=LakebaseConnection(
            endpoint="projects/p/branches/b/endpoints/primary",
            host=None,
            port=5432,
            database="databricks_postgres",
            username=None,
            password=None,
            schema="dqx_studio",
        ),
        warehouse_id="warehouse-id",
        job_id="27",
        tmp_schema="dqx_studio_tmp",
        genie_schema="genie",
    )


def _make_orchestrator(
    resources: ActiveResources,
    *,
    publish_wheels: Callable[[], Awaitable[list[str]]] | None = None,
    activation: FakeActivation | None = None,
) -> OrchestratorFixture:
    events: list[str] = []
    runtime = SetupRuntime()
    checkers = FakeCheckers(events)
    jobs = FakeJobs(events)

    async def default_publish() -> list[str]:
        events.append("publish_wheels")
        return [f"{resources.volume.path}/dqx.whl", f"{resources.volume.path}/task-runner.whl"]

    activation_service = activation or FakeActivation(events)
    activation_service.events = events
    activation_service.runtime = runtime
    orchestrator = SetupOrchestrator(
        runtime=runtime,
        resources=resources,
        checkers=checkers,
        jobs=jobs,
        pg_migrations=FakeMigrationRunner("pg_migrations", events),
        delta_migrations=FakeMigrationRunner("delta_migrations", events),
        app_settings=FakeAppSettings(events),
        publish_wheels=publish_wheels or default_publish,
        activation=activation_service,
        app_sp_id="app-service-principal",
    )
    return OrchestratorFixture(orchestrator, runtime, checkers, jobs, events)


@pytest.mark.asyncio
async def test_reconcile_stops_at_missing_catalog_grants(resources: ActiveResources) -> None:
    fixture = _make_orchestrator(resources)
    fixture.checkers.results[SetupStepId.UNITY_CATALOG] = _action_required(
        SetupStepId.UNITY_CATALOG, "catalog_permissions_missing"
    )

    report = await fixture.orchestrator.reconcile(setup_user="admin@example.com")

    assert report.state == SetupState.SETUP_REQUIRED
    assert report.current_step == SetupStepId.UNITY_CATALOG
    assert fixture.events == ["identity", "volume", "unity_catalog"]


@pytest.mark.asyncio
async def test_reconcile_creates_sibling_schemas_in_order(resources: ActiveResources) -> None:
    fixture = _make_orchestrator(resources)

    report = await fixture.orchestrator.reconcile()

    assert report.state == SetupState.READY
    assert fixture.events.index("schemas") > fixture.events.index("unity_catalog")
    assert fixture.events.index("schemas") < fixture.events.index("lakebase")


@pytest.mark.asyncio
async def test_reconcile_stops_at_external_run_as_action(resources: ActiveResources) -> None:
    fixture = _make_orchestrator(resources)
    fixture.jobs.run_as_result = _action_required(SetupStepId.TASK_RUNNER, "run_as_required")

    report = await fixture.orchestrator.reconcile(setup_user="admin@example.com")

    assert report.state == SetupState.SETUP_REQUIRED
    assert report.current_step == SetupStepId.TASK_RUNNER
    assert "publish_wheels" not in fixture.events
    assert "pg_migrations" not in fixture.events
    assert "delta_migrations" not in fixture.events


@pytest.mark.asyncio
async def test_reconcile_blocks_wheel_publication_until_runner_can_read_volume(resources: ActiveResources) -> None:
    fixture = _make_orchestrator(resources)
    fixture.checkers.results[SetupStepId.TASK_RUNNER] = _action_required(
        SetupStepId.TASK_RUNNER, "task_runner_permissions_missing"
    )

    report = await fixture.orchestrator.reconcile()

    assert report.state == SetupState.SETUP_REQUIRED
    assert report.current_step == SetupStepId.TASK_RUNNER
    assert report.step(SetupStepId.TASK_RUNNER).code == "task_runner_permissions_missing"
    assert "publish_wheels" not in fixture.events
    assert "pg_migrations" not in fixture.events
    assert "activate" not in fixture.events

    del fixture.checkers.results[SetupStepId.TASK_RUNNER]
    report = await fixture.orchestrator.reconcile()

    assert report.state == SetupState.READY
    assert fixture.events.index("runner_volume:27") < fixture.events.index("publish_wheels")


@pytest.mark.asyncio
async def test_runner_output_permissions_block_activation_after_migrations(resources: ActiveResources) -> None:
    fixture = _make_orchestrator(resources)
    fixture.checkers.output_result = _action_required(SetupStepId.TASK_RUNNER, "task_runner_permissions_missing")

    report = await fixture.orchestrator.reconcile()

    assert report.state == SetupState.SETUP_REQUIRED
    assert report.current_step == SetupStepId.TASK_RUNNER
    assert "delta_migrations" in fixture.events
    assert "activate" not in fixture.events
    assert not any(event.startswith("persist:") for event in fixture.events)

    fixture.checkers.output_result = _passed(SetupStepId.TASK_RUNNER)
    report = await fixture.orchestrator.reconcile()

    assert report.state == SetupState.READY
    assert fixture.events.index("runner_outputs:27") > fixture.events.index("delta_migrations")
    assert sum(step.id == SetupStepId.TASK_RUNNER for step in report.steps) == 1


@pytest.mark.asyncio
async def test_reconcile_passes_request_scoped_admin_reader_to_runner_check(resources: ActiveResources) -> None:
    fixture = _make_orchestrator(resources)
    reader = create_autospec(WorkspaceClient, instance=True)
    reader_sql = create_autospec(SqlExecutor, instance=True)

    report = await fixture.orchestrator.reconcile(
        setup_user="admin@example.com", reader_ws=reader, reader_sql=reader_sql
    )

    assert report.state == SetupState.READY
    assert fixture.checkers.runner_reader is reader
    assert fixture.checkers.runner_sql is reader_sql


@pytest.mark.asyncio
async def test_discovered_job_becomes_runtime_state_before_external_action(resources: ActiveResources) -> None:
    unresolved_resources = ActiveResources(
        volume=resources.volume,
        lakebase=resources.lakebase,
        warehouse_id=resources.warehouse_id,
        job_id=None,
        tmp_schema=resources.tmp_schema,
        genie_schema=resources.genie_schema,
    )
    fixture = _make_orchestrator(unresolved_resources)
    fixture.jobs.resolved = ResolvedJob(job_id=81, created=True)
    fixture.jobs.run_as_result = _action_required(SetupStepId.TASK_RUNNER, "task_runner_run_as_missing")

    report = await fixture.orchestrator.reconcile(setup_user="admin@example.com")

    assert report.current_step == SetupStepId.TASK_RUNNER
    assert fixture.runtime.require_job_id() == 81
    assert "resolve_job:discover" in fixture.events
    assert "grant_admin:81:admin@example.com" in fixture.events
    assert not any(event.startswith("persist:") for event in fixture.events)


@pytest.mark.asyncio
async def test_later_setup_admin_can_manage_job_created_during_startup(resources: ActiveResources) -> None:
    unresolved_resources = ActiveResources(
        volume=resources.volume,
        lakebase=resources.lakebase,
        warehouse_id=resources.warehouse_id,
        job_id=None,
        tmp_schema=resources.tmp_schema,
        genie_schema=resources.genie_schema,
    )
    fixture = _make_orchestrator(unresolved_resources)
    fixture.jobs.resolved = ResolvedJob(job_id=81, created=True)
    fixture.jobs.run_as_result = _action_required(SetupStepId.TASK_RUNNER, "task_runner_run_as_missing")

    await fixture.orchestrator.reconcile()
    fixture.jobs.resolved = ResolvedJob(job_id=81, created=False)
    await fixture.orchestrator.reconcile(setup_user="admin@example.com")

    assert "grant_admin:81:admin@example.com" in fixture.events


@pytest.mark.asyncio
async def test_ready_reconcile_grants_later_setup_admin_without_reinitializing(resources: ActiveResources) -> None:
    fixture = _make_orchestrator(resources)
    ready_report = await fixture.orchestrator.reconcile()
    fixture.events.clear()

    repeated_report = await fixture.orchestrator.reconcile(setup_user="admin@example.com")

    assert repeated_report is ready_report
    assert fixture.events == ["grant_admin:27:admin@example.com"]


@pytest.mark.asyncio
async def test_wheel_publication_failure_blocks_migrations(resources: ActiveResources) -> None:
    async def fail_publish() -> list[str]:
        raise RuntimeError("raw storage payload")

    fixture = _make_orchestrator(resources, publish_wheels=fail_publish)

    report = await fixture.orchestrator.reconcile()

    assert report.current_step == SetupStepId.WHEELS
    assert report.step(SetupStepId.WHEELS).state == StepState.FAILED
    assert "raw storage payload" not in report.step(SetupStepId.WHEELS).summary
    assert "pg_migrations" not in fixture.events


@pytest.mark.asyncio
async def test_lakebase_migration_failure_reports_stage_without_secret(
    resources: ActiveResources, caplog: pytest.LogCaptureFixture
) -> None:
    class PrivilegeError(RuntimeError):
        sqlstate = "42501"

    fixture = _make_orchestrator(resources)
    fixture.orchestrator.pg_migrations.failure = PrivilegeError("credential=secret")

    report = await fixture.orchestrator.reconcile(setup_user="admin@example.com")

    assert report.current_step == SetupStepId.MIGRATIONS
    assert report.step(SetupStepId.MIGRATIONS).state == StepState.FAILED
    assert report.step(SetupStepId.MIGRATIONS).code == "lakebase_migration_failed"
    assert "credential=secret" not in report.step(SetupStepId.MIGRATIONS).summary
    assert "Lakebase migration failed" in caplog.text
    assert "SQLSTATE 42501" in caplog.text
    assert "credential=secret" not in caplog.text
    assert not any(event.startswith("persist:") for event in fixture.events)
    assert "activate" not in fixture.events


@pytest.mark.asyncio
async def test_delta_migration_failure_reports_stage(
    resources: ActiveResources, caplog: pytest.LogCaptureFixture
) -> None:
    class PrivilegeError(RuntimeError):
        sqlstate = "42501"

    fixture = _make_orchestrator(resources)
    fixture.orchestrator.delta_migrations.failure = PrivilegeError("SQL: sensitive statement")

    report = await fixture.orchestrator.reconcile()

    assert report.step(SetupStepId.MIGRATIONS).code == "delta_migration_failed"
    assert "SQL: sensitive statement" not in report.step(SetupStepId.MIGRATIONS).summary
    assert "pg_migrations" in fixture.events
    assert "delta_migrations" in fixture.events
    assert "activate" not in fixture.events
    assert "SQLSTATE 42501" in caplog.text
    assert "sensitive statement" not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["pg_migrations", "delta_migrations"])
async def test_migration_log_rejects_untrusted_sqlstate(
    resources: ActiveResources, caplog: pytest.LogCaptureFixture, stage: str
) -> None:
    class InvalidDiagnosticError(RuntimeError):
        sqlstate = "42501\nforged diagnostic"

    fixture = _make_orchestrator(resources)
    getattr(fixture.orchestrator, stage).failure = InvalidDiagnosticError("sensitive statement")

    report = await fixture.orchestrator.reconcile()

    assert report.step(SetupStepId.MIGRATIONS).state == StepState.FAILED
    assert "SQLSTATE" not in caplog.text
    assert "forged diagnostic" not in caplog.text
    assert "sensitive statement" not in caplog.text


@pytest.mark.asyncio
async def test_completion_is_persisted_only_after_both_migrations(resources: ActiveResources) -> None:
    fixture = _make_orchestrator(resources)

    await fixture.orchestrator.reconcile(setup_user="admin@example.com")

    persist_index = fixture.events.index("persist:27:admin@example.com")
    assert fixture.events.index("pg_migrations") < persist_index
    assert fixture.events.index("delta_migrations") < persist_index
    assert persist_index < fixture.events.index("activate")


@pytest.mark.asyncio
async def test_complete_resources_activate_without_wizard(resources: ActiveResources) -> None:
    fixture = _make_orchestrator(resources)

    report = await fixture.orchestrator.reconcile()

    assert report.state == SetupState.READY
    assert report.current_step is None
    assert fixture.runtime.report() is report
    assert fixture.runtime.require_job_id() == 27


@pytest.mark.asyncio
async def test_background_services_start_only_after_ready_is_published(resources: ActiveResources) -> None:
    fixture = _make_orchestrator(resources)

    report = await fixture.orchestrator.reconcile()

    assert report.state == SetupState.READY
    assert fixture.events[-2:] == ["activate", "start_background:ready"]


@pytest.mark.asyncio
async def test_background_start_failure_keeps_activated_app_ready(resources: ActiveResources) -> None:
    """A background-service failure must not re-gate an activated application."""
    activation = FakeActivation([], background_failure=RuntimeError("background unavailable"))
    fixture = _make_orchestrator(resources, activation=activation)

    report = await fixture.orchestrator.reconcile()

    assert report.state == SetupState.READY
    assert fixture.runtime.report() is report


@pytest.mark.asyncio
async def test_required_view_failure_reports_uc_setup_instead_of_background_services(
    resources: ActiveResources,
) -> None:
    activation = FakeActivation([], activation_failure=RequiredViewSetupError())
    fixture = _make_orchestrator(resources, activation=activation)

    report = await fixture.orchestrator.reconcile()

    assert report.state == SetupState.SETUP_REQUIRED
    assert report.current_step == SetupStepId.ACTIVATION
    step = report.step(SetupStepId.ACTIVATION)
    assert step.state == StepState.FAILED
    assert step.code == "required_views_creation_failed"
    assert "CREATE TABLE" in " ".join(step.instructions)
    assert "Genie schema" in step.summary
    assert not any(event.startswith("start_background:") for event in fixture.events)


@pytest.mark.asyncio
async def test_concurrent_and_repeated_reconcile_activate_once(resources: ActiveResources) -> None:
    release_activation = asyncio.Event()
    events: list[str] = []
    activation = FakeActivation(events, wait_until=release_activation)
    fixture = _make_orchestrator(resources, activation=activation)
    activation.events = fixture.events

    first = asyncio.create_task(fixture.orchestrator.reconcile())
    while "activate" not in fixture.events:
        await asyncio.sleep(0)
    second = asyncio.create_task(fixture.orchestrator.reconcile())
    release_activation.set()

    first_report, second_report = await asyncio.gather(first, second)
    third_report = await fixture.orchestrator.reconcile()

    assert first_report.state == second_report.state == third_report.state == SetupState.READY
    assert fixture.events.count("activate") == 1
    assert fixture.events.count("pg_migrations") == 1


@pytest.mark.asyncio
async def test_ready_reconcile_keeps_app_available_when_admin_grant_fails(resources: ActiveResources) -> None:
    fixture = _make_orchestrator(resources)
    ready_report = await fixture.orchestrator.reconcile()
    fixture.jobs.grant_failure = RuntimeError("transient permissions failure")

    report = await fixture.orchestrator.reconcile(setup_user="admin@example.com")

    assert report is ready_report
    assert report.state == SetupState.READY
    assert fixture.runtime.report() is ready_report
