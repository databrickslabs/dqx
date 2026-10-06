"""Ordered-transition tests for the DQX Studio setup orchestrator."""

import asyncio
import dataclasses
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import create_autospec

import pytest
from databricks.sdk import WorkspaceClient

from databricks_labs_dqx_app.backend.setup.audience import resolve_audience
from databricks_labs_dqx_app.backend.setup.configuration import (
    ConfigurationSource,
    ResolvedConfiguration,
    SetupChoices,
    SetupConfigurationStore,
)
from databricks_labs_dqx_app.backend.setup.errors import RequiredViewSetupError
from databricks_labs_dqx_app.backend.setup.job_manager import ResolvedJob
from databricks_labs_dqx_app.backend.setup.models import (
    SetupActionId,
    SetupState,
    SetupStep,
    SetupStepId,
    StepState,
)
from databricks_labs_dqx_app.backend.setup.orchestrator import BoundSetup, SetupOrchestrator
from databricks_labs_dqx_app.backend.setup.resources import ActiveResources, BootstrapResources, LakebaseConnection
from databricks_labs_dqx_app.backend.setup.runtime import SetupRuntime
from databricks_labs_dqx_app.backend.setup.storage import derive_storage
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


SAVED = ResolvedConfiguration(
    ConfigurationSource.SAVED,
    SetupChoices("main", "studio", "data-team"),
    derive_storage("main", "studio"),
    resolve_audience(["data-team"], "admins", allow_broad=False),
    locked=False,
)

BOOTSTRAP = BootstrapResources(
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
)


@dataclass
class FakeBootstrap:
    events: list[str]
    results: dict[SetupStepId, SetupStep] = field(default_factory=dict)

    def check_app_identity(self) -> SetupStep:
        self.events.append("identity")
        return self.results.get(SetupStepId.IDENTITY, _passed(SetupStepId.IDENTITY))

    def check_lakebase(self) -> SetupStep:
        self.events.append("lakebase")
        return self.results.get(SetupStepId.LAKEBASE, _passed(SetupStepId.LAKEBASE))

    def ensure_lakebase_schema(self) -> SetupStep:
        self.events.append("lakebase_schema")
        return _passed(SetupStepId.LAKEBASE)


@dataclass
class FakeBound:
    events: list[str]
    results: dict[SetupStepId, SetupStep] = field(default_factory=dict)
    provisions: list[bool] = field(default_factory=list)
    output_result: SetupStep | None = None
    runner_reader: WorkspaceClient | None = None
    runner_sql: SqlExecutor | None = None
    catalog_reader_sql: SqlExecutor | None = None
    access_observer: Callable[[], None] | None = None
    access_reader: WorkspaceClient | None = None

    def _result(self, step_id: SetupStepId) -> SetupStep:
        self.events.append(step_id.value)
        return self.results.get(step_id, _passed(step_id))

    def check_unity_catalog(self, reader_sql: SqlExecutor | None = None) -> SetupStep:
        self.catalog_reader_sql = reader_sql
        return self._result(SetupStepId.UNITY_CATALOG)

    def ensure_storage(self, *, provision: bool) -> SetupStep:
        self.provisions.append(provision)
        return self._result(SetupStepId.STORAGE)

    def check_warehouse(self, warehouse_id: str | None = None, reader_ws: WorkspaceClient | None = None) -> SetupStep:
        return self._result(SetupStepId.WAREHOUSE)

    def check_runner_access(
        self,
        job_id: int,
        reader_ws: WorkspaceClient | None = None,
        *,
        reader_sql: SqlExecutor | None = None,
        include_outputs: bool = False,
    ) -> SetupStep:
        self.events.append(f"runner_outputs:{job_id}" if include_outputs else f"runner:{job_id}")
        self.runner_reader = reader_ws
        self.runner_sql = reader_sql
        if include_outputs and self.output_result is not None:
            return self.output_result
        return self.results.get(SetupStepId.TASK_RUNNER, _passed(SetupStepId.TASK_RUNNER))

    def reconcile_access(
        self,
        reader_sql: SqlExecutor | None = None,
        reader_ws: WorkspaceClient | None = None,
    ) -> SetupStep:
        self.access_reader = reader_ws
        if self.access_observer is not None:
            self.access_observer()
        return self._result(SetupStepId.ACCESS)

    def check_app_sharing(self, reader_ws: WorkspaceClient | None = None) -> SetupStep:
        return self._result(SetupStepId.APP_SHARING)


@dataclass
class FakeConfiguration:
    resolved: ResolvedConfiguration
    locks: list[str | None] = field(default_factory=list)

    def resolve(self) -> ResolvedConfiguration:
        return self.resolved

    def lock(self, *, user_email: str | None) -> None:
        self.locks.append(user_email)


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

    def record_setup_completion(self, job_id: int, completed_at: datetime, user_name: str | None) -> None:
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
        self.events.append(f"background:{self.runtime.report().state.value}")


@dataclass
class FakeBinder:
    bound: FakeBound
    delta: FakeMigrationRunner
    activation: FakeActivation
    publish_wheels: Callable[[], Awaitable[list[str]]]
    binds: list[ActiveResources] = field(default_factory=list)

    async def bind(self, resources: ActiveResources) -> BoundSetup:
        self.binds.append(resources)
        return BoundSetup(
            resources=resources,
            checkers=self.bound,
            access=self.bound,
            delta_migrations=self.delta,
            publish_wheels=self.publish_wheels,
            activation=self.activation,
        )


@dataclass
class Harness:
    orchestrator: SetupOrchestrator
    runtime: SetupRuntime
    bootstrap: FakeBootstrap
    bound: FakeBound
    binder: FakeBinder
    configuration: FakeConfiguration
    jobs: FakeJobs
    pg: FakeMigrationRunner
    delta: FakeMigrationRunner
    events: list[str]


def _harness(
    events: list[str] | None = None,
    *,
    resolved: ResolvedConfiguration = SAVED,
    bound_results: dict[SetupStepId, SetupStep] | None = None,
    bootstrap: BootstrapResources = BOOTSTRAP,
    publish_wheels: Callable[[], Awaitable[list[str]]] | None = None,
    activation: FakeActivation | None = None,
) -> Harness:
    events = [] if events is None else events
    runtime = SetupRuntime()
    bound = FakeBound(events, results=dict(bound_results or {}))

    async def default_publish() -> list[str]:
        events.append("publish_wheels")
        return ["/Volumes/main/studio/wheels/dqx.whl", "/Volumes/main/studio/wheels/task-runner.whl"]

    activation_service = activation or FakeActivation(events)
    activation_service.events = events
    activation_service.runtime = runtime
    delta = FakeMigrationRunner("delta", events)
    pg = FakeMigrationRunner("postgres", events)
    binder = FakeBinder(bound, delta, activation_service, publish_wheels or default_publish)
    configuration = FakeConfiguration(resolved)
    bootstrap_checks = FakeBootstrap(events)
    jobs = FakeJobs(events)
    orchestrator = SetupOrchestrator(
        runtime=runtime,
        bootstrap=bootstrap,
        bootstrap_checks=bootstrap_checks,
        pg_migrations=pg,
        configuration=configuration,
        binder=binder,
        jobs=jobs,
        app_settings=FakeAppSettings(events),
        app_sp_id="app-service-principal",
    )
    return Harness(orchestrator, runtime, bootstrap_checks, bound, binder, configuration, jobs, pg, delta, events)


def _orchestrator(
    events: list[str],
    *,
    resolved: ResolvedConfiguration,
    bound_results: dict[SetupStepId, SetupStep] | None = None,
) -> tuple[SetupOrchestrator, FakeBound]:
    harness = _harness(events, resolved=resolved, bound_results=bound_results)
    return harness.orchestrator, harness.bound


# ---------------------------------------------------------------------------
# Bootstrap / bound split, configuration, access and app sharing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fresh_install_without_configuration_stops_at_form_after_lakebase() -> None:
    events: list[str] = []
    orchestrator, _ = _orchestrator(
        events, resolved=ResolvedConfiguration(ConfigurationSource.NONE, None, None, None, False)
    )

    report = await orchestrator.reconcile()

    assert report.state == SetupState.SETUP_REQUIRED
    assert report.current_step == SetupStepId.CONFIGURATION
    assert report.step(SetupStepId.CONFIGURATION).actions == (SetupActionId.CONFIGURE,)
    assert report.step(SetupStepId.CONFIGURATION).code == "configuration_required"
    assert events == ["identity", "lakebase", "lakebase_schema", "postgres"]
    assert orchestrator.bound is None


@pytest.mark.asyncio
async def test_saved_configuration_provisions_storage_and_locks_choices() -> None:
    events: list[str] = []
    orchestrator, bound = _orchestrator(events, resolved=SAVED)

    report = await orchestrator.reconcile(setup_user="admin@example.com")

    assert report.state == SetupState.READY
    assert bound.provisions == [True]
    assert orchestrator.configuration.locks == ["admin@example.com"]
    assert [step.id for step in report.steps][-3:] == [
        SetupStepId.ACTIVATION,
        SetupStepId.ACCESS,
        SetupStepId.APP_SHARING,
    ]


@pytest.mark.asyncio
async def test_deployment_configuration_verifies_without_provisioning() -> None:
    events: list[str] = []
    deployment = dataclasses.replace(SAVED, source=ConfigurationSource.DEPLOYMENT, choices=None)
    orchestrator, bound = _orchestrator(events, resolved=deployment)

    await orchestrator.reconcile()

    assert bound.provisions == [False]


@pytest.mark.asyncio
async def test_locked_configuration_is_not_locked_again() -> None:
    harness = _harness(resolved=dataclasses.replace(SAVED, locked=True))

    report = await harness.orchestrator.reconcile(setup_user="admin@example.com")

    assert report.state == SetupState.READY
    assert harness.configuration.locks == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("step_id", "code"),
    [(SetupStepId.STORAGE, "storage_collision"), (SetupStepId.UNITY_CATALOG, "catalog_permissions_missing")],
)
async def test_unlocked_saved_configuration_stays_editable(step_id: SetupStepId, code: str) -> None:
    harness = _harness(bound_results={step_id: _action_required(step_id, code)})

    report = await harness.orchestrator.reconcile(setup_user="admin@example.com")

    assert report.current_step == step_id
    configuration = report.step(SetupStepId.CONFIGURATION)
    assert configuration.state == StepState.PASSED
    assert configuration.actions == (SetupActionId.CONFIGURE,)


@pytest.mark.asyncio
async def test_configuration_stops_being_editable_once_storage_is_locked() -> None:
    harness = _harness(
        bound_results={SetupStepId.WAREHOUSE: _action_required(SetupStepId.WAREHOUSE, "warehouse_access_missing")}
    )

    report = await harness.orchestrator.reconcile(setup_user="admin@example.com")

    assert report.current_step == SetupStepId.WAREHOUSE
    assert harness.configuration.locks == ["admin@example.com"]
    assert report.step(SetupStepId.CONFIGURATION).actions == ()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "resolved",
    [
        dataclasses.replace(SAVED, locked=True),
        dataclasses.replace(SAVED, source=ConfigurationSource.DEPLOYMENT, choices=None),
    ],
)
async def test_locked_or_deployment_configuration_is_not_editable(resolved: ResolvedConfiguration) -> None:
    harness = _harness(
        resolved=resolved,
        bound_results={SetupStepId.STORAGE: _action_required(SetupStepId.STORAGE, "storage_collision")},
    )

    report = await harness.orchestrator.reconcile(setup_user="admin@example.com")

    assert report.step(SetupStepId.CONFIGURATION).actions == ()


@pytest.mark.asyncio
async def test_storage_failure_does_not_lock_configuration() -> None:
    harness = _harness(
        bound_results={SetupStepId.STORAGE: _action_required(SetupStepId.STORAGE, "storage_permissions_missing")}
    )

    report = await harness.orchestrator.reconcile(setup_user="admin@example.com")

    assert report.current_step == SetupStepId.STORAGE
    assert harness.configuration.locks == []


@pytest.mark.asyncio
async def test_storage_warning_does_not_lock_configuration() -> None:
    warning = SetupStep(id=SetupStepId.STORAGE, state=StepState.WARNING, code="storage_unverified")
    harness = _harness(bound_results={SetupStepId.STORAGE: warning})

    report = await harness.orchestrator.reconcile(setup_user="admin@example.com")

    assert report.state == SetupState.READY
    assert harness.configuration.locks == []


@pytest.mark.asyncio
async def test_configuration_resolution_failure_is_logged_without_details(caplog: pytest.LogCaptureFixture) -> None:
    class FailingConfiguration(FakeConfiguration):
        def resolve(self) -> ResolvedConfiguration:
            raise RuntimeError("catalog=secret\nforged")

    harness = _harness()
    harness.orchestrator.configuration = FailingConfiguration(SAVED)

    report = await harness.orchestrator.reconcile()

    assert report.step(SetupStepId.CONFIGURATION).code == "configuration_resolution_failed"
    assert "Could not resolve the Studio setup configuration (RuntimeError)" in caplog.text
    assert "secret" not in caplog.text


@pytest.mark.asyncio
async def test_binding_failure_is_logged_without_details(caplog: pytest.LogCaptureFixture) -> None:
    harness = _harness()

    async def fail_bind(resources: ActiveResources) -> BoundSetup:
        raise ValueError("catalog=secret")

    harness.orchestrator.binder = SimpleNamespace(bind=fail_bind)

    report = await harness.orchestrator.reconcile()

    assert report.step(SetupStepId.CONFIGURATION).code == "configuration_binding_failed"
    assert "Could not bind Studio setup collaborators (ValueError)" in caplog.text
    assert "secret" not in caplog.text


@pytest.mark.asyncio
async def test_configuration_lock_failure_is_reported_and_logged(caplog: pytest.LogCaptureFixture) -> None:
    class FailingLock(FakeConfiguration):
        def lock(self, *, user_email: str | None) -> None:
            raise RuntimeError("user=secret")

    harness = _harness()
    harness.orchestrator.configuration = FailingLock(SAVED)

    report = await harness.orchestrator.reconcile(setup_user="admin@example.com")

    assert report.current_step == SetupStepId.STORAGE
    assert report.step(SetupStepId.STORAGE).code == "configuration_lock_failed"
    assert "Could not lock the Studio setup configuration (RuntimeError)" in caplog.text
    assert "secret" not in caplog.text


@pytest.mark.asyncio
async def test_full_setup_runs_steps_in_documented_order() -> None:
    harness = _harness()

    report = await harness.orchestrator.reconcile()

    assert report.state == SetupState.READY
    assert [step.id for step in report.steps] == [
        SetupStepId.IDENTITY,
        SetupStepId.LAKEBASE,
        SetupStepId.CONFIGURATION,
        SetupStepId.UNITY_CATALOG,
        SetupStepId.STORAGE,
        SetupStepId.WAREHOUSE,
        SetupStepId.TASK_RUNNER,
        SetupStepId.WHEELS,
        SetupStepId.MIGRATIONS,
        SetupStepId.ACTIVATION,
        SetupStepId.ACCESS,
        SetupStepId.APP_SHARING,
    ]
    assert harness.events == [
        "identity",
        "lakebase",
        "lakebase_schema",
        "postgres",
        "unity_catalog",
        "storage",
        "warehouse",
        "resolve_job:27",
        "validate_run_as:27:app-service-principal",
        "runner:27",
        "publish_wheels",
        "configure_job:27:2",
        "delta",
        "runner_outputs:27",
        "persist:27:system",
        "activate",
        "access",
        "app_sharing",
        "background:ready",
    ]


@pytest.mark.asyncio
async def test_configuration_binds_resources_built_from_bootstrap_and_storage() -> None:
    harness = _harness()

    await harness.orchestrator.reconcile()

    assert len(harness.binder.binds) == 1
    bound = harness.orchestrator.bound
    assert bound is not None
    assert bound.resources.volume.catalog == "main"
    assert bound.resources.volume.schema == "studio"
    assert bound.resources.tmp_schema == "studio_tmp"
    assert bound.resources.warehouse_id == BOOTSTRAP.warehouse_id
    assert bound.resources.lakebase == BOOTSTRAP.lakebase
    assert bound.resources.audience == SAVED.audience


@pytest.mark.asyncio
async def test_unchanged_configuration_is_not_rebound() -> None:
    harness = _harness(
        bound_results={SetupStepId.WAREHOUSE: _action_required(SetupStepId.WAREHOUSE, "warehouse_permissions_missing")}
    )

    await harness.orchestrator.reconcile()
    await harness.orchestrator.reconcile()

    assert len(harness.binder.binds) == 1


@pytest.mark.asyncio
async def test_changed_configuration_is_rebound() -> None:
    harness = _harness(
        bound_results={SetupStepId.WAREHOUSE: _action_required(SetupStepId.WAREHOUSE, "warehouse_permissions_missing")}
    )
    await harness.orchestrator.reconcile()

    harness.configuration.resolved = dataclasses.replace(
        SAVED, choices=SetupChoices("other", "studio", "data-team"), storage=derive_storage("other", "studio")
    )
    await harness.orchestrator.reconcile()

    assert [resources.volume.catalog for resources in harness.binder.binds] == ["main", "other"]


@pytest.mark.asyncio
async def test_failed_rebind_clears_the_previous_bound_setup() -> None:
    harness = _harness(
        bound_results={SetupStepId.WAREHOUSE: _action_required(SetupStepId.WAREHOUSE, "warehouse_permissions_missing")}
    )
    await harness.orchestrator.reconcile()
    original_bind = harness.binder.bind

    async def fail_bind(resources: ActiveResources) -> BoundSetup:
        raise RuntimeError("previous context cleanup failed")

    harness.orchestrator.binder = SimpleNamespace(bind=fail_bind)
    harness.configuration.resolved = dataclasses.replace(
        SAVED, choices=SetupChoices("other", "studio", "data-team"), storage=derive_storage("other", "studio")
    )

    report = await harness.orchestrator.reconcile()

    assert report.step(SetupStepId.CONFIGURATION).code == "configuration_binding_failed"
    assert harness.orchestrator.bound is None

    harness.orchestrator.binder = SimpleNamespace(bind=original_bind)
    harness.configuration.resolved = SAVED
    await harness.orchestrator.reconcile()

    bound = harness.orchestrator.bound
    assert bound is not None
    assert bound.resources.volume.catalog == "main"
    assert [resources.volume.catalog for resources in harness.binder.binds] == ["main", "main"]


@pytest.mark.asyncio
async def test_unity_catalog_check_receives_request_scoped_reader_sql() -> None:
    harness = _harness()
    reader_sql = create_autospec(SqlExecutor, instance=True)

    await harness.orchestrator.reconcile(setup_user="admin@example.com", reader_sql=reader_sql)

    assert harness.bound.catalog_reader_sql is reader_sql


@pytest.mark.asyncio
async def test_missing_access_blocks_ready_after_activation() -> None:
    events: list[str] = []
    orchestrator, _ = _orchestrator(
        events,
        resolved=SAVED,
        bound_results={SetupStepId.ACCESS: _action_required(SetupStepId.ACCESS, "audience_grants_missing")},
    )

    report = await orchestrator.reconcile()

    assert report.state == SetupState.SETUP_REQUIRED
    assert report.current_step == SetupStepId.ACCESS
    assert "activate" in events
    assert not any(event.startswith("background") for event in events)


@pytest.mark.asyncio
async def test_app_sharing_warning_does_not_block_ready() -> None:
    events: list[str] = []
    warning = SetupStep(id=SetupStepId.APP_SHARING, state=StepState.WARNING, code="app_sharing_unverified")
    orchestrator, _ = _orchestrator(events, resolved=SAVED, bound_results={SetupStepId.APP_SHARING: warning})

    report = await orchestrator.reconcile()

    assert report.state == SetupState.READY
    assert report.step(SetupStepId.APP_SHARING).state == StepState.WARNING


@pytest.mark.asyncio
async def test_warning_in_middle_step_does_not_stop_setup() -> None:
    warning = SetupStep(id=SetupStepId.WAREHOUSE, state=StepState.WARNING, code="warehouse_unverified")
    harness = _harness(bound_results={SetupStepId.WAREHOUSE: warning})

    report = await harness.orchestrator.reconcile()

    assert report.state == SetupState.READY
    assert report.step(SetupStepId.WAREHOUSE).state == StepState.WARNING


@pytest.mark.asyncio
async def test_ready_admin_recheck_reports_missing_grant() -> None:
    events: list[str] = []
    orchestrator, bound = _orchestrator(events, resolved=SAVED)
    assert (await orchestrator.reconcile()).state == SetupState.READY

    bound.results[SetupStepId.ACCESS] = _action_required(SetupStepId.ACCESS, "audience_grants_missing")
    report = await orchestrator.reconcile(setup_user="admin@example.com")

    assert report.state == SetupState.SETUP_REQUIRED
    assert report.current_step == SetupStepId.ACCESS


@pytest.mark.asyncio
async def test_ready_admin_recheck_keeps_ready_visible_while_running() -> None:
    harness = _harness()
    ready = await harness.orchestrator.reconcile()
    observed: list[SetupState] = []
    harness.bound.access_observer = lambda: observed.append(harness.runtime.report().state)

    report = await harness.orchestrator.reconcile(setup_user="admin@example.com")

    assert ready.state == report.state == SetupState.READY
    assert observed == [SetupState.READY]
    assert harness.runtime.report() is report


@pytest.mark.asyncio
async def test_ready_unattended_reconcile_returns_cached_report() -> None:
    events: list[str] = []
    orchestrator, _ = _orchestrator(events, resolved=SAVED)
    ready = await orchestrator.reconcile()
    events.clear()

    assert await orchestrator.reconcile() is ready
    assert events == []


@pytest.mark.asyncio
async def test_invalid_saved_configuration_is_action_required() -> None:
    events: list[str] = []
    invalid = ResolvedConfiguration(
        ConfigurationSource.SAVED, SAVED.choices, None, None, False, "saved_configuration_invalid"
    )
    orchestrator, _ = _orchestrator(events, resolved=invalid)

    report = await orchestrator.reconcile()

    assert report.step(SetupStepId.CONFIGURATION).code == "saved_configuration_invalid"
    assert report.step(SetupStepId.CONFIGURATION).state == StepState.ACTION_REQUIRED
    assert report.step(SetupStepId.CONFIGURATION).actions == (SetupActionId.CONFIGURE,)


@pytest.mark.asyncio
async def test_invalid_deployment_configuration_does_not_advertise_form() -> None:
    invalid = ResolvedConfiguration(
        ConfigurationSource.DEPLOYMENT, None, None, None, False, "deployment_configuration_invalid"
    )
    harness = _harness(resolved=invalid)

    report = await harness.orchestrator.reconcile()

    step = report.step(SetupStepId.CONFIGURATION)
    assert step.code == "deployment_configuration_invalid"
    assert SetupActionId.CONFIGURE not in step.actions


@pytest.mark.asyncio
async def test_identity_failure_stops_before_lakebase() -> None:
    harness = _harness()
    harness.bootstrap.results[SetupStepId.IDENTITY] = _action_required(SetupStepId.IDENTITY, "app_identity_unresolved")

    report = await harness.orchestrator.reconcile()

    assert report.current_step == SetupStepId.IDENTITY
    assert harness.events == ["identity"]


@pytest.mark.asyncio
async def test_lakebase_failure_stops_before_configuration() -> None:
    harness = _harness()
    harness.bootstrap.results[SetupStepId.LAKEBASE] = _action_required(
        SetupStepId.LAKEBASE, "lakebase_connectivity_failed"
    )

    report = await harness.orchestrator.reconcile()

    assert report.current_step == SetupStepId.LAKEBASE
    assert harness.events == ["identity", "lakebase"]
    assert harness.orchestrator.bound is None


def test_configuration_view_before_resolution_reports_none() -> None:
    harness = _harness()

    view = harness.orchestrator.configuration_view()

    assert view.source == "none"
    assert view.catalog == ""
    assert view.locked is False


@pytest.mark.asyncio
async def test_configuration_view_reflects_saved_choices() -> None:
    harness = _harness(resolved=dataclasses.replace(SAVED, locked=True))

    await harness.orchestrator.reconcile()
    view = harness.orchestrator.configuration_view()

    assert view.source == "saved"
    assert view.catalog == "main"
    assert view.prefix == "studio"
    assert view.audience_group == "data-team"
    assert view.schemas == ("studio", "studio_tmp", "studio_genie", "studio_demo")
    assert view.broad_audience is False
    assert view.locked is True


@pytest.mark.asyncio
async def test_configuration_view_sanitizes_control_characters() -> None:
    choices = SetupChoices("main\nforged", "studio", "data\x1bteam")
    invalid = ResolvedConfiguration(
        ConfigurationSource.SAVED, choices, None, None, False, "saved_configuration_invalid"
    )
    harness = _harness(resolved=invalid)

    await harness.orchestrator.reconcile()
    view = harness.orchestrator.configuration_view()

    assert "\n" not in view.catalog
    assert "\x1b" not in view.audience_group


# ---------------------------------------------------------------------------
# Ported behaviour: job resolution, wheels, migrations, activation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reconcile_stops_at_missing_catalog_grants() -> None:
    harness = _harness(
        bound_results={
            SetupStepId.UNITY_CATALOG: _action_required(SetupStepId.UNITY_CATALOG, "catalog_permissions_missing")
        }
    )

    report = await harness.orchestrator.reconcile(setup_user="admin@example.com")

    assert report.state == SetupState.SETUP_REQUIRED
    assert report.current_step == SetupStepId.UNITY_CATALOG
    assert harness.events == ["identity", "lakebase", "lakebase_schema", "postgres", "unity_catalog"]


@pytest.mark.asyncio
async def test_reconcile_provisions_storage_after_catalog_check() -> None:
    harness = _harness()

    report = await harness.orchestrator.reconcile()

    assert report.state == SetupState.READY
    assert harness.events.index("storage") > harness.events.index("unity_catalog")
    assert harness.events.index("storage") < harness.events.index("warehouse")


@pytest.mark.asyncio
async def test_reconcile_stops_at_external_run_as_action() -> None:
    harness = _harness()
    harness.jobs.run_as_result = _action_required(SetupStepId.TASK_RUNNER, "run_as_required")

    report = await harness.orchestrator.reconcile(setup_user="admin@example.com")

    assert report.state == SetupState.SETUP_REQUIRED
    assert report.current_step == SetupStepId.TASK_RUNNER
    assert "publish_wheels" not in harness.events
    assert "delta" not in harness.events


@pytest.mark.asyncio
async def test_reconcile_blocks_wheel_publication_until_runner_can_read_volume() -> None:
    harness = _harness()
    harness.bound.results[SetupStepId.TASK_RUNNER] = _action_required(
        SetupStepId.TASK_RUNNER, "task_runner_permissions_missing"
    )

    report = await harness.orchestrator.reconcile()

    assert report.state == SetupState.SETUP_REQUIRED
    assert report.current_step == SetupStepId.TASK_RUNNER
    assert report.step(SetupStepId.TASK_RUNNER).code == "task_runner_permissions_missing"
    assert "publish_wheels" not in harness.events
    assert "delta" not in harness.events
    assert "activate" not in harness.events

    del harness.bound.results[SetupStepId.TASK_RUNNER]
    report = await harness.orchestrator.reconcile()

    assert report.state == SetupState.READY
    assert harness.events.index("runner:27") < harness.events.index("publish_wheels")


@pytest.mark.asyncio
async def test_runner_output_permissions_block_activation_after_migrations() -> None:
    harness = _harness()
    harness.bound.output_result = _action_required(SetupStepId.TASK_RUNNER, "task_runner_permissions_missing")

    report = await harness.orchestrator.reconcile()

    assert report.state == SetupState.SETUP_REQUIRED
    assert report.current_step == SetupStepId.TASK_RUNNER
    assert "delta" in harness.events
    assert "activate" not in harness.events
    assert not any(event.startswith("persist:") for event in harness.events)

    harness.bound.output_result = _passed(SetupStepId.TASK_RUNNER)
    report = await harness.orchestrator.reconcile()

    assert report.state == SetupState.READY
    assert harness.events.index("runner_outputs:27") > harness.events.index("delta")
    assert sum(step.id == SetupStepId.TASK_RUNNER for step in report.steps) == 1


@pytest.mark.asyncio
async def test_reconcile_passes_request_scoped_admin_reader_to_runner_check() -> None:
    harness = _harness()
    reader = create_autospec(WorkspaceClient, instance=True)
    reader_sql = create_autospec(SqlExecutor, instance=True)

    report = await harness.orchestrator.reconcile(
        setup_user="admin@example.com", reader_ws=reader, reader_sql=reader_sql
    )

    assert report.state == SetupState.READY
    assert harness.bound.runner_reader is reader
    assert harness.bound.runner_sql is reader_sql
    assert harness.bound.access_reader is reader


@pytest.mark.asyncio
async def test_discovered_job_becomes_runtime_state_before_external_action() -> None:
    harness = _harness(bootstrap=dataclasses.replace(BOOTSTRAP, job_id=None))
    harness.jobs.resolved = ResolvedJob(job_id=81, created=True)
    harness.jobs.run_as_result = _action_required(SetupStepId.TASK_RUNNER, "task_runner_run_as_missing")

    report = await harness.orchestrator.reconcile(setup_user="admin@example.com")

    assert report.current_step == SetupStepId.TASK_RUNNER
    assert harness.runtime.require_job_id() == 81
    assert "resolve_job:discover" in harness.events
    assert "grant_admin:81:admin@example.com" in harness.events
    assert not any(event.startswith("persist:") for event in harness.events)


@pytest.mark.asyncio
async def test_later_setup_admin_can_manage_job_created_during_startup() -> None:
    harness = _harness(bootstrap=dataclasses.replace(BOOTSTRAP, job_id=None))
    harness.jobs.resolved = ResolvedJob(job_id=81, created=True)
    harness.jobs.run_as_result = _action_required(SetupStepId.TASK_RUNNER, "task_runner_run_as_missing")

    await harness.orchestrator.reconcile()
    harness.jobs.resolved = ResolvedJob(job_id=81, created=False)
    await harness.orchestrator.reconcile(setup_user="admin@example.com")

    assert "grant_admin:81:admin@example.com" in harness.events


@pytest.mark.asyncio
async def test_ready_admin_reconcile_grants_job_admin_before_full_recheck() -> None:
    harness = _harness()
    await harness.orchestrator.reconcile()
    harness.events.clear()

    report = await harness.orchestrator.reconcile(setup_user="admin@example.com")

    assert report.state == SetupState.READY
    assert harness.events[0] == "grant_admin:27:admin@example.com"
    assert harness.events.count("grant_admin:27:admin@example.com") == 1
    assert harness.events[1:5] == ["identity", "lakebase", "lakebase_schema", "postgres"]
    assert "access" in harness.events
    assert "app_sharing" in harness.events


@pytest.mark.asyncio
async def test_wheel_publication_failure_blocks_migrations() -> None:
    async def fail_publish() -> list[str]:
        raise RuntimeError("raw storage payload")

    harness = _harness(publish_wheels=fail_publish)

    report = await harness.orchestrator.reconcile()

    assert report.current_step == SetupStepId.WHEELS
    assert report.step(SetupStepId.WHEELS).state == StepState.FAILED
    assert "raw storage payload" not in report.step(SetupStepId.WHEELS).summary
    assert "delta" not in harness.events


@pytest.mark.asyncio
async def test_lakebase_migration_failure_reports_stage_without_secret(caplog: pytest.LogCaptureFixture) -> None:
    class PrivilegeError(RuntimeError):
        sqlstate = "42501"

    harness = _harness()
    harness.pg.failure = PrivilegeError("credential=secret")

    report = await harness.orchestrator.reconcile(setup_user="admin@example.com")

    assert report.current_step == SetupStepId.LAKEBASE
    assert report.step(SetupStepId.LAKEBASE).state == StepState.FAILED
    assert report.step(SetupStepId.LAKEBASE).code == "lakebase_migration_failed"
    assert "credential=secret" not in report.step(SetupStepId.LAKEBASE).summary
    assert "Lakebase migration failed" in caplog.text
    assert "SQLSTATE 42501" in caplog.text
    assert "credential=secret" not in caplog.text
    assert not any(event.startswith("persist:") for event in harness.events)
    assert "activate" not in harness.events
    assert harness.orchestrator.bound is None


@pytest.mark.asyncio
async def test_delta_migration_failure_reports_stage(caplog: pytest.LogCaptureFixture) -> None:
    class PrivilegeError(RuntimeError):
        sqlstate = "42501"

    harness = _harness()
    harness.delta.failure = PrivilegeError("SQL: sensitive statement")

    report = await harness.orchestrator.reconcile()

    assert report.current_step == SetupStepId.MIGRATIONS
    assert report.step(SetupStepId.MIGRATIONS).code == "delta_migration_failed"
    assert "SQL: sensitive statement" not in report.step(SetupStepId.MIGRATIONS).summary
    assert "postgres" in harness.events
    assert "delta" in harness.events
    assert "activate" not in harness.events
    assert "SQLSTATE 42501" in caplog.text
    assert "sensitive statement" not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["pg", "delta"])
async def test_migration_log_rejects_untrusted_sqlstate(caplog: pytest.LogCaptureFixture, stage: str) -> None:
    class InvalidDiagnosticError(RuntimeError):
        sqlstate = "42501\nforged diagnostic"

    harness = _harness()
    runner = harness.pg if stage == "pg" else harness.delta
    runner.failure = InvalidDiagnosticError("sensitive statement")

    report = await harness.orchestrator.reconcile()

    step_id = SetupStepId.LAKEBASE if stage == "pg" else SetupStepId.MIGRATIONS
    assert report.step(step_id).state == StepState.FAILED
    assert "SQLSTATE" not in caplog.text
    assert "forged diagnostic" not in caplog.text
    assert "sensitive statement" not in caplog.text


@pytest.mark.asyncio
async def test_completion_is_persisted_only_after_both_migrations() -> None:
    harness = _harness()

    await harness.orchestrator.reconcile(setup_user="admin@example.com")

    persist_index = harness.events.index("persist:27:admin@example.com")
    assert harness.events.index("postgres") < persist_index
    assert harness.events.index("delta") < persist_index
    assert persist_index < harness.events.index("activate")


@pytest.mark.asyncio
async def test_complete_resources_activate_without_wizard() -> None:
    harness = _harness()

    report = await harness.orchestrator.reconcile()

    assert report.state == SetupState.READY
    assert report.current_step is None
    assert harness.runtime.report() is report
    assert harness.runtime.require_job_id() == 27


@pytest.mark.asyncio
async def test_background_services_start_only_after_ready_is_published() -> None:
    harness = _harness()

    report = await harness.orchestrator.reconcile()

    assert report.state == SetupState.READY
    assert harness.events[-1] == "background:ready"
    assert harness.events.index("activate") < harness.events.index("access")


@pytest.mark.asyncio
async def test_background_start_failure_keeps_activated_app_ready() -> None:
    """A background-service failure must not re-gate an activated application."""
    activation = FakeActivation([], background_failure=RuntimeError("background unavailable"))
    harness = _harness(activation=activation)

    report = await harness.orchestrator.reconcile()

    assert report.state == SetupState.READY
    assert harness.runtime.report() is report


@pytest.mark.asyncio
async def test_required_view_failure_reports_uc_setup_instead_of_background_services() -> None:
    activation = FakeActivation([], activation_failure=RequiredViewSetupError())
    harness = _harness(activation=activation)

    report = await harness.orchestrator.reconcile()

    assert report.state == SetupState.SETUP_REQUIRED
    assert report.current_step == SetupStepId.ACTIVATION
    step = report.step(SetupStepId.ACTIVATION)
    assert step.state == StepState.FAILED
    assert step.code == "required_views_creation_failed"
    assert "CREATE TABLE" in " ".join(step.instructions)
    assert "Genie schema" in step.summary
    assert not any(event.startswith("background:") for event in harness.events)
    assert "access" not in harness.events


@pytest.mark.asyncio
async def test_required_view_failure_mentions_metadata_dimensions_and_recovers_on_reconcile() -> None:
    activation = FakeActivation([], activation_failure=RequiredViewSetupError())
    harness = _harness(activation=activation)

    failed = await harness.orchestrator.reconcile()

    step = failed.step(SetupStepId.ACTIVATION)
    assert step.code == "required_views_creation_failed"
    assert step.actions == (SetupActionId.RECONCILE,)
    assert "metadata dimension" in step.summary

    activation.activation_failure = None
    report = await harness.orchestrator.reconcile()

    assert report.state == SetupState.READY
    assert report.step(SetupStepId.ACTIVATION).state == StepState.PASSED


@pytest.mark.asyncio
async def test_generic_activation_failure_is_reported() -> None:
    activation = FakeActivation([], activation_failure=RuntimeError("raw failure"))
    harness = _harness(activation=activation)

    report = await harness.orchestrator.reconcile()

    assert report.current_step == SetupStepId.ACTIVATION
    assert report.step(SetupStepId.ACTIVATION).code == "studio_activation_failed"


@pytest.mark.asyncio
async def test_completion_persistence_failure_stops_before_activation() -> None:
    class FailingAppSettings:
        def record_setup_completion(self, job_id: int, completed_at: datetime, user_name: str | None) -> None:
            raise RuntimeError("database detail")

    harness = _harness()
    harness.orchestrator.app_settings = FailingAppSettings()

    report = await harness.orchestrator.reconcile()

    assert report.current_step == SetupStepId.MIGRATIONS
    assert report.step(SetupStepId.MIGRATIONS).code == "setup_completion_persistence_failed"
    assert "activate" not in harness.events


@pytest.mark.asyncio
async def test_concurrent_and_repeated_reconcile_activate_once() -> None:
    release_activation = asyncio.Event()
    events: list[str] = []
    activation = FakeActivation(events, wait_until=release_activation)
    harness = _harness(events, activation=activation)

    first = asyncio.create_task(harness.orchestrator.reconcile())
    while "activate" not in harness.events:
        await asyncio.sleep(0)
    second = asyncio.create_task(harness.orchestrator.reconcile())
    release_activation.set()

    first_report, second_report = await asyncio.gather(first, second)
    third_report = await harness.orchestrator.reconcile()

    assert first_report.state == second_report.state == third_report.state == SetupState.READY
    assert harness.events.count("activate") == 1
    assert harness.events.count("postgres") == 1


@pytest.mark.asyncio
async def test_ready_reconcile_keeps_app_available_when_admin_grant_fails() -> None:
    harness = _harness()
    await harness.orchestrator.reconcile()
    harness.jobs.grant_failure = RuntimeError("transient permissions failure")

    report = await harness.orchestrator.reconcile(setup_user="admin@example.com")

    assert report.state == SetupState.READY
    assert harness.runtime.report() is report


@pytest.mark.asyncio
async def test_cancelled_reconcile_propagates_cancellation() -> None:
    release_activation = asyncio.Event()
    events: list[str] = []
    activation = FakeActivation(events, wait_until=release_activation)
    harness = _harness(events, activation=activation)

    task = asyncio.create_task(harness.orchestrator.reconcile())
    while "activate" not in harness.events:
        await asyncio.sleep(0)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert harness.runtime.report().state != SetupState.READY


class _MemorySettings:
    def __init__(self, values: dict[str, str] | None = None) -> None:
        self.values = dict(values or {})

    def get_setting(self, key: str) -> str | None:
        return self.values.get(key)

    def save_setting(self, key: str, value: str, *, user_email: str | None = None) -> None:
        self.values[key] = value


_CHOICES = SetupChoices(catalog="main", prefix="dqx_studio", audience_group="data-team")
_LOCKED = {
    "setup_catalog": "main",
    "setup_prefix": "dqx_studio",
    "setup_audience_group": "other",
    "setup_storage_locked": "true",
}


@pytest.mark.asyncio
async def test_save_configuration_persists_when_unlocked() -> None:
    settings = _MemorySettings()

    outcome = await _harness().orchestrator.save_configuration(
        SetupConfigurationStore(settings), _CHOICES, user_email="a@example.com"
    )

    assert outcome == "saved"
    assert settings.values["setup_audience_group"] == "data-team"


@pytest.mark.asyncio
async def test_save_configuration_refuses_different_choices_when_locked() -> None:
    settings = _MemorySettings(_LOCKED)

    outcome = await _harness().orchestrator.save_configuration(
        SetupConfigurationStore(settings), _CHOICES, user_email=None
    )

    assert outcome == "locked"
    assert settings.values["setup_audience_group"] == "other"


@pytest.mark.asyncio
async def test_save_configuration_is_unchanged_for_identical_locked_choices() -> None:
    settings = _MemorySettings({**_LOCKED, "setup_audience_group": "data-team"})

    outcome = await _harness().orchestrator.save_configuration(
        SetupConfigurationStore(settings), _CHOICES, user_email=None
    )

    assert outcome == "unchanged"


@pytest.mark.asyncio
async def test_save_configuration_rechecks_lock_after_waiting_for_activation_lock() -> None:
    harness = _harness()
    settings = _MemorySettings()
    await harness.runtime.activation_lock.acquire()
    pending = asyncio.create_task(
        harness.orchestrator.save_configuration(SetupConfigurationStore(settings), _CHOICES, user_email=None)
    )
    await asyncio.sleep(0)
    settings.values.update(_LOCKED)  # a concurrent reconcile provisions storage and locks
    harness.runtime.activation_lock.release()

    assert await pending == "locked"
    assert settings.values["setup_audience_group"] == "other"
