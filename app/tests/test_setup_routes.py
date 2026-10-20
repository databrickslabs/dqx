"""HTTP contract tests for setup readiness and bootstrap administration."""

import uuid
from collections.abc import Iterator
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.errors import InvalidParameterError
from databricks.sdk.errors import InternalError, InvalidParameterValue, NotFound, PermissionDenied
from databricks.sdk.service.iam import ServicePrincipal
from fastapi.testclient import TestClient

from databricks_labs_dqx_app.backend.setup.audience import resolve_audience
from databricks_labs_dqx_app.backend.app import app
from databricks_labs_dqx_app.backend.config import AppConfig
from databricks_labs_dqx_app.backend.dependencies import (
    get_conf,
    get_obo_ws,
    get_setup_sql_reader_factory,
    get_setup_configuration_store,
    get_sp_ws,
)
from databricks_labs_dqx_app.backend.setup.configuration import SetupChoices, SetupConfigurationStore
from databricks_labs_dqx_app.backend.setup.errors import (
    AppServicePrincipalChosenError,
    TaskRunnerJobUnavailableError,
    TaskRunnerManagedByDeploymentError,
)
from databricks_labs_dqx_app.backend.setup.models import (
    SetupApplyGrantsResponse,
    SetupConfigurationView,
    SetupGrantFailure,
    SetupReport,
    SetupState,
    SetupStep,
    SetupStepId,
    StepState,
    TaskRunnerAccessResponse,
    TaskRunnerGrantsResponse,
)
from databricks_labs_dqx_app.backend.setup.orchestrator import SetupOrchestrator
from databricks_labs_dqx_app.backend.setup.runtime import setup_runtime
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor
from databricks_labs_dqx_app.backend.runtime import rt
from databricks_labs_dqx_app.backend.setup.resources import (
    ActiveResources,
    BootstrapResources,
    LakebaseConnection,
    VolumeLocation,
)


APP_APPLICATION_ID = "11111111-2222-4333-8444-555555555555"
RUNNER_APPLICATION_ID = "6f1c2d3e-4a5b-4c6d-8e7f-90a1b2c3d4e5"


def user_in_groups(*groups: str, user_name: str = "admin@example.com") -> MagicMock:
    """Return a complete-enough SCIM user for the setup access boundary."""
    user = MagicMock()
    user.user_name = user_name
    user.groups = [MagicMock(display=group) for group in groups]
    return user


@pytest.fixture
def obo_ws() -> MagicMock:
    """Return the OBO client exposing exactly one authenticated administrator."""
    workspace = create_autospec(WorkspaceClient, instance=True)
    workspace.current_user.me.return_value = user_in_groups("admins")
    return workspace


@pytest.fixture
def resources() -> ActiveResources:
    return ActiveResources(
        volume=VolumeLocation("main", "dqx_studio", "wheels", "/Volumes/main/dqx_studio/wheels"),
        lakebase=LakebaseConnection(
            endpoint="projects/p/branches/b/endpoints/e",
            host=None,
            port=5432,
            database="databricks_postgres",
            username=None,
            password=None,
            schema="dqx_studio",
        ),
        warehouse_id="warehouse-id",
        job_id=None,
        tmp_schema="dqx_studio_tmp",
        genie_schema="genie",
        demo_schema="studio_demo",
        audience=resolve_audience(["data-team"], "admins", allow_broad=False),
    )


@pytest.fixture
def orchestrator(resources: ActiveResources) -> MagicMock:
    """Return the setup transition boundary without external collaborators."""
    setup_orchestrator = create_autospec(SetupOrchestrator, instance=True)
    setup_orchestrator.reconcile = AsyncMock(return_value=SetupReport(state=SetupState.SETUP_REQUIRED, steps=()))
    setup_orchestrator.bootstrap = BootstrapResources(
        lakebase=resources.lakebase, warehouse_id=resources.warehouse_id, job_id=resources.job_id
    )
    setup_orchestrator.bound = MagicMock(resources=resources)
    setup_orchestrator.app_sp_id = APP_APPLICATION_ID
    setup_orchestrator.job_managed_by_deployment = False

    async def save_configuration(
        store: SetupConfigurationStore, choices: SetupChoices, *, user_email: str | None
    ) -> str:
        store.save(choices, user_email=user_email)
        return "saved"

    setup_orchestrator.save_configuration = AsyncMock(side_effect=save_configuration)
    return setup_orchestrator


@pytest.fixture
def reader_sql() -> MagicMock:
    return create_autospec(SqlExecutor, instance=True)


@pytest.fixture
def reader_sql_factory(reader_sql: MagicMock) -> MagicMock:
    return MagicMock(return_value=reader_sql)


class MemorySettings:
    """In-memory setup settings persistence."""

    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    def get_setting(self, key: str) -> str | None:
        return self.values.get(key)

    def save_setting(self, key: str, value: str, *, user_email: str | None = None) -> None:
        self.values[key] = value


@pytest.fixture
def settings() -> MemorySettings:
    return MemorySettings()


@pytest.fixture
def sp_ws() -> MagicMock:
    workspace = create_autospec(WorkspaceClient, instance=True)
    workspace.groups.list.return_value = [SimpleNamespace(display_name="data-team")]
    return workspace


@pytest.fixture
def client(
    obo_ws: MagicMock,
    orchestrator: MagicMock,
    reader_sql_factory: MagicMock,
    sp_ws: MagicMock,
    settings: MemorySettings,
) -> Iterator[TestClient]:
    """Expose the registered API with OBO identity and setup orchestration injected."""
    previous_report = setup_runtime.report()
    had_orchestrator = hasattr(app.state, "setup_orchestrator")
    previous_orchestrator = getattr(app.state, "setup_orchestrator", None)
    app.dependency_overrides[get_obo_ws] = lambda: obo_ws
    app.dependency_overrides[get_setup_sql_reader_factory] = lambda: reader_sql_factory
    app.dependency_overrides[get_conf] = lambda: AppConfig(admin_group="admins", catalog="")
    app.dependency_overrides[get_sp_ws] = lambda: sp_ws
    app.dependency_overrides[get_setup_configuration_store] = lambda: SetupConfigurationStore(settings)
    orchestrator.configuration_view.return_value = SetupConfigurationView(source="none")
    app.state.setup_orchestrator = orchestrator
    setup_runtime.publish(
        SetupReport(
            state=SetupState.SETUP_REQUIRED,
            current_step=SetupStepId.IDENTITY,
            steps=(
                SetupStep(
                    id=SetupStepId.IDENTITY,
                    state=StepState.ACTION_REQUIRED,
                    code="setup_required",
                ),
            ),
        )
    )
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.pop(get_obo_ws, None)
        app.dependency_overrides.pop(get_setup_sql_reader_factory, None)
        app.dependency_overrides.pop(get_conf, None)
        app.dependency_overrides.pop(get_sp_ws, None)
        app.dependency_overrides.pop(get_setup_configuration_store, None)
        if had_orchestrator:
            app.state.setup_orchestrator = previous_orchestrator
        else:
            delattr(app.state, "setup_orchestrator")
        setup_runtime.publish(previous_report)


def test_status_is_available_before_migrations(client: TestClient) -> None:
    """A missing setup route would leave initial installations at a 404."""
    response = client.get("/api/v1/setup/status")

    assert response.status_code == 200
    assert response.json()["report"]["state"] == "setup_required"
    assert response.json()["can_manage"] is True
    assert response.json()["admin_group"] == "admins"


JOB_ID = 914148714803833


def _report_with_task_runner(state: StepState) -> SetupReport:
    return SetupReport(
        state=SetupState.READY if state == StepState.PASSED else SetupState.SETUP_REQUIRED,
        steps=(SetupStep(id=SetupStepId.TASK_RUNNER, state=state),),
    )


@pytest.fixture
def resolved_job_id(client: TestClient) -> Iterator[int]:
    """Give the setup runtime a resolved task-runner job, restoring the previous one afterwards."""
    previous_job_id = setup_runtime.job_id
    setup_runtime.job_id = JOB_ID
    try:
        yield JOB_ID
    finally:
        setup_runtime.job_id = previous_job_id


def test_status_reports_the_signed_in_user_and_job_to_administrators(client: TestClient, resolved_job_id: int) -> None:
    """Administrators get a direct link to the task-runner job."""
    setup_runtime.publish(_report_with_task_runner(StepState.PASSED))

    response = client.get("/api/v1/setup/status")

    assert response.json()["user_name"] == "admin@example.com"
    assert response.json()["task_runner_job_id"] == resolved_job_id


def test_status_hides_the_job_from_non_admins_but_names_them(
    client: TestClient, obo_ws: MagicMock, resolved_job_id: int
) -> None:
    """A non-admin is told who they are signed in as, and never sees the job ID."""
    obo_ws.current_user.me.return_value = user_in_groups("users", user_name="analyst@example.com")
    setup_runtime.publish(_report_with_task_runner(StepState.PASSED))

    response = client.get("/api/v1/setup/status")

    assert response.json()["can_manage"] is False
    assert response.json()["user_name"] == "analyst@example.com"
    assert response.json()["task_runner_job_id"] is None


def test_status_links_the_job_when_its_step_failed(client: TestClient, resolved_job_id: int) -> None:
    """A failed run_as or permissions check is when the administrator most needs the job link."""
    setup_runtime.publish(_report_with_task_runner(StepState.FAILED))

    assert client.get("/api/v1/setup/status").json()["task_runner_job_id"] == resolved_job_id


def test_status_omits_a_job_setup_has_not_resolved(client: TestClient) -> None:
    """Without a resolved (or after a deleted) job, the UI falls back to the jobs list."""
    previous_job_id = setup_runtime.job_id
    setup_runtime.job_id = None
    try:
        assert client.get("/api/v1/setup/status").json()["task_runner_job_id"] is None
    finally:
        setup_runtime.job_id = previous_job_id


def test_status_never_shows_the_placeholder_for_an_unnamed_user(client: TestClient, obo_ws: MagicMock) -> None:
    obo_ws.current_user.me.return_value = user_in_groups("users", user_name="")

    response = client.get("/api/v1/setup/status")

    assert response.json()["user_name"] == ""


def test_status_marks_non_admin_as_waiting(client: TestClient, obo_ws: MagicMock) -> None:
    """Removing bootstrap-group membership must hide setup controls."""
    obo_ws.current_user.me.return_value = user_in_groups("users")

    response = client.get("/api/v1/setup/status")

    assert response.status_code == 200
    assert response.json()["can_manage"] is False


def test_status_reuses_setup_access_for_the_same_caller_token(client: TestClient, obo_ws: MagicMock) -> None:
    """Polling setup status must not repeat the same SCIM lookup on every request."""
    headers = {"X-Forwarded-Access-Token": "caller-token"}

    first = client.get("/api/v1/setup/status", headers=headers)
    second = client.get("/api/v1/setup/status", headers=headers)

    assert first.status_code == second.status_code == 200
    assert obo_ws.current_user.me.call_count == 1


def test_status_sanitizes_the_configured_administrator_group(client: TestClient, obo_ws: MagicMock) -> None:
    """A control character in the configured group must not reach the setup UI."""
    app.dependency_overrides[get_conf] = lambda: AppConfig(admin_group="admin\ns")
    obo_ws.current_user.me.return_value = user_in_groups("admin s")

    response = client.get("/api/v1/setup/status")

    assert response.status_code == 200
    assert response.json()["can_manage"] is True
    assert response.json()["admin_group"] == "admin s"


def test_reconcile_requires_bootstrap_admin_group(client: TestClient, obo_ws: MagicMock) -> None:
    """A removed bootstrap authorization check must reject reconciliation."""
    obo_ws.current_user.me.return_value = user_in_groups("users")

    response = client.post("/api/v1/setup/reconcile")

    assert response.status_code == 403


def test_reconcile_sql_reader_does_not_require_activated_resources(
    client: TestClient, orchestrator: MagicMock, resources: ActiveResources
) -> None:
    app.dependency_overrides.pop(get_setup_sql_reader_factory)
    previous_resources = rt.resources
    rt.resources = None
    try:
        response = client.post("/api/v1/setup/reconcile")
        assert response.status_code == 200
        factory = orchestrator.reconcile.call_args.kwargs["reader_sql_factory"]
        assert isinstance(factory(resources), SqlExecutor)
        assert rt.resources is None
    finally:
        rt.resources = previous_resources


def test_unbound_reconcile_reaches_orchestrator_with_a_deferred_sql_reader(
    client: TestClient, orchestrator: MagicMock
) -> None:
    """A pre-bind bootstrap failure must not lock administrators out of retrying setup."""
    app.dependency_overrides.pop(get_setup_sql_reader_factory)
    orchestrator.bound = None

    response = client.post("/api/v1/setup/reconcile")

    assert response.status_code == 200
    orchestrator.reconcile.assert_awaited_once()
    assert "reader_sql" not in orchestrator.reconcile.call_args.kwargs
    assert callable(orchestrator.reconcile.call_args.kwargs["reader_sql_factory"])


def test_reconcile_does_not_trust_cached_setup_access(client: TestClient, obo_ws: MagicMock) -> None:
    """A cached status lookup must not extend setup privileges after group removal."""
    headers = {"X-Forwarded-Access-Token": "caller-token"}
    assert client.get("/api/v1/setup/status", headers=headers).status_code == 200
    obo_ws.current_user.me.return_value = user_in_groups("users")

    response = client.post("/api/v1/setup/reconcile", headers=headers)

    assert response.status_code == 403
    assert obo_ws.current_user.me.call_count == 2


def test_reconcile_passes_authenticated_admin_to_orchestrator(
    client: TestClient, orchestrator: MagicMock, obo_ws: MagicMock, reader_sql_factory: MagicMock
) -> None:
    """The reconciliation transition must receive its trusted administrator actor."""
    response = client.post("/api/v1/setup/reconcile")

    assert response.status_code == 200
    orchestrator.reconcile.assert_awaited_once_with(
        setup_user="admin@example.com", reader_ws=obo_ws, reader_sql_factory=reader_sql_factory
    )


def test_reconcile_sanitizes_the_authenticated_administrator_name(
    client: TestClient, obo_ws: MagicMock, orchestrator: MagicMock, reader_sql_factory: MagicMock
) -> None:
    """Control characters in a trusted SCIM name must not reach setup side effects."""
    obo_ws.current_user.me.return_value = user_in_groups("admins", user_name=" admin\n@example.com ")

    response = client.post("/api/v1/setup/reconcile")

    assert response.status_code == 200
    orchestrator.reconcile.assert_awaited_once_with(
        setup_user="admin @example.com", reader_ws=obo_ws, reader_sql_factory=reader_sql_factory
    )


_VALID = {"catalog": "main", "prefix": "dqx_studio", "audience_group": "data-team"}
_CONFIG_URL = "/api/v1/setup/configuration"


def test_status_exposes_resolved_configuration(client: TestClient, orchestrator: MagicMock) -> None:
    orchestrator.configuration_view.return_value = SetupConfigurationView(source="saved", catalog="main")

    response = client.get("/api/v1/setup/status")

    assert response.json()["configuration"]["catalog"] == "main"


def test_configuration_requires_setup_admin(
    client: TestClient, obo_ws: MagicMock, sp_ws: MagicMock, orchestrator: MagicMock, settings: MemorySettings
) -> None:
    obo_ws.current_user.me.return_value = user_in_groups("data-team")

    response = client.post(_CONFIG_URL, json=_VALID)

    assert response.status_code == 403
    sp_ws.groups.list.assert_not_called()
    assert not settings.values
    orchestrator.reconcile.assert_not_awaited()


@pytest.mark.parametrize("group", ["users", "account users", "admins", "Account Users"])
def test_configuration_rejects_builtin_audience_group(client: TestClient, settings: MemorySettings, group: str) -> None:
    response = client.post(_CONFIG_URL, json={**_VALID, "audience_group": group})

    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "audience_group_builtin"
    assert not settings.values


@pytest.mark.parametrize("change", [{"prefix": "Bad-Prefix!"}, {"audience_group": "team`a"}])
def test_configuration_rejects_invalid_prefix_or_group_characters(
    client: TestClient, settings: MemorySettings, change: dict[str, str]
) -> None:
    response = client.post(_CONFIG_URL, json={**_VALID, **change})

    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "configuration_invalid"
    assert not settings.values


@pytest.mark.parametrize("catalog", ["main.sales", "a`b", "back\\slash", "", "   "])
def test_configuration_rejects_invalid_catalog_name(client: TestClient, settings: MemorySettings, catalog: str) -> None:
    response = client.post(_CONFIG_URL, json={**_VALID, "catalog": catalog})

    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "catalog_invalid"
    assert not settings.values


@pytest.mark.parametrize("group", ["", "   "])
def test_configuration_requires_an_audience_group(client: TestClient, settings: MemorySettings, group: str) -> None:
    response = client.post(_CONFIG_URL, json={**_VALID, "audience_group": group})

    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "audience_group_required"
    assert not settings.values


def test_configuration_is_rejected_when_deployment_managed(client: TestClient, settings: MemorySettings) -> None:
    app.dependency_overrides[get_conf] = lambda: AppConfig(admin_group="admins", catalog="main", warehouse_id="wh")

    response = client.post(_CONFIG_URL, json=_VALID)

    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "configuration_managed_by_deployment"
    assert not settings.values


def test_configuration_locked_rejects_different_values(
    client: TestClient, orchestrator: MagicMock, settings: MemorySettings
) -> None:
    orchestrator.save_configuration.side_effect = None
    orchestrator.save_configuration.return_value = "locked"

    response = client.post(_CONFIG_URL, json=_VALID)

    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "configuration_locked"
    assert not settings.values
    orchestrator.reconcile.assert_not_awaited()


def test_configuration_locked_identical_values_only_reconcile(client: TestClient, orchestrator: MagicMock) -> None:
    orchestrator.save_configuration.side_effect = None
    orchestrator.save_configuration.return_value = "unchanged"

    response = client.post(_CONFIG_URL, json=_VALID)

    assert response.status_code == 200
    orchestrator.reconcile.assert_awaited_once()


def test_configuration_rejects_unknown_catalog(client: TestClient, obo_ws: MagicMock, settings: MemorySettings) -> None:
    obo_ws.catalogs.get.side_effect = NotFound("missing")

    response = client.post(_CONFIG_URL, json=_VALID)

    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "catalog_not_found"
    assert not settings.values


def test_configuration_treats_catalog_permission_denied_as_not_found(client: TestClient, obo_ws: MagicMock) -> None:
    obo_ws.catalogs.get.side_effect = PermissionDenied("no")

    response = client.post(_CONFIG_URL, json=_VALID)

    assert response.json()["detail"]["code"] == "catalog_not_found"


def test_configuration_reports_unexpected_catalog_failure(
    client: TestClient, obo_ws: MagicMock, settings: MemorySettings, caplog: pytest.LogCaptureFixture
) -> None:
    obo_ws.catalogs.get.side_effect = InternalError("secret-detail")

    response = client.post(_CONFIG_URL, json=_VALID)

    assert response.status_code == 502
    assert response.json()["detail"]["code"] == "catalog_check_failed"
    assert "secret-detail" not in caplog.text
    assert "InternalError" in caplog.text
    assert not settings.values


def test_configuration_rejects_unknown_group(client: TestClient, sp_ws: MagicMock) -> None:
    sp_ws.groups.list.return_value = []

    response = client.post(_CONFIG_URL, json=_VALID)

    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "audience_group_not_found"


def test_configuration_rejects_group_with_different_display_name(client: TestClient, sp_ws: MagicMock) -> None:
    sp_ws.groups.list.return_value = [SimpleNamespace(display_name="data-team-2")]

    response = client.post(_CONFIG_URL, json=_VALID)

    assert response.json()["detail"]["code"] == "audience_group_not_found"


def test_configuration_reports_unexpected_group_failure(
    client: TestClient, sp_ws: MagicMock, caplog: pytest.LogCaptureFixture
) -> None:
    sp_ws.groups.list.side_effect = InternalError("secret-detail")

    response = client.post(_CONFIG_URL, json=_VALID)

    assert response.status_code == 502
    assert response.json()["detail"]["code"] == "group_check_failed"
    assert "secret-detail" not in caplog.text


def test_configuration_group_not_found_error_maps_to_not_found_code(client: TestClient, sp_ws: MagicMock) -> None:
    sp_ws.groups.list.side_effect = NotFound("gone")

    response = client.post(_CONFIG_URL, json=_VALID)

    assert response.json()["detail"]["code"] == "audience_group_not_found"


def test_configuration_encodes_group_in_scim_filter(client: TestClient, sp_ws: MagicMock) -> None:
    sp_ws.groups.list.return_value = [SimpleNamespace(display_name='team "a"')]

    response = client.post(_CONFIG_URL, json={**_VALID, "audience_group": 'team "a"'})

    assert response.status_code == 200
    assert sp_ws.groups.list.call_args.kwargs["filter"] == 'displayName eq "team \\"a\\""'


def test_configuration_saves_and_reconciles(
    client: TestClient,
    obo_ws: MagicMock,
    orchestrator: MagicMock,
    settings: MemorySettings,
    reader_sql_factory: MagicMock,
) -> None:
    """Reconcile gets a factory so the first check inspects grants on the newly saved storage."""
    response = client.post(_CONFIG_URL, json=_VALID)

    assert response.status_code == 200
    assert settings.values["setup_audience_group"] == "data-team"
    orchestrator.reconcile.assert_awaited_once_with(
        setup_user="admin@example.com", reader_ws=obo_ws, reader_sql_factory=reader_sql_factory
    )


_OVERRIDE_URL = "/api/v1/setup/override"


def test_override_reruns_setup_for_the_confirmed_step(
    client: TestClient, orchestrator: MagicMock, obo_ws: MagicMock, reader_sql_factory: MagicMock
) -> None:
    report = SetupReport(state=SetupState.READY, steps=())
    orchestrator.override = AsyncMock(return_value=report)

    response = client.post(_OVERRIDE_URL, json={"step_id": "warehouse"})

    assert response.status_code == 200
    assert response.json()["state"] == "ready"
    orchestrator.override.assert_awaited_once_with(
        SetupStepId.WAREHOUSE,
        setup_user="admin@example.com",
        reader_ws=obo_ws,
        reader_sql_factory=reader_sql_factory,
    )


def test_override_unavailable_step_is_a_conflict(client: TestClient, orchestrator: MagicMock) -> None:
    orchestrator.override = AsyncMock(return_value=None)

    response = client.post(_OVERRIDE_URL, json={"step_id": "storage"})

    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "override_not_available"


def test_override_requires_setup_admin(client: TestClient, orchestrator: MagicMock, obo_ws: MagicMock) -> None:
    obo_ws.current_user.me.return_value = user_in_groups("users")
    orchestrator.override = AsyncMock()

    response = client.post(_OVERRIDE_URL, json={"step_id": "warehouse"})

    assert response.status_code == 403
    orchestrator.override.assert_not_awaited()


def test_override_rejects_unknown_step(client: TestClient) -> None:
    assert client.post(_OVERRIDE_URL, json={"step_id": "nope"}).status_code == 422


_APPLY_GRANTS_URL = "/api/v1/setup/steps/task_runner/apply-grants"


def test_apply_grants_runs_as_the_signed_in_administrator(
    client: TestClient, orchestrator: MagicMock, obo_ws: MagicMock, reader_sql_factory: MagicMock
) -> None:
    orchestrator.apply_grants = AsyncMock(
        return_value=SetupApplyGrantsResponse(
            report=SetupReport(state=SetupState.SETUP_REQUIRED, steps=()),
            applied=1,
            total=2,
            failed=(SetupGrantFailure(statement="GRANT X ON SCHEMA `a`.`b` TO `c`;", error_code="permission_denied"),),
        )
    )

    response = client.post(_APPLY_GRANTS_URL)

    assert response.status_code == 200
    body = response.json()
    assert (body["applied"], body["total"]) == (1, 2)
    assert body["failed"] == [{"statement": "GRANT X ON SCHEMA `a`.`b` TO `c`;", "error_code": "permission_denied"}]
    orchestrator.apply_grants.assert_awaited_once_with(
        SetupStepId.TASK_RUNNER,
        setup_user="admin@example.com",
        reader_ws=obo_ws,
        reader_sql_factory=reader_sql_factory,
    )


def test_apply_grants_with_nothing_to_apply_is_a_conflict(client: TestClient, orchestrator: MagicMock) -> None:
    orchestrator.apply_grants = AsyncMock(return_value=None)

    response = client.post(_APPLY_GRANTS_URL)

    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "nothing_to_apply"


@pytest.mark.parametrize(
    "error", [PermissionDenied("internal detail"), InternalError("internal detail"), RuntimeError("internal detail")]
)
def test_apply_grants_maps_failures_to_a_code_without_raw_text(
    client: TestClient, orchestrator: MagicMock, error: Exception, caplog: pytest.LogCaptureFixture
) -> None:
    orchestrator.apply_grants = AsyncMock(side_effect=error)

    response = client.post(_APPLY_GRANTS_URL)

    assert response.status_code == 502
    assert response.json()["detail"] == {"code": "setup_grants_failed"}
    assert "internal detail" not in response.text
    assert "internal detail" not in caplog.text


def test_apply_grants_requires_setup_admin(client: TestClient, orchestrator: MagicMock, obo_ws: MagicMock) -> None:
    obo_ws.current_user.me.return_value = user_in_groups("users")
    orchestrator.apply_grants = AsyncMock()

    response = client.post(_APPLY_GRANTS_URL)

    assert response.status_code == 403
    orchestrator.apply_grants.assert_not_awaited()


def test_apply_grants_cannot_carry_sql_from_the_client(client: TestClient, orchestrator: MagicMock) -> None:
    """Statements are always re-derived on the server; the endpoint takes no request body."""
    orchestrator.apply_grants = AsyncMock(return_value=None)
    operation = client.get("/openapi.json").json()["paths"]["/api/v1/setup/steps/{step_id}/apply-grants"]["post"]

    client.post(_APPLY_GRANTS_URL, json={"sql": "DROP CATALOG main"})

    assert "requestBody" not in operation
    assert orchestrator.apply_grants.call_args.args == (SetupStepId.TASK_RUNNER,)
    assert "DROP" not in str(orchestrator.apply_grants.call_args)


def test_apply_grants_rejects_unknown_step(client: TestClient) -> None:
    setup_runtime.publish(SetupReport(state=SetupState.READY, steps=()))

    assert client.post("/api/v1/setup/steps/nope/apply-grants").status_code == 422


_PRINCIPALS_URL = "/api/v1/setup/task-runner/service-principals"
_ACCESS_URL = "/api/v1/setup/task-runner/access"
_RUNNER_GRANTS_URL = "/api/v1/setup/task-runner/grants"
_RUN_AS_URL = "/api/v1/setup/task-runner/run-as"
_PICKED_GRANT = f"GRANT USE_CATALOG ON CATALOG `main` TO `{RUNNER_APPLICATION_ID}`;"


def test_service_principal_list_excludes_the_app_and_names_it(client: TestClient, sp_ws: MagicMock) -> None:
    sp_ws.service_principals.list.return_value = iter(
        [
            ServicePrincipal(id="2", display_name="app-1a2b dqx-studio", application_id=APP_APPLICATION_ID),
            ServicePrincipal(id="1", display_name="dqx-studio-task-runner", application_id=RUNNER_APPLICATION_ID),
        ]
    )

    response = client.get(_PRINCIPALS_URL)

    assert response.status_code == 200
    assert response.json() == {
        "available": True,
        "principals": [{"id": "1", "display_name": "dqx-studio-task-runner", "application_id": RUNNER_APPLICATION_ID}],
        "app_name": "app-1a2b dqx-studio",
        "truncated": False,
    }


def test_service_principal_list_says_when_it_was_cut(client: TestClient, sp_ws: MagicMock) -> None:
    sp_ws.service_principals.list.return_value = iter(
        ServicePrincipal(id=str(index), display_name=f"runner {index}", application_id=str(uuid.UUID(int=index + 1)))
        for index in range(600)
    )

    body = client.get(_PRINCIPALS_URL).json()

    assert body["truncated"] is True
    assert len(body["principals"]) == 500


@pytest.mark.parametrize("error", [PermissionDenied("internal detail"), InternalError("internal detail")])
def test_service_principal_list_falls_back_to_pasting_an_id(
    client: TestClient, sp_ws: MagicMock, error: Exception
) -> None:
    sp_ws.service_principals.list.side_effect = error

    response = client.get(_PRINCIPALS_URL)

    assert response.status_code == 200
    body = response.json()
    assert (body["available"], body["principals"]) == (False, [])
    assert body["app_name"] == "dqx-studio"
    assert "internal detail" not in response.text


def test_service_principal_list_is_refused_for_a_bundle_managed_job(
    client: TestClient, orchestrator: MagicMock, sp_ws: MagicMock
) -> None:
    """A bundle-deployed job's service principal comes from the deployment, so there is nothing to pick."""
    orchestrator.job_managed_by_deployment = True

    response = client.get(_PRINCIPALS_URL)

    assert response.status_code == 409
    assert response.json()["detail"] == {"code": "task_runner_managed_by_deployment"}
    sp_ws.service_principals.list.assert_not_called()


@pytest.mark.parametrize(
    ("method", "url"),
    [("get", _PRINCIPALS_URL), ("post", _ACCESS_URL), ("post", _RUNNER_GRANTS_URL), ("post", _RUN_AS_URL)],
)
def test_task_runner_endpoints_require_setup_admin(
    client: TestClient, obo_ws: MagicMock, sp_ws: MagicMock, orchestrator: MagicMock, method: str, url: str
) -> None:
    obo_ws.current_user.me.return_value = user_in_groups("users")
    orchestrator.check_task_runner_access = AsyncMock()
    orchestrator.apply_task_runner_grants = AsyncMock()
    orchestrator.set_task_runner_run_as = AsyncMock()

    response = getattr(client, method)(url, **({} if method == "get" else {"json": {"application_id": "x"}}))

    assert response.status_code == 403
    sp_ws.service_principals.list.assert_not_called()
    orchestrator.check_task_runner_access.assert_not_awaited()
    orchestrator.apply_task_runner_grants.assert_not_awaited()
    orchestrator.set_task_runner_run_as.assert_not_awaited()


def test_confirm_access_reports_the_picked_principal_with_the_admin_reader(
    client: TestClient, orchestrator: MagicMock, obo_ws: MagicMock, reader_sql_factory: MagicMock
) -> None:
    orchestrator.check_task_runner_access = AsyncMock(
        return_value=TaskRunnerAccessResponse(sp_permission="missing", grant_statements=(_PICKED_GRANT,))
    )

    response = client.post(_ACCESS_URL, json={"application_id": RUNNER_APPLICATION_ID})

    assert response.status_code == 200
    assert response.json() == {
        "sp_permission": "missing",
        "grant_statements": [_PICKED_GRANT],
        "automatic_grants": [],
        "grants_unverified": False,
    }
    orchestrator.check_task_runner_access.assert_awaited_once_with(
        RUNNER_APPLICATION_ID, reader_ws=obo_ws, reader_sql_factory=reader_sql_factory
    )


def test_runner_grants_run_with_the_admin_sql_reader_and_report_failures(
    client: TestClient, orchestrator: MagicMock, obo_ws: MagicMock, reader_sql_factory: MagicMock
) -> None:
    orchestrator.apply_task_runner_grants = AsyncMock(
        return_value=TaskRunnerGrantsResponse(
            applied=1,
            total=2,
            failed=(SetupGrantFailure(statement=_PICKED_GRANT, error_code="permission_denied"),),
            grant_statements=(_PICKED_GRANT,),
        )
    )

    response = client.post(_RUNNER_GRANTS_URL, json={"application_id": RUNNER_APPLICATION_ID})

    assert response.status_code == 200
    body = response.json()
    assert (body["applied"], body["total"]) == (1, 2)
    assert body["failed"] == [{"statement": _PICKED_GRANT, "error_code": "permission_denied"}]
    assert body["grant_statements"] == [_PICKED_GRANT]
    orchestrator.apply_task_runner_grants.assert_awaited_once_with(
        RUNNER_APPLICATION_ID, reader_ws=obo_ws, reader_sql_factory=reader_sql_factory
    )


def test_runner_grants_cannot_carry_sql_from_the_client(client: TestClient, orchestrator: MagicMock) -> None:
    orchestrator.apply_task_runner_grants = AsyncMock()

    response = client.post(
        _RUNNER_GRANTS_URL, json={"application_id": RUNNER_APPLICATION_ID, "sql": "DROP CATALOG main"}
    )

    assert response.status_code == 422
    orchestrator.apply_task_runner_grants.assert_not_awaited()


def test_run_as_sets_the_job_and_returns_the_reconciled_report(
    client: TestClient, orchestrator: MagicMock, obo_ws: MagicMock, reader_sql_factory: MagicMock
) -> None:
    report = SetupReport(
        state=SetupState.SETUP_REQUIRED,
        current_step=SetupStepId.TASK_RUNNER,
        steps=(
            SetupStep(
                id=SetupStepId.TASK_RUNNER,
                state=StepState.ACTION_REQUIRED,
                code="task_runner_permissions_missing",
                grant_statements=(_PICKED_GRANT,),
            ),
        ),
    )
    orchestrator.set_task_runner_run_as = AsyncMock(return_value=report)

    response = client.post(_RUN_AS_URL, json={"application_id": RUNNER_APPLICATION_ID})

    assert response.status_code == 200
    assert response.json()["steps"][0]["grant_statements"] == [_PICKED_GRANT]
    orchestrator.set_task_runner_run_as.assert_awaited_once_with(
        RUNNER_APPLICATION_ID,
        setup_user="admin@example.com",
        reader_ws=obo_ws,
        reader_sql_factory=reader_sql_factory,
    )


_ERROR_CASES = [
    (TaskRunnerManagedByDeploymentError(), 409, "task_runner_managed_by_deployment"),
    (TaskRunnerJobUnavailableError(), 409, "task_runner_job_unavailable"),
    (AppServicePrincipalChosenError(), 422, "task_runner_principal_is_app"),
    (InvalidParameterError("not a uuid"), 422, "task_runner_principal_invalid"),
]


@pytest.mark.parametrize(("error", "status_code", "code"), _ERROR_CASES)
@pytest.mark.parametrize(
    ("url", "method"),
    [
        (_ACCESS_URL, "check_task_runner_access"),
        (_RUNNER_GRANTS_URL, "apply_task_runner_grants"),
        (_RUN_AS_URL, "set_task_runner_run_as"),
    ],
)
def test_task_runner_choices_map_refusals_to_codes(
    client: TestClient,
    orchestrator: MagicMock,
    url: str,
    method: str,
    error: Exception,
    status_code: int,
    code: str,
) -> None:
    setattr(orchestrator, method, AsyncMock(side_effect=error))

    response = client.post(url, json={"application_id": RUNNER_APPLICATION_ID})

    assert response.status_code == status_code
    assert response.json()["detail"] == {"code": code}


@pytest.mark.parametrize(
    ("error", "status_code", "code"),
    [
        (PermissionDenied("internal detail"), 403, "task_runner_run_as_forbidden"),
        (NotFound("internal detail"), 404, "task_runner_principal_not_found"),
        (InvalidParameterValue("internal detail"), 404, "task_runner_principal_not_found"),
        (InternalError("internal detail"), 502, "task_runner_run_as_failed"),
    ],
)
def test_run_as_maps_databricks_failures_to_codes_without_raw_text(
    client: TestClient, orchestrator: MagicMock, error: Exception, status_code: int, code: str
) -> None:
    orchestrator.set_task_runner_run_as = AsyncMock(side_effect=error)

    response = client.post(_RUN_AS_URL, json={"application_id": RUNNER_APPLICATION_ID})

    assert response.status_code == status_code
    assert response.json()["detail"] == {"code": code}
    assert "internal detail" not in response.text


@pytest.mark.parametrize(
    ("url", "method", "code"),
    [
        (_ACCESS_URL, "check_task_runner_access", "task_runner_access_failed"),
        (_RUNNER_GRANTS_URL, "apply_task_runner_grants", "task_runner_grants_failed"),
    ],
)
@pytest.mark.parametrize("error", [PermissionDenied("internal detail"), InternalError("internal detail")])
def test_task_runner_access_and_grants_map_databricks_failures_to_codes_without_raw_text(
    client: TestClient, orchestrator: MagicMock, url: str, method: str, code: str, error: Exception
) -> None:
    setattr(orchestrator, method, AsyncMock(side_effect=error))

    response = client.post(url, json={"application_id": RUNNER_APPLICATION_ID})

    assert response.status_code == 502
    assert response.json()["detail"] == {"code": code}
    assert "internal detail" not in response.text


_GROUPS_URL = "/api/v1/setup/groups"


def test_group_listing_excludes_builtins_sorts_and_names_only(client: TestClient, sp_ws: MagicMock) -> None:
    sp_ws.groups.list.return_value = [
        SimpleNamespace(display_name=name)
        for name in ["zeta", "Users", "data-team", "ACCOUNT USERS", "admins", "alpha", None]
    ]

    response = client.get(_GROUPS_URL)

    assert response.status_code == 200
    assert response.json() == {"groups": ["alpha", "data-team", "zeta"], "truncated": False}
    assert sp_ws.groups.list.call_args.kwargs["attributes"] == "id,displayName"


def test_group_listing_is_capped_reports_it_and_stops_paging(client: TestClient, sp_ws: MagicMock) -> None:
    read: list[int] = []

    def groups():
        for index in range(1500):
            read.append(index)
            yield SimpleNamespace(display_name=f"group-{index:05d}")

    sp_ws.groups.list.return_value = groups()

    body = client.get(_GROUPS_URL).json()

    assert len(body["groups"]) == 1000
    assert body["groups"][0] == "group-00000"
    assert body["truncated"] is True
    assert len(read) == 1001
    # One page holds the whole cap, plus the built-in groups that are left out.
    assert sp_ws.groups.list.call_args.kwargs["count"] > 1000


def test_group_listing_cap_ignores_built_in_and_repeated_groups(client: TestClient, sp_ws: MagicMock) -> None:
    sp_ws.groups.list.return_value = [
        SimpleNamespace(display_name=name)
        for name in ["users", "admins", "account users", "team", "team", *[f"g{index}" for index in range(999)]]
    ]

    body = client.get(_GROUPS_URL).json()

    assert len(body["groups"]) == 1000
    assert body["truncated"] is False


def test_group_listing_requires_setup_admin(client: TestClient, obo_ws: MagicMock, sp_ws: MagicMock) -> None:
    obo_ws.current_user.me.return_value = user_in_groups("data-team")

    response = client.get(_GROUPS_URL)

    assert response.status_code == 403
    sp_ws.groups.list.assert_not_called()


def test_group_listing_is_rejected_when_deployment_managed(client: TestClient, sp_ws: MagicMock) -> None:
    app.dependency_overrides[get_conf] = lambda: AppConfig(admin_group="admins", catalog="main", warehouse_id="wh")

    response = client.get(_GROUPS_URL)

    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "configuration_managed_by_deployment"
    sp_ws.groups.list.assert_not_called()


def test_group_listing_failure_hides_sdk_text(
    client: TestClient, sp_ws: MagicMock, caplog: pytest.LogCaptureFixture
) -> None:
    sp_ws.groups.list.side_effect = InternalError("secret-detail")

    response = client.get(_GROUPS_URL)

    assert response.status_code == 502
    assert response.json()["detail"] == {"code": "group_list_failed"}
    assert "secret-detail" not in response.text
    assert "secret-detail" not in caplog.text
