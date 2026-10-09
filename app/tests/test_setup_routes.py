"""HTTP contract tests for setup readiness and bootstrap administration."""

from collections.abc import Iterator
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import InternalError, NotFound, PermissionDenied
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
from databricks_labs_dqx_app.backend.setup.models import (
    SetupConfigurationView,
    SetupReport,
    SetupState,
    SetupStep,
    SetupStepId,
    StepState,
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


def test_configuration_rejects_broad_audience(client: TestClient, settings: MemorySettings) -> None:
    response = client.post(_CONFIG_URL, json={**_VALID, "audience_group": "users"})

    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "configuration_invalid"
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
