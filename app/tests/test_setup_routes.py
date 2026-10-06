"""HTTP contract tests for setup readiness and bootstrap administration."""

from collections.abc import Iterator
from unittest.mock import AsyncMock, MagicMock, create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from fastapi.testclient import TestClient

from databricks_labs_dqx_app.backend.setup.audience import resolve_audience
from databricks_labs_dqx_app.backend.app import app
from databricks_labs_dqx_app.backend.config import AppConfig
from databricks_labs_dqx_app.backend.dependencies import get_conf, get_setup_sql_executor, get_obo_ws
from databricks_labs_dqx_app.backend.setup.models import SetupReport, SetupState, SetupStep, SetupStepId, StepState
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
    return setup_orchestrator


@pytest.fixture
def reader_sql() -> MagicMock:
    return create_autospec(SqlExecutor, instance=True)


@pytest.fixture
def client(obo_ws: MagicMock, orchestrator: MagicMock, reader_sql: MagicMock) -> Iterator[TestClient]:
    """Expose the registered API with OBO identity and setup orchestration injected."""
    previous_report = setup_runtime.report()
    had_orchestrator = hasattr(app.state, "setup_orchestrator")
    previous_orchestrator = getattr(app.state, "setup_orchestrator", None)
    app.dependency_overrides[get_obo_ws] = lambda: obo_ws
    app.dependency_overrides[get_setup_sql_executor] = lambda: reader_sql
    app.dependency_overrides[get_conf] = lambda: AppConfig(admin_group="admins")
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
        app.dependency_overrides.pop(get_setup_sql_executor, None)
        app.dependency_overrides.pop(get_conf, None)
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


def test_reconcile_sql_reader_does_not_require_activated_resources(client: TestClient, orchestrator: MagicMock) -> None:
    app.dependency_overrides.pop(get_setup_sql_executor)
    previous_resources = rt.resources
    rt.resources = None
    try:
        response = client.post("/api/v1/setup/reconcile")
        assert response.status_code == 200
        assert isinstance(orchestrator.reconcile.call_args.kwargs["reader_sql"], SqlExecutor)
        assert rt.resources is None
    finally:
        rt.resources = previous_resources


def test_reconcile_requires_bound_storage_for_sql_reader(client: TestClient, orchestrator: MagicMock) -> None:
    app.dependency_overrides.pop(get_setup_sql_executor)
    orchestrator.bound = None

    response = client.post("/api/v1/setup/reconcile")

    assert response.status_code == 503
    assert response.json()["detail"] == "DQX Studio storage is not configured."
    orchestrator.reconcile.assert_not_called()


def test_reconcile_does_not_trust_cached_setup_access(client: TestClient, obo_ws: MagicMock) -> None:
    """A cached status lookup must not extend setup privileges after group removal."""
    headers = {"X-Forwarded-Access-Token": "caller-token"}
    assert client.get("/api/v1/setup/status", headers=headers).status_code == 200
    obo_ws.current_user.me.return_value = user_in_groups("users")

    response = client.post("/api/v1/setup/reconcile", headers=headers)

    assert response.status_code == 403
    assert obo_ws.current_user.me.call_count == 2


def test_reconcile_passes_authenticated_admin_to_orchestrator(
    client: TestClient, orchestrator: MagicMock, obo_ws: MagicMock, reader_sql: MagicMock
) -> None:
    """The reconciliation transition must receive its trusted administrator actor."""
    response = client.post("/api/v1/setup/reconcile")

    assert response.status_code == 200
    orchestrator.reconcile.assert_awaited_once_with(
        setup_user="admin@example.com", reader_ws=obo_ws, reader_sql=reader_sql
    )


def test_reconcile_sanitizes_the_authenticated_administrator_name(
    client: TestClient, obo_ws: MagicMock, orchestrator: MagicMock, reader_sql: MagicMock
) -> None:
    """Control characters in a trusted SCIM name must not reach setup side effects."""
    obo_ws.current_user.me.return_value = user_in_groups("admins", user_name=" admin\n@example.com ")

    response = client.post("/api/v1/setup/reconcile")

    assert response.status_code == 200
    orchestrator.reconcile.assert_awaited_once_with(
        setup_user="admin @example.com", reader_ws=obo_ws, reader_sql=reader_sql
    )
