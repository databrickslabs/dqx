"""HTTP contract tests for setup readiness and bootstrap administration."""

from collections.abc import Iterator
from unittest.mock import AsyncMock, MagicMock, create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from fastapi.testclient import TestClient

from databricks_labs_dqx_app.backend.app import app
from databricks_labs_dqx_app.backend.config import AppConfig
from databricks_labs_dqx_app.backend.dependencies import get_conf, get_obo_ws
from databricks_labs_dqx_app.backend.setup.models import SetupReport, SetupState, SetupStep, SetupStepId, StepState
from databricks_labs_dqx_app.backend.setup.orchestrator import SetupOrchestrator
from databricks_labs_dqx_app.backend.setup.runtime import setup_runtime


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
def orchestrator() -> MagicMock:
    """Return the setup transition boundary without external collaborators."""
    setup_orchestrator = create_autospec(SetupOrchestrator, instance=True)
    setup_orchestrator.reconcile = AsyncMock(return_value=SetupReport(state=SetupState.SETUP_REQUIRED, steps=()))
    return setup_orchestrator


@pytest.fixture
def client(obo_ws: MagicMock, orchestrator: MagicMock) -> Iterator[TestClient]:
    """Expose the registered API with OBO identity and setup orchestration injected."""
    previous_report = setup_runtime.report()
    had_orchestrator = hasattr(app.state, "setup_orchestrator")
    previous_orchestrator = getattr(app.state, "setup_orchestrator", None)
    app.dependency_overrides[get_obo_ws] = lambda: obo_ws
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


def test_reconcile_passes_authenticated_admin_to_orchestrator(client: TestClient, orchestrator: MagicMock) -> None:
    """The reconciliation transition must receive its trusted administrator actor."""
    response = client.post("/api/v1/setup/reconcile")

    assert response.status_code == 200
    orchestrator.reconcile.assert_awaited_once_with(setup_user="admin@example.com")


def test_reconcile_sanitizes_the_authenticated_administrator_name(
    client: TestClient, obo_ws: MagicMock, orchestrator: MagicMock
) -> None:
    """Control characters in a trusted SCIM name must not reach setup side effects."""
    obo_ws.current_user.me.return_value = user_in_groups("admins", user_name=" admin\n@example.com ")

    response = client.post("/api/v1/setup/reconcile")

    assert response.status_code == 200
    orchestrator.reconcile.assert_awaited_once_with(setup_user="admin @example.com")
