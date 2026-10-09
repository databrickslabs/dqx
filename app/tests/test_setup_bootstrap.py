"""Behavior tests for setup checks that run before Studio storage is configured."""

from types import SimpleNamespace
from unittest.mock import MagicMock, create_autospec

import pytest
from databricks.sdk import WorkspaceClient

from databricks_labs_dqx_app.backend.pg_executor import PgExecutor
from databricks_labs_dqx_app.backend.setup.bootstrap import BootstrapCheckers
from databricks_labs_dqx_app.backend.setup.models import SetupActionId, SetupStepId, StepState


@pytest.fixture
def workspace() -> MagicMock:
    workspace = create_autospec(WorkspaceClient, instance=True)
    workspace.current_user.me.return_value = SimpleNamespace(user_name="app-sp-id", id=None)
    return workspace


@pytest.fixture
def pg() -> MagicMock:
    executor = create_autospec(PgExecutor, instance=True)
    executor.q.side_effect = lambda identifier: '"' + identifier.replace('"', '""') + '"'
    return executor


@pytest.fixture
def checkers(workspace: MagicMock, pg: MagicMock) -> BootstrapCheckers:
    return BootstrapCheckers(workspace=workspace, pg=pg, lakebase_schema="dqx_studio")


def test_app_identity_resolves_service_principal_once(checkers: BootstrapCheckers, workspace: MagicMock) -> None:
    """Removing cached SP resolution would make this identity capability fail."""
    first = checkers.check_app_identity()
    second = checkers.check_app_identity()

    assert first.id == SetupStepId.IDENTITY
    assert first.state == StepState.PASSED
    assert second.state == StepState.PASSED
    assert checkers.app_sp_id() == "app-sp-id"
    workspace.current_user.me.assert_called_once_with()


def test_app_identity_falls_back_to_principal_id(checkers: BootstrapCheckers, workspace: MagicMock) -> None:
    workspace.current_user.me.return_value = SimpleNamespace(user_name=None, id="12345")

    assert checkers.check_app_identity().state == StepState.PASSED
    assert checkers.app_sp_id() == "12345"


def test_app_identity_resolution_failure_requires_action(checkers: BootstrapCheckers, workspace: MagicMock) -> None:
    """Swallowing an unresolved app identity would falsely pass setup."""
    workspace.current_user.me.side_effect = RuntimeError("platform detail\nthat must not escape")

    result = checkers.check_app_identity()

    assert result.state == StepState.ACTION_REQUIRED
    assert result.code == "app_identity_unresolved"
    assert result.actions == (SetupActionId.VERIFY_AGAIN,)
    assert "platform detail" not in result.summary
    assert "\n" not in result.summary
    assert checkers.app_sp_id() == ""


def test_transient_identity_failure_is_retried(checkers: BootstrapCheckers, workspace: MagicMock) -> None:
    """Caching a failed lookup would block setup until the app restarts."""
    workspace.current_user.me.side_effect = [
        RuntimeError("transient"),
        SimpleNamespace(user_name="app-sp-id", id=None),
    ]

    assert checkers.check_app_identity().state == StepState.ACTION_REQUIRED
    assert checkers.check_app_identity().state == StepState.PASSED
    assert checkers.app_sp_id() == "app-sp-id"
    assert workspace.current_user.me.call_count == 2


def test_app_identity_with_c1_control_character_requires_action(
    checkers: BootstrapCheckers, workspace: MagicMock
) -> None:
    """Accepting a C1 control character could inject terminal controls into setup output."""
    workspace.current_user.me.return_value = SimpleNamespace(user_name="app-sp\u0085id", id=None)

    result = checkers.check_app_identity()

    assert result.state == StepState.ACTION_REQUIRED
    assert result.code == "app_identity_unresolved"


def test_lakebase_connectivity_failure_is_sanitized(checkers: BootstrapCheckers, pg: MagicMock) -> None:
    """Returning database exceptions would disclose secrets and permit log injection."""
    pg.query.side_effect = RuntimeError("password=secret\nforged")

    result = checkers.check_lakebase()

    assert result.id == SetupStepId.LAKEBASE
    assert result.state == StepState.ACTION_REQUIRED
    assert result.code == "lakebase_connectivity_failed"
    assert "secret" not in result.summary
    assert "\n" not in result.summary


def test_lakebase_connectivity_passes_after_non_mutating_probe(checkers: BootstrapCheckers, pg: MagicMock) -> None:
    """Replacing the health probe with a mutating statement would violate readiness checks."""
    pg.query.return_value = [["1"]]

    result = checkers.check_lakebase()

    assert result.state == StepState.PASSED
    pg.query.assert_called_once_with("SELECT 1")


def test_lakebase_schema_creation_is_idempotent(checkers: BootstrapCheckers, pg: MagicMock) -> None:
    """Removing IF NOT EXISTS would make a repeated schema reconciliation fail."""
    result = checkers.ensure_lakebase_schema()

    assert result.state == StepState.PASSED
    pg.execute_no_schema.assert_called_once_with('CREATE SCHEMA IF NOT EXISTS "dqx_studio"')


def test_lakebase_schema_creation_failure_requires_reconcile(checkers: BootstrapCheckers, pg: MagicMock) -> None:
    pg.execute_no_schema.side_effect = RuntimeError("permission denied for database")

    result = checkers.ensure_lakebase_schema()

    assert result.id == SetupStepId.LAKEBASE
    assert result.state == StepState.ACTION_REQUIRED
    assert result.code == "lakebase_schema_creation_failed"
    assert result.actions == (SetupActionId.RECONCILE,)
    assert "permission denied" not in result.summary


def test_invalid_lakebase_schema_is_never_executed(workspace: MagicMock, pg: MagicMock) -> None:
    checkers = BootstrapCheckers(workspace=workspace, pg=pg, lakebase_schema="bad\\schema")

    result = checkers.ensure_lakebase_schema()

    assert result.state == StepState.ACTION_REQUIRED
    assert result.code == "lakebase_schema_creation_failed"
    pg.execute_no_schema.assert_not_called()
