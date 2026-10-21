"""Catalog listing must work before setup is ready, as the signed-in user only."""

from collections.abc import Iterator
from types import SimpleNamespace
from unittest.mock import MagicMock, create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import InternalError
from fastapi.testclient import TestClient

from databricks_labs_dqx_app.backend.app import app
from databricks_labs_dqx_app.backend.dependencies import get_obo_ws, get_sp_ws
from databricks_labs_dqx_app.backend.setup.models import SetupReport, SetupState
from databricks_labs_dqx_app.backend.setup.runtime import setup_runtime

_URL = "/api/v1/discovery/catalogs"


@pytest.fixture
def sp_ws() -> MagicMock:
    return create_autospec(WorkspaceClient, instance=True)


def _obo_ws(user_name: str) -> MagicMock:
    workspace = create_autospec(WorkspaceClient, instance=True)
    workspace.current_user.me.return_value = SimpleNamespace(user_name=user_name, id="1")
    return workspace


@pytest.fixture
def pre_ready_client(sp_ws: MagicMock) -> Iterator[TestClient]:
    """The registered app with setup not ready: no storage, warehouse or bound resources."""
    previous_report = setup_runtime.report()
    app.dependency_overrides[get_sp_ws] = lambda: sp_ws
    setup_runtime.publish(SetupReport(state=SetupState.SETUP_REQUIRED, steps=()))
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.pop(get_obo_ws, None)
        app.dependency_overrides.pop(get_sp_ws, None)
        setup_runtime.publish(previous_report)


def test_catalogs_are_listed_before_setup_with_only_the_signed_in_users_client(
    pre_ready_client: TestClient, sp_ws: MagicMock
) -> None:
    obo_ws = _obo_ws("catalog-lister@example.com")
    obo_ws.catalogs.list.return_value = [SimpleNamespace(name="main", comment=None)]
    app.dependency_overrides[get_obo_ws] = lambda: obo_ws

    response = pre_ready_client.get(_URL)

    assert response.status_code == 200
    assert [catalog["name"] for catalog in response.json()] == ["main"]
    obo_ws.catalogs.list.assert_called_once()
    sp_ws.catalogs.list.assert_not_called()


def test_catalog_listing_failure_does_not_echo_sdk_text(pre_ready_client: TestClient) -> None:
    obo_ws = _obo_ws("catalog-failure@example.com")
    obo_ws.catalogs.list.side_effect = InternalError("secret internal detail")
    app.dependency_overrides[get_obo_ws] = lambda: obo_ws

    response = pre_ready_client.get(_URL)

    assert response.status_code == 500
    assert response.json()["detail"] == "Failed to list catalogs."
    assert "secret" not in response.text
