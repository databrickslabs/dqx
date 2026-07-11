"""Unit tests for ``backend.routes.v1.genie``.

Follows the route-test convention (minimal FastAPI app + dependency
overrides): pins the availability-when-no-space contract (200 with
``available=false``, never an error), the OBO-identity wiring (the chat
proxy drives the CALLER's WorkspaceClient, with the SP client only as the
scope fallback), the /space payload (status fallback, sample questions,
deep link), input validation on the feedback/poll bodies, and viewer-level
RBAC.
"""

from __future__ import annotations

from unittest.mock import MagicMock, create_autospec

import pytest
from databricks.sdk.errors import PermissionDenied
from fastapi import FastAPI
from fastapi.testclient import TestClient

from databricks_labs_dqx_app.backend.common.authorization import UserRole, get_user_email
from databricks_labs_dqx_app.backend.dependencies import (
    get_app_settings_service,
    get_entitlement_service,
    get_obo_ws,
    get_preview_sql_executor,
    get_sp_ws,
    get_user_role,
)
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.entitlement_service import EntitlementService
from databricks_labs_dqx_app.backend.services.genie_space_service import (
    SAMPLE_QUESTIONS,
    SETTING_CONFIG_HASH,
    SETTING_SPACE_ID,
    SETTING_STATUS,
)

SPACE = "space-1"
BASE = f"/api/2.0/genie/spaces/{SPACE}"


@pytest.fixture
def settings_store() -> dict[str, str]:
    return {}


@pytest.fixture
def settings_mock(settings_store: dict[str, str]) -> MagicMock:
    mock = create_autospec(AppSettingsService, instance=True)
    mock.get_setting.side_effect = settings_store.get
    return mock


@pytest.fixture
def sp_ws_mock() -> MagicMock:
    ws = MagicMock(name="WorkspaceClient")
    ws.config.host = "https://ws.example.com/"
    return ws


@pytest.fixture
def entitlement_mock() -> MagicMock:
    mock = create_autospec(EntitlementService, instance=True)
    mock.verify_and_record.return_value = {}
    return mock


@pytest.fixture
def obo_sql_mock() -> MagicMock:
    return MagicMock(name="obo_sql")


@pytest.fixture
def obo_ws_mock() -> MagicMock:
    return MagicMock(name="obo_ws")


@pytest.fixture
def client(
    settings_mock: MagicMock,
    sp_ws_mock: MagicMock,
    entitlement_mock: MagicMock,
    obo_sql_mock: MagicMock,
    obo_ws_mock: MagicMock,
) -> TestClient:
    from databricks_labs_dqx_app.backend.routes.v1.genie import router

    app = FastAPI()
    app.include_router(router, prefix="/api/v1/genie")
    app.dependency_overrides[get_app_settings_service] = lambda: settings_mock
    app.dependency_overrides[get_sp_ws] = lambda: sp_ws_mock
    app.dependency_overrides[get_user_role] = lambda: UserRole.VIEWER
    app.dependency_overrides[get_user_email] = lambda: "viewer@example.com"
    app.dependency_overrides[get_preview_sql_executor] = lambda: obo_sql_mock
    app.dependency_overrides[get_obo_ws] = lambda: obo_ws_mock
    app.dependency_overrides[get_entitlement_service] = lambda: entitlement_mock
    return TestClient(app)


def provision(settings_store: dict[str, str]) -> None:
    settings_store[SETTING_SPACE_ID] = SPACE
    settings_store[SETTING_CONFIG_HASH] = "some-hash"
    settings_store[SETTING_STATUS] = "ready"


# ---------------------------------------------------------------------------
# Availability contract — no space provisioned
# ---------------------------------------------------------------------------


def test_ask_unavailable_without_space(client: TestClient, sp_ws_mock: MagicMock) -> None:
    resp = client.post("/api/v1/genie/ask", json={"question": "What is my score?"})
    assert resp.status_code == 200
    assert resp.json()["available"] is False
    sp_ws_mock.api_client.do.assert_not_called()


def test_start_unavailable_without_space(client: TestClient) -> None:
    resp = client.post("/api/v1/genie/start", json={"question": "q"})
    assert resp.status_code == 200
    assert resp.json()["available"] is False


def test_poll_unavailable_without_space(client: TestClient) -> None:
    resp = client.post("/api/v1/genie/poll", json={"conversation_id": "c1", "message_id": "m1"})
    assert resp.status_code == 200
    assert resp.json()["available"] is False


def test_space_unavailable_without_space(client: TestClient) -> None:
    resp = client.get("/api/v1/genie/space")
    assert resp.status_code == 200
    body = resp.json()
    assert body["available"] is False
    assert body["space_id"] is None
    assert body["space_url"] is None
    # Sample questions are static config — served even before provisioning.
    assert body["sample_questions"] == list(SAMPLE_QUESTIONS)


# ---------------------------------------------------------------------------
# Happy paths — driven end-to-end through the mocked OBO REST surface
# ---------------------------------------------------------------------------


def test_start_proxies_to_genie_as_the_caller(
    client: TestClient, settings_store: dict[str, str], obo_ws_mock: MagicMock, sp_ws_mock: MagicMock
) -> None:
    provision(settings_store)
    obo_ws_mock.api_client.do.return_value = {
        "conversation_id": "c1",
        "message_id": "m1",
        "status": "SUBMITTED",
    }
    resp = client.post("/api/v1/genie/start", json={"question": "What is my score?"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["available"] is True
    assert body["conversation_id"] == "c1"
    assert body["message_id"] == "m1"
    assert body["stage"] == "Understanding your question"
    obo_ws_mock.api_client.do.assert_called_once_with(
        "POST", f"{BASE}/start-conversation", body={"content": "What is my score?"}
    )
    # OBO identity: the SP client is only the fallback and stays untouched.
    sp_ws_mock.api_client.do.assert_not_called()


def test_start_continues_existing_conversation(
    client: TestClient, settings_store: dict[str, str], obo_ws_mock: MagicMock
) -> None:
    provision(settings_store)
    obo_ws_mock.api_client.do.return_value = {"conversation_id": "c1", "message_id": "m2"}
    resp = client.post("/api/v1/genie/start", json={"question": "and now?", "conversation_id": "c1"})
    assert resp.status_code == 200
    obo_ws_mock.api_client.do.assert_called_once_with(
        "POST", f"{BASE}/conversations/c1/messages", body={"content": "and now?"}
    )


def test_start_falls_back_to_sp_when_obo_is_rejected(
    client: TestClient, settings_store: dict[str, str], obo_ws_mock: MagicMock, sp_ws_mock: MagicMock
) -> None:
    provision(settings_store)
    obo_ws_mock.api_client.do.side_effect = PermissionDenied("Token missing required scopes")
    sp_ws_mock.api_client.do.return_value = {"conversation_id": "c1", "message_id": "m1"}
    resp = client.post("/api/v1/genie/start", json={"question": "q"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["available"] is True
    assert body["error"] is None
    assert body["conversation_id"] == "c1"
    sp_ws_mock.api_client.do.assert_called_once_with("POST", f"{BASE}/start-conversation", body={"content": "q"})


def test_poll_returns_partial_answer_payload(
    client: TestClient, settings_store: dict[str, str], obo_ws_mock: MagicMock
) -> None:
    provision(settings_store)
    message = {
        "status": "COMPLETED",
        "attachments": [
            {"text": {"content": "2 of 100 failed."}},
            {"attachment_id": "att-1", "query": {"query": "SELECT 1", "description": "d"}},
        ],
    }
    query_result = {
        "statement_response": {
            "manifest": {"schema": {"columns": [{"name": "failed_tests"}]}},
            "result": {"data_array": [["2"]]},
        }
    }
    obo_ws_mock.api_client.do.side_effect = [message, query_result]
    resp = client.post("/api/v1/genie/poll", json={"conversation_id": "c1", "message_id": "m1"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["available"] is True
    assert body["answer_text"] == "2 of 100 failed."
    assert body["sql"] == "SELECT 1"
    assert body["result_columns"] == ["failed_tests"]
    assert body["result_rows"] == [["2"]]
    assert body["stage"] == "Done"


def test_ask_blocking_flow(client: TestClient, settings_store: dict[str, str], obo_ws_mock: MagicMock) -> None:
    provision(settings_store)
    obo_ws_mock.api_client.do.side_effect = [
        {"conversation_id": "c1", "message_id": "m1", "status": "SUBMITTED"},
        {"status": "COMPLETED", "attachments": [{"text": {"content": "All good."}}]},
    ]
    resp = client.post("/api/v1/genie/ask", json={"question": "q"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["available"] is True
    assert body["answer_text"] == "All good."
    assert body["status"] == "COMPLETED"


def test_genie_error_is_a_clean_payload_not_a_500(
    client: TestClient, settings_store: dict[str, str], obo_ws_mock: MagicMock, sp_ws_mock: MagicMock
) -> None:
    provision(settings_store)
    obo_ws_mock.api_client.do.side_effect = RuntimeError("genie down")
    resp = client.post("/api/v1/genie/start", json={"question": "q"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["available"] is True
    assert body["error"] == "genie down"
    # Non-permission failures never silently switch identity.
    sp_ws_mock.api_client.do.assert_not_called()


# ---------------------------------------------------------------------------
# /space
# ---------------------------------------------------------------------------


def test_space_payload_when_provisioned(client: TestClient, settings_store: dict[str, str]) -> None:
    provision(settings_store)
    resp = client.get("/api/v1/genie/space")
    assert resp.status_code == 200
    body = resp.json()
    assert body["available"] is True
    assert body["space_id"] == SPACE
    assert body["status"] == "ready"
    assert body["space_url"] == f"https://ws.example.com/genie/rooms/{SPACE}"
    assert body["sample_questions"] == list(SAMPLE_QUESTIONS)


def test_space_status_defaults_to_ready_when_id_present_without_status(
    client: TestClient, settings_store: dict[str, str]
) -> None:
    settings_store[SETTING_SPACE_ID] = SPACE
    resp = client.get("/api/v1/genie/space")
    assert resp.json()["status"] == "ready"


def test_space_surfaces_provisioning_status(client: TestClient, settings_store: dict[str, str]) -> None:
    settings_store[SETTING_STATUS] = "provisioning"
    resp = client.get("/api/v1/genie/space")
    body = resp.json()
    assert body["available"] is False
    assert body["status"] == "provisioning"


# ---------------------------------------------------------------------------
# /feedback + input validation
# ---------------------------------------------------------------------------


def test_feedback_ok(client: TestClient) -> None:
    resp = client.post("/api/v1/genie/feedback", json={"message_id": "m1", "vote": "up"})
    assert resp.status_code == 200
    assert resp.json() == {"ok": True}


def test_feedback_rejects_unknown_vote(client: TestClient) -> None:
    resp = client.post("/api/v1/genie/feedback", json={"message_id": "m1", "vote": "sideways"})
    assert resp.status_code == 422


def test_feedback_rejects_log_injection_shaped_message_id(client: TestClient) -> None:
    resp = client.post("/api/v1/genie/feedback", json={"message_id": "m1\nFORGED", "vote": "up"})
    assert resp.status_code == 422


def test_poll_rejects_malformed_ids(client: TestClient) -> None:
    resp = client.post("/api/v1/genie/poll", json={"conversation_id": "c/../1", "message_id": "m1"})
    assert resp.status_code == 422


def test_ask_rejects_empty_question(client: TestClient) -> None:
    resp = client.post("/api/v1/genie/ask", json={"question": ""})
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# /verify-entitlements (P4.1)
# ---------------------------------------------------------------------------


def test_verify_entitlements_returns_per_fqn_outcomes(
    client: TestClient, entitlement_mock: MagicMock, obo_sql_mock: MagicMock, obo_ws_mock: MagicMock
) -> None:
    entitlement_mock.verify_and_record.return_value = {
        "main.sales.orders": "verified",
        "main.sales.secret": "denied",
        "main.sales.masked": "suppressed",
        "bad name": "error",
    }
    resp = client.post(
        "/api/v1/genie/verify-entitlements",
        json={"table_fqns": ["main.sales.orders", "main.sales.secret", "main.sales.masked", "bad name"]},
    )
    assert resp.status_code == 200
    assert resp.json() == {
        "results": {
            "main.sales.orders": "verified",
            "main.sales.secret": "denied",
            "main.sales.masked": "suppressed",
            "bad name": "error",
        }
    }
    # The service runs as the resolved caller with BOTH the caller's OBO
    # executor (SELECT probe) and OBO client (fine-grained-control check).
    entitlement_mock.verify_and_record.assert_awaited_once_with(
        obo_sql_mock,
        obo_ws_mock,
        "viewer@example.com",
        ["main.sales.orders", "main.sales.secret", "main.sales.masked", "bad name"],
    )


def test_verify_entitlements_caps_the_batch_at_50(client: TestClient, entitlement_mock: MagicMock) -> None:
    fqns = [f"main.sales.t{i}" for i in range(51)]
    resp = client.post("/api/v1/genie/verify-entitlements", json={"table_fqns": fqns})
    assert resp.status_code == 422
    entitlement_mock.verify_and_record.assert_not_awaited()


def test_verify_entitlements_rejects_an_empty_batch(client: TestClient) -> None:
    resp = client.post("/api/v1/genie/verify-entitlements", json={"table_fqns": []})
    assert resp.status_code == 422


def test_verify_entitlements_accepts_exactly_50(client: TestClient, entitlement_mock: MagicMock) -> None:
    fqns = [f"main.sales.t{i}" for i in range(50)]
    entitlement_mock.verify_and_record.return_value = dict.fromkeys(fqns, "verified")
    resp = client.post("/api/v1/genie/verify-entitlements", json={"table_fqns": fqns})
    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# RBAC — viewer+ on every endpoint
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("role", [UserRole.VIEWER, UserRole.RULE_AUTHOR, UserRole.RULE_APPROVER, UserRole.ADMIN])
def test_every_role_may_use_the_chat(
    role: UserRole, settings_mock: MagicMock, sp_ws_mock: MagicMock, obo_ws_mock: MagicMock
) -> None:
    from databricks_labs_dqx_app.backend.routes.v1.genie import router

    app = FastAPI()
    app.include_router(router, prefix="/api/v1/genie")
    app.dependency_overrides[get_app_settings_service] = lambda: settings_mock
    app.dependency_overrides[get_sp_ws] = lambda: sp_ws_mock
    app.dependency_overrides[get_obo_ws] = lambda: obo_ws_mock
    app.dependency_overrides[get_user_role] = lambda: role
    client = TestClient(app)
    assert client.get("/api/v1/genie/space").status_code == 200
    assert client.post("/api/v1/genie/ask", json={"question": "q"}).status_code == 200
