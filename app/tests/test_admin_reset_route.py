"""Route-wiring tests for ``POST /admin/reset-database`` (B2-29).

Driven through a real FastAPI ``TestClient`` over the admin router so the
``require_role(ADMIN)`` gate and the confirmation-token check are exercised
exactly as they run in production.

The reset now runs on a background daemon thread and the endpoint returns
immediately with ``running`` (the long reset can otherwise outlive the
Databricks Apps gateway idle timeout and strand the UI spinner). The
``_launch_reset`` module-level seam is monkeypatched to run the target inline so
the terminal-status behaviour is asserted deterministically. The reset service
is backed by in-memory fake executors, so the exact set of cleared tables is
asserted end-to-end.
"""

from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from databricks_labs_dqx_app.backend import dependencies as deps
from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.migrations import ALL_APP_TABLE_NAMES
from databricks_labs_dqx_app.backend.routes.v1 import admin as admin_module
from databricks_labs_dqx_app.backend.routes.v1.admin import router as admin_router
from databricks_labs_dqx_app.backend.services.database_reset_service import (
    RESET_CONFIRMATION_PHRASE,
    DatabaseResetService,
)

_ADMIN = "admin@x.com"


class _FakeExecutor:
    def __init__(self) -> None:
        self.executed: list[str] = []
        self.upserted_keys: list[str] = []

    def fqn(self, table: str) -> str:
        return f"cat.sch.{table}"

    def execute(self, sql: str, *, timeout_seconds: int = 120) -> None:
        self.executed.append(sql)

    def query(self, sql: str, *, timeout_seconds: int = 120) -> list[list[str]]:
        # Settings read absent right after the clear, so the reset's default
        # re-provisioning (seed-if-absent) writes the fresh-install seeds.
        return []

    def upsert(self, table: str, key_cols: dict, value_cols: dict, **_: object) -> None:
        self.upserted_keys.append(str(key_cols.get("setting_key")))


def _build_client(
    *,
    role: UserRole,
    service: DatabaseResetService | MagicMock | None = None,
    reset_status_store: MagicMock | None = None,
    demo_status_store: MagicMock | None = None,
) -> TestClient:
    app = FastAPI()
    app.include_router(admin_router, prefix="/admin")

    obo_ws = MagicMock()
    obo_ws.current_user.me.return_value.user_name = _ADMIN

    app.dependency_overrides[deps.get_user_role] = lambda: role
    app.dependency_overrides[deps.get_obo_ws] = lambda: obo_ws
    if service is not None:
        app.dependency_overrides[deps.get_database_reset_service] = lambda: service
    # Both status stores are always overridden so the route never reaches for a
    # real SP executor. Default (when not supplied): nothing running, so the
    # reset is free to launch.
    if reset_status_store is None:
        reset_status_store = MagicMock()
        reset_status_store.is_running.return_value = False
    if demo_status_store is None:
        demo_status_store = MagicMock()
        demo_status_store.is_running.return_value = False
    app.dependency_overrides[deps.get_reset_status_store] = lambda: reset_status_store
    app.dependency_overrides[deps.get_demo_status_store] = lambda: demo_status_store
    return TestClient(app, raise_server_exceptions=True)


class TestAdminGate:
    @pytest.mark.parametrize(
        "role",
        [UserRole.VIEWER, UserRole.RULE_AUTHOR, UserRole.RULE_APPROVER],
    )
    def test_non_admin_is_rejected_403_and_service_not_called(self, role):
        # A spy service proves the reset never runs for a non-admin even
        # when the correct confirmation phrase is supplied.
        spy = MagicMock(spec=DatabaseResetService)
        client = _build_client(role=role, service=spy)

        resp = client.post(
            "/admin/reset-database",
            json={"confirmation_phrase": RESET_CONFIRMATION_PHRASE},
        )

        assert resp.status_code == 403
        spy.reset_all_data.assert_not_called()


class TestConfirmationToken:
    def test_wrong_phrase_is_rejected_400_and_service_not_called(self):
        spy = MagicMock(spec=DatabaseResetService)
        client = _build_client(role=UserRole.ADMIN, service=spy)

        resp = client.post(
            "/admin/reset-database",
            json={"confirmation_phrase": "please reset"},
        )

        assert resp.status_code == 400
        spy.reset_all_data.assert_not_called()

    def test_missing_phrase_is_rejected_and_service_not_called(self):
        spy = MagicMock(spec=DatabaseResetService)
        client = _build_client(role=UserRole.ADMIN, service=spy)

        resp = client.post("/admin/reset-database", json={})

        # Missing/empty token never reaches the service.
        assert resp.status_code in (400, 422)
        spy.reset_all_data.assert_not_called()


class TestMutualExclusion:
    def test_reset_conflicts_when_reset_already_running(self):
        spy = MagicMock(spec=DatabaseResetService)
        reset_store = MagicMock()
        reset_store.is_running.return_value = True
        client = _build_client(role=UserRole.ADMIN, service=spy, reset_status_store=reset_store)

        resp = client.post(
            "/admin/reset-database",
            json={"confirmation_phrase": RESET_CONFIRMATION_PHRASE},
        )

        assert resp.status_code == 409
        spy.reset_all_data.assert_not_called()

    def test_reset_conflicts_when_demo_deploy_running(self):
        spy = MagicMock(spec=DatabaseResetService)
        demo_store = MagicMock()
        demo_store.is_running.return_value = True
        client = _build_client(role=UserRole.ADMIN, service=spy, demo_status_store=demo_store)

        resp = client.post(
            "/admin/reset-database",
            json={"confirmation_phrase": RESET_CONFIRMATION_PHRASE},
        )

        assert resp.status_code == 409
        spy.reset_all_data.assert_not_called()


class TestAsyncLaunch:
    def test_admin_launch_returns_running_immediately_and_records_succeeded(self, monkeypatch):
        # Run the launched target inline so the terminal status is deterministic.
        monkeypatch.setattr(admin_module, "_launch_reset", lambda target: target())
        delta = _FakeExecutor()
        oltp = _FakeExecutor()
        service = DatabaseResetService(delta_sql=delta, oltp_sql=oltp)
        reset_store = MagicMock()
        reset_store.is_running.return_value = False
        client = _build_client(role=UserRole.ADMIN, service=service, reset_status_store=reset_store)

        resp = client.post(
            "/admin/reset-database",
            json={"confirmation_phrase": RESET_CONFIRMATION_PHRASE},
        )

        # The endpoint returns immediately with the initial running state.
        assert resp.status_code == 200
        body = resp.json()
        assert body["state"] == "running"
        assert body["started_at"]

        # First a running status is persisted, then a terminal succeeded status
        # with the cleared/failed counts.
        states = [call.args[0].state for call in reset_store.set.call_args_list]
        assert states[0] == "running"
        assert states[-1] == "succeeded"
        terminal = reset_store.set.call_args_list[-1].args[0]
        assert terminal.cleared_count == len(ALL_APP_TABLE_NAMES)
        assert terminal.failed_count == 0

        # Fresh-install defaults were re-provisioned through the route so the
        # app lands on a clean first-install state, not an empty one (B2-113).
        assert "run_review_statuses_v1" in oltp.upserted_keys
        assert "label_definitions" in oltp.upserted_keys
        # Admin role mappings preserved through the route.
        role_stmts = [s for s in oltp.executed if "cat.sch.dq_role_mappings " in s]
        assert role_stmts == ["DELETE FROM cat.sch.dq_role_mappings WHERE role <> 'admin'"]

    def test_thread_records_failed_when_reset_raises(self, monkeypatch):
        monkeypatch.setattr(admin_module, "_launch_reset", lambda target: target())
        service = MagicMock(spec=DatabaseResetService)
        service.reset_all_data.side_effect = RuntimeError("boom\nsecond line")
        reset_store = MagicMock()
        reset_store.is_running.return_value = False
        client = _build_client(role=UserRole.ADMIN, service=service, reset_status_store=reset_store)

        resp = client.post(
            "/admin/reset-database",
            json={"confirmation_phrase": RESET_CONFIRMATION_PHRASE},
        )

        # A failure in the thread is recorded, never lost: the endpoint still
        # returned running, and the terminal status is 'failed' with a
        # newline-stripped message (CWE-117).
        assert resp.status_code == 200
        assert resp.json()["state"] == "running"
        terminal = reset_store.set.call_args_list[-1].args[0]
        assert terminal.state == "failed"
        assert "\n" not in terminal.message and "\r" not in terminal.message
        assert "boom" in terminal.message
