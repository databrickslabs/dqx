"""Route-wiring tests for the ADMIN-only demo-content endpoints.

Driven through a real FastAPI ``TestClient`` over the admin router so the
router-level ``require_role(ADMIN)`` gate is exercised exactly as it runs in
production. The seed service and status store are mocked via
``app.dependency_overrides`` so no real workspace or warehouse is touched, and
the daemon-thread launch is made synchronous in-test by monkeypatching the
module-level ``_launch_seed`` indirection to run the target inline.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from databricks_labs_dqx_app.backend import dependencies as deps
from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.demo.status import DemoStatus
from databricks_labs_dqx_app.backend.routes.v1 import admin as admin_module
from databricks_labs_dqx_app.backend.routes.v1.admin import router as admin_router

_ADMIN = "admin@x.com"


@pytest.fixture
def demo_seed_service_mock() -> MagicMock:
    return MagicMock()


@pytest.fixture
def demo_status_store_mock() -> MagicMock:
    return MagicMock()


def _build_client(
    *,
    role: UserRole,
    seeder: MagicMock | None = None,
    status_store: MagicMock | None = None,
) -> TestClient:
    app = FastAPI()
    app.include_router(admin_router, prefix="/admin")

    obo_ws = MagicMock()
    obo_ws.current_user.me.return_value.user_name = _ADMIN

    app.dependency_overrides[deps.get_user_role] = lambda: role
    app.dependency_overrides[deps.get_obo_ws] = lambda: obo_ws
    if seeder is not None:
        app.dependency_overrides[deps.get_demo_seed_service] = lambda: seeder
    if status_store is not None:
        app.dependency_overrides[deps.get_demo_status_store] = lambda: status_store
    return TestClient(app, raise_server_exceptions=True)


def test_deploy_requires_admin(demo_seed_service_mock, demo_status_store_mock):
    demo_status_store_mock.is_running.return_value = False
    client = _build_client(
        role=UserRole.VIEWER,
        seeder=demo_seed_service_mock,
        status_store=demo_status_store_mock,
    )

    resp = client.post("/admin/demo/deploy", json={"wipe_first": False})

    assert resp.status_code == 403
    demo_seed_service_mock.run.assert_not_called()


def test_deploy_launches_and_returns_running(
    monkeypatch, demo_seed_service_mock, demo_status_store_mock
):
    demo_status_store_mock.is_running.return_value = False
    # Run the launched target inline so the assertion is deterministic — no
    # real thread, no sleep.
    monkeypatch.setattr(admin_module, "_launch_seed", lambda target: target())
    client = _build_client(
        role=UserRole.ADMIN,
        seeder=demo_seed_service_mock,
        status_store=demo_status_store_mock,
    )

    resp = client.post("/admin/demo/deploy", json={"wipe_first": True})

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "running"
    assert body["started_at"]
    # An initial 'running' status is persisted before the launch.
    assert demo_status_store_mock.set.call_count == 1
    persisted = demo_status_store_mock.set.call_args.args[0]
    assert persisted.state == "running"
    # The seed job is scheduled with the requested wipe_first flag.
    demo_seed_service_mock.run.assert_called_once_with(user_email=_ADMIN, wipe_first=True)


def test_deploy_conflict_when_already_running(
    demo_seed_service_mock, demo_status_store_mock
):
    demo_status_store_mock.is_running.return_value = True
    client = _build_client(
        role=UserRole.ADMIN,
        seeder=demo_seed_service_mock,
        status_store=demo_status_store_mock,
    )

    resp = client.post("/admin/demo/deploy", json={"wipe_first": False})

    assert resp.status_code == 409
    demo_seed_service_mock.run.assert_not_called()


def test_status_endpoint_returns_state(demo_status_store_mock):
    demo_status_store_mock.get.return_value = DemoStatus(
        "running", "weekly", "week 3/9", "2026-07-14 10:00:00", "2026-07-14 10:20:00"
    )
    client = _build_client(role=UserRole.ADMIN, status_store=demo_status_store_mock)

    resp = client.get("/admin/demo/status")

    assert resp.status_code == 200
    body = resp.json()
    assert body["state"] == "running"
    assert body["phase"] == "weekly"
    assert body["message"] == "week 3/9"
    assert body["started_at"] == "2026-07-14 10:00:00"
    assert body["updated_at"] == "2026-07-14 10:20:00"
