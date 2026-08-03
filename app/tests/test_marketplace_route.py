from fastapi import FastAPI
from fastapi.testclient import TestClient

from databricks.labs.dqx.engine import DQEngine

from databricks_labs_dqx_app.backend import dependencies as deps
from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.marketplace import loader
from databricks_labs_dqx_app.backend.routes.v1.marketplace import router as marketplace_router


def _make_app(role: UserRole) -> FastAPI:
    loader.clear_cache()
    app = FastAPI()
    app.include_router(marketplace_router, prefix="/api/v1/marketplace")
    app.dependency_overrides[deps.get_user_role] = lambda: role
    app.dependency_overrides[deps.get_check_validator] = lambda: DQEngine.validate_checks
    return app


def test_admin_gets_catalogue():
    client = TestClient(_make_app(UserRole.ADMIN))
    resp = client.get("/api/v1/marketplace/packs")
    assert resp.status_code == 200
    data = resp.json()
    assert data["packs"], "expected packs"
    rule = data["packs"][0]["rules"][0]
    assert {"rule_key", "name", "dimension", "severity", "check", "industries", "regions"} <= set(rule)


def test_non_admin_rejected():
    client = TestClient(_make_app(UserRole.RULE_AUTHOR))
    resp = client.get("/api/v1/marketplace/packs")
    assert resp.status_code == 403
