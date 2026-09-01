from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from databricks.labs.dqx.engine import DQEngine

from databricks_labs_dqx_app.backend import dependencies as deps
from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.marketplace import loader
from databricks_labs_dqx_app.backend.routes.v1.marketplace import router as marketplace_router


class _FakeRegistry:
    """Stub RegistryService returning rules whose reserved ``name`` metadata
    matches *existing_names*, so the route can flag those pack rules imported."""

    def __init__(self, existing_names: set[str]) -> None:
        self._names = existing_names

    def list_rules(self, **_kwargs):  # noqa: ANN003, ANN201 — mirrors the real signature loosely
        return [SimpleNamespace(user_metadata={"name": n}) for n in self._names]


def _make_app(role: UserRole, existing_names: set[str] | None = None) -> FastAPI:
    loader.clear_cache()
    app = FastAPI()
    app.include_router(marketplace_router, prefix="/api/v1/marketplace")
    app.dependency_overrides[deps.get_user_role] = lambda: role
    app.dependency_overrides[deps.get_check_validator] = lambda: DQEngine.validate_checks
    app.dependency_overrides[deps.get_registry_service] = lambda: _FakeRegistry(existing_names or set())
    return app


def test_admin_gets_catalogue():
    client = TestClient(_make_app(UserRole.ADMIN))
    resp = client.get("/api/v1/marketplace/packs")
    assert resp.status_code == 200
    data = resp.json()
    assert data["packs"], "expected packs"
    rule = data["packs"][0]["rules"][0]
    assert {"rule_key", "name", "dimension", "severity", "check", "industries", "regions", "imported"} <= set(rule)
    assert rule["imported"] is False


def test_existing_rule_name_is_flagged_imported():
    # Pre-load once to learn a real pack-rule name, then assert the route flags
    # exactly that rule (and not its neighbours) as imported.
    client0 = TestClient(_make_app(UserRole.ADMIN))
    first_rule = client0.get("/api/v1/marketplace/packs").json()["packs"][0]["rules"][0]
    target = first_rule["name"]

    client = TestClient(_make_app(UserRole.ADMIN, existing_names={target}))
    packs = client.get("/api/v1/marketplace/packs").json()["packs"]
    flagged = [r["name"] for p in packs for r in p["rules"] if r["imported"]]
    assert flagged == [target]


def test_non_admin_rejected():
    client = TestClient(_make_app(UserRole.RULE_AUTHOR))
    resp = client.get("/api/v1/marketplace/packs")
    assert resp.status_code == 403
