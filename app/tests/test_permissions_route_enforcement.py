"""Route-wiring enforcement pins for the object-permission gated mutations.

Unlike ``test_permissions_routes.py`` (which calls handlers directly with a
MagicMock ``PermissionsService``), these tests drive the gated entity routes
through a real FastAPI ``TestClient`` with the **real** ``PermissionsService``
resolved by the actual dependency graph (backed by an in-memory fake OLTP
executor). Only the auth leaves (role / principal ids / email / OBO client) and
the downstream entity services are overridden — the privilege check is genuine.

Each gated mutation route gets a 403 denial pin: a non-creator ``RULE_AUTHOR``
with no personal grant is rejected *through the route*. For the ``APPLY`` family
the object's users-group default is narrowed to ``SELECT`` (APPLY revoked), which
is what makes the ``APPLY`` check bite. One ``200``-after-grant case proves a
matching personal grant lets the same caller through.
"""

from __future__ import annotations

import re
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from databricks_labs_dqx_app.backend.common import authorization
from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.common.permissions import USERS_GROUP_PRINCIPAL_ID
from databricks_labs_dqx_app.backend import dependencies as deps
from databricks_labs_dqx_app.backend.routes.v1.data_products import router as data_products_router
from databricks_labs_dqx_app.backend.routes.v1.monitored_tables import router as monitored_tables_router
from databricks_labs_dqx_app.backend.routes.v1.registry_rules import router as registry_rules_router
from databricks_labs_dqx_app.backend.services.permissions_service import PermissionsService

_OWNER = "owner@x.com"
_AUTHOR = "author@x.com"


class _FakeOltp:
    """In-memory OLTP executor answering the SELECT shapes the service emits.

    ``created_by`` owner lookups resolve to :data:`_OWNER`; grants are keyed by
    (object_type, object_id) in ``list_grants`` column order; membership is
    empty. ``execute`` is a no-op (no mutation reaches the DB in these tests).
    """

    def __init__(self) -> None:
        self.grants: dict[tuple[str, str], list[list[object]]] = {}

    def fqn(self, table: str) -> str:
        return table

    def ts_text(self, col: str) -> str:
        return col

    def execute(self, sql: str, *, timeout_seconds: int = 120) -> None:
        return None

    def query(self, sql: str, *, timeout_seconds: int = 120) -> list[list[object]]:
        if "created_by" in sql:
            return [[_OWNER]]
        if "dq_data_product_members" in sql:
            return []
        ot = re.search(r"object_type = '([^']*)'", sql)
        oid = re.search(r"object_id = '([^']*)'", sql)
        key = (ot.group(1) if ot else "", oid.group(1) if oid else "")
        return list(self.grants.get(key, []))

    def add_grant(
        self,
        object_type: str,
        object_id: str,
        principal_id: str,
        privileges: str,
        *,
        principal_type: str = "user",
    ) -> None:
        self.grants.setdefault((object_type, object_id), []).append(
            [object_type, object_id, principal_id, principal_type, "n", privileges, "false", "g@x.com", None]
        )


def _build_client(fake: _FakeOltp, *, principal_ids: frozenset[str]) -> TestClient:
    """Build a TestClient over the gated routers with real permission wiring."""
    app = FastAPI()
    app.include_router(registry_rules_router, prefix="/registry-rules")
    app.include_router(monitored_tables_router, prefix="/monitored-tables")
    app.include_router(data_products_router, prefix="/data-products")

    app_settings = MagicMock()
    app_settings.get_permissions_default_inherit.return_value = False
    obo_ws = MagicMock()
    obo_ws.current_user.me.return_value.user_name = _AUTHOR

    # Real PermissionsService over the fake executor — this is the genuine gate.
    app.dependency_overrides[deps.get_sp_oltp_executor] = lambda: fake
    app.dependency_overrides[deps.get_app_settings_service] = lambda: app_settings
    # Auth leaves.
    app.dependency_overrides[deps.get_user_role] = lambda: UserRole.RULE_AUTHOR
    app.dependency_overrides[deps.get_current_principal_ids] = lambda: principal_ids
    app.dependency_overrides[authorization.get_user_email] = lambda: _AUTHOR
    app.dependency_overrides[deps.get_obo_ws] = lambda: obo_ws
    # Downstream entity services — mocked; never reached on a 403.
    app.dependency_overrides[deps.get_registry_service] = lambda: MagicMock()
    app.dependency_overrides[deps.get_apply_rules_service] = lambda: MagicMock()
    app.dependency_overrides[deps.get_monitored_table_service] = lambda: MagicMock()
    app.dependency_overrides[deps.get_data_product_service] = lambda: MagicMock()
    return TestClient(app, raise_server_exceptions=True)


# Each entry: HTTP method, path, JSON body, and the (object_type, object_id)
# whose users-group default must be narrowed for an APPLY route's check to bite.
# MODIFY routes are denied by the default (which never includes MODIFY); APPLY
# routes need the object's users-group default narrowed to SELECT.
_MODIFY_ROUTES = [
    ("put", "/registry-rules/r1", {}),  # updateRegistryRule
    ("delete", "/registry-rules/r1", None),  # deleteRegistryRule
    ("patch", "/monitored-tables/b1/schedule", {}),  # updateMonitoredTableSchedule
    ("delete", "/monitored-tables/b1", None),  # deleteMonitoredTable
    ("patch", "/data-products/p1", {}),  # updateDataProduct
    ("delete", "/data-products/p1", None),  # deleteDataProduct
]

_APPLY_ROUTES = [
    ("post", "/monitored-tables/b1/applied-rules", {"rule_id": "x", "column_mapping": []}, ("monitored_table", "b1")),  # applyRuleToTable
    ("post", "/data-products/p1/members", {"binding_id": "b1"}, ("data_product", "p1")),  # addDataProductMember
    ("delete", "/data-products/p1/members/m1", None, ("data_product", "p1")),  # removeDataProductMember
]


@pytest.mark.parametrize("method,path,body", _MODIFY_ROUTES)
def test_modify_routes_deny_non_owner_author_without_grant(method, path, body):
    fake = _FakeOltp()
    client = _build_client(fake, principal_ids=frozenset())
    resp = getattr(client, method)(path, json=body) if body is not None else getattr(client, method)(path)
    assert resp.status_code == 403


@pytest.mark.parametrize("method,path,body,obj", _APPLY_ROUTES)
def test_apply_routes_deny_when_users_group_default_narrowed(method, path, body, obj):
    fake = _FakeOltp()
    # Revoke APPLY on the users group (narrow the default to SELECT) so the
    # APPLY check bites for a non-owner author with no personal grant.
    fake.add_grant(obj[0], obj[1], USERS_GROUP_PRINCIPAL_ID, "SELECT", principal_type="group")
    client = _build_client(fake, principal_ids=frozenset())
    resp = getattr(client, method)(path, json=body) if body is not None else getattr(client, method)(path)
    assert resp.status_code == 403


def test_gated_route_allows_author_with_matching_grant():
    # A personal MODIFY grant on the rule lets the same non-owner author through.
    fake = _FakeOltp()
    fake.add_grant("registry_rule", "r1", "author-id", "MODIFY")
    client = _build_client(fake, principal_ids=frozenset({"author-id"}))
    # deleteRegistryRule: enforcement passes, then the (mocked) services return
    # a clean delete. Configure the two service calls the handler makes.
    apply_rules = MagicMock()
    apply_rules.count_applications_for_rule.return_value = 0
    client.app.dependency_overrides[deps.get_apply_rules_service] = lambda: apply_rules
    client.app.dependency_overrides[deps.get_registry_service] = lambda: MagicMock()
    resp = client.delete("/registry-rules/r1")
    assert resp.status_code == 200
    assert resp.json()["status"] == "deleted"
