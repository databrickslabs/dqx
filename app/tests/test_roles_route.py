"""Unit tests for the privileged-principals endpoint in ``backend.routes.v1.roles``.

Follows the route-test convention (minimal FastAPI app + dependency overrides):
covers the happy path (workspace admins from SCIM + app CAN_MANAGE holders),
CAN_USE principals being excluded, graceful degradation when app-permissions
lookup fails, and the absent-admins-group edge case.
"""

from __future__ import annotations

from unittest.mock import MagicMock, create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from fastapi import FastAPI
from fastapi.testclient import TestClient

from databricks.sdk.service.apps import (
    AppAccessControlResponse,
    AppPermission,
    AppPermissionLevel,
    AppPermissions,
)
from databricks.sdk.service.iam import ComplexValue, Group

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.dependencies import (
    get_sp_ws,
    get_user_role,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_sp_ws(
    admins_members: list[tuple[str, str]] | None = None,
    acl_entries: list[AppAccessControlResponse] | None = None,
    app_permissions_error: Exception | None = None,
) -> MagicMock:
    """Build a spec-bound mock WorkspaceClient for the privileged-principals endpoint.

    Using ``create_autospec(WorkspaceClient)`` so misuse (wrong method name,
    wrong positional args) fails loudly at test time rather than silently
    passing on an unchecked MagicMock attribute.

    Args:
        admins_members: list of (display, value) tuples for the admins group members.
        acl_entries: ACL entries for ``apps.get_permissions``.
        app_permissions_error: If set, ``apps.get_permissions`` raises this exception.
    """
    ws = create_autospec(WorkspaceClient, instance=True)

    # SCIM groups.list — return "admins" group when queried
    if admins_members is not None:
        members = [ComplexValue(display=display, value=value) for display, value in admins_members]
        admins_group = Group(display_name="admins", members=members)
        ws.groups.list.return_value = iter([admins_group])
    else:
        # No admins group found
        ws.groups.list.return_value = iter([])

    # apps.get_permissions
    if app_permissions_error is not None:
        ws.apps.get_permissions.side_effect = app_permissions_error
    elif acl_entries is not None:
        ws.apps.get_permissions.return_value = AppPermissions(access_control_list=acl_entries)
    else:
        ws.apps.get_permissions.return_value = AppPermissions(access_control_list=[])

    return ws


def _build_client(sp_ws: MagicMock) -> TestClient:
    from databricks_labs_dqx_app.backend.routes.v1.roles import router

    app = FastAPI()
    app.include_router(router, prefix="/api/v1/roles")
    app.dependency_overrides[get_sp_ws] = lambda: sp_ws
    app.dependency_overrides[get_user_role] = lambda: UserRole.ADMIN
    return TestClient(app, raise_server_exceptions=True)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_list_privileged_principals_returns_workspace_admins_and_app_owners() -> None:
    """Workspace admins from SCIM + CAN_MANAGE app principals are all returned;
    CAN_USE principals are excluded."""
    can_use_entry = AppAccessControlResponse(
        user_name="user.d@x",
        all_permissions=[AppPermission(permission_level=AppPermissionLevel.CAN_USE)],
    )
    can_manage_entry = AppAccessControlResponse(
        user_name="owner.c@x",
        all_permissions=[AppPermission(permission_level=AppPermissionLevel.CAN_MANAGE)],
    )

    sp_ws = _make_sp_ws(
        admins_members=[("admin.a@x", "1"), ("admin.b@x", "2")],
        acl_entries=[can_use_entry, can_manage_entry],
    )

    client = _build_client(sp_ws)
    resp = client.get("/api/v1/roles/privileged-principals")
    assert resp.status_code == 200

    result = {(item["principal"], item["kind"]) for item in resp.json()}
    assert result == {
        ("admin.a@x", "workspace_admin"),
        ("admin.b@x", "workspace_admin"),
        ("owner.c@x", "app_owner"),
    }
    # CAN_USE principal must be absent
    assert not any(item["principal"] == "user.d@x" for item in resp.json())


def test_list_privileged_principals_app_owner_prefers_user_name() -> None:
    """Principal display string prefers user_name over group_name/display_name."""
    entry = AppAccessControlResponse(
        user_name="alice@example.com",
        group_name="some-group",
        display_name="Alice",
        all_permissions=[AppPermission(permission_level=AppPermissionLevel.CAN_MANAGE)],
    )
    sp_ws = _make_sp_ws(admins_members=[], acl_entries=[entry])
    client = _build_client(sp_ws)
    resp = client.get("/api/v1/roles/privileged-principals")
    assert resp.status_code == 200
    assert any(item["principal"] == "alice@example.com" for item in resp.json())


def test_list_privileged_principals_app_owner_falls_back_to_group_name() -> None:
    """Falls back to group_name when user_name is absent."""
    entry = AppAccessControlResponse(
        user_name=None,
        group_name="data-stewards",
        display_name="Data Stewards",
        all_permissions=[AppPermission(permission_level=AppPermissionLevel.CAN_MANAGE)],
    )
    sp_ws = _make_sp_ws(admins_members=[], acl_entries=[entry])
    client = _build_client(sp_ws)
    resp = client.get("/api/v1/roles/privileged-principals")
    assert resp.status_code == 200
    assert any(item["principal"] == "data-stewards" for item in resp.json())


def test_list_privileged_principals_absent_admins_group_returns_empty_admins() -> None:
    """When the SCIM admins group is absent, workspace_admin list is empty."""
    entry = AppAccessControlResponse(
        user_name="owner@example.com",
        all_permissions=[AppPermission(permission_level=AppPermissionLevel.CAN_MANAGE)],
    )
    sp_ws = _make_sp_ws(admins_members=None, acl_entries=[entry])
    client = _build_client(sp_ws)
    resp = client.get("/api/v1/roles/privileged-principals")
    assert resp.status_code == 200
    principals = resp.json()
    assert not any(item["kind"] == "workspace_admin" for item in principals)
    assert any(item["principal"] == "owner@example.com" for item in principals)


# ---------------------------------------------------------------------------
# Graceful degradation
# ---------------------------------------------------------------------------


def test_list_privileged_principals_degrades_gracefully_when_app_permissions_fails() -> None:
    """When apps.get_permissions raises, the endpoint still returns admins (no 500)."""
    sp_ws = _make_sp_ws(
        admins_members=[("admin@example.com", "1")],
        app_permissions_error=RuntimeError("Simulated SDK error"),
    )
    client = _build_client(sp_ws)
    resp = client.get("/api/v1/roles/privileged-principals")
    # Must succeed — not a 500
    assert resp.status_code == 200
    principals = resp.json()
    assert any(item["principal"] == "admin@example.com" and item["kind"] == "workspace_admin" for item in principals)
    # No app_owner entries because permissions lookup failed
    assert not any(item["kind"] == "app_owner" for item in principals)
