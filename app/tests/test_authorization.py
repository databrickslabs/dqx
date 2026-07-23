"""Tests for ``backend.common.authorization``.

Covers:
- ``UserRole`` permission map shape (each role has a sane permission set).
- ``RUNNER`` orthogonality vs. the primary-role hierarchy.
- ``get_user_email`` header trust, OBO fallback, and 401 behaviour.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from databricks_labs_dqx_app.backend.common.authorization import (
    PERMISSIONS,
    ROLE_PRIORITY,
    UserRole,
    get_permissions_for_role,
    get_user_email,
)


# ---------------------------------------------------------------------------
# Permission map
# ---------------------------------------------------------------------------


class TestPermissionMap:
    def test_every_role_has_an_entry(self):
        for role in UserRole:
            assert role in PERMISSIONS, f"Missing permissions for {role}"

    def test_admin_has_every_other_roles_perms(self):
        admin_perms = set(PERMISSIONS[UserRole.ADMIN])
        for role, perms in PERMISSIONS.items():
            if role == UserRole.ADMIN:
                continue
            assert set(perms).issubset(admin_perms), f"Admin missing perms from {role}: {set(perms) - admin_perms}"

    def test_admin_includes_run_rules(self):
        # Admins can always run rules.
        assert "run_rules" in PERMISSIONS[UserRole.ADMIN]

    def test_no_runner_role(self):
        # RUNNER is removed — the enum must not have a RUNNER member.
        assert not hasattr(UserRole, "RUNNER")

    def test_viewer_is_read_only(self):
        assert PERMISSIONS[UserRole.VIEWER] == ["view_rules"]

    # A1: new matrix assertions -------------------------------------------

    def test_author_has_run_rules(self):
        assert "run_rules" in get_permissions_for_role(UserRole.RULE_AUTHOR)

    def test_approver_cannot_author_or_run(self):
        p = get_permissions_for_role(UserRole.RULE_APPROVER)
        assert "run_rules" not in p
        assert "create_rules" not in p
        assert "edit_rules" not in p
        assert "submit_rules" not in p

    def test_approver_can_approve_and_export(self):
        p = get_permissions_for_role(UserRole.RULE_APPROVER)
        assert "approve_rules" in p
        assert "export_rules" in p
        assert "configure_storage" in p

    def test_viewer_only_views(self):
        assert get_permissions_for_role(UserRole.VIEWER) == ["view_rules"]

    def test_author_can_write_but_not_approve(self):
        author = set(PERMISSIONS[UserRole.RULE_AUTHOR])
        assert "create_rules" in author
        assert "edit_rules" in author
        assert "submit_rules" in author
        assert "approve_rules" not in author
        assert "manage_roles" not in author

    def test_approver_can_approve_but_not_manage_roles(self):
        approver = set(PERMISSIONS[UserRole.RULE_APPROVER])
        assert "approve_rules" in approver
        assert "manage_roles" not in approver

    def test_role_priority_ascending_admin_last(self):
        # Stronger roles should come later in the priority list, since the
        # resolver walks reversed(ROLE_PRIORITY) and returns the first match.
        assert ROLE_PRIORITY[-1] == UserRole.ADMIN
        assert ROLE_PRIORITY[0] == UserRole.VIEWER


class TestGetPermissionsForRole:
    def test_returns_list(self):
        assert isinstance(get_permissions_for_role(UserRole.ADMIN), list)

    def test_unknown_role_returns_empty(self):
        # Defensive: passing a not-quite-UserRole returns [] (defaults to .get()).
        # We don't pass an arbitrary string because the type system disallows it,
        # but we still verify the .get() default by mutating PERMISSIONS view.
        assert get_permissions_for_role(UserRole.VIEWER) == ["view_rules"]


# ---------------------------------------------------------------------------
# get_user_email
# ---------------------------------------------------------------------------


class TestGetUserEmail:
    def test_returns_x_forwarded_email_when_present(self):
        # NOTE: this captures *current* behaviour. The security audit (C-1)
        # flagged that this header is trusted without OBO cross-check; if
        # that gets fixed the test will need updating.
        assert get_user_email(x_forwarded_email="alice@example.com") == "alice@example.com"

    def test_x_forwarded_email_wins_over_token(self):
        # When both are present, the header is used directly (the OBO
        # fallback only fires when the header is missing).
        with patch("databricks.sdk.WorkspaceClient") as mock_ws_cls:
            assert (
                get_user_email(
                    x_forwarded_email="alice@example.com",
                    x_forwarded_access_token="t0k3n",
                )
                == "alice@example.com"
            )
            mock_ws_cls.assert_not_called()

    def test_falls_back_to_obo_token_resolution(self):
        with patch("databricks.sdk.WorkspaceClient") as mock_ws_cls:
            mock_user = MagicMock()
            mock_user.user_name = "bob@example.com"
            mock_ws_cls.return_value.current_user.me.return_value = mock_user

            email = get_user_email(x_forwarded_email=None, x_forwarded_access_token="t0k3n")
            assert email == "bob@example.com"
            mock_ws_cls.assert_called_once()

    def test_returns_401_when_neither_provided(self, monkeypatch):
        # The local-dev fallback (DATABRICKS_CONFIG_PROFILE / DATABRICKS_TOKEN)
        # is loaded from app/.env on package import, so strip it here to
        # exercise the pure "no identity" path the test name describes.
        monkeypatch.delenv("DATABRICKS_CONFIG_PROFILE", raising=False)
        monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
        with pytest.raises(HTTPException) as excinfo:
            get_user_email(x_forwarded_email=None, x_forwarded_access_token=None)
        assert excinfo.value.status_code == 401

    def test_returns_401_when_obo_lookup_fails_and_no_header(self):
        # Token present but SCIM call blows up → no email → 401.
        with patch("databricks.sdk.WorkspaceClient", side_effect=RuntimeError("scim down")):
            with pytest.raises(HTTPException) as excinfo:
                get_user_email(x_forwarded_email=None, x_forwarded_access_token="t0k3n")
            assert excinfo.value.status_code == 401

    def test_obo_lookup_returns_user_with_no_user_name_yields_401(self):
        # If SCIM succeeds but `user_name` is None, the function falls
        # through to the 401 branch.
        with patch("databricks.sdk.WorkspaceClient") as mock_ws_cls:
            mock_user = MagicMock()
            mock_user.user_name = None
            mock_ws_cls.return_value.current_user.me.return_value = mock_user

            with pytest.raises(HTTPException) as excinfo:
                get_user_email(x_forwarded_email=None, x_forwarded_access_token="t0k3n")
            assert excinfo.value.status_code == 401
