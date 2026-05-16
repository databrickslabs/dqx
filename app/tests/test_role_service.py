"""Tests for ``RoleService`` — primary-role resolution + RUNNER orthogonality.

The service hits Delta tables for mappings, but ``resolve_role`` and
``has_runner_role`` accept a pre-cached mapping list. We swap the
mappings in via ``_mappings_cache`` so no SQL is touched.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

import pytest

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.services.role_service import RoleMapping, RoleService


@pytest.fixture
def role_service(sql_executor_mock):
    svc = RoleService(sql=sql_executor_mock)
    return svc


def _seed_mappings(svc: RoleService, mappings: list[tuple[str, str]]) -> None:
    """Bypass DB by writing directly to the cache."""
    svc._mappings_cache = [
        RoleMapping(
            role=role,
            group_name=group,
            created_by="seed",
            created_at=datetime.now(timezone.utc),
            updated_by="seed",
            updated_at=datetime.now(timezone.utc),
        )
        for role, group in mappings
    ]
    svc._mappings_cache_expires = time.monotonic() + 3600


# ---------------------------------------------------------------------------
# resolve_role — primary role hierarchy
# ---------------------------------------------------------------------------


class TestResolveRolePrimary:
    def test_bootstrap_admin_group_short_circuits_to_admin(self, role_service):
        # Even with no mappings configured, the bootstrap admin group wins.
        assert role_service.resolve_role(["admins", "data-eng"], admin_group="admins") == UserRole.ADMIN

    def test_no_mappings_defaults_to_viewer(self, role_service):
        _seed_mappings(role_service, [])
        assert role_service.resolve_role(["data-eng"], admin_group="other-admins") == UserRole.VIEWER

    def test_user_with_no_matching_groups_is_viewer(self, role_service):
        _seed_mappings(role_service, [(UserRole.RULE_AUTHOR.value, "writers")])
        assert role_service.resolve_role(["unrelated-team"]) == UserRole.VIEWER

    def test_author_mapping_resolves_to_author(self, role_service):
        _seed_mappings(role_service, [(UserRole.RULE_AUTHOR.value, "writers")])
        assert role_service.resolve_role(["writers"]) == UserRole.RULE_AUTHOR

    def test_approver_mapping_resolves_to_approver(self, role_service):
        _seed_mappings(role_service, [(UserRole.RULE_APPROVER.value, "approvers")])
        assert role_service.resolve_role(["approvers"]) == UserRole.RULE_APPROVER

    def test_priority_picks_highest_when_multiple_match(self, role_service):
        # User has both author AND approver groups → approver wins.
        _seed_mappings(
            role_service,
            [
                (UserRole.RULE_AUTHOR.value, "writers"),
                (UserRole.RULE_APPROVER.value, "approvers"),
            ],
        )
        assert role_service.resolve_role(["writers", "approvers"]) == UserRole.RULE_APPROVER

    def test_admin_mapping_beats_approver(self, role_service):
        _seed_mappings(
            role_service,
            [
                (UserRole.ADMIN.value, "platform-admins"),
                (UserRole.RULE_APPROVER.value, "approvers"),
            ],
        )
        assert role_service.resolve_role(["platform-admins", "approvers"]) == UserRole.ADMIN


# ---------------------------------------------------------------------------
# resolve_role — RUNNER must NOT show up as a primary role
# ---------------------------------------------------------------------------


class TestResolveRoleRunnerOrthogonality:
    def test_runner_only_user_resolves_to_viewer(self, role_service):
        # The orthogonality contract: a group mapped only to RUNNER must not
        # promote the primary role above VIEWER.
        _seed_mappings(role_service, [(UserRole.RUNNER.value, "runners")])
        assert role_service.resolve_role(["runners"]) == UserRole.VIEWER

    def test_runner_plus_author_resolves_to_author_not_higher(self, role_service):
        _seed_mappings(
            role_service,
            [
                (UserRole.RUNNER.value, "runners"),
                (UserRole.RULE_AUTHOR.value, "writers"),
            ],
        )
        assert role_service.resolve_role(["runners", "writers"]) == UserRole.RULE_AUTHOR


# ---------------------------------------------------------------------------
# has_runner_role
# ---------------------------------------------------------------------------


class TestHasRunnerRole:
    def test_admin_is_implicit_runner(self, role_service):
        # Member of the bootstrap admin group is always a runner, even with
        # no explicit RUNNER mapping configured.
        _seed_mappings(role_service, [])
        assert role_service.has_runner_role(["admins"], admin_group="admins") is True

    def test_explicit_runner_group_membership(self, role_service):
        _seed_mappings(role_service, [(UserRole.RUNNER.value, "runners")])
        assert role_service.has_runner_role(["runners"], admin_group="other-admins") is True

    def test_no_runner_mapping_no_admin_returns_false(self, role_service):
        _seed_mappings(role_service, [(UserRole.RULE_AUTHOR.value, "writers")])
        assert role_service.has_runner_role(["writers"], admin_group="other-admins") is False

    def test_author_role_does_not_imply_runner(self, role_service):
        # The whole point of orthogonality: RULE_AUTHOR alone does NOT
        # confer runner privilege.
        _seed_mappings(role_service, [(UserRole.RULE_AUTHOR.value, "writers")])
        assert role_service.has_runner_role(["writers"], admin_group="other-admins") is False

    def test_approver_role_does_not_imply_runner(self, role_service):
        _seed_mappings(role_service, [(UserRole.RULE_APPROVER.value, "approvers")])
        assert role_service.has_runner_role(["approvers"], admin_group="other-admins") is False

    def test_no_mappings_only_admin_check(self, role_service):
        _seed_mappings(role_service, [])
        # Admin-group-via-bootstrap path still works when no DB mappings exist.
        assert role_service.has_runner_role(["admins"], admin_group="admins") is True
        # And non-admins are not runners with empty mappings.
        assert role_service.has_runner_role(["data-eng"], admin_group="admins") is False


# ---------------------------------------------------------------------------
# Cache invalidation
# ---------------------------------------------------------------------------


class TestMappingsCache:
    def test_invalidate_forces_next_lookup(self, role_service, sql_executor_mock):
        _seed_mappings(role_service, [(UserRole.RULE_AUTHOR.value, "writers")])
        # First call uses cache — no SQL.
        assert role_service.list_mappings(use_cache=True)
        sql_executor_mock.query.assert_not_called()

        role_service.invalidate_mappings_cache()
        # Now the cache is gone — next call will hit SQL.
        sql_executor_mock.query.return_value = []
        role_service.list_mappings(use_cache=True)
        sql_executor_mock.query.assert_called_once()
