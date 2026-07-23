"""Tests for ``RoleService`` — primary-role resolution.

The service hits Delta tables for mappings, but ``resolve_role``
accepts a pre-cached mapping list. We swap the
mappings in via ``_mappings_cache`` so no SQL is touched.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

import pytest

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.services.role_service import (
    RoleMapping,
    RoleMappingHistoryEntry,
    RoleService,
)


@pytest.fixture
def role_service(sql_executor_mock):
    # ``fqn`` and ``ts_text`` are passthroughs so the SQL strings the
    # service emits stay inspectable in tests. The Protocol contract is
    # that ``fqn(t)`` returns a fully-qualified identifier and
    # ``ts_text(c)`` returns a SELECT-projection-safe expression — both
    # are dialect-specific, but the tests only care about the literal
    # column / table name appearing in the right slot.
    sql_executor_mock.fqn.side_effect = lambda t: t
    sql_executor_mock.ts_text.side_effect = lambda c: c
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

    def test_user_level_entitlement_resolves(self, role_service):
        # USER-level entitlements store the user's own identity string in the
        # ``group_name`` column. The caller (get_user_role) folds the user's
        # identity into the list it passes, so a mapping keyed on the user's
        # email/username must resolve the same as a group mapping would.
        _seed_mappings(role_service, [(UserRole.RULE_APPROVER.value, "alice@example.com")])
        assert role_service.resolve_role(["alice@example.com"]) == UserRole.RULE_APPROVER

    def test_user_level_entitlement_beats_group_when_higher(self, role_service):
        # A user's direct ADMIN entitlement out-ranks a lower group mapping.
        _seed_mappings(
            role_service,
            [
                (UserRole.ADMIN.value, "alice@example.com"),
                (UserRole.RULE_AUTHOR.value, "writers"),
            ],
        )
        # Caller passes both group memberships and the user's own identity.
        assert role_service.resolve_role(["writers", "alice@example.com"]) == UserRole.ADMIN


# ---------------------------------------------------------------------------
# resolve_role — unknown/stale role strings in DB are silently skipped
# ---------------------------------------------------------------------------


class TestResolveRoleUnknownMapping:
    def test_unknown_role_string_is_silently_skipped(self, role_service):
        # A DB row with a stale role value (e.g. the old "runner" string) must
        # not crash resolve_role — it should be skipped via the ValueError path.
        _seed_mappings(role_service, [("runner", "runners")])
        assert role_service.resolve_role(["runners"]) == UserRole.VIEWER

    def test_unknown_role_alongside_known_resolves_to_known(self, role_service):
        _seed_mappings(
            role_service,
            [
                ("runner", "runners"),
                (UserRole.RULE_AUTHOR.value, "writers"),
            ],
        )
        assert role_service.resolve_role(["runners", "writers"]) == UserRole.RULE_AUTHOR


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


# ---------------------------------------------------------------------------
# create_mapping — dialect-agnostic upsert via the executor Protocol
# ---------------------------------------------------------------------------


class TestCreateMappingDelegatesToProtocol:
    """``create_mapping`` must NOT branch on ``self._sql.dialect``.

    The pre-refactor code hand-built two SQL strings (MERGE for Delta,
    INSERT ... ON CONFLICT for Postgres). The Protocol-based design
    delegates that choice to :meth:`OltpExecutorProtocol.upsert_with_audit`
    so adding a new backend (e.g. SQLite for local dev) only requires
    implementing the Protocol — not touching every service.

    These tests lock down the contract from the service side:

    1. ``create_mapping`` calls ``upsert_with_audit`` exactly once.
    2. It never calls ``execute`` directly with a hand-built SQL string
       (a regression would mean the dialect branch came back).
    3. The kwargs forwarded to the Protocol carry the right audit
       semantics: ``preserve_created=True`` and no ``increment_on_update``
       (role mappings don't have a version column).
    4. ``created_*`` and ``updated_*`` are passed as ``RawSql("now()")``
       so the executor renders the dialect-correct timestamp function.
    """

    def test_calls_upsert_with_audit_once_with_correct_audit_kwargs(self, role_service, sql_executor_mock):
        from databricks_labs_dqx_app.backend.sql_executor import RawSql

        role_service.create_mapping(
            role=UserRole.RULE_AUTHOR.value,
            group_name="writers",
            user_email="alice@example.com",
        )

        # The primary mutation MUST go through the Protocol's
        # upsert_with_audit. Going around it (e.g. by hand-building a
        # MERGE/INSERT string and calling .execute()) would mean the
        # dialect branch came back. The single .execute() call we DO
        # expect is the best-effort history-row INSERT, covered by
        # TestCreateMappingHistoryRecording below — assertions there
        # lock down its shape so it can't accidentally absorb the
        # primary write.
        sql_executor_mock.upsert_with_audit.assert_called_once()

        call = sql_executor_mock.upsert_with_audit.call_args
        # Audit semantics: preserve created_* on UPDATE; no version column.
        assert call.kwargs.get("preserve_created") is True
        assert call.kwargs.get("increment_on_update") is None

        # Keys are the natural identity of the row.
        assert call.kwargs["key_cols"] == {
            "role": UserRole.RULE_AUTHOR.value,
            "group_name": "writers",
        }

        # Audit cols carry the actor + dialect-portable "now" sentinel.
        value_cols = call.kwargs["value_cols"]
        assert value_cols["created_by"] == "alice@example.com"
        assert value_cols["updated_by"] == "alice@example.com"
        # Timestamps go through RawSql so the executor renders the
        # right function call per dialect (now() on both, but the
        # plumbing must be Protocol-mediated, not stringified).
        assert isinstance(value_cols["created_at"], RawSql)
        assert isinstance(value_cols["updated_at"], RawSql)

    def test_invalid_role_short_circuits_before_touching_executor(self, role_service, sql_executor_mock):
        # Validation runs before the executor — a bad role must not
        # leak a partial write attempt.
        with pytest.raises(ValueError, match="Invalid role"):
            role_service.create_mapping(role="not-a-real-role", group_name="g", user_email="u@x")
        sql_executor_mock.upsert_with_audit.assert_not_called()
        sql_executor_mock.execute.assert_not_called()


# ---------------------------------------------------------------------------
# Audit history — every mutation must land a row in
# ``dq_role_mappings_history`` so the audit trail survives even after the
# live mapping is deleted. The pattern mirrors
# ``RulesCatalogService._record_history`` / ``ScheduleConfigService._record_history``.
# ---------------------------------------------------------------------------


def _history_inserts(sql_executor_mock) -> list[str]:
    """Return the SQL strings of every ``INSERT INTO ... _history`` call."""
    return [
        call.args[0]
        for call in sql_executor_mock.execute.call_args_list
        if call.args and "INSERT INTO" in call.args[0] and "history" in call.args[0]
    ]


class TestCreateMappingHistoryRecording:
    def test_create_records_create_action_in_history(self, role_service, sql_executor_mock):
        role_service.create_mapping(
            role=UserRole.RULE_APPROVER.value,
            group_name="approvers",
            user_email="alice@example.com",
        )
        inserts = _history_inserts(sql_executor_mock)
        assert len(inserts) == 1, f"expected exactly one history INSERT, got: {inserts}"
        sql = inserts[0]
        assert "'create'" in sql
        assert f"'{UserRole.RULE_APPROVER.value}'" in sql
        assert "'approvers'" in sql
        assert "'alice@example.com'" in sql
        # Timestamps must come from the DB, not the app process clock,
        # so the audit log doesn't drift with app server timezone bugs.
        assert "now()" in sql

    def test_history_failure_does_not_break_primary_write(self, role_service, sql_executor_mock):
        # The history INSERT is best-effort — losing one audit row is
        # less harmful than refusing a legitimate admin change.
        # Simulate a Postgres serialization error on the history table.
        sql_executor_mock.execute.side_effect = RuntimeError("history table unavailable")

        # Should not raise — primary upsert already succeeded.
        result = role_service.create_mapping(
            role=UserRole.VIEWER.value,
            group_name="viewers",
            user_email="bob@example.com",
        )
        assert result.role == UserRole.VIEWER.value
        sql_executor_mock.upsert_with_audit.assert_called_once()


class TestDeleteMappingHistoryRecording:
    def test_delete_records_delete_action_in_history(self, role_service, sql_executor_mock):
        role_service.delete_mapping(
            role=UserRole.RULE_AUTHOR.value,
            group_name="writers",
            user_email="charlie@example.com",
        )
        # Two execute() calls: the DELETE against dq_role_mappings, then
        # the INSERT into dq_role_mappings_history.
        inserts = _history_inserts(sql_executor_mock)
        assert len(inserts) == 1, f"expected exactly one history INSERT, got: {inserts}"
        sql = inserts[0]
        assert "'delete'" in sql
        assert f"'{UserRole.RULE_AUTHOR.value}'" in sql
        assert "'writers'" in sql
        assert "'charlie@example.com'" in sql

    def test_delete_without_user_email_records_null_actor(self, role_service, sql_executor_mock):
        # Legacy call sites that don't pass user_email still produce an
        # audit row (with changed_by = NULL) — the audit trail prefers
        # an anonymous row over a missing one.
        role_service.delete_mapping(role=UserRole.VIEWER.value, group_name="viewers")
        inserts = _history_inserts(sql_executor_mock)
        assert len(inserts) == 1
        sql = inserts[0]
        assert "'delete'" in sql
        # NULL appears unquoted — confirms we didn't stringify None.
        assert "NULL" in sql

    def test_history_failure_does_not_break_delete(self, role_service, sql_executor_mock):
        # First execute() call (DELETE) succeeds; second (history INSERT)
        # fails. The mapping deletion has already committed so the
        # service must not propagate the failure.
        outcomes = [None, RuntimeError("history table unavailable")]

        def _exec(_sql):
            outcome = outcomes.pop(0)
            if isinstance(outcome, Exception):
                raise outcome

        sql_executor_mock.execute.side_effect = _exec
        role_service.delete_mapping(
            role=UserRole.VIEWER.value,
            group_name="viewers",
            user_email="dan@example.com",
        )


class TestListHistory:
    def test_list_history_orders_newest_first_with_limit(self, role_service, sql_executor_mock):
        # Build two rows that look like what the executor's ``.query()``
        # returns: list[tuple[role, group_name, action, changed_by, changed_at]].
        sql_executor_mock.query.return_value = [
            (UserRole.RULE_APPROVER.value, "approvers", "create", "alice@example.com", "2026-01-02T00:00:00+00:00"),
            (UserRole.RULE_APPROVER.value, "approvers", "delete", "alice@example.com", "2026-01-01T00:00:00+00:00"),
        ]
        entries = role_service.list_history(limit=50)
        assert [e.action for e in entries] == ["create", "delete"]
        assert all(isinstance(e, RoleMappingHistoryEntry) for e in entries)

        sent_sql = sql_executor_mock.query.call_args.args[0]
        assert "ORDER BY changed_at DESC" in sent_sql
        assert "LIMIT 50" in sent_sql

    def test_list_history_clamps_limit(self, role_service, sql_executor_mock):
        # Hostile callers can't OOM the app by passing a huge limit.
        sql_executor_mock.query.return_value = []
        role_service.list_history(limit=10_000_000)
        sent_sql = sql_executor_mock.query.call_args.args[0]
        assert "LIMIT 1000" in sent_sql

    def test_list_history_applies_filters(self, role_service, sql_executor_mock):
        sql_executor_mock.query.return_value = []
        role_service.list_history(role=UserRole.ADMIN.value, group_name="platform-admins")
        sent_sql = sql_executor_mock.query.call_args.args[0]
        assert f"role = '{UserRole.ADMIN.value}'" in sent_sql
        assert "group_name = 'platform-admins'" in sent_sql
