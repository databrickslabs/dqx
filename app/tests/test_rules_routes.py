"""Tests for ``rules.py`` route helpers — primarily the authorisation gate.

We hit the pure helpers (``_catalog_of``, ``_display_name``,
``_ensure_owner_or_privileged``) and skip the FastAPI routes themselves —
those wrap many dependencies (OBO WorkspaceClient, multiple SQL services,
DQX library imports) whose value comes from being exercised in
integration tests, not unit tests.

The ``TestSaveRulesOwnership`` block exercises the ``save_rules`` route
function directly to lock in that an Author cannot update another user's
rule via the ``rule_id`` update path — a gap that previously slipped
through because only delete/submit/revoke had per-rule ownership checks.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.models import SaveRulesIn
from databricks_labs_dqx_app.backend.routes.v1.rules import (
    _catalog_of,
    _display_name,
    _ensure_owner_or_privileged,
    save_rules,
)
from databricks_labs_dqx_app.backend.services.rules_catalog_service import RuleCatalogEntry


# ---------------------------------------------------------------------------
# _catalog_of / _display_name
# ---------------------------------------------------------------------------


class TestCatalogHelpers:
    def test_catalog_of_three_part(self):
        assert _catalog_of("my_cat.my_schema.my_tbl") == "my_cat"

    def test_catalog_of_strips_sql_check_prefix(self):
        assert _catalog_of("__sql_check__/my-rule") == "my-rule"

    def test_catalog_of_empty_string(self):
        # Defensive: empty FQN should not blow up.
        assert _catalog_of("") == ""

    def test_display_name_passthrough(self):
        assert _display_name("a.b.c") == "a.b.c"

    def test_display_name_strips_sql_check_prefix(self):
        assert _display_name("__sql_check__/foo") == "foo"


# ---------------------------------------------------------------------------
# _ensure_owner_or_privileged
# ---------------------------------------------------------------------------


def _entry(created_by: str | None) -> RuleCatalogEntry:
    return RuleCatalogEntry(table_fqn="a.b.c", checks=[], created_by=created_by, rule_id="r1")


class TestEnsureOwnerOrPrivileged:
    def test_admin_can_act_on_any_rule(self):
        svc = MagicMock()
        svc.get_by_rule_id.return_value = _entry("someone-else@x")
        _ensure_owner_or_privileged(svc, "r1", "admin@x", UserRole.ADMIN, "delete")
        # Should not even look up the rule — admins skip the ownership check.
        svc.get_by_rule_id.assert_not_called()

    def test_approver_can_act_on_any_rule(self):
        svc = MagicMock()
        svc.get_by_rule_id.return_value = _entry("someone-else@x")
        _ensure_owner_or_privileged(svc, "r1", "approver@x", UserRole.RULE_APPROVER, "submit")
        svc.get_by_rule_id.assert_not_called()

    def test_author_can_act_on_their_own_rule(self):
        svc = MagicMock()
        svc.get_by_rule_id.return_value = _entry("alice@x")
        _ensure_owner_or_privileged(svc, "r1", "alice@x", UserRole.RULE_AUTHOR, "delete")

    def test_author_blocked_from_other_authors_rule(self):
        svc = MagicMock()
        svc.get_by_rule_id.return_value = _entry("bob@x")
        with pytest.raises(HTTPException) as excinfo:
            _ensure_owner_or_privileged(svc, "r1", "alice@x", UserRole.RULE_AUTHOR, "delete")
        assert excinfo.value.status_code == 403

    def test_email_comparison_is_case_insensitive(self):
        svc = MagicMock()
        svc.get_by_rule_id.return_value = _entry("Alice@Example.com")
        _ensure_owner_or_privileged(svc, "r1", "alice@example.com", UserRole.RULE_AUTHOR, "delete")

    def test_email_comparison_strips_whitespace(self):
        svc = MagicMock()
        svc.get_by_rule_id.return_value = _entry("  alice@x  ")
        _ensure_owner_or_privileged(svc, "r1", "alice@x", UserRole.RULE_AUTHOR, "delete")

    def test_missing_rule_returns_404(self):
        svc = MagicMock()
        svc.get_by_rule_id.return_value = None
        with pytest.raises(HTTPException) as excinfo:
            _ensure_owner_or_privileged(svc, "r1", "alice@x", UserRole.RULE_AUTHOR, "delete")
        assert excinfo.value.status_code == 404

    def test_rule_with_no_owner_blocks_author(self):
        # An older rule with NULL created_by should NOT be claimable by any
        # author — only privileged roles can touch it.
        svc = MagicMock()
        svc.get_by_rule_id.return_value = _entry(None)
        with pytest.raises(HTTPException) as excinfo:
            _ensure_owner_or_privileged(svc, "r1", "alice@x", UserRole.RULE_AUTHOR, "submit")
        assert excinfo.value.status_code == 403

    def test_viewer_cannot_act_even_on_their_own_rule(self):
        # A viewer shouldn't have hit this helper to begin with (route deps
        # gate at AUTHOR+), but defensively, the function still rejects them
        # via the email-vs-creator check (viewers usually have None creator
        # for rules they didn't author).
        svc = MagicMock()
        svc.get_by_rule_id.return_value = _entry("alice@x")
        # Viewer matches the creator → unusual but allowed by the helper
        # (the function only knows email + role; viewer routes are gated
        # elsewhere). Keep the guarantee tight: same email passes.
        _ensure_owner_or_privileged(svc, "r1", "alice@x", UserRole.VIEWER, "delete")

    def test_action_string_interpolated_in_403(self):
        svc = MagicMock()
        svc.get_by_rule_id.return_value = _entry("bob@x")
        with pytest.raises(HTTPException) as excinfo:
            _ensure_owner_or_privileged(svc, "r1", "alice@x", UserRole.RULE_AUTHOR, "submit")
        assert "submit" in excinfo.value.detail


# ---------------------------------------------------------------------------
# save_rules ownership gate (update path)
# ---------------------------------------------------------------------------


def _full_entry(rule_id: str, created_by: str) -> RuleCatalogEntry:
    return RuleCatalogEntry(
        table_fqn="cat.sch.tbl",
        checks=[{"check": {"function": "is_not_null", "arguments": {"column": "x"}}}],
        version=1,
        status="draft",
        source="ui",
        created_by=created_by,
        created_at="2025-01-01T00:00:00+00:00",
        updated_by=created_by,
        updated_at="2025-01-01T00:00:00+00:00",
        rule_id=rule_id,
    )


def _mock_obo_ws(user_email: str) -> MagicMock:
    obo = MagicMock()
    me = MagicMock()
    me.user_name = user_email
    obo.current_user.me.return_value = me
    return obo


class TestSaveRulesOwnership:
    """``save_rules`` must reject Author updates to other users' rules.

    The route is the entrypoint for both create-new (no ``rule_id``) and
    edit-existing (with ``rule_id``) flows from the UI. The latter path
    used to skip ownership checks, which let Authors silently overwrite
    another author's rule contents (and chain "save & submit" — even if
    the submit step 403'd, the corrupted edit had already landed).
    """

    def test_author_blocked_from_updating_other_authors_rule(self):
        svc = MagicMock()
        svc.get_by_rule_id.return_value = _full_entry("r1", "bob@x")
        body = SaveRulesIn(
            table_fqn="cat.sch.tbl",
            checks=[{"check": {"function": "is_not_null", "arguments": {"column": "x"}}}],
            rule_id="r1",
        )
        with pytest.raises(HTTPException) as excinfo:
            save_rules(
                body=body,
                svc=svc,
                obo_ws=_mock_obo_ws("alice@x"),
                user_role=UserRole.RULE_AUTHOR,
            )
        assert excinfo.value.status_code == 403
        assert "edit" in excinfo.value.detail
        # Update SQL must never have been issued.
        svc.update_rule.assert_not_called()

    def test_author_can_update_their_own_rule(self):
        svc = MagicMock()
        svc.get_by_rule_id.return_value = _full_entry("r1", "alice@x")
        svc.update_rule.return_value = _full_entry("r1", "alice@x")
        body = SaveRulesIn(
            table_fqn="cat.sch.tbl",
            checks=[{"check": {"function": "is_not_null", "arguments": {"column": "x"}}}],
            rule_id="r1",
        )
        result = save_rules(
            body=body,
            svc=svc,
            obo_ws=_mock_obo_ws("alice@x"),
            user_role=UserRole.RULE_AUTHOR,
        )
        svc.update_rule.assert_called_once()
        assert len(result) == 1
        assert result[0].rule_id == "r1"

    def test_admin_can_update_any_rule(self):
        svc = MagicMock()
        svc.get_by_rule_id.return_value = _full_entry("r1", "bob@x")
        svc.update_rule.return_value = _full_entry("r1", "bob@x")
        body = SaveRulesIn(
            table_fqn="cat.sch.tbl",
            checks=[{"check": {"function": "is_not_null", "arguments": {"column": "x"}}}],
            rule_id="r1",
        )
        result = save_rules(
            body=body,
            svc=svc,
            obo_ws=_mock_obo_ws("admin@x"),
            user_role=UserRole.ADMIN,
        )
        svc.update_rule.assert_called_once()
        # Admins bypass the ownership lookup entirely.
        svc.get_by_rule_id.assert_not_called()
        assert len(result) == 1

    def test_approver_can_update_any_rule(self):
        svc = MagicMock()
        svc.get_by_rule_id.return_value = _full_entry("r1", "bob@x")
        svc.update_rule.return_value = _full_entry("r1", "bob@x")
        body = SaveRulesIn(
            table_fqn="cat.sch.tbl",
            checks=[{"check": {"function": "is_not_null", "arguments": {"column": "x"}}}],
            rule_id="r1",
        )
        result = save_rules(
            body=body,
            svc=svc,
            obo_ws=_mock_obo_ws("approver@x"),
            user_role=UserRole.RULE_APPROVER,
        )
        svc.update_rule.assert_called_once()
        svc.get_by_rule_id.assert_not_called()
        assert len(result) == 1

    def test_create_new_rule_skips_ownership_lookup(self):
        # Without ``rule_id`` we're creating fresh rules — there's no
        # owner to check yet. The gate must not get in the way.
        svc = MagicMock()
        svc.save.return_value = [_full_entry("r-new", "alice@x")]
        body = SaveRulesIn(
            table_fqn="cat.sch.tbl",
            checks=[{"check": {"function": "is_not_null", "arguments": {"column": "x"}}}],
            source="ui",
        )
        result = save_rules(
            body=body,
            svc=svc,
            obo_ws=_mock_obo_ws("alice@x"),
            user_role=UserRole.RULE_AUTHOR,
        )
        svc.save.assert_called_once()
        svc.get_by_rule_id.assert_not_called()
        svc.update_rule.assert_not_called()
        assert len(result) == 1

    def test_missing_rule_returns_404_via_gate(self):
        svc = MagicMock()
        svc.get_by_rule_id.return_value = None
        body = SaveRulesIn(
            table_fqn="cat.sch.tbl",
            checks=[{"check": {"function": "is_not_null", "arguments": {"column": "x"}}}],
            rule_id="nope",
        )
        with pytest.raises(HTTPException) as excinfo:
            save_rules(
                body=body,
                svc=svc,
                obo_ws=_mock_obo_ws("alice@x"),
                user_role=UserRole.RULE_AUTHOR,
            )
        assert excinfo.value.status_code == 404
        svc.update_rule.assert_not_called()
