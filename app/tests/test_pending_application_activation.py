"""Tests for the approval-time activation hook (Bulk Contract Import Phase 2).

``_publish_registry_rule`` drains staged ``dq_pending_applications`` rows for a
just-approved rule into real applied-rule links via
``ApplyRulesService.apply_rule``. This covers that drain (
``_activate_pending_applications``) in isolation: happy path, per-row
best-effort failure, and a list-failure that must never bubble up and fail an
otherwise-successful publish.
"""

from __future__ import annotations

from unittest.mock import create_autospec

from databricks_labs_dqx_app.backend.routes.v1.registry_rules import _activate_pending_applications
from databricks_labs_dqx_app.backend.services.apply_rules_service import ApplyRulesService
from databricks_labs_dqx_app.backend.services.pending_application_service import (
    PendingApplication,
    PendingApplicationService,
)


def _pending(pid: str, binding: str, rule: str = "r1", created_by: str | None = "author@example.com"):
    return PendingApplication(
        id=pid,
        binding_id=binding,
        rule_id=rule,
        column_mapping=[{"column": f"col_{binding}"}],
        created_by=created_by,
    )


def test_activate_applies_and_deletes_each_pending():
    apply_rules = create_autospec(ApplyRulesService, instance=True)
    pending = create_autospec(PendingApplicationService, instance=True)
    pending.list_for_rule.return_value = [_pending("pa1", "b1"), _pending("pa2", "b2")]

    _activate_pending_applications("r1", "approver@example.com", apply_rules=apply_rules, pending=pending)

    assert apply_rules.apply_rule.call_count == 2
    apply_rules.apply_rule.assert_any_call("b1", "r1", [{"column": "col_b1"}], "author@example.com")
    apply_rules.apply_rule.assert_any_call("b2", "r1", [{"column": "col_b2"}], "author@example.com")
    assert {c.args[0] for c in pending.delete.call_args_list} == {"pa1", "pa2"}


def test_activate_uses_approver_when_created_by_missing():
    apply_rules = create_autospec(ApplyRulesService, instance=True)
    pending = create_autospec(PendingApplicationService, instance=True)
    pending.list_for_rule.return_value = [_pending("pa1", "b1", created_by=None)]

    _activate_pending_applications("r1", "approver@example.com", apply_rules=apply_rules, pending=pending)

    apply_rules.apply_rule.assert_called_once_with("b1", "r1", [{"column": "col_b1"}], "approver@example.com")


def test_activate_is_best_effort_per_row():
    apply_rules = create_autospec(ApplyRulesService, instance=True)
    pending = create_autospec(PendingApplicationService, instance=True)
    pending.list_for_rule.return_value = [_pending("pa1", "b1"), _pending("pa2", "b2")]
    # First apply blows up; the loop must still process the second one and
    # never re-raise (a bad row can't fail the publish).
    apply_rules.apply_rule.side_effect = [RuntimeError("boom"), None]

    _activate_pending_applications("r1", "approver@example.com", apply_rules=apply_rules, pending=pending)

    assert apply_rules.apply_rule.call_count == 2
    # The failed row is NOT deleted (survives for a later retry); the good one is.
    deleted = {c.args[0] for c in pending.delete.call_args_list}
    assert deleted == {"pa2"}


def test_activate_swallows_list_failure():
    apply_rules = create_autospec(ApplyRulesService, instance=True)
    pending = create_autospec(PendingApplicationService, instance=True)
    pending.list_for_rule.side_effect = RuntimeError("db down")

    # Must not raise — publish side effects are best-effort.
    _activate_pending_applications("r1", "approver@example.com", apply_rules=apply_rules, pending=pending)

    apply_rules.apply_rule.assert_not_called()
    pending.delete.assert_not_called()
