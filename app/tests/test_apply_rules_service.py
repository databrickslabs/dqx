"""Tests for ``ApplyRulesService`` — Phase 3C tier-2 apply/map layer.

Follows the same testing shape as ``test_monitored_table_service.py``:
spec-bound ``create_autospec(SqlExecutor)`` mocks with dialect-helper side
effects wired to their real Delta-flavoured behaviour.
"""

from __future__ import annotations

import json
from unittest.mock import create_autospec

import pytest

from databricks_labs_dqx_app.backend.registry_models import RegistryRule, RuleDefinition
from databricks_labs_dqx_app.backend.services.apply_rules_service import (
    ApplyRulesService,
    MappingIncompleteError,
    RuleNotPublishedError,
)
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService


@pytest.fixture
def sql(sql_executor_mock):
    sql_executor_mock.dialect = "delta"
    sql_executor_mock.fqn.side_effect = lambda t: f"dqx_test.dqx_app_test.{t}"
    sql_executor_mock.q.side_effect = lambda i: f"`{i}`"
    sql_executor_mock.json_literal_expr.side_effect = lambda j: f"parse_json('{j}')"
    sql_executor_mock.select_json_text.side_effect = lambda c: f"to_json({c})"
    sql_executor_mock.ts_text.side_effect = lambda c: f"CAST({c} AS STRING)"
    sql_executor_mock.query.return_value = []
    return sql_executor_mock


@pytest.fixture
def registry():
    return create_autospec(RegistryService, instance=True)


@pytest.fixture
def svc(sql, registry):
    return ApplyRulesService(sql=sql, registry=registry)


def _published_rule(rule_id: str = "r1", slot_names: list[str] | None = None) -> RegistryRule:
    slot_names = slot_names if slot_names is not None else ["column"]
    definition = RuleDefinition.model_validate(
        {
            "body": {"function": "is_not_null", "arguments": {n: f"{{{{{n}}}}}" for n in slot_names}},
            "slots": [{"name": n, "family": "any", "position": i, "cardinality": "one"} for i, n in enumerate(slot_names)],
            "parameters": [],
        }
    )
    return RegistryRule(
        rule_id=rule_id,
        mode="dqx_native",
        status="approved",
        version=1,
        definition=definition,
        user_metadata={"name": "Not Null Check", "severity": "High"},
    )


def _applied_row(
    id_: str = "ar1",
    binding_id: str = "b1",
    rule_id: str = "r1",
    pinned_version: str | None = None,
    severity_override: str | None = None,
    column_mapping: list[dict[str, str]] | None = None,
    user_metadata: dict | None = None,
    mapping_hash: str = "deadbeef",
) -> list[str]:
    return [
        id_,
        binding_id,
        rule_id,
        pinned_version,
        severity_override,
        json.dumps(column_mapping if column_mapping is not None else [{"column": "customer_id"}]),
        json.dumps(user_metadata or {}),
        mapping_hash,
        "alice@x",
        "2026-07-02T00:00:00+00:00",
    ]


# ---------------------------------------------------------------------------
# apply_rule
# ---------------------------------------------------------------------------


class TestApplyRule:
    def test_applies_published_rule(self, svc, sql, registry):
        registry.get_rule.return_value = _published_rule()
        sql.query.side_effect = [
            [["b1"]],  # binding exists
            [],  # no existing application with this natural key
        ]
        applied = svc.apply_rule(
            "b1", "r1", [{"column": "customer_id"}], "alice@x", pinned_version=None, severity_override=None
        )
        assert applied.binding_id == "b1"
        assert applied.rule_id == "r1"
        assert applied.column_mapping == [{"column": "customer_id"}]
        assert applied.mapping_hash
        insert_sql = sql.execute.call_args[0][0]
        assert "INSERT INTO dqx_test.dqx_app_test.dq_applied_rules" in insert_sql

    def test_rejects_missing_binding(self, svc, sql, registry):
        sql.query.return_value = []  # binding lookup empty
        with pytest.raises(RuntimeError, match="Monitored table not found"):
            svc.apply_rule("missing", "r1", [{"column": "customer_id"}], "alice@x")
        registry.get_rule.assert_not_called()

    def test_rejects_missing_rule(self, svc, sql, registry):
        sql.query.return_value = [["b1"]]
        registry.get_rule.return_value = None
        with pytest.raises(RuntimeError, match="Registry rule not found"):
            svc.apply_rule("b1", "missing", [{"column": "customer_id"}], "alice@x")

    def test_rejects_unpublished_rule(self, svc, sql, registry):
        sql.query.return_value = [["b1"]]
        draft_rule = _published_rule()
        draft_rule.status = "draft"
        registry.get_rule.return_value = draft_rule
        with pytest.raises(RuleNotPublishedError):
            svc.apply_rule("b1", "r1", [{"column": "customer_id"}], "alice@x")
        sql.execute.assert_not_called()

    def test_allows_empty_mapping_to_stage_application(self, svc, sql, registry):
        # An empty column_mapping stages the rule application without any
        # mapping groups yet — the by-rule card completes the mapping with a
        # follow-up apply_rule() call. materializer.py skips rows with zero
        # groups, so nothing runs until then.
        registry.get_rule.return_value = _published_rule()
        sql.query.side_effect = [
            [["b1"]],  # binding exists
            [],  # no existing application with this natural key
        ]
        applied = svc.apply_rule("b1", "r1", [], "alice@x")
        assert applied.column_mapping == []
        insert_sql = sql.execute.call_args[0][0]
        assert "INSERT INTO dqx_test.dqx_app_test.dq_applied_rules" in insert_sql

    def test_rejects_mapping_missing_a_slot(self, svc, sql, registry):
        sql.query.return_value = [["b1"]]
        registry.get_rule.return_value = _published_rule(slot_names=["column", "reference_column"])
        with pytest.raises(MappingIncompleteError):
            svc.apply_rule("b1", "r1", [{"column": "customer_id"}], "alice@x")
        sql.execute.assert_not_called()

    def test_rejects_mapping_with_unknown_slot(self, svc, sql, registry):
        sql.query.return_value = [["b1"]]
        registry.get_rule.return_value = _published_rule()
        with pytest.raises(MappingIncompleteError):
            svc.apply_rule("b1", "r1", [{"column": "customer_id", "extra": "x"}], "alice@x")

    def test_reapplying_identical_mapping_updates_instead_of_duplicating(self, svc, sql, registry):
        registry.get_rule.return_value = _published_rule()
        existing_row = _applied_row(severity_override=None, pinned_version=None)
        sql.query.side_effect = [
            [["b1"]],  # binding exists
            [existing_row],  # existing application found by natural key
        ]
        applied = svc.apply_rule(
            "b1", "r1", [{"column": "customer_id"}], "alice@x", pinned_version=2, severity_override="Critical"
        )
        assert applied.id == "ar1"
        assert applied.pinned_version == 2
        assert applied.severity_override == "Critical"
        update_sql = sql.execute.call_args[0][0]
        assert "UPDATE dqx_test.dqx_app_test.dq_applied_rules" in update_sql
        assert "INSERT INTO" not in update_sql


# ---------------------------------------------------------------------------
# list_applied / get_applied
# ---------------------------------------------------------------------------


class TestListAndGet:
    def test_list_applied(self, svc, sql):
        sql.query.return_value = [_applied_row(id_="ar1"), _applied_row(id_="ar2")]
        applied = svc.list_applied("b1")
        assert [a.id for a in applied] == ["ar1", "ar2"]

    def test_get_applied_returns_none_when_missing(self, svc, sql):
        sql.query.return_value = []
        assert svc.get_applied("missing") is None


# ---------------------------------------------------------------------------
# count_applications_for_rule
# ---------------------------------------------------------------------------


class TestCountApplicationsForRule:
    def test_counts_applications(self, svc, sql):
        sql.query.return_value = [["3"]]
        assert svc.count_applications_for_rule("r1") == 3

    def test_zero_when_no_rows(self, svc, sql):
        sql.query.return_value = []
        assert svc.count_applications_for_rule("r1") == 0

    def test_zero_when_null_count(self, svc, sql):
        sql.query.return_value = [[None]]
        assert svc.count_applications_for_rule("r1") == 0


# ---------------------------------------------------------------------------
# remove_applied
# ---------------------------------------------------------------------------


class TestRemoveApplied:
    def test_removes_link_and_materialized_rows(self, svc, sql):
        sql.query.return_value = [_applied_row(id_="ar1")]
        svc.remove_applied("ar1")
        calls = [c.args[0] for c in sql.execute.call_args_list]
        assert any("DELETE FROM dqx_test.dqx_app_test.dq_quality_rules" in c and "applied_rule_id" in c for c in calls)
        assert any("DELETE FROM dqx_test.dqx_app_test.dq_applied_rules" in c for c in calls)

    def test_raises_when_missing(self, svc, sql):
        sql.query.return_value = []
        with pytest.raises(RuntimeError):
            svc.remove_applied("missing")
        sql.execute.assert_not_called()


# ---------------------------------------------------------------------------
# set_pin / set_severity_override
# ---------------------------------------------------------------------------


class TestSetPin:
    def test_sets_pinned_version(self, svc, sql):
        sql.query.return_value = [_applied_row(id_="ar1")]
        applied = svc.set_pin("ar1", 3)
        assert applied.pinned_version == 3
        update_sql = sql.execute.call_args[0][0]
        assert "pinned_version = 3" in update_sql

    def test_clears_pinned_version(self, svc, sql):
        sql.query.return_value = [_applied_row(id_="ar1", pinned_version="2")]
        applied = svc.set_pin("ar1", None)
        assert applied.pinned_version is None
        update_sql = sql.execute.call_args[0][0]
        assert "pinned_version = NULL" in update_sql

    def test_raises_when_missing(self, svc, sql):
        sql.query.return_value = []
        with pytest.raises(RuntimeError):
            svc.set_pin("missing", 1)


class TestSetSeverityOverride:
    def test_sets_severity_override(self, svc, sql):
        sql.query.return_value = [_applied_row(id_="ar1")]
        applied = svc.set_severity_override("ar1", "Critical")
        assert applied.severity_override == "Critical"
        update_sql = sql.execute.call_args[0][0]
        assert "severity_override = 'Critical'" in update_sql

    def test_clears_severity_override(self, svc, sql):
        sql.query.return_value = [_applied_row(id_="ar1", severity_override="High")]
        applied = svc.set_severity_override("ar1", None)
        assert applied.severity_override is None
        update_sql = sql.execute.call_args[0][0]
        assert "severity_override = NULL" in update_sql

    def test_raises_when_missing(self, svc, sql):
        sql.query.return_value = []
        with pytest.raises(RuntimeError):
            svc.set_severity_override("missing", "Low")
