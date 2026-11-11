"""Tests for ``RulePolicyService`` — CRUD over ``dq_rule_policies``.

Uses the spec-bound ``sql_executor_mock`` fixture with Delta-flavoured dialect
helpers, asserting on emitted SQL and row parsing.
"""

import json

import pytest

from databricks_labs_dqx_app.backend.policy_models import (
    PolicyScope,
    RulePolicy,
    RulePolicyIn,
    TableCondition,
)
from databricks_labs_dqx_app.backend.services.rule_policy_service import (
    DuplicatePolicyNameError,
    RulePolicyNotFoundError,
    RulePolicyService,
)


@pytest.fixture
def sql(sql_executor_mock):
    sql_executor_mock.dialect = "delta"
    sql_executor_mock.fqn.side_effect = lambda t: f"dqx_test.dqx_app_test.{t}"
    sql_executor_mock.json_literal_expr.side_effect = lambda j: f"parse_json('{j}')"
    sql_executor_mock.select_json_text.side_effect = lambda c: f"to_json({c})"
    sql_executor_mock.ts_text.side_effect = lambda c: f"CAST({c} AS STRING)"
    sql_executor_mock.query.return_value = []
    sql_executor_mock.query_dicts.return_value = []
    return sql_executor_mock


@pytest.fixture
def svc(sql):
    return RulePolicyService(sql=sql)


def _executed_sql(sql) -> list[str]:
    return [call.args[0] for call in sql.execute.call_args_list]


def _body(name: str = "PII tables") -> RulePolicyIn:
    return RulePolicyIn(
        name=name,
        description="d",
        scope=PolicyScope(catalogs=["main"]),
        table_condition=TableCondition(kind="tags", tags=["class=pii"]),
        register_tables=True,
    )


def _row(policy_id: str = "p1", rule_id: str = "r1", name: str = "PII tables") -> dict:
    return {
        "policy_id": policy_id,
        "rule_id": rule_id,
        "name": name,
        "description": "d",
        "scope": json.dumps({"catalogs": ["main"], "schemas": "*", "tables": "*"}),
        "table_condition": json.dumps({"kind": "tags", "tags": ["class=pii"], "expr": None}),
        "column_conditions": json.dumps({}),
        "register_tables": "true",
        "created_by": "alice@example.com",
        "created_at": "2026-11-01T00:00:00+00:00",
        "updated_by": "alice@example.com",
        "updated_at": "2026-11-01T00:00:00+00:00",
    }


def test_create_inserts_json_columns_and_returns_policy(svc, sql):
    result = svc.create("r1", _body(), "alice@example.com")

    assert isinstance(result, RulePolicy)
    assert result.rule_id == "r1"
    assert len(result.policy_id) == 36  # uuid4
    assert result.created_by == "alice@example.com"
    assert result.register_tables is True

    inserts = [s for s in _executed_sql(sql) if s.strip().startswith("INSERT INTO")]
    assert len(inserts) == 1
    insert = inserts[0]
    assert "dq_rule_policies" in insert
    assert "'r1'" in insert
    assert "'PII tables'" in insert
    assert "parse_json('" in insert
    assert '"catalogs": ["main"]' in insert
    assert "TRUE" in insert
    assert "current_timestamp()" in insert


def test_create_duplicate_name_for_same_rule_raises(svc, sql):
    sql.query.return_value = [["p-existing"]]

    with pytest.raises(DuplicatePolicyNameError):
        svc.create("r1", _body("  pii TABLES "), "alice@example.com")

    check = sql.query.call_args.args[0]
    assert "lower(name) = 'pii tables'" in check
    assert "rule_id = 'r1'" in check
    assert not any(s.strip().startswith("INSERT INTO") for s in _executed_sql(sql))


def test_list_for_rule_parses_rows(svc, sql):
    sql.query_dicts.return_value = [_row("p1"), _row("p2", name="Other")]

    policies = svc.list_for_rule("r1")

    assert [p.policy_id for p in policies] == ["p1", "p2"]
    assert policies[0].scope.catalogs == ["main"]
    assert policies[0].scope.schemas == "*"
    assert policies[0].table_condition.tags == ["class=pii"]
    assert policies[0].register_tables is True
    assert "WHERE rule_id = 'r1'" in sql.query_dicts.call_args.args[0]
    assert "to_json(scope)" in sql.query_dicts.call_args.args[0]


def test_list_all_has_no_rule_filter(svc, sql):
    sql.query_dicts.return_value = [_row()]

    assert len(svc.list_all()) == 1
    assert "WHERE" not in sql.query_dicts.call_args.args[0]


def test_get_returns_none_when_missing(svc):
    assert svc.get("nope") is None


def test_update_bumps_updated_and_keeps_created(svc, sql):
    refreshed = _row(name="Renamed")
    refreshed["updated_by"] = "bob@example.com"
    sql.query_dicts.side_effect = [[_row()], [refreshed]]

    result = svc.update("p1", _body("Renamed"), "bob@example.com")

    updates = [s for s in _executed_sql(sql) if s.strip().startswith("UPDATE")]
    assert len(updates) == 1
    assert "`updated_by` = 'bob@example.com'" in updates[0]
    assert "`updated_at` = current_timestamp()" in updates[0]
    assert "created_by" not in updates[0]
    assert "created_at" not in updates[0]
    assert "WHERE `policy_id` = 'p1'" in updates[0]
    assert result.name == "Renamed"
    assert result.created_by == "alice@example.com"
    assert result.updated_by == "bob@example.com"


def test_update_unknown_policy_raises(svc):
    with pytest.raises(RulePolicyNotFoundError):
        svc.update("nope", _body(), "bob@example.com")


def test_update_duplicate_name_excludes_self_and_raises(svc, sql):
    sql.query_dicts.return_value = [_row()]
    sql.query.return_value = [["p-other"]]

    with pytest.raises(DuplicatePolicyNameError):
        svc.update("p1", _body("Taken"), "bob@example.com")

    assert "policy_id <> 'p1'" in sql.query.call_args.args[0]


def test_delete_unknown_raises(svc, sql):
    sql.query.return_value = [["0"]]

    with pytest.raises(RulePolicyNotFoundError):
        svc.delete("nope")


def test_delete_removes_row(svc, sql):
    sql.query.return_value = [["1"]]

    svc.delete("p1")

    deletes = [s for s in _executed_sql(sql) if s.strip().startswith("DELETE")]
    assert len(deletes) == 1
    assert "WHERE `policy_id` = 'p1'" in deletes[0]


def test_delete_for_rule(svc, sql):
    sql.query.return_value = [["3"]]

    assert svc.delete_for_rule("r1") == 3

    deletes = [s for s in _executed_sql(sql) if s.strip().startswith("DELETE")]
    assert len(deletes) == 1
    assert "WHERE `rule_id` = 'r1'" in deletes[0]


def test_delete_for_rule_with_no_policies_skips_delete(svc, sql):
    sql.query.return_value = [["0"]]

    assert svc.delete_for_rule("r1") == 0
    assert not any(s.strip().startswith("DELETE") for s in _executed_sql(sql))


class _UniqueViolation(Exception):
    """Stands in for psycopg's UniqueViolation (SQLSTATE 23505) raised by a concurrent save."""

    sqlstate = "23505"


def test_create_concurrent_duplicate_maps_unique_violation(svc, sql):
    sql.execute.side_effect = _UniqueViolation("duplicate key value violates unique constraint")

    with pytest.raises(DuplicatePolicyNameError):
        svc.create("r1", _body(), "alice@example.com")


def test_update_concurrent_duplicate_maps_unique_violation(svc, sql):
    sql.query_dicts.return_value = [_row()]
    sql.execute.side_effect = _UniqueViolation("duplicate key value violates unique constraint")

    with pytest.raises(DuplicatePolicyNameError):
        svc.update("p1", _body("Taken"), "bob@example.com")


def test_create_other_db_errors_propagate(svc, sql):
    sql.execute.side_effect = RuntimeError("connection lost")

    with pytest.raises(RuntimeError):
        svc.create("r1", _body(), "alice@example.com")
