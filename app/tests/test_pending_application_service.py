"""Tests for ``PendingApplicationService`` — Bulk Contract Import Phase 2.

Follows the same shape as ``test_apply_rules_service.py``: a spec-bound
``create_autospec(SqlExecutor)`` mock with dialect-helper side effects wired
to their Delta-flavoured behaviour, so we assert on the emitted SQL and the
row → domain parsing without a live backend.
"""

from __future__ import annotations

import json

import pytest

from databricks_labs_dqx_app.backend.services.pending_application_service import (
    PendingApplication,
    PendingApplicationService,
)


@pytest.fixture
def sql(sql_executor_mock):
    sql_executor_mock.dialect = "delta"
    sql_executor_mock.fqn.side_effect = lambda t: f"dqx_test.dqx_app_test.{t}"
    sql_executor_mock.json_literal_expr.side_effect = lambda j: f"parse_json('{j}')"
    sql_executor_mock.select_json_text.side_effect = lambda c: f"to_json({c})"
    sql_executor_mock.ts_text.side_effect = lambda c: f"CAST({c} AS STRING)"
    sql_executor_mock.query.return_value = []
    return sql_executor_mock


@pytest.fixture
def svc(sql):
    return PendingApplicationService(sql=sql)


def _executed_sql(sql) -> list[str]:
    return [call.args[0] for call in sql.execute.call_args_list]


def test_record_inserts_new_when_none_exists(svc, sql):
    sql.query.return_value = []  # no existing row for (binding, rule)

    result = svc.record("b1", "r1", [{"column": "customer_id"}], "alice@example.com")

    assert isinstance(result, PendingApplication)
    assert result.binding_id == "b1"
    assert result.rule_id == "r1"
    assert result.column_mapping == [{"column": "customer_id"}]

    inserts = [s for s in _executed_sql(sql) if s.strip().startswith("INSERT INTO")]
    assert len(inserts) == 1
    insert = inserts[0]
    assert "dq_pending_applications" in insert
    assert "'b1'" in insert
    assert "'r1'" in insert
    assert "'alice@example.com'" in insert
    # column_mapping goes through the json literal helper.
    assert "parse_json('[{\"column\": \"customer_id\"}]')" in insert


def test_record_updates_existing_row(svc, sql):
    # An existing pending row for the same (binding, rule) → UPDATE, not INSERT.
    sql.query.return_value = [
        ["pa1", "b1", "r1", json.dumps([{"column": "old_col"}]), "bob@example.com", "2026-07-01T00:00:00+00:00"],
    ]

    result = svc.record("b1", "r1", [{"column": "new_col"}], "alice@example.com")

    assert result.id == "pa1"
    assert result.column_mapping == [{"column": "new_col"}]

    executed = _executed_sql(sql)
    assert not any(s.strip().startswith("INSERT INTO") for s in executed)
    updates = [s for s in executed if s.strip().startswith("UPDATE")]
    assert len(updates) == 1
    update = updates[0]
    assert "SET column_mapping" in update
    assert "WHERE id = 'pa1'" in update
    assert "parse_json('[{\"column\": \"new_col\"}]')" in update


def test_list_for_rule_parses_rows(svc, sql):
    sql.query.return_value = [
        ["pa1", "b1", "r1", json.dumps([{"column": "a"}]), "alice@example.com", "2026-07-01T00:00:00+00:00"],
        ["pa2", "b2", "r1", None, None, None],
    ]

    rows = svc.list_for_rule("r1")

    assert [r.id for r in rows] == ["pa1", "pa2"]
    assert rows[0].binding_id == "b1"
    assert rows[0].column_mapping == [{"column": "a"}]
    # Null column_mapping / created_at degrade gracefully.
    assert rows[1].column_mapping == []
    assert rows[1].created_at is None

    # The read is scoped to the requested rule id.
    query_sql = sql.query.call_args_list[-1].args[0]
    assert "WHERE rule_id = 'r1'" in query_sql


def test_list_for_binding_scopes_by_binding(svc, sql):
    sql.query.return_value = []
    svc.list_for_binding("b9")
    query_sql = sql.query.call_args_list[-1].args[0]
    assert "WHERE binding_id = 'b9'" in query_sql


def test_delete_emits_delete_by_id(svc, sql):
    svc.delete("pa1")
    deletes = [s for s in _executed_sql(sql) if s.strip().startswith("DELETE FROM")]
    assert len(deletes) == 1
    assert "WHERE id = 'pa1'" in deletes[0]


def test_parse_column_mapping_ignores_non_dict_and_bad_json():
    assert PendingApplicationService._parse_column_mapping(None) == []
    assert PendingApplicationService._parse_column_mapping("not json") == []
    assert PendingApplicationService._parse_column_mapping('"a string"') == []
    assert PendingApplicationService._parse_column_mapping('[{"col": "x"}, "skip", 5]') == [{"col": "x"}]
