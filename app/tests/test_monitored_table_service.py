"""Tests for ``MonitoredTableService`` — Phase 3B CRUD + profiling read.

Follows the same testing shape as ``test_registry_service.py``: spec-bound
``create_autospec(SqlExecutor)`` mocks with dialect-helper side effects
wired to their real Delta-flavoured behaviour, so assertions read real
SQL/JSON rather than MagicMock reprs. Two executors are used — one for the
OLTP tables (``dq_monitored_tables``/``dq_applied_rules``/``dq_rules``) and
one for the always-Delta ``dq_profiling_results`` read path — mirroring how
``dependencies.get_monitored_table_service`` wires the real service.
"""

from __future__ import annotations

import json

import pytest

from databricks_labs_dqx_app.backend.services.monitored_table_service import (
    DuplicateMonitoredTableError,
    MonitoredTableService,
)


@pytest.fixture
def sql(sql_executor_mock):
    """The OLTP executor mock (dq_monitored_tables / dq_applied_rules / dq_rules)."""
    sql_executor_mock.dialect = "delta"
    sql_executor_mock.fqn.side_effect = lambda t: f"dqx_test.dqx_app_test.{t}"
    sql_executor_mock.q.side_effect = lambda i: f"`{i}`"
    sql_executor_mock.json_literal_expr.side_effect = lambda j: f"parse_json('{j}')"
    sql_executor_mock.select_json_text.side_effect = lambda c: f"to_json({c})"
    sql_executor_mock.ts_text.side_effect = lambda c: f"CAST({c} AS STRING)"
    sql_executor_mock.query.return_value = []
    return sql_executor_mock


@pytest.fixture
def profiling_sql(sql_executor_mock):
    """A second, independent Delta executor mock for the profiling read path."""
    from unittest.mock import create_autospec

    from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor

    mock = create_autospec(SqlExecutor, instance=True)
    mock.catalog = "dqx_test"
    mock.schema = "dqx_app_test"
    mock.dialect = "delta"
    mock.fqn.side_effect = lambda t: f"dqx_test.dqx_app_test.{t}"
    mock.query_dicts.return_value = []
    return mock


@pytest.fixture
def svc(sql, profiling_sql):
    return MonitoredTableService(sql=sql, profiling_sql=profiling_sql)


def _table_row(
    binding_id: str = "b1",
    table_fqn: str = "cat.schema.tbl",
    steward: str | None = "alice@x",
    status: str = "draft",
    last_profiled_at: str | None = None,
) -> list[str]:
    return [
        binding_id,
        table_fqn,
        steward,
        status,
        last_profiled_at,
        "alice@x",
        "2026-07-02T00:00:00+00:00",
        "alice@x",
        "2026-07-02T00:00:00+00:00",
    ]


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
        json.dumps(column_mapping if column_mapping is not None else [{"column": "id"}]),
        json.dumps(user_metadata or {}),
        mapping_hash,
        "alice@x",
        "2026-07-02T00:00:00+00:00",
    ]


# ---------------------------------------------------------------------------
# register
# ---------------------------------------------------------------------------


class TestRegister:
    def test_registers_new_binding_as_draft(self, svc, sql):
        sql.query.return_value = []  # no existing binding
        table = svc.register("cat.schema.tbl", "alice@x", steward="bob@x")
        assert table.table_fqn == "cat.schema.tbl"
        assert table.status == "draft"
        assert table.steward == "bob@x"
        assert table.binding_id
        sql.execute.assert_called_once()
        insert_sql = sql.execute.call_args[0][0]
        assert "INSERT INTO dqx_test.dqx_app_test.dq_monitored_tables" in insert_sql
        assert "cat.schema.tbl" in insert_sql

    def test_rejects_duplicate_table_fqn(self, svc, sql):
        sql.query.return_value = [_table_row(table_fqn="cat.schema.tbl")]
        with pytest.raises(DuplicateMonitoredTableError):
            svc.register("cat.schema.tbl", "alice@x")
        sql.execute.assert_not_called()

    def test_rejects_invalid_fqn(self, svc, sql):
        with pytest.raises(ValueError):
            svc.register("not-a-valid-fqn", "alice@x")
        sql.execute.assert_not_called()


# ---------------------------------------------------------------------------
# list_monitored_tables
# ---------------------------------------------------------------------------


class TestListMonitoredTables:
    def test_lists_with_applied_rule_counts(self, svc, sql):
        sql.query.side_effect = [
            [_table_row(binding_id="b1"), _table_row(binding_id="b2")],
            [["2"]],  # count for b1
            [["0"]],  # count for b2
        ]
        summaries = svc.list_monitored_tables()
        assert len(summaries) == 2
        assert summaries[0].applied_rule_count == 2
        assert summaries[1].applied_rule_count == 0

    def test_filters_by_status_pushed_to_sql(self, svc, sql):
        sql.query.return_value = []
        svc.list_monitored_tables(status="published")
        list_sql = sql.query.call_args[0][0]
        assert "status = 'published'" in list_sql

    def test_filters_by_steward_pushed_to_sql(self, svc, sql):
        sql.query.return_value = []
        svc.list_monitored_tables(steward="bob@x")
        list_sql = sql.query.call_args[0][0]
        assert "steward = 'bob@x'" in list_sql

    def test_filters_by_catalog_in_python(self, svc, sql):
        sql.query.side_effect = [
            [_table_row(binding_id="b1", table_fqn="cat1.schema.tbl"), _table_row(binding_id="b2", table_fqn="cat2.schema.tbl")],
            [["0"]],
        ]
        summaries = svc.list_monitored_tables(catalog="cat1")
        assert len(summaries) == 1
        assert summaries[0].table.table_fqn == "cat1.schema.tbl"

    def test_filters_by_name_substring(self, svc, sql):
        sql.query.side_effect = [
            [_table_row(binding_id="b1", table_fqn="cat.schema.orders"), _table_row(binding_id="b2", table_fqn="cat.schema.customers")],
            [["0"]],
        ]
        summaries = svc.list_monitored_tables(name="order")
        assert len(summaries) == 1
        assert summaries[0].table.table_fqn == "cat.schema.orders"


# ---------------------------------------------------------------------------
# get
# ---------------------------------------------------------------------------


class TestGet:
    def test_returns_none_when_missing(self, svc, sql):
        sql.query.return_value = []
        assert svc.get("missing") is None

    def test_returns_binding_with_joined_applied_rules(self, svc, sql):
        sql.query.side_effect = [
            [_table_row(binding_id="b1")],
            [_applied_row(binding_id="b1", rule_id="r1")],
            [[json.dumps({"name": "Not Null Check", "dimension": "Completeness", "severity": "High"})]],
        ]
        detail = svc.get("b1")
        assert detail is not None
        assert detail.table.binding_id == "b1"
        assert len(detail.applied_rules) == 1
        summary = detail.applied_rules[0]
        assert summary.applied_rule.rule_id == "r1"
        assert summary.applied_rule.column_mapping == [{"column": "id"}]
        assert summary.rule_name == "Not Null Check"
        assert summary.rule_dimension == "Completeness"
        assert summary.rule_severity == "High"

    def test_applied_rule_with_missing_registry_rule_has_no_tags(self, svc, sql):
        sql.query.side_effect = [
            [_table_row(binding_id="b1")],
            [_applied_row(binding_id="b1", rule_id="r-deleted")],
            [],  # dq_rules lookup returns nothing
        ]
        detail = svc.get("b1")
        assert detail is not None
        summary = detail.applied_rules[0]
        assert summary.rule_name is None
        assert summary.rule_dimension is None
        assert summary.rule_severity is None


# ---------------------------------------------------------------------------
# delete
# ---------------------------------------------------------------------------


class TestDelete:
    def test_deletes_binding_and_applied_rules(self, svc, sql):
        sql.query.return_value = [_table_row(binding_id="b1")]
        svc.delete("b1", "alice@x")
        assert sql.execute.call_count == 2
        applied_delete_sql = sql.execute.call_args_list[0][0][0]
        table_delete_sql = sql.execute.call_args_list[1][0][0]
        assert "DELETE FROM dqx_test.dqx_app_test.dq_applied_rules" in applied_delete_sql
        assert "DELETE FROM dqx_test.dqx_app_test.dq_monitored_tables" in table_delete_sql

    def test_raises_when_missing(self, svc, sql):
        sql.query.return_value = []
        with pytest.raises(RuntimeError):
            svc.delete("missing", "alice@x")
        sql.execute.assert_not_called()


# ---------------------------------------------------------------------------
# get_latest_profile
# ---------------------------------------------------------------------------


class TestGetLatestProfile:
    def test_returns_none_when_no_rows(self, svc, profiling_sql):
        profiling_sql.query_dicts.return_value = []
        assert svc.get_latest_profile("cat.schema.tbl") is None

    def test_returns_latest_profile_shape(self, svc, profiling_sql):
        profiling_sql.query_dicts.return_value = [
            {
                "run_id": "run1",
                "source_table_fqn": "cat.schema.tbl",
                "rows_profiled": "1000",
                "columns_profiled": "5",
                "duration_seconds": "1.5",
                "summary_json": json.dumps({"columns": {"id": {"null_pct": 0.0}}}),
                "generated_rules_json": json.dumps([{"check": {"function": "is_not_null"}}]),
                "status": "SUCCESS",
                "created_at": "2026-07-01T00:00:00",
            }
        ]
        profile = svc.get_latest_profile("cat.schema.tbl")
        assert profile is not None
        assert profile.run_id == "run1"
        assert profile.source_table_fqn == "cat.schema.tbl"
        assert profile.rows_profiled == 1000
        assert profile.columns_profiled == 5
        assert profile.duration_seconds == 1.5
        assert profile.summary == {"columns": {"id": {"null_pct": 0.0}}}
        assert profile.generated_rules == [{"check": {"function": "is_not_null"}}]
        assert profile.profiled_at == "2026-07-01T00:00:00"

    def test_queries_by_source_table_fqn_and_success_status(self, svc, profiling_sql):
        profiling_sql.query_dicts.return_value = []
        svc.get_latest_profile("cat.schema.tbl")
        query = profiling_sql.query_dicts.call_args[0][0]
        assert "source_table_fqn = 'cat.schema.tbl'" in query
        assert "status = 'SUCCESS'" in query
        assert "ORDER BY created_at DESC" in query
