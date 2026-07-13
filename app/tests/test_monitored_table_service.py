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
    version: int = 0,
    schedule_cron: str | None = None,
    schedule_tz: str | None = None,
    last_profiled_at: str | None = None,
) -> list[str]:
    return [
        binding_id,
        table_fqn,
        steward,
        status,
        version,
        schedule_cron,
        schedule_tz,
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

    def test_registers_table_in_schema_with_special_characters(self, svc, sql):
        # Regression: Unity Catalog schema/table names created outside the SQL
        # parser (e.g. via the REST API) can legitimately contain characters
        # like single quotes — discovery must not block registering them.
        sql.query.return_value = []  # no existing binding
        fqn = "main.'ftr_mv_test'.'ftr_gold_mv_bkp'"
        table = svc.register(fqn, "alice@x", steward="bob@x")
        assert table.table_fqn == fqn
        insert_sql = sql.execute.call_args[0][0]
        # table_fqn is stored as a SQL string literal, so the embedded single
        # quotes are doubled by escape_sql_string() rather than appearing raw.
        assert "main.''ftr_mv_test''.''ftr_gold_mv_bkp''" in insert_sql

    def test_rejects_fqn_with_embedded_backtick(self, svc, sql):
        # A raw backtick would let the identifier break out of quote_fqn's
        # backtick-quoting — this must stay rejected even though quotes and
        # other punctuation are now accepted.
        with pytest.raises(ValueError):
            svc.register("main.weird`schema.tbl", "alice@x")
        sql.execute.assert_not_called()


# ---------------------------------------------------------------------------
# bulk_register
# ---------------------------------------------------------------------------


class TestBulkRegister:
    def test_empty_list_returns_empty_summary(self, svc, sql):
        result = svc.bulk_register([], "alice@x")
        assert result.registered == []
        assert result.skipped_existing == []
        assert result.invalid == []
        sql.query.assert_not_called()
        sql.execute.assert_not_called()

    def test_partitions_new_existing_and_invalid(self, svc, sql):
        # cat.schema.existing is already monitored; cat.schema.new1/new2 are not;
        # bad-fqn fails validate_fqn.
        sql.query.return_value = [["cat.schema.existing"]]
        result = svc.bulk_register(
            ["cat.schema.existing", "cat.schema.new1", "cat.schema.new2", "bad-fqn"],
            "alice@x",
            steward="bob@x",
        )
        assert sorted(result.registered) == ["cat.schema.new1", "cat.schema.new2"]
        assert result.skipped_existing == ["cat.schema.existing"]
        assert result.invalid == ["bad-fqn"]
        assert sql.execute.call_count == 2
        inserted_fqns = {call.args[0].split("VALUES")[1] for call in sql.execute.call_args_list}
        assert any("cat.schema.new1" in v for v in inserted_fqns)
        assert any("cat.schema.new2" in v for v in inserted_fqns)

    def test_dedupes_within_input(self, svc, sql):
        sql.query.return_value = []
        result = svc.bulk_register(["cat.schema.new1", "cat.schema.new1"], "alice@x")
        assert result.registered == ["cat.schema.new1"]
        assert sql.execute.call_count == 1

    def test_all_invalid_skips_existence_query(self, svc, sql):
        result = svc.bulk_register(["bad-fqn", "also bad"], "alice@x")
        assert result.registered == []
        assert result.skipped_existing == []
        assert sorted(result.invalid) == ["also bad", "bad-fqn"]
        sql.query.assert_not_called()
        sql.execute.assert_not_called()

    def test_existence_check_uses_single_query(self, svc, sql):
        sql.query.return_value = []
        svc.bulk_register(["cat.schema.new1", "cat.schema.new2"], "alice@x")
        assert sql.query.call_count == 1
        select_sql = sql.query.call_args[0][0]
        assert "cat.schema.new1" in select_sql
        assert "cat.schema.new2" in select_sql


# ---------------------------------------------------------------------------
# list_monitored_tables
# ---------------------------------------------------------------------------


class TestListMonitoredTables:
    def test_lists_with_applied_rule_counts(self, svc, sql):
        sql.query.side_effect = [
            [_table_row(binding_id="b1"), _table_row(binding_id="b2")],
            [["2"]],  # applied-rule count for b1
            [["3"]],  # materialized-check count for b1
            [["0"]],  # applied-rule count for b2
            [["0"]],  # materialized-check count for b2
        ]
        summaries = svc.list_monitored_tables()
        assert len(summaries) == 2
        assert summaries[0].applied_rule_count == 2
        assert summaries[0].check_count == 3
        assert summaries[1].applied_rule_count == 0
        assert summaries[1].check_count == 0

    def test_filters_by_status_pushed_to_sql(self, svc, sql):
        sql.query.return_value = []
        svc.list_monitored_tables(status="approved")
        list_sql = sql.query.call_args[0][0]
        assert "status = 'approved'" in list_sql

    def test_filters_by_steward_pushed_to_sql(self, svc, sql):
        sql.query.return_value = []
        svc.list_monitored_tables(steward="bob@x")
        list_sql = sql.query.call_args[0][0]
        assert "steward = 'bob@x'" in list_sql

    def test_filters_by_catalog_in_python(self, svc, sql):
        sql.query.side_effect = [
            [
                _table_row(binding_id="b1", table_fqn="cat1.schema.tbl"),
                _table_row(binding_id="b2", table_fqn="cat2.schema.tbl"),
            ],
            [["0"]],  # applied-rule count for the one row surviving the filter
            [["0"]],  # materialized-check count for the one row surviving the filter
        ]
        summaries = svc.list_monitored_tables(catalog="cat1")
        assert len(summaries) == 1
        assert summaries[0].table.table_fqn == "cat1.schema.tbl"

    def test_filters_by_name_substring(self, svc, sql):
        sql.query.side_effect = [
            [
                _table_row(binding_id="b1", table_fqn="cat.schema.orders"),
                _table_row(binding_id="b2", table_fqn="cat.schema.customers"),
            ],
            [["0"]],  # applied-rule count for the one row surviving the filter
            [["0"]],  # materialized-check count for the one row surviving the filter
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
# set_status (submit-for-review lifecycle)
# ---------------------------------------------------------------------------


class TestSetStatus:
    def test_flips_binding_to_pending_approval(self, svc, sql):
        sql.query.return_value = [_table_row(binding_id="b1", status="draft")]
        table = svc.set_status("b1", "pending_approval", "alice@x")
        assert table.status == "pending_approval"
        update_sql = sql.execute.call_args[0][0]
        assert "UPDATE dqx_test.dqx_app_test.dq_monitored_tables" in update_sql
        assert "status = 'pending_approval'" in update_sql

    def test_flips_binding_to_approved(self, svc, sql):
        sql.query.return_value = [_table_row(binding_id="b1", status="pending_approval")]
        table = svc.set_status("b1", "approved", "alice@x")
        assert table.status == "approved"
        assert "status = 'approved'" in sql.execute.call_args[0][0]

    def test_rejects_invalid_status(self, svc, sql):
        with pytest.raises(ValueError):
            svc.set_status("b1", "published", "alice@x")
        sql.execute.assert_not_called()

    def test_raises_when_missing(self, svc, sql):
        sql.query.return_value = []
        with pytest.raises(RuntimeError):
            svc.set_status("missing", "approved", "alice@x")
        sql.execute.assert_not_called()


# ---------------------------------------------------------------------------
# update_schedule (P21 item 14)
# ---------------------------------------------------------------------------


class TestUpdateSchedule:
    def test_sets_schedule_without_touching_status(self, svc, sql):
        sql.query.return_value = [_table_row(binding_id="b1", status="approved")]
        table = svc.update_schedule("b1", "0 6 * * *", "UTC", "alice@x")
        assert table.schedule_cron == "0 6 * * *"
        assert table.schedule_tz == "UTC"
        update_sql = sql.execute.call_args[0][0]
        assert "schedule_cron = '0 6 * * *'" in update_sql
        assert "schedule_tz = 'UTC'" in update_sql
        # Schedule is orthogonal to the review lifecycle — status is untouched.
        assert "status =" not in update_sql

    def test_clearing_cron_also_nulls_timezone(self, svc, sql):
        sql.query.return_value = [_table_row(binding_id="b1", status="approved", schedule_cron="0 6 * * *")]
        table = svc.update_schedule("b1", None, "UTC", "alice@x")
        assert table.schedule_cron is None
        assert table.schedule_tz is None
        update_sql = sql.execute.call_args[0][0]
        assert "schedule_cron = NULL" in update_sql
        assert "schedule_tz = NULL" in update_sql

    def test_raises_when_missing(self, svc, sql):
        sql.query.return_value = []
        with pytest.raises(RuntimeError):
            svc.update_schedule("missing", "0 6 * * *", "UTC", "alice@x")
        sql.execute.assert_not_called()


# ---------------------------------------------------------------------------
# list_materialized_rule_statuses (binding -> materialized rows linkage)
# ---------------------------------------------------------------------------


class TestListMaterializedRuleStatuses:
    def test_resolves_rows_via_applied_rule_id_linkage(self, svc, sql):
        # 1st query: applied-rule ids for the binding; 2nd: their materialized rows.
        sql.query.side_effect = [
            [["ar1"], ["ar2"]],
            [["ar1-0", "pending_approval"], ["ar2-0", "approved"]],
        ]
        result = svc.list_materialized_rule_statuses("b1")
        assert result == [("ar1-0", "pending_approval"), ("ar2-0", "approved")]
        row_query = sql.query.call_args_list[1][0][0]
        # Precise linkage — filters by applied_rule_id, never table_fqn.
        assert "applied_rule_id IN ('ar1', 'ar2')" in row_query
        assert "table_fqn" not in row_query

    def test_returns_empty_when_no_applied_rules(self, svc, sql):
        sql.query.return_value = []
        assert svc.list_materialized_rule_statuses("b1") == []
        # No second query fired when there are no applied rules.
        assert sql.query.call_count == 1


# ---------------------------------------------------------------------------
# rollup_status (P23 item 1 — post-republish binding status sync)
# ---------------------------------------------------------------------------


def _rollup_dispatch(sql, *, table_status: str, materialized: list[list[str]]) -> None:
    """Wire ``sql.query`` for a ``rollup_status`` call.

    Serves the ``_get`` table row, the applied-rule ids, and the
    materialized ``(rule_id, status)`` rows off the emitted SQL.
    """

    def _fn(query: str, **_kwargs):
        if "FROM dqx_test.dqx_app_test.dq_monitored_tables" in query:
            return [_table_row(binding_id="b1", status=table_status)]
        if "FROM dqx_test.dqx_app_test.dq_applied_rules" in query:
            return [["ar1"], ["ar2"]]
        if "FROM dqx_test.dqx_app_test.dq_quality_rules" in query:
            return materialized
        return []

    sql.query.side_effect = _fn


class TestRollupStatus:
    def test_pending_check_moves_approved_binding_to_pending(self, svc, sql):
        # An approved binding whose republish left one check pending must be
        # rolled up to pending_approval (P23 item 1 regression).
        _rollup_dispatch(
            sql,
            table_status="approved",
            materialized=[["ar1-0", "pending_approval"], ["ar2-0", "approved"]],
        )
        table = svc.rollup_status("b1", "alice@x")
        assert table is not None
        assert table.status == "pending_approval"
        assert any("status = 'pending_approval'" in c.args[0] for c in sql.execute.call_args_list)

    def test_all_approved_is_idempotent_noop(self, svc, sql):
        _rollup_dispatch(
            sql,
            table_status="approved",
            materialized=[["ar1-0", "approved"], ["ar2-0", "approved"]],
        )
        table = svc.rollup_status("b1", "alice@x")
        assert table is not None
        assert table.status == "approved"
        # Already-matching target => no UPDATE emitted.
        sql.execute.assert_not_called()

    def test_no_materialized_checks_leaves_status_untouched(self, svc, sql):
        _rollup_dispatch(sql, table_status="approved", materialized=[])

        def _fn(query: str, **_kwargs):
            if "FROM dqx_test.dqx_app_test.dq_monitored_tables" in query:
                return [_table_row(binding_id="b1", status="approved")]
            return []  # no applied rules

        sql.query.side_effect = _fn
        table = svc.rollup_status("b1", "alice@x")
        assert table is not None
        assert table.status == "approved"
        sql.execute.assert_not_called()

    def test_missing_binding_returns_none(self, svc, sql):
        sql.query.return_value = []
        assert svc.rollup_status("missing", "alice@x") is None
        sql.execute.assert_not_called()


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
