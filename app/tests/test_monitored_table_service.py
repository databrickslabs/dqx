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
    last_run_at: str | None = None,
    schedule_kind: str | None = "profiling_and_dq",
    score: str | None = None,
    failed_tests: str | None = None,
    total_tests: str | None = None,
    score_computed_at: str | None = None,
) -> list[str]:
    # ``schedule_kind`` (B2-52) is at index 13; the trailing 4 cells are the
    # dq_score_cache LEFT-JOIN columns the list query selects (P3.4, indices
    # 14..17) — all None when the table has never been scored. The single-row
    # read paths select only the first 14 columns; the extra cells are simply
    # ignored by _row_to_table.
    return [
        binding_id,
        table_fqn,
        steward,
        status,
        version,
        schedule_cron,
        schedule_tz,
        last_profiled_at,
        last_run_at,
        "alice@x",
        "2026-07-02T00:00:00+00:00",
        "alice@x",
        "2026-07-02T00:00:00+00:00",
        schedule_kind,
        score,
        failed_tests,
        total_tests,
        score_computed_at,
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

    def test_insert_writes_concrete_schedule_kind(self, svc, sql):
        # Regression: the Delta CHECK constraint
        # chk_dq_monitored_tables_schedule_kind rejects a NULL schedule_kind on
        # insert, so registration must write the concrete enum default rather
        # than omitting the column (which defaulted it to NULL and failed).
        sql.query.return_value = []
        svc.register("cat.schema.tbl", "alice@x")
        insert_sql = sql.execute.call_args[0][0]
        assert "schedule_kind" in insert_sql
        assert "'dq_only'" in insert_sql

    def test_defaults_steward_to_creator_when_unset(self, svc, sql):
        # No steward supplied (owner unresolved upstream) -> the creator becomes
        # the accountable steward so no binding is ever left ownerless.
        sql.query.return_value = []
        table = svc.register("cat.schema.tbl", "alice@x")
        assert table.steward == "alice@x"
        insert_sql = sql.execute.call_args[0][0]
        assert "alice@x" in insert_sql

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

    def test_defaults_steward_to_creator_when_unset(self, svc, sql):
        # Bulk register with no shared steward -> each binding defaults to the
        # creator (no per-table UC owner lookup on the bulk path).
        sql.query.return_value = []
        svc.bulk_register(["cat.schema.new1"], "alice@x")
        insert_sql = sql.execute.call_args[0][0]
        assert "alice@x" in insert_sql

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
            [
                _table_row(binding_id="b1", table_fqn="cat.schema.t1"),
                _table_row(binding_id="b2", table_fqn="cat.schema.t2"),
            ],
            [["b1", "2"]],  # grouped applied-rule counts (b2 absent -> 0)
            [["cat.schema.t1", "3"]],  # grouped materialized-check counts (t2 absent -> 0)
        ]
        summaries = svc.list_monitored_tables()
        assert len(summaries) == 2
        assert summaries[0].applied_rule_count == 2
        assert summaries[0].check_count == 3
        assert summaries[1].applied_rule_count == 0
        assert summaries[1].check_count == 0
        # Counts are batched: bindings query + ONE grouped query per count
        # kind, regardless of how many bindings are listed (no per-binding N+1).
        assert sql.query.call_count == 3
        applied_sql = sql.query.call_args_list[1][0][0]
        assert "GROUP BY binding_id" in applied_sql
        assert "IN ('b1', 'b2')" in applied_sql
        checks_sql = sql.query.call_args_list[2][0][0]
        assert "GROUP BY table_fqn" in checks_sql
        assert "IN ('cat.schema.t1', 'cat.schema.t2')" in checks_sql
        assert "status != 'rejected'" in checks_sql

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
            [],  # grouped applied-rule counts — none for the surviving row
            [],  # grouped materialized-check counts — none for the surviving row
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
            [],  # grouped applied-rule counts — none for the surviving row
            [],  # grouped materialized-check counts — none for the surviving row
        ]
        summaries = svc.list_monitored_tables(name="order")
        assert len(summaries) == 1
        assert summaries[0].table.table_fqn == "cat.schema.orders"

    def test_list_left_joins_score_cache_in_same_round_trip(self, svc, sql):
        """P3.4: the cached score columns ride along the bindings query —
        no extra round trip and NEVER a warehouse recompute on page load."""
        sql.query.side_effect = [
            [
                _table_row(
                    binding_id="b1",
                    score="0.9876",
                    failed_tests="12",
                    total_tests="1000",
                    score_computed_at="2026-07-10T00:00:00",
                )
            ],
            [["b1", "2"]],  # grouped applied-rule counts
            [["cat.schema.tbl", "3"]],  # grouped materialized-check counts
        ]
        summaries = svc.list_monitored_tables()
        list_sql = sql.query.call_args_list[0][0][0]
        assert "LEFT JOIN dqx_test.dqx_app_test.dq_score_cache" in list_sql
        assert "sc.scope_type = 'table'" in list_sql
        assert "sc.scope_key = mt.table_fqn" in list_sql
        summary = summaries[0]
        assert summary.score == 0.9876
        assert summary.failed_tests == 12
        assert summary.total_tests == 1000
        assert summary.score_computed_at == "2026-07-10T00:00:00"

    def test_list_score_fields_none_when_never_scored(self, svc, sql):
        sql.query.side_effect = [
            [_table_row(binding_id="b1")],
            [],  # grouped applied-rule counts
            [],  # grouped materialized-check counts
        ]
        summary = svc.list_monitored_tables()[0]
        assert summary.score is None
        assert summary.failed_tests is None
        assert summary.total_tests is None
        assert summary.score_computed_at is None


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


class TestRunTimestampsReadFromOltp:
    """`last_profiled_at` / `last_run_at` are read straight off the OLTP row
    (T-perf): they are denormalized on run/profiler completion by
    ``refresh_run_timestamps``, so the list/detail read paths must NOT touch
    the always-Delta ``dq_profiling_results`` / ``dq_validation_runs`` tables.
    """

    def test_get_reads_timestamps_from_oltp_without_warehouse(self, svc, sql, profiling_sql):
        sql.query.side_effect = [
            [
                _table_row(
                    binding_id="b1",
                    table_fqn="cat.schema.tbl",
                    last_profiled_at="2026-07-10 12:00:00",
                    last_run_at="2026-07-11 09:00:00",
                )
            ],
            [],  # no applied rules
        ]
        detail = svc.get("b1")

        assert detail is not None
        assert detail.table.last_profiled_at is not None
        assert detail.table.last_profiled_at.year == 2026
        assert detail.table.last_run_at is not None
        assert detail.table.last_run_at.day == 11
        # The detail load never touches the warehouse executor.
        profiling_sql.query.assert_not_called()

    def test_list_reads_timestamps_from_oltp_without_warehouse(self, svc, sql, profiling_sql):
        sql.query.side_effect = [
            [
                _table_row(binding_id="b1", table_fqn="cat.s.t1", last_run_at="2026-07-09 08:00:00"),
                _table_row(binding_id="b2", table_fqn="cat.s.t2"),
            ],
            [],  # applied-rule counts
            [],  # materialized-check counts
        ]
        summaries = svc.list_monitored_tables()

        by_id = {s.table.binding_id: s for s in summaries}
        assert by_id["b1"].table.last_run_at is not None
        assert by_id["b1"].table.last_run_at.year == 2026
        assert by_id["b2"].table.last_run_at is None
        # P3.4/T-perf contract: a page load NEVER hits the warehouse executor.
        profiling_sql.query.assert_not_called()


class TestRefreshRunTimestamps:
    """Write-on-complete: derive last_run_at / last_profiled_at from their Delta
    sources and denormalize them into the OLTP ``dq_monitored_tables`` row.
    """

    def test_writes_both_columns_from_delta_sources(self, svc, sql, profiling_sql):
        profiling_sql.ts_text.side_effect = lambda c: f"CAST({c} AS STRING)"
        # 1st profiling_sql.query = validation-runs MAX; 2nd = profiling MAX.
        profiling_sql.query.side_effect = [
            [["cat.s.t1", "2026-07-11 09:00:00"]],
            [["cat.s.t1", "2026-07-10 12:00:00"]],
        ]
        written = svc.refresh_run_timestamps(["cat.s.t1"])

        assert written == 1
        # last_run_at is read from dq_validation_runs, terminal + non-preview.
        run_sql = profiling_sql.query.call_args_list[0][0][0]
        assert "dq_validation_runs" in run_sql
        assert "UPPER(status) <> 'RUNNING'" in run_sql
        assert "run_type" in run_sql and "preview" in run_sql
        assert "GROUP BY source_table_fqn" in run_sql
        # One OLTP UPDATE that sets BOTH columns and never touches updated_at.
        sql.execute.assert_called_once()
        update_sql = sql.execute.call_args[0][0]
        assert "UPDATE dqx_test.dqx_app_test.dq_monitored_tables" in update_sql
        assert "last_run_at = CAST(" in update_sql
        assert "last_profiled_at = CAST(" in update_sql
        assert "updated_at" not in update_sql
        assert "WHERE table_fqn = 'cat.s.t1'" in update_sql

    def test_skips_tables_with_no_runs_and_no_profiles(self, svc, sql, profiling_sql):
        profiling_sql.ts_text.side_effect = lambda c: f"CAST({c} AS STRING)"
        profiling_sql.query.side_effect = [[], []]  # neither a run nor a profile
        written = svc.refresh_run_timestamps(["cat.s.t1"])
        assert written == 0
        sql.execute.assert_not_called()

    def test_writes_null_for_missing_side(self, svc, sql, profiling_sql):
        # A table with a run but no successful profile writes last_run_at and
        # NULLs last_profiled_at (idempotent, self-healing).
        profiling_sql.ts_text.side_effect = lambda c: f"CAST({c} AS STRING)"
        profiling_sql.query.side_effect = [
            [["cat.s.t1", "2026-07-11 09:00:00"]],  # validation run
            [],  # no profile
        ]
        written = svc.refresh_run_timestamps(["cat.s.t1"])
        assert written == 1
        update_sql = sql.execute.call_args[0][0]
        assert "last_run_at = CAST(" in update_sql
        assert "last_profiled_at = NULL" in update_sql

    def test_drops_invalid_fqns_before_interpolation(self, svc, sql, profiling_sql):
        profiling_sql.ts_text.side_effect = lambda c: f"CAST({c} AS STRING)"
        profiling_sql.query.side_effect = [[], []]
        written = svc.refresh_run_timestamps(["cat.s.evil\\", "not-three-parts"])
        assert written == 0
        # All inputs invalid -> no Delta lookups, no OLTP write.
        profiling_sql.query.assert_not_called()
        sql.execute.assert_not_called()

    def test_empty_input_short_circuits(self, svc, sql, profiling_sql):
        assert svc.refresh_run_timestamps([]) == 0
        profiling_sql.query.assert_not_called()
        sql.execute.assert_not_called()


class TestGetBindingIdsByTableFqn:
    """Batched ``table_fqn -> binding_id`` lookup for the by_table axis."""

    def test_maps_fqns_to_binding_ids_in_one_query(self, svc, sql):
        sql.query.return_value = [["main.a.t1", "b1"], ["main.a.t2", "b2"]]
        out = svc.get_binding_ids_by_table_fqn(["main.a.t1", "main.a.t2", "main.a.unmonitored"])
        assert out == {"main.a.t1": "b1", "main.a.t2": "b2"}
        assert sql.query.call_count == 1
        stmt = sql.query.call_args[0][0]
        assert "SELECT table_fqn, binding_id FROM dqx_test.dqx_app_test.dq_monitored_tables" in stmt
        assert "IN ('main.a.t1', 'main.a.t2', 'main.a.unmonitored')" in stmt

    def test_empty_input_short_circuits_without_sql(self, svc, sql):
        assert svc.get_binding_ids_by_table_fqn([]) == {}
        sql.query.assert_not_called()

    def test_invalid_fqns_are_dropped_before_interpolation(self, svc, sql):
        # Inputs can be warehouse-sourced (dq_metrics.input_location):
        # anything failing validate_fqn never reaches the IN list.
        sql.query.return_value = [["main.a.t1", "b1"]]
        out = svc.get_binding_ids_by_table_fqn(["main.a.t1", "main.a.evil\\", "not-three-parts"])
        assert out == {"main.a.t1": "b1"}
        stmt = sql.query.call_args[0][0]
        assert "evil" not in stmt
        assert "not-three-parts" not in stmt

    def test_all_invalid_input_short_circuits_without_sql(self, svc, sql):
        assert svc.get_binding_ids_by_table_fqn(["main.a.evil\\"]) == {}
        sql.query.assert_not_called()


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

    def test_persists_schedule_kind(self, svc, sql):
        sql.query.return_value = [_table_row(binding_id="b1", status="approved")]
        table = svc.update_schedule("b1", "0 6 * * *", "UTC", "alice@x", schedule_kind="profiling_only")
        assert table.schedule_kind == "profiling_only"
        update_sql = sql.execute.call_args[0][0]
        assert "schedule_kind = 'profiling_only'" in update_sql

    def test_defaults_schedule_kind_to_dq_only(self, svc, sql):
        sql.query.return_value = [_table_row(binding_id="b1", status="approved")]
        table = svc.update_schedule("b1", "0 6 * * *", "UTC", "alice@x")
        assert table.schedule_kind == "dq_only"
        assert "schedule_kind = 'dq_only'" in sql.execute.call_args[0][0]

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


class TestListAppliedRulesMany:
    """``list_applied_rules_many`` — batched applied-rule fetch for the draft check-count path (B2-141)."""

    def test_groups_by_binding_in_one_query(self, svc, sql):
        sql.query.return_value = [
            _applied_row(id_="ar1", binding_id="b1", rule_id="r1"),
            _applied_row(id_="ar2", binding_id="b1", rule_id="r2"),
            _applied_row(id_="ar3", binding_id="b2", rule_id="r1"),
        ]
        result = svc.list_applied_rules_many(["b1", "b2", "b1"])  # dup binding collapses
        assert set(result) == {"b1", "b2"}
        assert [a.id for a in result["b1"]] == ["ar1", "ar2"]
        assert [a.id for a in result["b2"]] == ["ar3"]
        # ONE grouped query — never a per-binding round-trip; and NO per-rule
        # tag lookup (the counting path doesn't need name/dimension/severity).
        assert sql.query.call_count == 1
        called_sql = sql.query.call_args[0][0]
        assert "binding_id IN (" in called_sql

    def test_empty_input_issues_no_query(self, svc, sql):
        assert svc.list_applied_rules_many([]) == {}
        sql.query.assert_not_called()

    def test_binding_with_no_rows_absent(self, svc, sql):
        sql.query.return_value = [_applied_row(id_="ar1", binding_id="b1", rule_id="r1")]
        result = svc.list_applied_rules_many(["b1", "b2"])
        assert set(result) == {"b1"}
