"""Tests for :class:`ScoreCacheService` — the P3.4 Lakebase score cache.

Drives the service against two spec-bound ``SqlExecutor`` mocks (the OLTP
executor owning ``dq_score_cache`` + membership lookups, and the SP
warehouse executor the batched metric-view recompute runs on) — no Spark
or workspace needed. Pins:

- the batched recompute query (ONE warehouse round-trip: IN-list over the
  fqns, ``run_mode = 'published'`` filter, latest-run-per-table window);
- the per-scope upserts (NULL-score row for never-run tables, rounded
  scores, ``current_timestamp()`` computed_at, CAST'd run_time);
- the derived product/global recomputes staying entirely OLTP-side;
- the run-completion orchestration (tables -> containing products ->
  global) the refresh-scores route calls;
- ``get_many`` parsing.
"""

from __future__ import annotations

from unittest.mock import MagicMock, create_autospec

import pytest

from databricks_labs_dqx_app.backend.services.score_cache_service import (
    GLOBAL_SCOPE_KEY,
    CachedScore,
    ScoreCacheService,
    parse_cached_score,
)
from databricks_labs_dqx_app.backend.sql_executor import RawSql, SqlExecutor

_CACHE = "dqx_test.dqx_app_test.dq_score_cache"
_MEMBERS = "dqx_test.dqx_app_test.dq_data_product_members"
_MONITORED = "dqx_test.dqx_app_test.dq_monitored_tables"

FQN_A = "main.sales.orders"
FQN_B = "main.sales.customers"


def _mock_executor() -> MagicMock:
    mock = create_autospec(SqlExecutor, instance=True)
    mock.catalog = "dqx_test"
    mock.schema = "dqx_app_test"
    mock.dialect = "delta"
    mock.fqn.side_effect = lambda t: f"dqx_test.dqx_app_test.{t}"
    mock.ts_text.side_effect = lambda c: f"CAST({c} AS STRING)"
    mock.query.return_value = []
    mock.query_dicts.return_value = []
    return mock


@pytest.fixture
def oltp() -> MagicMock:
    return _mock_executor()


@pytest.fixture
def warehouse() -> MagicMock:
    return _mock_executor()


@pytest.fixture
def svc(oltp, warehouse) -> ScoreCacheService:
    return ScoreCacheService(oltp=oltp, warehouse_sql=warehouse)


def _measure_row(
    fqn: str,
    run_id: str = "r1",
    run_time: str = "2026-07-10 08:00:00",
    score: str | None = "0.98765",
    failed_tests: str | None = "12",
    total_tests: str | None = "1000",
) -> dict[str, str | None]:
    """One batched-recompute result row, Statement-Execution shaped (strings)."""
    return {
        "input_location": fqn,
        "run_id": run_id,
        "run_time": run_time,
        "score": score,
        "failed_tests": failed_tests,
        "total_tests": total_tests,
    }


def _upsert_by_key(oltp: MagicMock) -> dict[tuple[str, str], dict[str, object]]:
    """Map ``(scope_type, scope_key) -> value_cols`` over every upsert call."""
    out: dict[tuple[str, str], dict[str, object]] = {}
    for call in oltp.upsert.call_args_list:
        table, key_cols, value_cols = call[0]
        assert table == _CACHE
        out[(key_cols["scope_type"], key_cols["scope_key"])] = value_cols
    return out


class TestRefreshForTables:
    def test_one_batched_warehouse_query_published_only_latest_run_window(self, svc, warehouse):
        svc.refresh_for_tables([FQN_A, FQN_B])
        assert warehouse.query_dicts.call_count == 1
        stmt = warehouse.query_dicts.call_args[0][0]
        assert "`dqx_test`.`dqx_app_test`.mv_dq_scores" in stmt
        assert f"input_location IN ('{FQN_A}', '{FQN_B}')" in stmt
        assert "run_mode = 'published'" in stmt
        assert "MEASURE(score)" in stmt
        assert "QUALIFY ROW_NUMBER() OVER (PARTITION BY input_location ORDER BY run_time DESC) = 1" in stmt

    def test_upserts_one_table_row_per_fqn(self, svc, oltp, warehouse):
        warehouse.query_dicts.return_value = [_measure_row(FQN_A)]
        refreshed = svc.refresh_for_tables([FQN_A])
        assert refreshed == 1
        values = _upsert_by_key(oltp)[("table", FQN_A)]
        assert values["score"] == 0.9877  # rounded to 4 places
        assert values["failed_tests"] == 12
        assert values["total_tests"] == 1000
        assert values["latest_run_id"] == "r1"
        run_time = values["run_time"]
        assert isinstance(run_time, RawSql)
        assert run_time.expr == "CAST('2026-07-10 08:00:00' AS TIMESTAMP)"
        computed_at = values["computed_at"]
        assert isinstance(computed_at, RawSql)
        assert computed_at.expr == "current_timestamp()"

    def test_table_with_no_published_run_still_gets_a_null_score_row(self, svc, oltp, warehouse):
        """'Computed, nothing found' must be distinguishable from 'never
        computed' — the row exists (computed_at set) with a NULL score."""
        warehouse.query_dicts.return_value = [_measure_row(FQN_A)]
        refreshed = svc.refresh_for_tables([FQN_A, FQN_B])
        assert refreshed == 2
        values = _upsert_by_key(oltp)[("table", FQN_B)]
        assert values["score"] is None
        assert values["failed_tests"] is None
        assert values["total_tests"] is None
        assert values["latest_run_id"] is None
        assert values["run_time"] is None
        assert isinstance(values["computed_at"], RawSql)

    def test_invalid_fqns_are_dropped_before_interpolation(self, svc, oltp, warehouse):
        svc.refresh_for_tables(["bad`fqn", FQN_A])
        stmt = warehouse.query_dicts.call_args[0][0]
        assert "bad`fqn" not in stmt
        assert FQN_A in stmt
        assert ("table", FQN_A) in _upsert_by_key(oltp)
        assert len(oltp.upsert.call_args_list) == 1

    def test_all_invalid_short_circuits_without_warehouse_call(self, svc, oltp, warehouse):
        assert svc.refresh_for_tables(["not-an-fqn"]) == 0
        warehouse.query_dicts.assert_not_called()
        oltp.upsert.assert_not_called()

    def test_duplicate_fqns_are_deduped(self, svc, oltp, warehouse):
        warehouse.query_dicts.return_value = [_measure_row(FQN_A)]
        assert svc.refresh_for_tables([FQN_A, FQN_A]) == 1
        stmt = warehouse.query_dicts.call_args[0][0]
        assert stmt.count(f"'{FQN_A}'") == 1
        assert len(oltp.upsert.call_args_list) == 1


class TestRefreshProduct:
    def test_derives_from_cached_table_rows_oltp_only(self, svc, oltp, warehouse):
        oltp.query_dicts.return_value = [{"score": "0.75", "failed_tests": "5", "total_tests": "20"}]
        svc.refresh_product("p1")
        warehouse.query_dicts.assert_not_called()  # never hits the warehouse
        stmt = oltp.query_dicts.call_args[0][0]
        assert "AVG(sc.score)" in stmt
        assert _MEMBERS in stmt
        assert _MONITORED in stmt
        assert _CACHE in stmt
        assert "m.product_id = 'p1'" in stmt
        assert "sc.score IS NOT NULL" in stmt
        values = _upsert_by_key(oltp)[("product", "p1")]
        assert values["score"] == 0.75
        assert values["failed_tests"] == 5
        assert values["total_tests"] == 20
        assert values["latest_run_id"] is None
        assert values["run_time"] is None

    def test_product_with_no_scored_members_gets_null_score_row(self, svc, oltp):
        oltp.query_dicts.return_value = [{"score": None, "failed_tests": None, "total_tests": None}]
        svc.refresh_product("p1")
        values = _upsert_by_key(oltp)[("product", "p1")]
        assert values["score"] is None
        assert isinstance(values["computed_at"], RawSql)


class TestRefreshGlobal:
    def test_aggregates_all_cached_table_rows(self, svc, oltp, warehouse):
        oltp.query_dicts.return_value = [{"score": "0.912345", "failed_tests": "7", "total_tests": "70"}]
        svc.refresh_global()
        warehouse.query_dicts.assert_not_called()
        stmt = oltp.query_dicts.call_args[0][0]
        assert "scope_type = 'table'" in stmt
        assert "score IS NOT NULL" in stmt
        values = _upsert_by_key(oltp)[("global", GLOBAL_SCOPE_KEY)]
        assert values["score"] == 0.9123


class TestRefreshAllForTables:
    def test_orchestrates_tables_then_containing_products_then_global(self, svc, oltp, warehouse):
        warehouse.query_dicts.return_value = [_measure_row(FQN_A)]
        # First OLTP query: product ids containing the table; the two
        # query_dicts calls after it are the product + global aggregates.
        oltp.query.return_value = [["p1"], ["p2"]]
        oltp.query_dicts.return_value = [{"score": "0.5", "failed_tests": "1", "total_tests": "2"}]
        refreshed_tables, refreshed_products = svc.refresh_all_for_tables([FQN_A])
        assert refreshed_tables == 1
        assert refreshed_products == 2
        upserts = _upsert_by_key(oltp)
        assert ("table", FQN_A) in upserts
        assert ("product", "p1") in upserts
        assert ("product", "p2") in upserts
        assert ("global", GLOBAL_SCOPE_KEY) in upserts
        # Membership lookup goes through the binding join, batched.
        membership_stmt = oltp.query.call_args[0][0]
        assert _MEMBERS in membership_stmt
        assert _MONITORED in membership_stmt
        assert f"mt.table_fqn IN ('{FQN_A}')" in membership_stmt

    def test_no_valid_tables_still_refreshes_global_only(self, svc, oltp, warehouse):
        refreshed_tables, refreshed_products = svc.refresh_all_for_tables(["bad-fqn"])
        assert (refreshed_tables, refreshed_products) == (0, 0)
        warehouse.query_dicts.assert_not_called()
        upserts = _upsert_by_key(oltp)
        assert list(upserts) == [("global", GLOBAL_SCOPE_KEY)]


class TestGetMany:
    def test_parses_rows(self, svc, oltp):
        oltp.query_dicts.return_value = [
            {
                "scope_key": FQN_A,
                "score": "0.9",
                "failed_tests": "3",
                "total_tests": "30",
                "latest_run_id": "r9",
                "run_time": "2026-07-10 08:00:00",
                "computed_at": "2026-07-10 08:01:00",
            }
        ]
        out = svc.get_many("table", [FQN_A, FQN_B])
        assert out == {
            FQN_A: CachedScore(
                score=0.9,
                failed_tests=3,
                total_tests=30,
                latest_run_id="r9",
                run_time="2026-07-10 08:00:00",
                computed_at="2026-07-10 08:01:00",
            )
        }
        stmt = oltp.query_dicts.call_args[0][0]
        assert "scope_type = 'table'" in stmt
        assert f"scope_key IN ('{FQN_A}', '{FQN_B}')" in stmt

    def test_empty_keys_short_circuit(self, svc, oltp):
        assert svc.get_many("table", []) == {}
        oltp.query_dicts.assert_not_called()


class TestParseCachedScore:
    def test_coerces_statement_execution_strings(self):
        cached = parse_cached_score("0.5", "1", "2", "2026-07-10T00:00:00")
        assert cached == CachedScore(score=0.5, failed_tests=1, total_tests=2, computed_at="2026-07-10T00:00:00")

    def test_all_none_yields_empty_cached_score(self):
        assert parse_cached_score(None, None, None, None) == CachedScore()

    def test_malformed_numbers_become_none(self):
        cached = parse_cached_score("not-a-number", "x", "y", None)
        assert cached.score is None
        assert cached.failed_tests is None
        assert cached.total_tests is None


_HISTORY = "dqx_test.dqx_app_test.dq_score_history"


def _history_inserts(oltp: MagicMock) -> list[str]:
    """Every INSERT statement targeting the history table, in order."""
    return [
        call[0][0]
        for call in oltp.execute.call_args_list
        if call[0][0].lstrip().upper().startswith("INSERT") and _HISTORY in call[0][0]
    ]


def _history_trims(oltp: MagicMock) -> list[str]:
    """Every DELETE (trim) statement targeting the history table, in order."""
    return [
        call[0][0]
        for call in oltp.execute.call_args_list
        if call[0][0].lstrip().upper().startswith("DELETE") and _HISTORY in call[0][0]
    ]


class TestScoreHistoryAppend:
    """P3.5 follow-on: every scored upsert appends a `dq_score_history` row."""

    def test_global_refresh_appends_a_history_row(self, svc, oltp):
        oltp.query_dicts.return_value = [{"score": "0.912345", "failed_tests": "7", "total_tests": "70"}]
        svc.refresh_global()
        inserts = _history_inserts(oltp)
        assert len(inserts) == 1
        stmt = inserts[0]
        assert "'global', 'global'" in stmt
        assert "0.9123" in stmt
        assert " 7," in stmt
        assert " 70," in stmt
        assert "now()" in stmt

    def test_table_refresh_appends_history_with_run_time(self, svc, oltp, warehouse):
        warehouse.query_dicts.return_value = [_measure_row(FQN_A)]
        svc.refresh_for_tables([FQN_A])
        inserts = _history_inserts(oltp)
        assert len(inserts) == 1
        assert f"'table', '{FQN_A}'" in inserts[0]
        assert "CAST('2026-07-10 08:00:00' AS TIMESTAMP)" in inserts[0]

    def test_null_score_upserts_do_not_pollute_history(self, svc, oltp, warehouse):
        """'Computed, nothing found' rows update the cache but must not
        append NULL points to the trend."""
        warehouse.query_dicts.return_value = []
        svc.refresh_for_tables([FQN_A])
        assert _history_inserts(oltp) == []
        assert _history_trims(oltp) == []

    def test_append_trims_to_the_keep_cap_per_scope(self, svc, oltp):
        oltp.query_dicts.return_value = [{"score": "0.5", "failed_tests": "1", "total_tests": "2"}]
        svc.refresh_global()
        trims = _history_trims(oltp)
        assert len(trims) == 1
        stmt = trims[0]
        assert "scope_type = 'global'" in stmt
        assert "scope_key = 'global'" in stmt
        # count-trim: keep the newest HISTORY_KEEP_ROWS rows.
        assert "ORDER BY computed_at DESC LIMIT 200" in stmt
        assert "computed_at <" in stmt

    def test_scope_key_is_escaped_in_history_statements(self, svc, oltp):
        oltp.query_dicts.return_value = [{"score": "0.5", "failed_tests": "1", "total_tests": "2"}]
        svc.refresh_product("p'1")
        for stmt in _history_inserts(oltp) + _history_trims(oltp):
            assert "p''1" in stmt
            assert "'p'1'" not in stmt


class TestGetHistory:
    def test_reads_newest_first_and_returns_ascending(self, svc, oltp):
        oltp.query_dicts.return_value = [
            {
                "score": "0.9",
                "failed_tests": "1",
                "total_tests": "10",
                "run_time": None,
                "computed_at": "2026-07-10 09:00:00",
            },
            {
                "score": "0.8",
                "failed_tests": "2",
                "total_tests": "10",
                "run_time": None,
                "computed_at": "2026-07-09 09:00:00",
            },
        ]
        points = svc.get_history("global", GLOBAL_SCOPE_KEY, limit=30)
        stmt = oltp.query_dicts.call_args[0][0]
        assert _HISTORY in stmt
        assert "scope_type = 'global'" in stmt
        assert "scope_key = 'global'" in stmt
        assert "ORDER BY computed_at DESC LIMIT 30" in stmt
        # DESC read, ascending return — oldest first for charting.
        assert [p.score for p in points] == [0.8, 0.9]
        assert points[0].computed_at == "2026-07-09 09:00:00"
        assert points[1] == CachedScore(score=0.9, failed_tests=1, total_tests=10, computed_at="2026-07-10 09:00:00")

    def test_empty_history(self, svc, oltp):
        oltp.query_dicts.return_value = []
        assert svc.get_history("global", GLOBAL_SCOPE_KEY) == []
