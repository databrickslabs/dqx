"""Schema tests for the DQ score cache + history tables (P3.4 / P3.5).

``dq_score_cache`` and ``dq_score_history`` ship on both baselines — the
Postgres v1 baseline and the Delta OLTP fallback. Both must declare the same
logical columns so the ``ScoreCacheService`` read/write path and the list
endpoints' LEFT JOINs are portable across backends.
"""

from databricks_labs_dqx_app.backend.migrations import _V2_OLTP_FALLBACK
from databricks_labs_dqx_app.backend.migrations.postgres import PG_MIGRATIONS

_SCORE_COLS = (
    "scope_type",
    "scope_key",
    "score",
    "failed_tests",
    "total_tests",
    "latest_run_id",
    "run_time",
    "computed_at",
)

_HISTORY_COLS = (
    "scope_type",
    "scope_key",
    "score",
    "failed_tests",
    "total_tests",
    "run_time",
    "computed_at",
)

_PG_BASELINE = PG_MIGRATIONS[0].sql


def _create_stmt(sql: str, table: str) -> str:
    """The one CREATE TABLE statement for ``table``, whitespace-normalized.

    Scoping assertions to a single statement keeps them meaningful: a bare
    substring check against the whole baseline would pass on a column that
    happens to exist on some unrelated table.
    """
    stmts = [" ".join(s.split()) for s in sql.split(";")]
    matches = [s for s in stmts if s.startswith("CREATE TABLE") and f".{table} (" in s]
    assert len(matches) == 1, f"expected one CREATE TABLE for {table}, found {len(matches)}"
    return matches[0]


class TestScoreCachePostgres:
    def test_columns(self):
        ddl = _create_stmt(_PG_BASELINE, "dq_score_cache")
        for col in _SCORE_COLS:
            assert col in ddl

    def test_scope_type_check_constraint(self):
        ddl = _create_stmt(_PG_BASELINE, "dq_score_cache")
        assert "chk_dq_score_cache_scope_type" in ddl
        for scope in ("'table'", "'product'", "'global'"):
            assert scope in ddl

    def test_primary_key_is_scope_type_scope_key(self):
        ddl = _create_stmt(_PG_BASELINE, "dq_score_cache")
        assert "PRIMARY KEY (scope_type, scope_key)" in ddl


class TestScoreCacheDelta:
    def test_columns(self):
        ddl = _create_stmt(_V2_OLTP_FALLBACK, "dq_score_cache")
        for col in _SCORE_COLS:
            assert col in ddl

    def test_primary_key_is_scope_type_scope_key(self):
        ddl = _create_stmt(_V2_OLTP_FALLBACK, "dq_score_cache")
        assert "PRIMARY KEY (scope_type, scope_key)" in ddl

    def test_check_constraint_follows_the_table(self):
        # Delta accepts only PK/FK inline, so the CHECK arrives as a separate
        # ALTER TABLE … ADD CONSTRAINT statement in the same baseline.
        assert "chk_dq_score_cache_scope_type" in _V2_OLTP_FALLBACK


class TestScoreHistoryPostgres:
    """``dq_score_history`` — append-only score trend rows."""

    def test_columns(self):
        ddl = _create_stmt(_PG_BASELINE, "dq_score_history")
        for col in _HISTORY_COLS:
            assert col in ddl

    def test_read_path_index(self):
        # The only read is "last N points for one scope, newest first".
        assert "idx_dq_score_history_scope_computed_at" in _PG_BASELINE
        assert "(scope_type, scope_key, computed_at DESC)" in _PG_BASELINE


class TestScoreHistoryDelta:
    def test_columns(self):
        ddl = _create_stmt(_V2_OLTP_FALLBACK, "dq_score_history")
        for col in _HISTORY_COLS:
            assert col in ddl

    def test_clustered_on_the_read_path(self):
        # Delta has no secondary indexes; liquid clustering covers the same
        # "last N points for one scope" read the Postgres index serves.
        ddl = _create_stmt(_V2_OLTP_FALLBACK, "dq_score_history")
        assert "CLUSTER BY (scope_type, scope_key, computed_at)" in ddl
