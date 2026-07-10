"""Schema tests for the DQ score cache table (P3.4).

``dq_score_cache`` is added as a new Postgres migration (v11) and the
matching Delta OLTP-fallback template (_V15). Both must declare the same
logical columns so the ``ScoreCacheService`` read/write path and the list
endpoints' LEFT JOINs are portable across backends.
"""

from __future__ import annotations

from databricks_labs_dqx_app.backend.migrations import (
    _V15_SCORE_CACHE,
    _V16_SCORE_HISTORY,
    MIGRATIONS,
    DeltaMigration,
)
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


class TestScoreCachePostgres:
    def test_added_as_new_migration_v11(self):
        v11 = next(m for m in PG_MIGRATIONS if m.version == 11)
        assert "dq_score_cache" in v11.sql

    def test_columns(self):
        v11 = next(m for m in PG_MIGRATIONS if m.version == 11)
        for col in _SCORE_COLS:
            assert col in v11.sql

    def test_scope_type_check_constraint(self):
        v11 = next(m for m in PG_MIGRATIONS if m.version == 11)
        assert "chk_dq_score_cache_scope_type" in v11.sql
        for scope in ("'table'", "'product'", "'global'"):
            assert scope in v11.sql

    def test_primary_key_is_scope_type_scope_key(self):
        v11 = next(m for m in PG_MIGRATIONS if m.version == 11)
        assert "PRIMARY KEY (scope_type, scope_key)" in v11.sql

    def test_versions_monotonic(self):
        versions = [m.version for m in PG_MIGRATIONS]
        assert versions == sorted(versions)
        assert len(versions) == len(set(versions))


class TestScoreCacheDelta:
    def test_delta_fallback_declares_table(self):
        assert "dq_score_cache" in _V15_SCORE_CACHE

    def test_delta_columns(self):
        for col in _SCORE_COLS:
            assert col in _V15_SCORE_CACHE

    def test_delta_scope_type_check_constraint(self):
        assert "chk_dq_score_cache_scope_type" in _V15_SCORE_CACHE

    def test_registered_as_oltp_fallback_migration(self):
        v15 = next(m for m in MIGRATIONS if m.version == 15)
        assert isinstance(v15, DeltaMigration)
        assert v15.oltp_fallback is True
        assert v15.sql_template == _V15_SCORE_CACHE

    def test_versions_monotonic(self):
        versions = [m.version for m in MIGRATIONS]
        assert versions == sorted(versions)
        assert len(versions) == len(set(versions))


_HISTORY_COLS = (
    "scope_type",
    "scope_key",
    "score",
    "failed_tests",
    "total_tests",
    "run_time",
    "computed_at",
)


class TestScoreHistoryPostgres:
    """``dq_score_history`` (P3.5 follow-on) — append-only score trend rows."""

    def test_added_as_new_migration_v12(self):
        v12 = next(m for m in PG_MIGRATIONS if m.version == 12)
        assert "dq_score_history" in v12.sql

    def test_columns(self):
        v12 = next(m for m in PG_MIGRATIONS if m.version == 12)
        for col in _HISTORY_COLS:
            assert col in v12.sql

    def test_read_path_index(self):
        # The only read is "last N points for one scope, newest first".
        v12 = next(m for m in PG_MIGRATIONS if m.version == 12)
        assert "idx_dq_score_history_scope_computed_at" in v12.sql
        assert "(scope_type, scope_key, computed_at DESC)" in v12.sql


class TestScoreHistoryDelta:
    def test_delta_fallback_declares_table(self):
        assert "dq_score_history" in _V16_SCORE_HISTORY

    def test_delta_columns(self):
        for col in _HISTORY_COLS:
            assert col in _V16_SCORE_HISTORY

    def test_registered_as_oltp_fallback_migration_v16(self):
        v16 = next(m for m in MIGRATIONS if m.version == 16)
        assert isinstance(v16, DeltaMigration)
        assert v16.oltp_fallback is True
        assert v16.sql_template == _V16_SCORE_HISTORY
