"""Schema tests for Phase 4B — rule embeddings corpus (``dq_rule_embeddings``).

The table ships on both baselines — the Postgres v1 baseline and the Delta
OLTP fallback — so semantic search over the registry works whichever backend
owns the OLTP tables.
"""

from databricks_labs_dqx_app.backend.migrations import _V2_OLTP_FALLBACK
from databricks_labs_dqx_app.backend.migrations.postgres import PG_MIGRATIONS

_EXPECTED_COLUMNS = ("rule_id", "rule_version", "embed_text", "embedding", "model", "updated_at")

_PG_BASELINE = PG_MIGRATIONS[0].sql

_TABLE = "dq_rule_embeddings"


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


class TestDqRuleEmbeddingsPostgres:
    def test_has_expected_columns(self):
        ddl = _create_stmt(_PG_BASELINE, _TABLE)
        for col in _EXPECTED_COLUMNS:
            assert col in ddl

    def test_primary_key_on_rule_id(self):
        ddl = _create_stmt(_PG_BASELINE, _TABLE)
        assert "rule_id TEXT PRIMARY KEY" in ddl


class TestDqRuleEmbeddingsDelta:
    def test_has_expected_columns(self):
        ddl = _create_stmt(_V2_OLTP_FALLBACK, _TABLE)
        for col in _EXPECTED_COLUMNS:
            assert col in ddl

    def test_primary_key_on_rule_id(self):
        ddl = _create_stmt(_V2_OLTP_FALLBACK, _TABLE)
        assert "PRIMARY KEY (rule_id)" in ddl
