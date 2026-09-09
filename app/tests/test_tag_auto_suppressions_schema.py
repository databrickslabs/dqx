"""Schema tests for the tag-auto suppressions table (apply-on-tag).

``dq_tag_auto_suppressions`` ships in the Postgres v1 baseline with the
``(binding_id, rule_id, mapping_hash)`` primary key.
"""

from databricks_labs_dqx_app.backend.migrations.postgres import PG_MIGRATIONS

_SUPPRESSION_COLS = (
    "binding_id",
    "rule_id",
    "mapping_hash",
    "suppressed_by",
    "suppressed_at",
)

_PG_BASELINE = PG_MIGRATIONS[0].sql

_TABLE = "dq_tag_auto_suppressions"


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


class TestTagAutoSuppressionsPostgres:
    def test_columns(self):
        ddl = _create_stmt(_PG_BASELINE, _TABLE)
        for col in _SUPPRESSION_COLS:
            assert col in ddl

    def test_primary_key_is_natural_key(self):
        ddl = _create_stmt(_PG_BASELINE, _TABLE)
        assert "PRIMARY KEY (binding_id, rule_id, mapping_hash)" in ddl

    def test_binding_id_index(self):
        assert "idx_dq_tag_auto_suppressions_binding_id" in _PG_BASELINE
