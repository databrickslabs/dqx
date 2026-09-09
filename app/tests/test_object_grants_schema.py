"""Schema tests for the UC-style object-permissions tables (P22-D item 10).

``dq_object_grants`` (+ history) ship in the Postgres v1 OLTP baseline.
"""

from databricks_labs_dqx_app.backend.migrations.postgres import PG_MIGRATIONS

_GRANT_COLS = (
    "object_type",
    "object_id",
    "principal_id",
    "principal_type",
    "principal_name",
    "privileges",
    "inherit",
    "grantor",
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


class TestObjectGrantsPostgres:
    def test_baseline_declares_both_tables(self):
        assert _create_stmt(_PG_BASELINE, "dq_object_grants")
        assert _create_stmt(_PG_BASELINE, "dq_object_grants_history")

    def test_grants_columns(self):
        ddl = _create_stmt(_PG_BASELINE, "dq_object_grants")
        for col in (*_GRANT_COLS, "grant_id"):
            assert col in ddl

    def test_object_type_check_constraint(self):
        ddl = _create_stmt(_PG_BASELINE, "dq_object_grants")
        assert "chk_dq_object_grants_object_type" in ddl
        for object_type in ("'registry_rule'", "'monitored_table'", "'data_product'"):
            assert object_type in ddl

    def test_unique_object_principal(self):
        ddl = _create_stmt(_PG_BASELINE, "dq_object_grants")
        assert "uq_dq_object_grants_object_principal" in ddl

    def test_read_path_index(self):
        # Every permission check reads "all grants for this object".
        assert "idx_dq_object_grants_object" in _PG_BASELINE
