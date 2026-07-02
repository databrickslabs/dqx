"""Schema tests for Phase 4B — rule embeddings corpus (``dq_rule_embeddings``).

Follows the ``dq_role_mappings_history`` precedent: a genuinely new table
added as its own migration version (Postgres v4, Delta v8) rather than
piggy-backing on the v1/v2 baseline, since it belongs to a distinct phase.
"""

from __future__ import annotations

from databricks_labs_dqx_app.backend.migrations import MIGRATIONS
from databricks_labs_dqx_app.backend.migrations.postgres import PG_MIGRATIONS

_EXPECTED_COLUMNS = ("rule_id", "rule_version", "embed_text", "embedding", "model", "updated_at")


class TestDqRuleEmbeddingsPostgres:
    def test_table_defined_in_v4(self):
        migration = next(m for m in PG_MIGRATIONS if m.version == 4)
        assert "dq_rule_embeddings" in migration.sql

    def test_has_expected_columns(self):
        migration = next(m for m in PG_MIGRATIONS if m.version == 4)
        for col in _EXPECTED_COLUMNS:
            assert col in migration.sql

    def test_primary_key_on_rule_id(self):
        migration = next(m for m in PG_MIGRATIONS if m.version == 4)
        assert "rule_id      TEXT PRIMARY KEY" in migration.sql


class TestDqRuleEmbeddingsDelta:
    def test_table_defined_in_v8(self):
        migration = next(m for m in MIGRATIONS if m.version == 8)
        assert "dq_rule_embeddings" in migration.sql_template

    def test_has_expected_columns(self):
        migration = next(m for m in MIGRATIONS if m.version == 8)
        for col in _EXPECTED_COLUMNS:
            assert col in migration.sql_template

    def test_marked_oltp_fallback(self):
        migration = next(m for m in MIGRATIONS if m.version == 8)
        assert migration.oltp_fallback is True
