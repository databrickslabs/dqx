"""Schema tests for the Rules Registry audit table (``dq_rules_history``).

Phase 2B adds a minimal append-only history table for the registry rule
lifecycle (create/update/status transitions/delete), following the same
greenfield-baseline convention Phase 2A used for ``dq_rules`` /
``dq_rule_versions`` — appended directly to the existing baseline
migration (Postgres version=1, Delta v2 OLTP fallback), not a new
migration version.
"""

from __future__ import annotations

from databricks_labs_dqx_app.backend.migrations import _V2_OLTP_FALLBACK
from databricks_labs_dqx_app.backend.migrations.postgres import PG_MIGRATIONS


class TestDqRulesHistoryPostgres:
    def test_table_defined_in_baseline_version(self):
        baseline = next(m for m in PG_MIGRATIONS if m.version == 1)
        assert "CREATE TABLE IF NOT EXISTS" in baseline.sql
        assert "dq_rules_history" in baseline.sql

    def test_has_expected_columns(self):
        baseline = next(m for m in PG_MIGRATIONS if m.version == 1)
        for col in ("rule_id", "definition", "version", "action", "prev_status", "new_status", "changed_by", "changed_at"):
            assert col in baseline.sql

    def test_no_new_migration_version_added(self):
        # Greenfield constraint: dq_rules_history piggybacks on the
        # existing baseline rather than introducing a new version.
        versions = [m.version for m in PG_MIGRATIONS]
        assert versions == sorted(set(versions))


class TestDqRulesHistoryDelta:
    def test_table_defined_in_delta_fallback(self):
        assert "dq_rules_history" in _V2_OLTP_FALLBACK

    def test_has_expected_columns(self):
        for col in ("rule_id", "definition", "version", "action", "prev_status", "new_status", "changed_by", "changed_at"):
            assert col in _V2_OLTP_FALLBACK
