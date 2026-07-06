"""Schema tests for Phase 3A — monitored tables + applied rules.

Adds ``dq_monitored_tables`` and ``dq_applied_rules`` (Layer 2 of the Rules
Registry design, see docs/superpowers/specs/2026-07-02-rules-registry-design.md
§3.1/§7) plus provenance columns on ``dq_quality_rules``. Follows the same
greenfield-baseline convention Phase 2A/2B used: appended directly to the
existing baseline migration (Postgres version=1, Delta v2 OLTP fallback),
not a new migration version.
"""

from __future__ import annotations

from databricks_labs_dqx_app.backend.migrations import _V2_OLTP_FALLBACK
from databricks_labs_dqx_app.backend.migrations.postgres import PG_MIGRATIONS


class TestDqMonitoredTablesPostgres:
    def test_table_defined_in_baseline_version(self):
        baseline = next(m for m in PG_MIGRATIONS if m.version == 1)
        assert "dq_monitored_tables" in baseline.sql

    def test_has_expected_columns(self):
        baseline = next(m for m in PG_MIGRATIONS if m.version == 1)
        for col in (
            "binding_id",
            "table_fqn",
            "steward",
            "status",
            "last_profiled_at",
            "created_by",
            "created_at",
            "updated_by",
            "updated_at",
        ):
            assert col in baseline.sql

    def test_table_fqn_unique(self):
        baseline = next(m for m in PG_MIGRATIONS if m.version == 1)
        assert "uq_dq_monitored_tables_table_fqn" in baseline.sql
        assert "UNIQUE (table_fqn)" in baseline.sql

    def test_status_check_constraint(self):
        baseline = next(m for m in PG_MIGRATIONS if m.version == 1)
        assert "chk_dq_monitored_tables_status" in baseline.sql
        assert "CHECK (status IN ('draft','pending_approval','approved','rejected'))" in baseline.sql


class TestDqAppliedRulesPostgres:
    def test_table_defined_in_baseline_version(self):
        baseline = next(m for m in PG_MIGRATIONS if m.version == 1)
        assert "dq_applied_rules" in baseline.sql

    def test_has_expected_columns(self):
        baseline = next(m for m in PG_MIGRATIONS if m.version == 1)
        for col in (
            "binding_id",
            "rule_id",
            "pinned_version",
            "severity_override",
            "column_mapping",
            "user_metadata",
            "mapping_hash",
            "created_by",
            "created_at",
        ):
            assert col in baseline.sql

    def test_unique_binding_rule_mapping_hash(self):
        baseline = next(m for m in PG_MIGRATIONS if m.version == 1)
        assert "uq_dq_applied_rules_binding_rule_mapping" in baseline.sql
        assert "UNIQUE (binding_id, rule_id, mapping_hash)" in baseline.sql

    def test_no_new_migration_version_added(self):
        # Greenfield constraint: the new tables piggyback on the existing
        # baseline rather than introducing a new version.
        versions = [m.version for m in PG_MIGRATIONS]
        assert versions == sorted(set(versions))


class TestDqQualityRulesProvenancePostgres:
    def test_provenance_columns_present(self):
        baseline = next(m for m in PG_MIGRATIONS if m.version == 1)
        for col in ("registry_rule_id", "registry_version", "applied_rule_id"):
            assert col in baseline.sql


class TestDqMonitoredTablesDelta:
    def test_table_defined_in_delta_fallback(self):
        assert "dq_monitored_tables" in _V2_OLTP_FALLBACK

    def test_has_expected_columns(self):
        for col in (
            "binding_id",
            "table_fqn",
            "steward",
            "status",
            "last_profiled_at",
            "created_by",
            "created_at",
            "updated_by",
            "updated_at",
        ):
            assert col in _V2_OLTP_FALLBACK

    def test_status_check_constraint(self):
        assert "chk_dq_monitored_tables_status" in _V2_OLTP_FALLBACK
        assert "CHECK (status IN ('draft','pending_approval','approved','rejected'))" in _V2_OLTP_FALLBACK


class TestDqAppliedRulesDelta:
    def test_table_defined_in_delta_fallback(self):
        assert "dq_applied_rules" in _V2_OLTP_FALLBACK

    def test_has_expected_columns(self):
        for col in (
            "binding_id",
            "rule_id",
            "pinned_version",
            "severity_override",
            "column_mapping",
            "user_metadata",
            "mapping_hash",
            "created_by",
            "created_at",
        ):
            assert col in _V2_OLTP_FALLBACK


class TestDqQualityRulesProvenanceDelta:
    def test_provenance_columns_present(self):
        for col in ("registry_rule_id", "registry_version", "applied_rule_id"):
            assert col in _V2_OLTP_FALLBACK
