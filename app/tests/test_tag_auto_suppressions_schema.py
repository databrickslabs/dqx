"""Schema tests for the tag-auto suppressions table (apply-on-tag).

``dq_tag_auto_suppressions`` is added as a new Postgres migration (v15) and
the matching Delta OLTP-fallback template (_V19). Both must declare the same
logical columns and the ``(binding_id, rule_id, mapping_hash)`` primary key so
``ApplyRulesService._is_suppressed`` / ``_record_suppression`` are portable
across backends.
"""

from __future__ import annotations

from databricks_labs_dqx_app.backend.migrations import (
    _V19_TAG_AUTO_SUPPRESSIONS,
    MIGRATIONS,
    DeltaMigration,
)
from databricks_labs_dqx_app.backend.migrations.postgres import PG_MIGRATIONS

_SUPPRESSION_COLS = (
    "binding_id",
    "rule_id",
    "mapping_hash",
    "suppressed_by",
    "suppressed_at",
)


class TestTagAutoSuppressionsPostgres:
    def test_added_as_new_migration_v15(self):
        v15 = next(m for m in PG_MIGRATIONS if m.version == 15)
        assert "dq_tag_auto_suppressions" in v15.sql

    def test_columns(self):
        v15 = next(m for m in PG_MIGRATIONS if m.version == 15)
        for col in _SUPPRESSION_COLS:
            assert col in v15.sql

    def test_primary_key_is_natural_key(self):
        v15 = next(m for m in PG_MIGRATIONS if m.version == 15)
        assert "PRIMARY KEY (binding_id, rule_id, mapping_hash)" in v15.sql

    def test_binding_id_index(self):
        v15 = next(m for m in PG_MIGRATIONS if m.version == 15)
        assert "idx_dq_tag_auto_suppressions_binding_id" in v15.sql

    def test_versions_monotonic(self):
        versions = [m.version for m in PG_MIGRATIONS]
        assert versions == sorted(versions)
        assert len(versions) == len(set(versions))


class TestTagAutoSuppressionsDelta:
    def test_delta_fallback_declares_table(self):
        assert "dq_tag_auto_suppressions" in _V19_TAG_AUTO_SUPPRESSIONS

    def test_delta_columns(self):
        for col in _SUPPRESSION_COLS:
            assert col in _V19_TAG_AUTO_SUPPRESSIONS

    def test_delta_primary_key(self):
        assert "PRIMARY KEY (binding_id, rule_id, mapping_hash)" in _V19_TAG_AUTO_SUPPRESSIONS

    def test_registered_as_oltp_fallback_migration_v19(self):
        v19 = next(m for m in MIGRATIONS if m.version == 19)
        assert isinstance(v19, DeltaMigration)
        assert v19.oltp_fallback is True
        assert v19.sql_template == _V19_TAG_AUTO_SUPPRESSIONS

    def test_versions_monotonic(self):
        versions = [m.version for m in MIGRATIONS]
        assert versions == sorted(versions)
        assert len(versions) == len(set(versions))
