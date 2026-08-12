"""Schema tests for the tag-auto suppressions table (apply-on-tag).

``dq_tag_auto_suppressions`` is added as a new Postgres migration (v19) and
the matching Delta OLTP-fallback template (v23). Both must declare the same
logical columns and the ``(binding_id, rule_id, mapping_hash)`` primary key so
``ApplyRulesService._is_suppressed`` / ``_record_suppression`` are portable
across backends.
"""

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

_PG_VERSION = 19
_DELTA_VERSION = 23


class TestTagAutoSuppressionsPostgres:
    def test_added_as_new_migration(self):
        migration = next(m for m in PG_MIGRATIONS if m.version == _PG_VERSION)
        assert "dq_tag_auto_suppressions" in migration.sql

    def test_columns(self):
        migration = next(m for m in PG_MIGRATIONS if m.version == _PG_VERSION)
        for col in _SUPPRESSION_COLS:
            assert col in migration.sql

    def test_primary_key_is_natural_key(self):
        migration = next(m for m in PG_MIGRATIONS if m.version == _PG_VERSION)
        assert "PRIMARY KEY (binding_id, rule_id, mapping_hash)" in migration.sql

    def test_binding_id_index(self):
        migration = next(m for m in PG_MIGRATIONS if m.version == _PG_VERSION)
        assert "idx_dq_tag_auto_suppressions_binding_id" in migration.sql

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

    def test_registered_as_oltp_fallback_migration(self):
        migration = next(m for m in MIGRATIONS if m.version == _DELTA_VERSION)
        assert isinstance(migration, DeltaMigration)
        assert migration.oltp_fallback is True
        assert migration.sql_template == _V19_TAG_AUTO_SUPPRESSIONS

    def test_versions_monotonic(self):
        versions = [m.version for m in MIGRATIONS]
        assert versions == sorted(versions)
        assert len(versions) == len(set(versions))
