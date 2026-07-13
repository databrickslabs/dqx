"""Schema tests for the UC-style object-permissions tables (P22-D item 10).

``dq_object_grants`` (+ history) are added as a new Postgres migration (v10)
and the matching Delta OLTP-fallback template (_V14). Both must declare the
same logical columns so the ``PermissionsService`` read/write path is portable.
"""

from __future__ import annotations

from databricks_labs_dqx_app.backend.migrations import _V14_OBJECT_GRANTS, MIGRATIONS
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


class TestObjectGrantsPostgres:
    def test_added_as_new_migration_v10(self):
        v10 = next(m for m in PG_MIGRATIONS if m.version == 10)
        assert "dq_object_grants" in v10.sql
        assert "dq_object_grants_history" in v10.sql

    def test_grants_columns(self):
        v10 = next(m for m in PG_MIGRATIONS if m.version == 10)
        for col in (*_GRANT_COLS, "grant_id"):
            assert col in v10.sql

    def test_object_type_check_constraint(self):
        v10 = next(m for m in PG_MIGRATIONS if m.version == 10)
        for ot in ("registry_rule", "monitored_table", "data_product"):
            assert ot in v10.sql

    def test_unique_object_principal(self):
        v10 = next(m for m in PG_MIGRATIONS if m.version == 10)
        assert "uq_dq_object_grants_object_principal" in v10.sql

    def test_versions_monotonic(self):
        versions = [m.version for m in PG_MIGRATIONS]
        assert versions == sorted(versions)
        assert len(versions) == len(set(versions))


class TestObjectGrantsDelta:
    def test_delta_fallback_declares_tables(self):
        assert "dq_object_grants" in _V14_OBJECT_GRANTS
        assert "dq_object_grants_history" in _V14_OBJECT_GRANTS

    def test_delta_columns(self):
        for col in _GRANT_COLS:
            assert col in _V14_OBJECT_GRANTS

    def test_registered_as_oltp_fallback_migration(self):
        v14 = next(m for m in MIGRATIONS if m.version == 14)
        assert getattr(v14, "oltp_fallback", False) is True
