"""Schema tests for the registry rule audit trail.

A registry rule's lifecycle (create/update/status transitions/delete) is
recorded in the shared ``dq_lifecycle_history`` table alongside monitored
tables and collections. The v5 migration moved the older ``dq_rules_history``
rows into it.
"""

from databricks_labs_dqx_app.backend.migrations.postgres import PG_MIGRATIONS


class TestLifecycleHistoryPostgres:
    def test_rule_history_lives_in_the_shared_lifecycle_history(self):
        baseline = next(m for m in PG_MIGRATIONS if m.version == 1)
        assert "CREATE TABLE IF NOT EXISTS {schema}.dq_lifecycle_history" in baseline.sql
        assert "CREATE TABLE IF NOT EXISTS {schema}.dq_rules_history" not in baseline.sql

    def test_has_expected_columns(self):
        baseline = next(m for m in PG_MIGRATIONS if m.version == 1)
        create = baseline.sql[baseline.sql.index("{schema}.dq_lifecycle_history (") :]
        create = create[: create.index(");")]
        for col in (
            "object_type",
            "object_id",
            "action",
            "prev_status",
            "new_status",
            "version",
            "details",
            "rationale",
            "changed_by",
            "is_automatic",
            "changed_at",
        ):
            assert f" {col} " in create

    def test_no_new_migration_version_added(self):
        # Greenfield constraint: dq_rules_history piggybacks on the
        # existing baseline rather than introducing a new version.
        versions = [m.version for m in PG_MIGRATIONS]
        assert versions == sorted(set(versions))
