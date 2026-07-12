"""Schema tests for Phase 3A — monitored tables + applied rules.

Adds ``dq_monitored_tables`` and ``dq_applied_rules`` (Layer 2 of the Rules
Registry design, see docs/superpowers/specs/2026-07-02-rules-registry-design.md
§3.1/§7) plus provenance columns on ``dq_quality_rules``. Follows the same
greenfield-baseline convention Phase 2A/2B used: appended directly to the
existing baseline migration (Postgres version=1, Delta v2 OLTP fallback),
not a new migration version.
"""

from __future__ import annotations

from databricks_labs_dqx_app.backend.migrations import MIGRATIONS, _V2_OLTP_FALLBACK
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
            "schedule_cron",
            "schedule_tz",
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
            "schedule_cron",
            "schedule_tz",
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


class TestMonitoredTableStatusConvergePostgres:
    """P16-H: appended PG migration that converges DBs deployed with the
    original 2-state (`draft`/`published`) constraint to the 4-state review set.

    Editing the v1 baseline constraint in place could never reach an
    already-migrated DB (the runner skips versions recorded in
    ``dq_migrations``), so a new appended version is required.
    """

    def _converge(self):
        # The converge migration is whichever appended version carries the
        # drop-and-re-add of chk_dq_monitored_tables_status (not the v1
        # baseline, which CREATEs the table).
        candidates = [m for m in PG_MIGRATIONS if m.version > 1 and "chk_dq_monitored_tables_status" in m.sql]
        assert len(candidates) == 1, "exactly one appended PG converge migration expected"
        return candidates[0]

    def test_appended_after_baseline(self):
        assert self._converge().version > 1

    def test_drops_then_updates_then_readds_constraint(self):
        sql = self._converge().sql
        drop_at = sql.index("DROP CONSTRAINT IF EXISTS chk_dq_monitored_tables_status")
        update_at = sql.index("SET status = 'approved' WHERE status = 'published'")
        add_at = sql.index("ADD CONSTRAINT chk_dq_monitored_tables_status")
        # Order matters: drop the old constraint, rewrite the legacy value,
        # then re-add the final constraint — all inside one transaction.
        assert drop_at < update_at < add_at

    def test_readds_four_state_constraint(self):
        sql = self._converge().sql
        assert "CHECK (status IN ('draft','pending_approval','approved','rejected'))" in sql

    def test_backfills_missing_tables_before_converge(self):
        """DBs that recorded v1 before Phase 3A never got dq_monitored_tables."""
        sql = self._converge().sql
        create_at = sql.index("CREATE TABLE IF NOT EXISTS")
        drop_at = sql.index("DROP CONSTRAINT IF EXISTS chk_dq_monitored_tables_status")
        assert "dq_monitored_tables" in sql[:drop_at]
        assert "dq_applied_rules" in sql[:drop_at]
        assert create_at < drop_at


class TestDataProductsPostgres:
    """Task 1 (Data Products plan): PG v6 appends the versioned-monitored-table
    snapshot, data-product grouping, and run-set tables. See
    docs/superpowers/plans/2026-07-07-data-products.md Task 1 and
    docs/superpowers/specs/2026-07-07-data-products-design.md §3.
    """

    def _v6(self):
        return next(m for m in PG_MIGRATIONS if m.version == 6)

    def test_appended_after_baseline(self):
        assert self._v6().version > 1

    def test_monitored_tables_gains_version_column(self):
        sql = self._v6().sql
        assert "ADD COLUMN IF NOT EXISTS version" in sql
        assert "dq_monitored_tables" in sql

    def test_monitored_table_versions_table(self):
        sql = self._v6().sql
        assert "dq_monitored_table_versions" in sql
        for col in (
            "id",
            "binding_id",
            "version",
            "checks_json",
            "state_json",
            "created_by",
            "created_at",
            "refrozen_at",
        ):
            assert col in sql
        assert "UNIQUE (binding_id, version)" in sql

    def test_data_products_table(self):
        sql = self._v6().sql
        assert "dq_data_products" in sql
        for col in (
            "product_id",
            "name",
            "description",
            "steward",
            "schedule_cron",
            "schedule_tz",
            "status",
            "version",
            "created_by",
            "created_at",
            "updated_by",
            "updated_at",
        ):
            assert col in sql
        assert "UNIQUE (name)" in sql
        assert "CHECK (status IN ('draft','pending_approval','approved','rejected'))" in sql

    def test_data_product_members_table(self):
        sql = self._v6().sql
        assert "dq_data_product_members" in sql
        for col in ("id", "product_id", "binding_id", "pinned_version"):
            assert col in sql
        assert "UNIQUE (product_id, binding_id)" in sql

    def test_run_sets_table(self):
        sql = self._v6().sql
        assert "dq_run_sets" in sql
        for col in ("run_set_id", "product_id", "product_version", "source", "created_by", "created_at"):
            assert col in sql
        assert "trigger" in sql
        assert "CHECK (source IN ('approved','draft'))" in sql
        assert "'manual'" in sql and "'scheduled'" in sql

    def test_run_set_members_table(self):
        sql = self._v6().sql
        assert "dq_run_set_members" in sql
        for col in ("id", "run_set_id", "run_id", "binding_id", "binding_version"):
            assert col in sql

    def test_no_earlier_migration_touched(self):
        # Greenfield-append constraint: the pre-existing baseline/converge
        # migrations must be untouched by this task.
        baseline = next(m for m in PG_MIGRATIONS if m.version == 1)
        assert "dq_data_products" not in baseline.sql
        assert "dq_monitored_table_versions" not in baseline.sql


class TestDataProductsDelta:
    """Delta v10 mirror of PG v6 — see TestDataProductsPostgres."""

    def _v10(self):
        return next(m for m in MIGRATIONS if m.version == 10)

    def test_appended_after_oltp_baseline(self):
        assert self._v10().version > 2

    def test_is_oltp_fallback(self):
        # All five new tables + the version column follow the same
        # oltp_fallback placement as dq_monitored_tables itself.
        assert getattr(self._v10(), "oltp_fallback", False) is True

    def test_monitored_tables_gains_version_column(self):
        sql = self._v10().sql_template
        assert "dq_monitored_tables" in sql
        assert "ADD COLUMN version" in sql

    def test_monitored_table_versions_table(self):
        sql = self._v10().sql_template
        assert "dq_monitored_table_versions" in sql
        for col in (
            "binding_id",
            "version",
            "checks_json",
            "state_json",
            "created_by",
            "created_at",
            "refrozen_at",
        ):
            assert col in sql

    def test_data_products_table(self):
        sql = self._v10().sql_template
        assert "dq_data_products" in sql
        for col in (
            "product_id",
            "name",
            "description",
            "steward",
            "schedule_cron",
            "schedule_tz",
            "status",
            "version",
            "created_by",
            "created_at",
            "updated_by",
            "updated_at",
        ):
            assert col in sql
        assert "CHECK (status IN ('draft','pending_approval','approved','rejected'))" in sql

    def test_data_product_members_table(self):
        sql = self._v10().sql_template
        assert "dq_data_product_members" in sql
        for col in ("product_id", "binding_id", "pinned_version"):
            assert col in sql

    def test_run_sets_table(self):
        sql = self._v10().sql_template
        assert "dq_run_sets" in sql
        for col in ("run_set_id", "product_id", "product_version", "source", "created_by", "created_at"):
            assert col in sql
        assert "trigger" in sql
        assert "CHECK (source IN ('approved','draft'))" in sql

    def test_run_set_members_table(self):
        sql = self._v10().sql_template
        assert "dq_run_set_members" in sql
        for col in ("run_set_id", "run_id", "binding_id", "binding_version"):
            assert col in sql

    def test_no_earlier_migration_touched(self):
        assert "dq_data_products" not in _V2_OLTP_FALLBACK


class TestMonitoredTableStatusConvergeDelta:
    """P16-H: the Delta mirror of the converge migration. Marked
    ``oltp_fallback=True`` so it only runs on Delta when Lakebase is disabled.
    """

    def _converge(self):
        candidates = [
            m
            for m in MIGRATIONS
            if m.version > 2 and "DROP CONSTRAINT IF EXISTS chk_dq_monitored_tables_status" in m.sql_template
        ]
        assert len(candidates) == 1, "exactly one appended Delta converge migration expected"
        return candidates[0]

    def test_appended_after_oltp_baseline(self):
        assert self._converge().version > 2

    def test_is_oltp_fallback(self):
        # dq_monitored_tables lives in Lakebase when enabled — the Delta
        # converge must be skipped there (the Postgres mirror handles it).
        assert getattr(self._converge(), "oltp_fallback", False) is True

    def test_drops_then_updates_then_readds_constraint(self):
        sql = self._converge().sql_template
        drop_at = sql.index("DROP CONSTRAINT IF EXISTS chk_dq_monitored_tables_status")
        update_at = sql.index("SET status = 'approved' WHERE status = 'published'")
        add_at = sql.index("ADD CONSTRAINT chk_dq_monitored_tables_status")
        assert drop_at < update_at < add_at

    def test_readds_four_state_constraint(self):
        sql = self._converge().sql_template
        assert "CHECK (status IN ('draft','pending_approval','approved','rejected'))" in sql

    def test_backfills_missing_tables_before_converge(self):
        """Delta-OLTP DBs that recorded v2 before Phase 3A never got dq_monitored_tables."""
        sql = self._converge().sql_template
        create_at = sql.index("CREATE TABLE IF NOT EXISTS")
        drop_at = sql.index("DROP CONSTRAINT IF EXISTS chk_dq_monitored_tables_status")
        assert "dq_monitored_tables" in sql[:drop_at]
        assert "dq_applied_rules" in sql[:drop_at]
        assert create_at < drop_at

    def test_splits_into_individual_statements(self):
        """Delta runner splits on ';' — every DDL boundary must be terminated."""
        sql = self._converge().sql_template.format(catalog="c", schema="s")
        stmts = [s.strip() for s in sql.split(";") if s.strip()]
        assert len(stmts) == 5
        assert stmts[0].startswith("CREATE TABLE IF NOT EXISTS")
        assert "dq_monitored_tables" in stmts[0]
        assert "dq_applied_rules" in stmts[1]
        assert "DROP CONSTRAINT IF EXISTS chk_dq_monitored_tables_status" in stmts[2]
        assert "SET status = 'approved' WHERE status = 'published'" in stmts[3]
        assert "ADD CONSTRAINT chk_dq_monitored_tables_status" in stmts[4]


class TestMonitoredTableScheduleConvergePostgres:
    """P21 item 14: appended PG migration adding schedule_cron/schedule_tz to
    ``dq_monitored_tables``. The v1 baseline now declares both columns, so this
    only converges DBs already deployed without them (editing v1 in place can
    never reach an already-migrated DB).
    """

    def _converge(self):
        candidates = [
            m
            for m in PG_MIGRATIONS
            if m.version > 1 and "dq_monitored_tables ADD COLUMN IF NOT EXISTS schedule_cron" in m.sql
        ]
        assert len(candidates) == 1, "exactly one appended PG schedule-column migration expected"
        return candidates[0]

    def test_appended_after_baseline(self):
        assert self._converge().version > 1

    def test_adds_both_columns_idempotently(self):
        sql = self._converge().sql
        assert "ADD COLUMN IF NOT EXISTS schedule_cron" in sql
        assert "ADD COLUMN IF NOT EXISTS schedule_tz" in sql

    def test_baseline_also_declares_columns(self):
        baseline = next(m for m in PG_MIGRATIONS if m.version == 1)
        assert "schedule_cron" in baseline.sql
        assert "schedule_tz" in baseline.sql


class TestMonitoredTableScheduleConvergeDelta:
    """P21 item 14: the Delta mirror of the schedule-column migration. Marked
    ``oltp_fallback=True`` so it only runs on Delta when Lakebase is disabled.
    """

    def _converge(self):
        candidates = [
            m
            for m in MIGRATIONS
            if m.version > 2 and "dq_monitored_tables ADD COLUMN schedule_cron" in m.sql_template
        ]
        assert len(candidates) == 1, "exactly one appended Delta schedule-column migration expected"
        return candidates[0]

    def test_appended_after_oltp_baseline(self):
        assert self._converge().version > 2

    def test_is_oltp_fallback(self):
        assert getattr(self._converge(), "oltp_fallback", False) is True

    def test_adds_both_columns(self):
        sql = self._converge().sql_template
        assert "ADD COLUMN schedule_cron" in sql
        assert "ADD COLUMN schedule_tz" in sql

    def test_baseline_also_declares_columns(self):
        assert "schedule_cron" in _V2_OLTP_FALLBACK
        assert "schedule_tz" in _V2_OLTP_FALLBACK


class TestDataProductStatusConvergePostgres:
    """P21-D: appended PG migration that converges DBs deployed with the
    original 2-state (`draft`/`published`) ``dq_data_products.status``
    constraint to the 4-state review set.

    The v6 baseline's ``chk_dq_data_products_status`` CHECK was edited in
    place from 2-state to 4-state — exactly the class of bug the v5
    monitored-table converge (above) fixed. Editing v6 in place can never
    reach a DB already migrated past it (the runner skips versions recorded
    in ``dq_migrations``), so a new appended version is required.
    """

    def _converge(self):
        candidates = [m for m in PG_MIGRATIONS if m.version > 6 and "chk_dq_data_products_status" in m.sql]
        assert len(candidates) == 1, "exactly one appended PG converge migration expected"
        return candidates[0]

    def test_appended_after_data_products_baseline(self):
        assert self._converge().version > 6

    def test_drops_then_updates_then_readds_constraint(self):
        sql = self._converge().sql
        drop_at = sql.index("DROP CONSTRAINT IF EXISTS chk_dq_data_products_status")
        update_at = sql.index("SET status = 'approved' WHERE status = 'published'")
        add_at = sql.index("ADD CONSTRAINT chk_dq_data_products_status")
        assert drop_at < update_at < add_at

    def test_readds_four_state_constraint(self):
        sql = self._converge().sql
        assert "CHECK (status IN ('draft','pending_approval','approved','rejected'))" in sql


class TestDataProductStatusConvergeDelta:
    """P21-D: the Delta mirror of the converge migration. Marked
    ``oltp_fallback=True`` so it only runs on Delta when Lakebase is disabled.
    """

    def _converge(self):
        candidates = [
            m
            for m in MIGRATIONS
            if m.version > 10 and "DROP CONSTRAINT IF EXISTS chk_dq_data_products_status" in m.sql_template
        ]
        assert len(candidates) == 1, "exactly one appended Delta converge migration expected"
        return candidates[0]

    def test_appended_after_data_products_baseline(self):
        assert self._converge().version > 10

    def test_is_oltp_fallback(self):
        # dq_data_products lives in Lakebase when enabled — the Delta
        # converge must be skipped there (the Postgres mirror handles it).
        assert getattr(self._converge(), "oltp_fallback", False) is True

    def test_drops_then_updates_then_readds_constraint(self):
        sql = self._converge().sql_template
        drop_at = sql.index("DROP CONSTRAINT IF EXISTS chk_dq_data_products_status")
        update_at = sql.index("SET status = 'approved' WHERE status = 'published'")
        add_at = sql.index("ADD CONSTRAINT chk_dq_data_products_status")
        assert drop_at < update_at < add_at

    def test_readds_four_state_constraint(self):
        sql = self._converge().sql_template
        assert "CHECK (status IN ('draft','pending_approval','approved','rejected'))" in sql
