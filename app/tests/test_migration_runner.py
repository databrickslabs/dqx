"""Unit tests for the Delta :class:`MigrationRunner`.

Scope: the structural invariants that the Postgres runner gets "for
free" from transactional atomicity but the Delta runner has to enforce
manually — see the "Atomicity model" section of
:mod:`databricks_labs_dqx_app.backend.migrations` for the full
recovery contract. Plus the identifier-quoting contract that both
runners share via ``executor.q()``.

The runner itself is exercised in integration tests against a real
SQL warehouse; here we focus on the pure-Python guards that prevent
future migration authors from silently breaking the contract:

1. ``_validate_template_safe`` rejects any template containing
   ``;`` inside a single-quoted string literal, and any
   ``{catalog}``/``{schema}`` placeholder appearing in literal
   position (where the quoted-form substitution would be incorrect).
2. Every entry in the live :data:`MIGRATIONS` list passes the
   validator — regression coverage against a future author editing
   a template into a no-longer-safe shape.
3. The runner consistently uses ``sql.q()`` for catalog/schema
   identifiers — so a deployment with a hyphenated Databricks
   catalog name like ``prod-east`` emits parseable DDL instead of
   raw ``CREATE SCHEMA prod-east.dqx_studio``.
"""

from unittest.mock import MagicMock

import pytest

from databricks_labs_dqx_app.backend.migrations import (
    ANALYTICAL_TABLE_NAMES,
    MIGRATIONS,
    OLTP_TABLE_NAMES,
    MigrationRunner,
    _validate_template_safe,
)
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor

# ---------------------------------------------------------------------------
# Template scanner: positive + negative + live regression
# ---------------------------------------------------------------------------


class TestValidateTemplateSafe:
    """Direct tests of the import-time scanner."""

    @pytest.mark.parametrize(
        "template",
        [
            # Empty / trivial
            "",
            "SELECT 1",
            # Multi-statement, no literals
            "CREATE TABLE foo (x INT); ALTER TABLE foo ADD COLUMN y INT;",
            # Literals without forbidden characters
            "ALTER TABLE foo ADD CONSTRAINT chk CHECK (status IN ('a','b','c'))",
            # Apostrophe escape inside literal (``''`` is one literal apostrophe)
            "INSERT INTO foo VALUES ('it''s fine')",
            # Adjacent literals (closing + opening) — the scanner must NOT
            # treat `''` between two separate strings as an escape.
            "INSERT INTO foo VALUES ('a','b')",
            # Semicolon AFTER closing the literal is fine
            "INSERT INTO foo VALUES ('a'); INSERT INTO foo VALUES ('b')",
            # Placeholders in object-name positions are the supported use case
            "CREATE TABLE IF NOT EXISTS {catalog}.{schema}.foo (x INT)",
            "INSERT INTO {catalog}.{schema}.bar (note) VALUES ('description with {curly} braces')",
        ],
    )
    def test_safe_templates_pass(self, template: str) -> None:
        # Must not raise.
        _validate_template_safe(template)

    @pytest.mark.parametrize(
        ("template", "expected_fragment"),
        [
            # Invariant 1: ``;`` inside a CHECK constraint literal.
            (
                "ALTER TABLE foo ADD CONSTRAINT chk CHECK (label IN ('one;two', 'three'))",
                "';' inside",
            ),
            # Invariant 1: ``;`` inside a default value
            ("ALTER TABLE foo ADD COLUMN note STRING DEFAULT 'a; b'", "';' inside"),
            # Invariant 1: ``;`` inside an escaped-apostrophe literal — the
            # scanner must keep the literal flag set across ``''``.
            ("INSERT INTO foo VALUES ('it''s; bad')", "';' inside"),
            # Invariant 1: trailing ``;`` inside literal
            ("INSERT INTO foo VALUES ('trailing;')", "';' inside"),
            # Invariant 2: ``{catalog}`` placeholder inside literal
            ("INSERT INTO foo VALUES ('catalog={catalog}')", "{catalog}"),
            # Invariant 2: ``{schema}`` placeholder inside literal
            ("INSERT INTO foo VALUES ('schema={schema}')", "{schema}"),
        ],
    )
    def test_unsafe_templates_raise(self, template: str, expected_fragment: str) -> None:
        with pytest.raises(AssertionError, match="inside a single-quoted string literal"):
            _validate_template_safe(template)
        # The specific violation should appear in the message so the
        # author knows which invariant they broke.
        with pytest.raises(AssertionError) as excinfo:
            _validate_template_safe(template)
        assert expected_fragment in str(excinfo.value)

    def test_assertion_message_includes_offset_and_excerpt(self) -> None:
        """The error message must be actionable — give the offset and a snippet."""
        prefix = "ALTER TABLE foo ADD COLUMN name STRING; "
        template = prefix + "INSERT INTO foo VALUES ('bad;value')"
        with pytest.raises(AssertionError) as excinfo:
            _validate_template_safe(template)
        msg = str(excinfo.value)
        assert "offset" in msg
        # Excerpt should include some text near the offending ``;`` —
        # we don't pin the exact window because the window size is an
        # implementation detail, but the literal contents should appear.
        assert "bad" in msg


class TestLiveMigrationsAreTemplateSafe:
    """Regression: every shipped migration template must pass the validator.

    Without this, the runner's startup-time check would still catch a
    violation at app boot — but a unit-test failure on PR is much
    faster feedback than a deploy-time crash.
    """

    def test_no_migration_template_violates_invariants(self) -> None:
        for migration in MIGRATIONS:
            try:
                _validate_template_safe(migration.sql_template)
            except AssertionError as exc:  # pragma: no cover - surfaced via fail()
                pytest.fail(
                    f"Migration v{migration.version} ({migration.description}) "
                    f"violates a template scanner invariant: {exc}"
                )


class TestScheduleKindDeltaMigration:
    """B2-52: schedule_kind is declared on the Delta baseline + converged by v18."""

    def test_v18_adds_schedule_kind_to_both_tables(self) -> None:
        v18 = next(m for m in MIGRATIONS if m.version == 18)
        assert "dq_monitored_tables ADD COLUMN schedule_kind STRING" in v18.sql_template
        assert "dq_data_products ADD COLUMN schedule_kind STRING" in v18.sql_template
        assert "chk_dq_monitored_tables_schedule_kind" in v18.sql_template
        assert "chk_dq_data_products_schedule_kind" in v18.sql_template

    def test_delta_baselines_declare_schedule_kind(self) -> None:
        # v2 = Delta OLTP-fallback baseline (dq_monitored_tables);
        # v10 = Data Products baseline (dq_data_products).
        v2 = next(m for m in MIGRATIONS if m.version == 2)
        v10 = next(m for m in MIGRATIONS if m.version == 10)
        assert "schedule_kind" in v2.sql_template
        assert "chk_dq_monitored_tables_schedule_kind" in v2.sql_template
        assert "schedule_kind" in v10.sql_template
        assert "chk_dq_data_products_schedule_kind" in v10.sql_template


class TestPendingApplicationsMigration:
    """Bulk Contract Import Phase 2: dq_pending_applications (v19, OLTP fallback)."""

    def test_v19_creates_pending_applications_table(self) -> None:
        v19 = next(m for m in MIGRATIONS if m.version == 19)
        assert "CREATE TABLE IF NOT EXISTS {catalog}.{schema}.dq_pending_applications" in v19.sql_template
        assert "binding_id" in v19.sql_template
        assert "rule_id" in v19.sql_template
        assert "column_mapping" in v19.sql_template
        # Must be an OLTP-fallback table so it's skipped when Lakebase owns it.
        assert v19.oltp_fallback is True

    def test_pending_applications_is_an_oltp_table(self) -> None:
        # The reset feature and physical routing derive the table set from the
        # CREATE statements — the new table must land in the OLTP bucket only.
        assert "dq_pending_applications" in OLTP_TABLE_NAMES
        assert "dq_pending_applications" not in ANALYTICAL_TABLE_NAMES


class TestBackfillDefaultGrantsMigration:
    """v24: backfill default object grants for pre-existing objects (OLTP fallback)."""

    def test_v24_covers_all_object_types(self) -> None:
        v24 = next(m for m in MIGRATIONS if m.version == 24)
        for obj_type in ("registry_rule", "monitored_table", "data_product"):
            assert obj_type in v24.sql_template, f"v24 must cover object type '{obj_type}'"
        assert "SELECT,APPLY,EXECUTE" in v24.sql_template, "users-group privileges must be present"
        assert "ALL_PRIVILEGES" in v24.sql_template, "owner ALL_PRIVILEGES must be present"
        assert "NOT EXISTS" in v24.sql_template, "backfill must be idempotent via NOT EXISTS"
        assert "dq_object_grants" in v24.sql_template

    def test_v24_is_oltp_fallback(self) -> None:
        v24 = next(m for m in MIGRATIONS if m.version == 24)
        assert v24.oltp_fallback is True


class TestStewardDisplayNameMigration:
    """v26: add steward_display_name to dq_rules, dq_monitored_tables, dq_data_products (OLTP fallback)."""

    def test_v26_adds_steward_display_name_to_three_tables(self) -> None:
        v26 = next(m for m in MIGRATIONS if m.version == 26)
        sql = v26.sql_template
        assert "dq_rules" in sql, "v26 must touch dq_rules"
        assert "dq_monitored_tables" in sql, "v26 must touch dq_monitored_tables"
        assert "dq_data_products" in sql, "v26 must touch dq_data_products"
        assert "steward_display_name" in sql, "v26 must add steward_display_name"
        assert sql.count("ADD COLUMN") == 3, "v26 must have exactly 3 ADD COLUMN statements"

    def test_v26_is_oltp_fallback(self) -> None:
        v26 = next(m for m in MIGRATIONS if m.version == 26)
        assert v26.oltp_fallback is True

    def test_v26_version_follows_v25(self) -> None:
        versions = [m.version for m in MIGRATIONS]
        idx25 = versions.index(25)
        idx26 = versions.index(26)
        assert idx26 == idx25 + 1, "v26 must immediately follow v25 in MIGRATIONS"


class TestOwnerRenameMigration:
    """v28: add owner/owner_display_name (copy from steward) on OLTP fallback tables."""

    def test_v28_adds_owner_columns_and_copies(self) -> None:
        v28 = next(m for m in MIGRATIONS if m.version == 28)
        sql = v28.sql_template
        assert "dq_rules" in sql
        assert "dq_monitored_tables" in sql
        assert "dq_data_products" in sql
        assert "ADD COLUMN owner" in sql
        assert "ADD COLUMN owner_display_name" in sql
        assert "SET owner = steward" in sql
        assert "SET owner_display_name = steward_display_name" in sql

    def test_v28_is_oltp_fallback(self) -> None:
        v28 = next(m for m in MIGRATIONS if m.version == 28)
        assert v28.oltp_fallback is True

    def test_v28_version_follows_v27(self) -> None:
        versions = [m.version for m in MIGRATIONS]
        idx27 = versions.index(27)
        idx28 = versions.index(28)
        assert idx28 == idx27 + 1, "v28 must immediately follow v27 in MIGRATIONS"


class TestNotesAndRationaleMigration:
    """v27: notes + change-rationale columns (OLTP fallback)."""

    def test_v27_adds_notes_and_rationale_columns(self) -> None:
        v27 = next(m for m in MIGRATIONS if m.version == 27)
        sql = v27.sql_template
        assert "dq_monitored_tables" in sql
        assert "dq_data_products" in sql
        assert "dq_rules" in sql
        assert "dq_rules_history" in sql
        assert "notes" in sql
        assert "pending_rationale" in sql
        assert "last_decision_rationale" in sql
        assert "rationale" in sql
        # Rule notes stay in user_metadata — no notes column on dq_rules.
        assert "dq_rules ADD COLUMN notes" not in sql.replace("\n", " ")

    def test_v27_is_oltp_fallback(self) -> None:
        v27 = next(m for m in MIGRATIONS if m.version == 27)
        assert v27.oltp_fallback is True

    def test_v27_version_follows_v26(self) -> None:
        versions = [m.version for m in MIGRATIONS]
        idx26 = versions.index(26)
        idx27 = versions.index(27)
        assert idx27 == idx26 + 1, "v27 must immediately follow v26 in MIGRATIONS"


class TestDropNotesMigration:
    """v29: drop sticky object notes columns (OLTP fallback)."""

    def test_v29_drops_notes_columns(self) -> None:
        v29 = next(m for m in MIGRATIONS if m.version == 29)
        sql = v29.sql_template
        assert "dq_monitored_tables" in sql
        assert "dq_data_products" in sql
        assert "DROP COLUMN notes" in sql
        assert "pending_rationale" not in sql
        assert "last_decision_rationale" not in sql

    def test_v29_is_oltp_fallback(self) -> None:
        v29 = next(m for m in MIGRATIONS if m.version == 29)
        assert v29.oltp_fallback is True

    def test_v29_version_follows_v28(self) -> None:
        versions = [m.version for m in MIGRATIONS]
        idx28 = versions.index(28)
        idx29 = versions.index(29)
        assert idx29 == idx28 + 1, "v29 must immediately follow v28 in MIGRATIONS"


class TestScheduleSampleSizeMigration:
    """v30: per-schedule run scope on both scheduled entities (OLTP fallback)."""

    def test_v30_adds_the_column_to_both_tables(self) -> None:
        v30 = next(m for m in MIGRATIONS if m.version == 30)
        sql = v30.sql_template
        assert "dq_monitored_tables ADD COLUMN schedule_sample_size INT" in sql
        assert "dq_data_products ADD COLUMN schedule_sample_size INT" in sql

    def test_v30_is_oltp_fallback(self) -> None:
        v30 = next(m for m in MIGRATIONS if m.version == 30)
        assert v30.oltp_fallback is True

    def test_v30_version_follows_v29(self) -> None:
        versions = [m.version for m in MIGRATIONS]
        assert versions.index(30) == versions.index(29) + 1, "v30 must immediately follow v29 in MIGRATIONS"

    def test_column_already_exists_is_idempotent(self) -> None:
        """A re-run against a converged DB must be swallowed, not fatal."""
        assert "COLUMN_ALREADY_EXISTS" in MigrationRunner._IDEMPOTENT_ERROR_FRAGMENTS


class TestStripExecuteFromRegistryRuleGrantsMigration:
    """v25: strip EXECUTE from registry_rule users-group grant rows (OLTP fallback)."""

    def test_v25_targets_registry_rule_execute_rows(self) -> None:
        v25 = next(m for m in MIGRATIONS if m.version == 25)
        sql = v25.sql_template
        assert "UPDATE" in sql, "v25 must be an UPDATE statement"
        assert "dq_object_grants" in sql
        assert "registry_rule" in sql, "must target registry_rule rows"
        assert "EXECUTE" in sql, "must reference the EXECUTE token to strip it"
        # Spark SQL array helpers used to strip the token
        assert "array_remove" in sql, "must use array_remove to strip EXECUTE"
        assert "split" in sql
        assert "array_join" in sql
        # Must NOT touch ALL_PRIVILEGES rows (owner rows)
        assert "ALL_PRIVILEGES" in sql, "must guard against touching ALL_PRIVILEGES rows"
        assert "<> 'ALL_PRIVILEGES'" in sql, "must explicitly exclude ALL_PRIVILEGES rows"

    def test_v25_is_oltp_fallback(self) -> None:
        v25 = next(m for m in MIGRATIONS if m.version == 25)
        assert v25.oltp_fallback is True


# ---------------------------------------------------------------------------
# Identifier-quoting contract — review item #8.
# ---------------------------------------------------------------------------


class TestSqlExecutorQuoting:
    """Direct tests of :meth:`SqlExecutor.q` (the Delta quoter)."""

    @pytest.fixture
    def executor(self) -> SqlExecutor:
        return SqlExecutor(ws=MagicMock(name="WorkspaceClient"), warehouse_id="wh", catalog="c", schema="s")

    @pytest.mark.parametrize(
        ("identifier", "expected"),
        [
            ("foo", "`foo`"),
            # The exact hyphenated-catalog case the reviewer flagged
            ("prod-east", "`prod-east`"),
            ("team-data-platform", "`team-data-platform`"),
            # Reserved-word column — backticks are mandatory
            ("check", "`check`"),
            # Internal backticks must be doubled per Databricks SQL convention,
            # mirroring PgExecutor.q which doubles internal ``"``.
            ("weird`name", "`weird``name`"),
            # Pre-doubled (no over-escaping)
            ("a``b", "`a````b`"),
            # Unicode is fine — Databricks identifiers permit it when quoted
            ("café", "`café`"),
        ],
    )
    def test_quotes_identifier(self, executor: SqlExecutor, identifier: str, expected: str) -> None:
        assert executor.q(identifier) == expected


class TestMigrationRunnerUsesQuotedIdentifiers:
    """End-to-end regression for the catalog/schema raw-interpolation bug.

    Builds a runner against a fake executor whose ``catalog`` is the
    reviewer-flagged ``prod-east`` and asserts every SQL the runner
    issues references the *quoted* form. A regression here would mean
    a hyphenated-catalog deployment silently emits parse-invalid DDL.
    """

    @pytest.fixture
    def runner_state(self) -> tuple[MigrationRunner, MagicMock]:
        ws = MagicMock(name="WorkspaceClient")
        # Use a real SqlExecutor so we exercise its real ``.q()`` path
        # rather than re-mocking the quoting behaviour.
        sql = SqlExecutor(ws=ws, warehouse_id="wh", catalog="prod-east", schema="dqx_studio")
        runner = MigrationRunner(sql=sql)
        return runner, ws

    def test_meta_table_is_quoted(self, runner_state: tuple[MigrationRunner, MagicMock]) -> None:
        runner, _ = runner_state
        # `prod-east`.`dqx_studio`.dq_migrations
        assert runner._meta_table == "`prod-east`.`dqx_studio`.dq_migrations"

    def test_ensure_schema_emits_quoted_ddl(self, runner_state: tuple[MigrationRunner, MagicMock]) -> None:
        runner, _ = runner_state
        # Mock out the execute path so we can read the SQL it would send.
        captured: list[str] = []
        runner._sql.execute_no_schema = lambda s: captured.append(s)  # type: ignore[method-assign]
        runner._ensure_schema()
        assert captured == ["CREATE SCHEMA IF NOT EXISTS `prod-east`.`dqx_studio`"]

    def test_apply_substitutes_quoted_form_into_placeholders(
        self, runner_state: tuple[MigrationRunner, MagicMock]
    ) -> None:
        """``{catalog}`` / ``{schema}`` must receive the backtick-quoted form."""
        runner, _ = runner_state
        captured: list[str] = []
        runner._sql.execute = lambda s, **_: captured.append(s)  # type: ignore[method-assign]

        # Use a tiny ad-hoc migration that won't trip the idempotency
        # swallow list (so we can read the captured SQL directly).
        from databricks_labs_dqx_app.backend.migrations import DeltaMigration

        m = DeltaMigration(
            version=999,
            description="test",
            sql_template="CREATE TABLE IF NOT EXISTS {catalog}.{schema}.test_t (x INT)",
            oltp_fallback=False,
        )
        runner._apply(m)

        # The first captured SQL is the CREATE TABLE; the second is the
        # INSERT INTO dq_migrations. Both must reference the quoted form.
        assert captured[0] == "CREATE TABLE IF NOT EXISTS `prod-east`.`dqx_studio`.test_t (x INT)"
        assert captured[1].startswith("INSERT INTO `prod-east`.`dqx_studio`.dq_migrations")
        # And specifically NOT the raw form
        assert "prod-east.dqx_studio" not in " ".join(
            captured
        ), "Found raw (un-quoted) interpolation — hyphenated catalogs would emit parse-invalid DDL"


# ---------------------------------------------------------------------------
# Quarantine table liquid-clustering keys
# ---------------------------------------------------------------------------


class TestQuarantineClustering:
    """dq_quarantine_records is liquid-clustered by (run_id, source_table_fqn)."""

    def test_quarantine_clustered_by_run_id_then_source_table_fqn(self) -> None:
        from databricks_labs_dqx_app.backend.migrations import MIGRATIONS

        v1 = next(m for m in MIGRATIONS if m.version == 1)
        sql = v1.sql_template
        assert "dq_quarantine_records" in sql
        assert (
            "CLUSTER BY (run_id, source_table_fqn)" in sql
        ), "quarantine table must be liquid-clustered by (run_id, source_table_fqn)"
        # Guard against a stray leftover single-key clause.
        assert "dq_quarantine_records" not in sql or "CLUSTER BY (run_id)" not in sql.replace(
            "CLUSTER BY (run_id, source_table_fqn)", ""
        )
