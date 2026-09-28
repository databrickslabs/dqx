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

import re
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


# ---------------------------------------------------------------------------
# Baseline shape — the schema is CREATE-only
# ---------------------------------------------------------------------------


def _statements(template: str) -> list[str]:
    """Split a migration template the way ``_apply`` does, whitespace-normalized."""
    return [" ".join(stmt.split()) for stmt in template.split(";") if stmt.strip()]


class TestBaselineOnlyCatalogue:
    """The catalogue is one analytical baseline expressed as CREATE TABLE.

    There are no external installs to upgrade yet, so a shape change is
    edited into the baseline rather than appended as an ALTER. These tests
    pin that decision: an ``ADD COLUMN``, ``DROP COLUMN``, or backfill
    ``UPDATE`` creeping back in would mean the schema is once again split
    across a replay chain — and ``DROP COLUMN`` in particular is rejected
    outright by Delta tables without the column-mapping feature, which is
    how the previous chain broke in CI.
    """

    def test_catalogue_contains_only_the_analytical_baseline(self) -> None:
        assert [(m.version, m.description) for m in MIGRATIONS] == [
            (1, "Delta analytical baseline (validation, profiling, quarantine, metrics)")
        ]

    def test_delta_migrations_do_not_create_oltp_tables(self) -> None:
        created = {
            match
            for migration in MIGRATIONS
            for match in re.findall(
                r"CREATE TABLE IF NOT EXISTS \{catalog\}\.\{schema\}\.([a-z_][a-z0-9_]*)",
                migration.sql_template,
            )
        }
        assert created.isdisjoint(OLTP_TABLE_NAMES)

    def test_every_statement_creates_a_table_or_adds_a_constraint(self) -> None:
        # ADD CONSTRAINT is the one unavoidable ALTER: Delta accepts only
        # PK/FK inline in CREATE TABLE, so CHECKs follow their table.
        for migration in MIGRATIONS:
            for stmt in _statements(migration.sql_template):
                creates = stmt.startswith("CREATE TABLE IF NOT EXISTS")
                constrains = stmt.startswith("ALTER TABLE") and "ADD CONSTRAINT" in stmt
                assert creates or constrains, f"v{migration.version} statement is neither: {stmt[:120]}"

    def test_no_table_is_created_by_both_baselines(self) -> None:
        assert not set(ANALYTICAL_TABLE_NAMES) & set(OLTP_TABLE_NAMES)

    def test_analytical_baseline_owns_the_spark_written_tables(self) -> None:
        assert ANALYTICAL_TABLE_NAMES == (
            "dq_profiling_results",
            "dq_validation_runs",
            "dq_quarantine_records",
            "dq_metrics",
        )

    @pytest.mark.parametrize(
        "table",
        [
            "dq_rules",
            "dq_monitored_tables",
            "dq_data_products",
            "dq_pending_applications",
            "dq_tag_auto_suppressions",
            "dq_object_grants",
            "dq_score_cache",
            "dq_rule_embeddings",
        ],
    )
    def test_oltp_tables_come_from_the_postgres_baseline(self, table: str) -> None:
        assert table in OLTP_TABLE_NAMES


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
        from databricks_labs_dqx_app.backend.migrations import Migration

        m = Migration(
            version=999,
            description="test",
            sql_template="CREATE TABLE IF NOT EXISTS {catalog}.{schema}.test_t (x INT)",
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
