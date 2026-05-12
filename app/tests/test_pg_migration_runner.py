"""Unit tests for :class:`PgMigrationRunner` (Lakebase Postgres migrations).

The runner is tested in pure isolation against a mock satisfying the
``_Executor`` Protocol. The real :class:`PgExecutor` is exercised in
integration tests against a live Lakebase instance.

Two non-obvious things the helpers handle:

1. ``MagicMock``'s auto-generated ``__exit__`` returns a (truthy)
   :class:`MagicMock`, which would *suppress* every exception raised
   inside a ``with`` block. We explicitly wire ``__exit__`` to ``None``
   on both the connection and cursor context managers so a real
   ``RuntimeError`` from ``cur.execute`` propagates out — otherwise the
   partial-failure test would pass for the wrong reason.

2. The runner uses ``with self._exec.connection() as conn: with
   conn.cursor() as cur: cur.execute(...)``. The cursor used inside
   ``_apply`` therefore lives several attribute hops deep in the mock
   chain. :func:`_cursor_of` and :func:`_connection_of` walk that chain
   so assertions stay legible.
"""

from __future__ import annotations

import dataclasses
from unittest.mock import MagicMock

import pytest

from databricks_labs_dqx_app.backend.migrations.postgres import (
    PG_MIGRATIONS,
    PgMigration,
    PgMigrationRunner,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_executor(*, applied_versions: tuple[int, ...] = (), schema: str = "public") -> MagicMock:
    """Build a mock ``_Executor`` pre-seeded with the given applied-versions list."""
    exec_mock = MagicMock(name="PgExecutor")
    exec_mock.schema = schema
    exec_mock.database = "test_db"
    # _applied_versions issues one SELECT and parses ``[[str(v)], ...]``.
    exec_mock.query.return_value = [[str(v)] for v in applied_versions]

    # Disarm MagicMock's truthy auto-__exit__ on both context managers
    # so exceptions inside ``with self._exec.connection() as conn:`` and
    # ``with conn.cursor() as cur:`` propagate normally.
    conn_cm = exec_mock.connection.return_value
    conn_cm.__exit__.return_value = None
    conn = conn_cm.__enter__.return_value
    cur_cm = conn.cursor.return_value
    cur_cm.__exit__.return_value = None

    return exec_mock


def _connection_of(exec_mock: MagicMock) -> MagicMock:
    """Return the ``conn`` object the runner sees inside its ``with`` block."""
    return exec_mock.connection.return_value.__enter__.return_value


def _cursor_of(exec_mock: MagicMock) -> MagicMock:
    """Return the ``cur`` object the runner uses inside :meth:`_apply`."""
    return _connection_of(exec_mock).cursor.return_value.__enter__.return_value


def _executed_sqls(cursor_mock: MagicMock) -> list[str]:
    """All SQL strings that hit the inner cursor, in call order."""
    return [c.args[0] for c in cursor_mock.execute.call_args_list]


def _insert_meta_sqls(cursor_mock: MagicMock) -> list[str]:
    """Just the ``INSERT INTO ...dq_migrations`` calls."""
    return [s for s in _executed_sqls(cursor_mock) if "INSERT INTO" in s and "dq_migrations" in s]


# ---------------------------------------------------------------------------
# PG_MIGRATIONS catalogue invariants
# ---------------------------------------------------------------------------


class TestPgMigrationsCatalogue:
    def test_versions_are_unique(self):
        versions = [m.version for m in PG_MIGRATIONS]
        assert len(versions) == len(set(versions)), "PG_MIGRATIONS version numbers must be unique"

    def test_versions_are_monotonically_increasing(self):
        versions = [m.version for m in PG_MIGRATIONS]
        assert versions == sorted(versions), "PG_MIGRATIONS entries must be listed in ascending version order"

    def test_every_migration_has_a_description(self):
        # Empty descriptions break the dq_migrations INSERT (NOT NULL).
        for m in PG_MIGRATIONS:
            assert m.description.strip(), f"v{m.version} migration has an empty description"


# ---------------------------------------------------------------------------
# PgMigration dataclass behaviour
# ---------------------------------------------------------------------------


class TestPgMigrationDataclass:
    def test_is_frozen(self):
        m = PgMigration(version=99, description="test", sql="SELECT 1")
        with pytest.raises(dataclasses.FrozenInstanceError):
            m.version = 100  # type: ignore[misc]

    def test_equality_is_by_value(self):
        a = PgMigration(version=1, description="d", sql="s")
        b = PgMigration(version=1, description="d", sql="s")
        c = PgMigration(version=2, description="d", sql="s")
        assert a == b
        assert a != c


# ---------------------------------------------------------------------------
# run_all — pending/applied dispatch
# ---------------------------------------------------------------------------


class TestRunAllDispatch:
    def test_applies_all_when_none_applied(self):
        exec_mock = _make_executor(applied_versions=())
        runner = PgMigrationRunner(exec_mock)

        applied = runner.run_all()

        assert applied == len(PG_MIGRATIONS)
        # Each migration records its version in dq_migrations.
        assert len(_insert_meta_sqls(_cursor_of(exec_mock))) == len(PG_MIGRATIONS)

    def test_skips_all_when_already_applied(self):
        all_versions = tuple(m.version for m in PG_MIGRATIONS)
        exec_mock = _make_executor(applied_versions=all_versions)
        runner = PgMigrationRunner(exec_mock)

        applied = runner.run_all()

        assert applied == 0
        # _apply is never entered, so connection() must not be touched.
        exec_mock.connection.assert_not_called()

    def test_runs_only_pending_when_some_already_applied(self, monkeypatch):
        """With v1 already applied, only the new v2 should run."""
        fake_migrations = [
            PgMigration(version=1, description="v1", sql="CREATE TABLE {schema}.t1 (id int);"),
            PgMigration(version=2, description="v2", sql="CREATE TABLE {schema}.t2 (id int);"),
        ]
        monkeypatch.setattr(
            "databricks_labs_dqx_app.backend.migrations.postgres.PG_MIGRATIONS",
            fake_migrations,
        )
        exec_mock = _make_executor(applied_versions=(1,))
        runner = PgMigrationRunner(exec_mock)

        applied = runner.run_all()

        assert applied == 1
        cur = _cursor_of(exec_mock)
        inserts = _insert_meta_sqls(cur)
        assert len(inserts) == 1
        assert "VALUES (2," in inserts[0]
        # v1's DDL must not be re-applied.
        sqls = _executed_sqls(cur)
        assert not any(".t1 " in s for s in sqls), "v1 DDL ran a second time"
        assert any(".t2 " in s for s in sqls), "v2 DDL did not run"

    def test_bootstraps_schema_and_meta_table_before_any_migration(self):
        exec_mock = _make_executor(applied_versions=())
        runner = PgMigrationRunner(exec_mock)
        runner.run_all()

        # _ensure_schema and _ensure_meta_table go through executor.execute
        # (NOT cursor.execute) — they are one-shot statements applied
        # before the transactional _apply loop.
        bootstrap = [c.args[0] for c in exec_mock.execute.call_args_list]
        assert any("CREATE SCHEMA IF NOT EXISTS" in s for s in bootstrap)
        assert any("CREATE TABLE IF NOT EXISTS" in s and "dq_migrations" in s for s in bootstrap)

    def test_schema_placeholder_is_substituted(self):
        """Every {schema} placeholder must be replaced before SQL leaves the runner."""
        exec_mock = _make_executor(applied_versions=(), schema="custom_app_schema")
        runner = PgMigrationRunner(exec_mock)
        runner.run_all()

        cur = _cursor_of(exec_mock)
        for sql in _executed_sqls(cur):
            assert "{schema}" not in sql, f"Unsubstituted placeholder in: {sql!r}"
        # And the configured schema name actually appears somewhere.
        assert any("custom_app_schema" in s for s in _executed_sqls(cur))


# ---------------------------------------------------------------------------
# _apply — atomicity contract
# ---------------------------------------------------------------------------


class TestApplyAtomicity:
    """The contract Laurence's review surfaced: DDL + version row are atomic."""

    def test_one_commit_per_migration(self):
        """Each migration ends with exactly one ``conn.commit()`` — the
        DDL and the dq_migrations INSERT share that single transaction."""
        exec_mock = _make_executor(applied_versions=())
        runner = PgMigrationRunner(exec_mock)
        runner.run_all()

        conn = _connection_of(exec_mock)
        assert conn.commit.call_count == len(PG_MIGRATIONS)

    def test_per_statement_split_emits_multiple_cursor_executes(self):
        """The runner still splits on ``;`` so cursor errors pinpoint the
        exact failing DDL statement rather than a position inside a
        multi-kilobyte compound string."""
        exec_mock = _make_executor(applied_versions=())
        runner = PgMigrationRunner(exec_mock)
        runner.run_all()

        cur = _cursor_of(exec_mock)
        # Floor: every migration produces at least one DDL execute plus
        # the bookkeeping INSERT.
        assert cur.execute.call_count >= 2 * len(PG_MIGRATIONS)

    def test_version_row_insert_runs_through_the_same_cursor(self):
        """The dq_migrations INSERT must reach the same transactional
        cursor as the DDL — never the executor's auto-committing
        ``execute`` path."""
        exec_mock = _make_executor(applied_versions=())
        runner = PgMigrationRunner(exec_mock)
        runner.run_all()

        cur_inserts = _insert_meta_sqls(_cursor_of(exec_mock))
        exec_inserts = [c.args[0] for c in exec_mock.execute.call_args_list if "INSERT INTO" in c.args[0]]
        assert cur_inserts, "dq_migrations INSERT must go through the transactional cursor"
        assert not any("dq_migrations" in s for s in exec_inserts), (
            "dq_migrations INSERT leaked onto the auto-committing executor.execute path — "
            "would break migration atomicity"
        )

    def test_failure_during_ddl_aborts_transaction_and_propagates(self, monkeypatch):
        """If a DDL statement raises, the transaction rolls back (no
        commit), the version row is never written, and the exception
        propagates so the next start retries cleanly."""
        # Replace PG_MIGRATIONS with a deterministic single migration so
        # we know exactly which cur.execute call should fail.
        fake_migration = PgMigration(
            version=42,
            description="failing test",
            sql=(
                "CREATE TABLE {schema}.t1 (id int);"
                "CREATE TABLE {schema}.t2 (id int);"
                "CREATE TABLE {schema}.t3 (id int);"
            ),
        )
        monkeypatch.setattr(
            "databricks_labs_dqx_app.backend.migrations.postgres.PG_MIGRATIONS",
            [fake_migration],
        )

        exec_mock = _make_executor(applied_versions=())
        cur = _cursor_of(exec_mock)
        # Pass: t1 → succeed, t2 → succeed, t3 → boom. INSERT never reached.
        cur.execute.side_effect = [None, None, RuntimeError("boom"), None, None]

        runner = PgMigrationRunner(exec_mock)
        with pytest.raises(RuntimeError, match="boom"):
            runner.run_all()

        conn = _connection_of(exec_mock)
        # commit() must NOT have run — Postgres would roll the
        # transaction back when the connection returns to the pool.
        conn.commit.assert_not_called()
        # And the version row must never have been requested either.
        assert not _insert_meta_sqls(cur)

    def test_failure_in_first_migration_skips_subsequent_migrations(self, monkeypatch):
        """A failed migration must short-circuit the rest of the run —
        otherwise a later migration could land on top of a partially-
        rolled-back schema."""
        fake_migrations = [
            PgMigration(version=1, description="v1", sql="BROKEN;"),
            PgMigration(version=2, description="v2", sql="CREATE TABLE {schema}.t2 (id int);"),
        ]
        monkeypatch.setattr(
            "databricks_labs_dqx_app.backend.migrations.postgres.PG_MIGRATIONS",
            fake_migrations,
        )

        exec_mock = _make_executor(applied_versions=())
        cur = _cursor_of(exec_mock)
        cur.execute.side_effect = RuntimeError("boom")  # every execute fails

        runner = PgMigrationRunner(exec_mock)
        with pytest.raises(RuntimeError, match="boom"):
            runner.run_all()

        # v2's DDL must not have been attempted.
        sqls = _executed_sqls(cur)
        assert not any(".t2 " in s for s in sqls), "v2 ran despite v1 failing — short-circuit broken"
