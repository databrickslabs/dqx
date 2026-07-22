"""Unit tests for :class:`PgMigrationRunner` (Lakebase Postgres migrations).

The runner is tested in pure isolation against a typed mock of the real
:class:`PgExecutor`. Production exercises a live Lakebase instance via
integration tests; here we focus on dispatch, atomicity, and identifier
quoting.

Why the helpers look the way they do
------------------------------------
* **`create_autospec(PgExecutor, instance=True)`** rather than a bare
  ``MagicMock``. The spec'd mock rejects attribute access for any
  attribute the real :class:`PgExecutor` doesn't expose, so a future
  rename (``query`` → ``run_query``, etc.) surfaces as an
  AttributeError at test time. AGENTS.md mandates this pattern:
  "construct dependencies with create_autospec rather than patching
  internal module state."

* **No explicit ``__exit__`` wiring.** The auto-generated context-
  manager support on the return values of method calls returns
  ``False`` from ``__exit__`` by default — exceptions inside the
  ``with`` block propagate out correctly without a manual override.
  (The earlier version of this file pre-emptively forced ``__exit__``
  to ``None`` "just in case"; that was cargo-culted from a different
  failure mode and is no longer needed once the helper is spec'd
  against the real surface.)

* **Migration list is injected via the constructor seam.** The runner
  accepts a ``migrations=`` kwarg defaulting to the production
  catalogue. Tests pass a deterministic fake list directly rather
  than monkey-patching the module-level ``PG_MIGRATIONS`` — the
  monkey-patch pattern coupled tests to an import path and produced
  surprise ordering issues when multiple tests ran in the same
  process.
"""

from __future__ import annotations

import dataclasses
from unittest.mock import MagicMock, create_autospec

import pytest

from databricks_labs_dqx_app.backend.migrations.postgres import (
    PG_MIGRATIONS,
    PgMigration,
    PgMigrationRunner,
)
from databricks_labs_dqx_app.backend.pg_executor import PgExecutor


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_executor(*, applied_versions: tuple[int, ...] = (), schema: str = "public") -> MagicMock:
    """Build a typed mock of :class:`PgExecutor` pre-seeded with applied versions.

    Uses ``create_autospec`` so the mock rejects access to attributes
    that aren't on the real class — a future rename of any method the
    runner calls produces a loud test-time AttributeError rather than
    a silent passthrough.
    """
    exec_mock = create_autospec(PgExecutor, instance=True)
    exec_mock.schema = schema
    exec_mock.database = "test_db"
    # Mirror :meth:`PgExecutor.q` — ANSI double-quotes with internal
    # ``"`` doubled. The runner uses ``q()`` for every schema reference
    # so a hyphenated schema name stays parseable end-to-end.
    exec_mock.q.side_effect = lambda ident: '"' + ident.replace('"', '""') + '"'
    # ``_applied_versions`` issues one SELECT and parses ``[[str(v)], ...]``.
    exec_mock.query.return_value = [[str(v)] for v in applied_versions]
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
    """Just the ``INSERT INTO ...dq_migrations`` SQL templates."""
    return [s for s in _executed_sqls(cursor_mock) if "INSERT INTO" in s and "dq_migrations" in s]


def _insert_meta_calls(cursor_mock: MagicMock) -> list[tuple[str, tuple[object, ...]]]:
    """Return ``(sql_template, params)`` pairs for every meta-table INSERT.

    Post-review the runner uses psycopg native parameter binding
    (:func:`run_parameterized_sql` -> ``cur.execute(template, params)``)
    rather than f-string-baking the version/description into the SQL
    string. Tests assert on the bound ``params`` tuple rather than
    on substrings of the template — manual escaping in the template
    is exactly the pattern the security review rejected. See
    ``backend.pg_executor.run_trusted_sql`` for the full contract.
    """
    out: list[tuple[str, tuple[object, ...]]] = []
    for c in cursor_mock.execute.call_args_list:
        sql = c.args[0]
        if "INSERT INTO" in sql and "dq_migrations" in sql:
            params = tuple(c.args[1]) if len(c.args) >= 2 else ()
            out.append((sql, params))
    return out


def _insert_meta_versions(cursor_mock: MagicMock) -> list[object]:
    """Just the migration-version values bound to dq_migrations INSERTs.

    The first positional ``%s`` in the template is ``version``; this
    helper hands the test the *bound* values so assertions don't have
    to know about the placeholder/params split.
    """
    return [params[0] for _, params in _insert_meta_calls(cursor_mock) if params]


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

    def test_schedule_kind_baseline_declares_column_on_both_tables(self):
        # Fresh installs must carry schedule_kind on both tables (B2-52).
        # Whitespace between column name and type varies (alignment), so
        # assert the identifier, default, and CHECK name independently.
        by_version = {m.version: m for m in PG_MIGRATIONS}
        baseline_sql = by_version[1].sql  # dq_monitored_tables baseline
        products_sql = by_version[6].sql  # dq_data_products CREATE
        assert "schedule_kind" in baseline_sql
        assert "schedule_kind" in products_sql
        assert baseline_sql.count("'profiling_and_dq'") >= 1
        assert products_sql.count("'profiling_and_dq'") >= 1
        assert "chk_dq_monitored_tables_schedule_kind" in baseline_sql
        assert "chk_dq_data_products_schedule_kind" in products_sql

    def test_v14_converges_schedule_kind_on_deployed_dbs(self):
        # Already-deployed DBs get schedule_kind via the v14 converge migration.
        v14 = next(m for m in PG_MIGRATIONS if m.version == 14)
        assert "ADD COLUMN IF NOT EXISTS schedule_kind TEXT NOT NULL DEFAULT 'dq_only'" in v14.sql
        assert "dq_monitored_tables" in v14.sql
        assert "dq_data_products" in v14.sql
        assert "chk_dq_monitored_tables_schedule_kind" in v14.sql
        assert "chk_dq_data_products_schedule_kind" in v14.sql

    def test_v15_creates_pending_applications_table(self):
        # Bulk Contract Import Phase 2: staged applications awaiting approval.
        v15 = next(m for m in PG_MIGRATIONS if m.version == 15)
        assert "CREATE TABLE IF NOT EXISTS" in v15.sql
        assert "dq_pending_applications" in v15.sql
        assert "column_mapping JSONB" in v15.sql
        # One pending row per (binding, rule) — enforced by a UNIQUE constraint.
        assert "UNIQUE (binding_id, rule_id)" in v15.sql

    def test_v20_backfills_default_grants_for_all_object_types(self):
        """v20 must INSERT…SELECT default grants for all three object types
        (registry_rule, monitored_table, data_product), each with a users-group
        row and an owner row, all guarded by WHERE NOT EXISTS."""
        v20 = next(m for m in PG_MIGRATIONS if m.version == 20)
        sql = v20.sql
        for obj_type in ("registry_rule", "monitored_table", "data_product"):
            assert obj_type in sql, f"v20 must cover object type '{obj_type}'"
        assert "SELECT,APPLY,EXECUTE" in sql, "users-group privileges must be present"
        assert "ALL_PRIVILEGES" in sql, "owner privileges must be present"
        assert "NOT EXISTS" in sql, "backfill must be idempotent via WHERE NOT EXISTS"
        assert "dq_object_grants" in sql

    def test_v21_strips_execute_from_registry_rule_grants(self):
        """v21 must UPDATE dq_object_grants to remove EXECUTE from
        registry_rule rows, leaving ALL_PRIVILEGES (owner) rows untouched."""
        v21 = next(m for m in PG_MIGRATIONS if m.version == 21)
        sql = v21.sql
        assert "UPDATE" in sql, "v21 must be an UPDATE statement"
        assert "dq_object_grants" in sql
        assert "registry_rule" in sql, "must target registry_rule rows"
        assert "EXECUTE" in sql, "must reference the EXECUTE token to strip it"
        # Postgres array helpers used to strip the token
        assert "array_remove" in sql, "must use array_remove to strip EXECUTE"
        assert "string_to_array" in sql
        assert "array_to_string" in sql
        # Must NOT touch ALL_PRIVILEGES rows (owner rows)
        assert "ALL_PRIVILEGES" in sql, "must guard against touching ALL_PRIVILEGES rows"
        assert "<> 'ALL_PRIVILEGES'" in sql or "!= 'ALL_PRIVILEGES'" in sql, (
            "must explicitly exclude ALL_PRIVILEGES rows from the UPDATE"
        )


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

    def test_runs_only_pending_when_some_already_applied(self):
        """With v1 already applied, only the new v2 should run."""
        fake_migrations = [
            PgMigration(version=1, description="v1", sql="CREATE TABLE {schema}.t1 (id int);"),
            PgMigration(version=2, description="v2", sql="CREATE TABLE {schema}.t2 (id int);"),
        ]
        exec_mock = _make_executor(applied_versions=(1,))
        runner = PgMigrationRunner(exec_mock, migrations=fake_migrations)

        applied = runner.run_all()

        assert applied == 1
        cur = _cursor_of(exec_mock)
        inserts = _insert_meta_calls(cur)
        assert len(inserts) == 1
        # Version 2 is bound through psycopg's parameter binder, not
        # baked into the SQL string.
        assert _insert_meta_versions(cur) == [2]
        # The template uses ``%s`` placeholders for both value
        # columns — confirms we didn't regress to f-string interpolation.
        template = inserts[0][0]
        assert "VALUES (%s, %s," in template
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
# Constructor injection — migrations= kwarg behaviour
# ---------------------------------------------------------------------------


class TestMigrationsInjection:
    """The ``migrations=`` constructor seam exists so tests don't have
    to monkey-patch the module-level constant. These tests pin down the
    contract so a future refactor doesn't silently drop the seam."""

    def test_default_is_the_module_level_catalogue(self):
        """``migrations=None`` (the default) must wire to ``PG_MIGRATIONS``."""
        exec_mock = _make_executor(applied_versions=tuple(m.version for m in PG_MIGRATIONS))
        runner = PgMigrationRunner(exec_mock)
        # If the default wiring is correct then "every version already
        # applied" causes zero new applications.
        assert runner.run_all() == 0

    def test_injected_list_is_used_in_place_of_default(self):
        """An explicit ``migrations=`` list must shadow ``PG_MIGRATIONS``."""
        fake = [
            PgMigration(version=9001, description="injected v9001", sql="CREATE TABLE {schema}.t9001 (id int);"),
            PgMigration(version=9002, description="injected v9002", sql="CREATE TABLE {schema}.t9002 (id int);"),
        ]
        exec_mock = _make_executor(applied_versions=())
        runner = PgMigrationRunner(exec_mock, migrations=fake)

        assert runner.run_all() == len(fake)
        cur = _cursor_of(exec_mock)
        inserts = _insert_meta_calls(cur)
        # Only the injected versions land in dq_migrations — no leak
        # from the production catalogue. The versions are bound via
        # psycopg's parameter binder, so we read them from the
        # ``params`` tuple rather than substring-searching the SQL.
        assert len(inserts) == len(fake)
        bound_versions = _insert_meta_versions(cur)
        assert 9001 in bound_versions
        assert 9002 in bound_versions
        production_versions = {m.version for m in PG_MIGRATIONS}
        assert not (set(bound_versions) & production_versions), (
            "Injected runner leaked production-catalogue versions: " f"{set(bound_versions) & production_versions}"
        )

    def test_injected_list_is_snapshotted_defensively(self):
        """Mutating the caller's list after construction must not move the runner's view.

        The runner stores a tuple snapshot to prevent the
        action-at-a-distance footgun where a test fixture mutates its
        shared list and quietly changes runner behaviour mid-suite.
        """
        fake = [PgMigration(version=1, description="v1", sql="CREATE TABLE {schema}.t (id int);")]
        runner = PgMigrationRunner(_make_executor(), migrations=fake)

        fake.append(PgMigration(version=2, description="v2", sql="CREATE TABLE {schema}.t2 (id int);"))
        # Snapshot length is what was passed at construction time.
        assert len(runner._migrations) == 1

    def test_empty_injected_list_runs_zero_migrations_but_still_bootstraps(self):
        """An empty migration list still produces schema + meta-table bootstrap."""
        exec_mock = _make_executor(applied_versions=())
        runner = PgMigrationRunner(exec_mock, migrations=[])

        assert runner.run_all() == 0
        bootstrap = [c.args[0] for c in exec_mock.execute.call_args_list]
        assert any("CREATE SCHEMA IF NOT EXISTS" in s for s in bootstrap)
        assert any("dq_migrations" in s and "CREATE TABLE IF NOT EXISTS" in s for s in bootstrap)
        # No _apply ever runs → connection() must not be touched.
        exec_mock.connection.assert_not_called()


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

    def test_failure_during_ddl_aborts_transaction_and_propagates(self):
        """If a DDL statement raises, the transaction rolls back (no
        commit), the version row is never written, and the exception
        propagates so the next start retries cleanly."""
        # Inject a deterministic single migration via the constructor
        # seam so we know exactly which DDL statement should fail.
        fake_migration = PgMigration(
            version=42,
            description="failing test",
            sql=(
                "CREATE TABLE {schema}.t1 (id int);"
                "CREATE TABLE {schema}.t2 (id int);"
                "CREATE TABLE {schema}.t3 (id int);"
            ),
        )

        exec_mock = _make_executor(applied_versions=())
        cur = _cursor_of(exec_mock)

        # Content-keyed failure instead of a positional ``[None, None,
        # RuntimeError, ...]`` list. The earlier form was brittle: if
        # the runner ever changed how it splits or batches statements
        # (e.g. adds an explicit ``BEGIN``, switches from per-statement
        # ``execute`` to ``executemany``, or skips empty trailing
        # tokens differently), the failure index would drift and the
        # test would pass for the wrong reason — green even though the
        # rollback path was never actually exercised. Keying the
        # failure to a substring of the offending DDL means the test
        # fails on the same statement regardless of how the runner
        # batches around it.
        boom_marker = ".t2 "  # space avoids matching the un-related ``t2x`` future-proofing

        def _fail_on_t2(sql: str, *_args: object, **_kwargs: object) -> None:
            if boom_marker in sql:
                raise RuntimeError("boom")

        cur.execute.side_effect = _fail_on_t2

        runner = PgMigrationRunner(exec_mock, migrations=[fake_migration])
        with pytest.raises(RuntimeError, match="boom"):
            runner.run_all()

        conn = _connection_of(exec_mock)
        # commit() must NOT have run — Postgres would roll the
        # transaction back when the connection returns to the pool.
        conn.commit.assert_not_called()
        # And the version row must never have been requested either.
        assert not _insert_meta_sqls(cur)

        # The failing statement WAS the one we targeted (the assertion
        # is what makes the content-keyed predicate load-bearing — if
        # the runner stops emitting ``.t2`` entirely the test fails
        # loudly here rather than silently). Equally, anything emitted
        # AFTER ``.t2`` proves the abort path is broken: a single
        # failed statement must stop the whole migration in its
        # tracks, not "let it skip and move on".
        sqls = _executed_sqls(cur)
        assert any(boom_marker in s for s in sqls), (
            "The targeted DDL statement was never issued — the runner "
            "may have stopped emitting it, in which case the rollback "
            "path is no longer exercised by this test."
        )
        idx_of_failure = next(i for i, s in enumerate(sqls) if boom_marker in s)
        assert not any(".t3 " in s for s in sqls[idx_of_failure + 1 :]), (
            "Runner kept executing DDL after a statement raised — the "
            "transaction's all-or-nothing semantics rely on the loop "
            "short-circuiting on first failure."
        )

    def test_failure_in_first_migration_skips_subsequent_migrations(self):
        """A failed migration must short-circuit the rest of the run —
        otherwise a later migration could land on top of a partially-
        rolled-back schema."""
        fake_migrations = [
            PgMigration(version=1, description="v1", sql="BROKEN;"),
            PgMigration(version=2, description="v2", sql="CREATE TABLE {schema}.t2 (id int);"),
        ]

        exec_mock = _make_executor(applied_versions=())
        cur = _cursor_of(exec_mock)
        cur.execute.side_effect = RuntimeError("boom")  # every execute fails

        runner = PgMigrationRunner(exec_mock, migrations=fake_migrations)
        with pytest.raises(RuntimeError, match="boom"):
            runner.run_all()

        # v2's DDL must not have been attempted.
        sqls = _executed_sqls(cur)
        assert not any(".t2 " in s for s in sqls), "v2 ran despite v1 failing — short-circuit broken"


# ---------------------------------------------------------------------------
# Executor-protocol consolidation
# ---------------------------------------------------------------------------


class TestExecutorProtocolConsolidation:
    """The migration runner's ``_Executor`` Protocol MUST extend
    :class:`OltpExecutorProtocol`.

    Why this matters: pre-consolidation the module re-declared its own
    ``schema`` / ``execute`` / ``query`` / ``q`` Protocol members
    independently of the shared OLTP contract. That drift is silent —
    a future addition to ``OltpExecutorProtocol`` (a new upsert
    variant, a new dialect helper) would NOT propagate here, and the
    structural-typing surface across the two callers would slowly
    diverge until someone tripped over a runtime AttributeError on a
    code path that only fires in production.

    Locking the inheritance relationship as a structural assertion
    means a regression to the independent-declaration form fails this
    test loudly instead of waiting for an integration smoke run.
    """

    def test_executor_protocol_extends_oltp_executor_protocol(self):
        from databricks_labs_dqx_app.backend.migrations.postgres import _Executor
        from databricks_labs_dqx_app.backend.sql_executor import OltpExecutorProtocol

        # Direct MRO check — any future refactor that drops the
        # inheritance (e.g. someone "simplifies" by re-listing the
        # members inline) trips this immediately.
        assert OltpExecutorProtocol in _Executor.__mro__, (
            "_Executor must extend OltpExecutorProtocol so the executor "
            "surface stays single-sourced. If you removed the inheritance, "
            "you've re-introduced the duplication the consolidation fixed."
        )

    def test_executor_protocol_adds_only_connection_beyond_oltp_surface(self):
        """The runner-specific surface is exactly one method: ``connection()``.

        Anything else added here is either (a) redundant with the OLTP
        contract — push it up to :class:`OltpExecutorProtocol` — or
        (b) leaking psycopg-specific concerns into the shared layer.
        Either way it deserves explicit review, which a failing assert
        forces.
        """
        from databricks_labs_dqx_app.backend.migrations.postgres import _Executor
        from databricks_labs_dqx_app.backend.sql_executor import OltpExecutorProtocol

        # The Protocol member set lives on ``__protocol_attrs__`` on
        # Python 3.12+; on 3.11 we approximate via __annotations__ +
        # dir() filtering. Either way: subtract the OLTP surface to
        # see what's unique to the migration runner.
        oltp_members = set(dir(OltpExecutorProtocol)) - set(dir(object))
        runner_members = set(dir(_Executor)) - set(dir(object))
        runner_only = runner_members - oltp_members

        assert runner_only == {"connection"}, (
            f"_Executor must add exactly 'connection' beyond OltpExecutorProtocol; "
            f"got runner-only members: {sorted(runner_only)}. "
            "Anything else either belongs on OltpExecutorProtocol "
            "(if the OLTP services would use it) or signals new "
            "psycopg-specific surface leaking into the shared layer."
        )

    def test_pg_executor_structurally_satisfies_extended_protocol(self):
        """PgExecutor remains the canonical implementation post-extension.

        Because ``OltpExecutorProtocol`` is ``@runtime_checkable``, an
        ``isinstance`` check passes iff PgExecutor exposes every member
        the Protocol requires. If a future PgExecutor refactor drops a
        Protocol-required method (or rename without an alias), this
        test fails BEFORE production hits the AttributeError.
        """
        from databricks_labs_dqx_app.backend.sql_executor import OltpExecutorProtocol

        exec_mock = _make_executor()
        assert isinstance(exec_mock, OltpExecutorProtocol), (
            "create_autospec(PgExecutor) must satisfy OltpExecutorProtocol — "
            "if this fails, PgExecutor no longer structurally satisfies the "
            "shared Protocol and every OLTP service annotation is now lying."
        )
        # And the runner-only surface is on the mock too.
        assert hasattr(exec_mock, "connection")
