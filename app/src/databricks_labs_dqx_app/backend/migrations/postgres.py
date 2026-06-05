"""Lakebase (Postgres) migration runner for DQX Studio OLTP tables.

The OLTP subset of the schema (rules catalog, app settings, RBAC,
comments, schedule configs, scheduler bookkeeping) lives in Lakebase
Postgres when ``conf.lakebase_enabled`` is true.  Append-mostly
analytical tables (``dq_validation_runs``, ``dq_profiling_results``,
``dq_metrics``, ``dq_quarantine_records``) stay in Delta because they
are written by the Spark task runner.

The runner mirrors :class:`backend.migrations.MigrationRunner` so the
operational story (versioned, idempotent, recorded in a
``dq_migrations`` meta table) is identical regardless of backend.

Schema mapping highlights
-------------------------
- Delta ``VARIANT`` (``check``, future blob columns) → Postgres
  ``JSONB``.  Both store JSON natively with predicate pushdown; JSONB
  also supports GIN indexes for low-latency lookups.
- Delta ``TIMESTAMP`` → Postgres ``TIMESTAMPTZ``.  Postgres'
  timezone-naive ``TIMESTAMP`` would silently drop offsets and break
  cross-region debugging.
- Delta ``BIGINT`` / ``INT`` / ``DOUBLE`` → ``BIGINT`` / ``INT`` /
  ``DOUBLE PRECISION`` respectively.
- Delta ``CLUSTER BY`` is not exposed; Postgres uses indexes
  declaratively where the access pattern justifies them.  Each table
  gets the small set of indexes the FastAPI services actually need.

Adding a new migration
----------------------
Append a new :class:`PgMigration` entry with the next monotonically
increasing version number.  Postgres supports ``ALTER TABLE ... ADD
COLUMN IF NOT EXISTS`` natively so re-running is safe out of the box.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import Any, Protocol

# Import directly from the psycopg-free helpers module — NOT from
# :mod:`..pg_executor`, which would transitively pull in :mod:`psycopg`
# at module load. The migration runner itself only needs the trust-
# boundary wrappers; deferring the heavy psycopg import to the
# executor's own users keeps this module importable in Delta-only
# environments (e.g. the dqx-library integration test rig).
from ..pg_cursor_helpers import run_parameterized_sql, run_trusted_sql
from ..sql_executor import OltpExecutorProtocol

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Migration protocol — extends the shared :class:`OltpExecutorProtocol`
# rather than re-declaring ``schema`` / ``q`` / ``execute`` / ``query``
# fresh. Keeps the executor surface single-sourced so a future addition
# to the OLTP contract (e.g. a new ``upsert_*`` variant) doesn't drift
# between callers.
#
# The migration runner needs ONE thing beyond the OLTP contract: a raw
# :meth:`connection` context manager so it can wrap multiple DDL
# statements + the meta-table INSERT in a single Postgres transaction
# (the auto-committing :meth:`OltpExecutorProtocol.execute` would split
# them across separate transactions and leave the meta row out of sync
# on partial failure). Only :class:`PgExecutor` exposes that today —
# Delta runs through its own :class:`MigrationRunner` because Spark SQL
# DDL is not transactional anyway.
# ---------------------------------------------------------------------------


class _Executor(OltpExecutorProtocol, Protocol):
    """OLTP executor with transactional ``connection()`` access.

    Structurally satisfied only by :class:`backend.pg_executor.PgExecutor`
    (and matching test doubles). The return type of :meth:`connection`
    is typed loosely as ``AbstractContextManager[Any]`` so this Protocol
    stays free of a psycopg dependency — Delta-only deployments can
    import this module without dragging psycopg into the graph.
    """

    def connection(self) -> AbstractContextManager[Any]: ...


@dataclass(frozen=True)
class PgMigration:
    version: int
    description: str
    sql: str


# ``{schema}`` is substituted at apply-time so a single migration list
# can be re-targeted for tests/dev/prod without copy-paste.
_S = "{schema}"


PG_MIGRATIONS: list[PgMigration] = [
    PgMigration(
        version=1,
        description="Lakebase OLTP baseline (app_settings, rules, role mappings, comments, schedules)",
        sql=(
            # ----------------------------------------------------------
            # dq_app_settings — single-row-per-key KV store.
            # ----------------------------------------------------------
            f"CREATE TABLE IF NOT EXISTS {_S}.dq_app_settings ("
            "  setting_key   TEXT PRIMARY KEY,"
            "  setting_value TEXT,"
            "  updated_at    TIMESTAMPTZ,"
            "  updated_by    TEXT"
            ");"
            # ----------------------------------------------------------
            # dq_quality_rules — active rule catalog.
            # ----------------------------------------------------------
            f"CREATE TABLE IF NOT EXISTS {_S}.dq_quality_rules ("
            "  rule_id    TEXT PRIMARY KEY,"
            "  table_fqn  TEXT NOT NULL,"
            '  "check"    JSONB NOT NULL,'
            "  version    INTEGER NOT NULL,"
            "  status     TEXT NOT NULL,"
            "  source     TEXT NOT NULL,"
            "  created_by TEXT,"
            "  created_at TIMESTAMPTZ,"
            "  updated_by TEXT,"
            "  updated_at TIMESTAMPTZ,"
            "  CONSTRAINT chk_dq_quality_rules_status "
            "    CHECK (status IN ('draft','pending_approval','approved','rejected')),"
            "  CONSTRAINT chk_dq_quality_rules_source "
            "    CHECK (source IN ('ui','sql','profiler','import','ai'))"
            ");"
            # Two read-paths dominate: by table_fqn (rules-list page) and
            # by status filter (review queue). One composite index covers
            # both since Postgres can use a leading-column-only scan.
            f"CREATE INDEX IF NOT EXISTS idx_dq_quality_rules_table_status "
            f"  ON {_S}.dq_quality_rules (table_fqn, status);"
            # ----------------------------------------------------------
            # dq_quality_rules_history — append-only audit trail.
            # ----------------------------------------------------------
            f"CREATE TABLE IF NOT EXISTS {_S}.dq_quality_rules_history ("
            "  history_id  BIGSERIAL PRIMARY KEY,"
            "  rule_id     TEXT,"
            "  table_fqn   TEXT NOT NULL,"
            '  "check"     JSONB,'
            "  version     INTEGER,"
            "  source      TEXT,"
            "  action      TEXT NOT NULL,"
            "  prev_status TEXT,"
            "  new_status  TEXT,"
            "  changed_by  TEXT,"
            "  changed_at  TIMESTAMPTZ"
            ");"
            f"CREATE INDEX IF NOT EXISTS idx_dq_quality_rules_history_rule_changed_at "
            f"  ON {_S}.dq_quality_rules_history (rule_id, changed_at DESC);"
            f"CREATE INDEX IF NOT EXISTS idx_dq_quality_rules_history_table_changed_at "
            f"  ON {_S}.dq_quality_rules_history (table_fqn, changed_at DESC);"
            # ----------------------------------------------------------
            # dq_role_mappings — RBAC.
            # ----------------------------------------------------------
            f"CREATE TABLE IF NOT EXISTS {_S}.dq_role_mappings ("
            "  role       TEXT NOT NULL,"
            "  group_name TEXT NOT NULL,"
            "  created_by TEXT,"
            "  created_at TIMESTAMPTZ,"
            "  updated_by TEXT,"
            "  updated_at TIMESTAMPTZ,"
            "  PRIMARY KEY (role, group_name),"
            "  CONSTRAINT chk_dq_role_mappings_role "
            "    CHECK (role IN ('admin','rule_approver','rule_author','viewer','runner'))"
            ");"
            # ----------------------------------------------------------
            # dq_comments — per-entity comment threads.
            # ----------------------------------------------------------
            f"CREATE TABLE IF NOT EXISTS {_S}.dq_comments ("
            "  comment_id  TEXT PRIMARY KEY,"
            "  entity_type TEXT NOT NULL,"
            "  entity_id   TEXT NOT NULL,"
            "  user_email  TEXT NOT NULL,"
            "  comment     TEXT NOT NULL,"
            "  created_at  TIMESTAMPTZ,"
            "  CONSTRAINT chk_dq_comments_entity_type "
            "    CHECK (entity_type IN ('run','rule'))"
            ");"
            f"CREATE INDEX IF NOT EXISTS idx_dq_comments_entity_created_at "
            f"  ON {_S}.dq_comments (entity_type, entity_id, created_at DESC);"
            # ----------------------------------------------------------
            # dq_schedule_runs — last/next-run pointer per schedule.
            # ----------------------------------------------------------
            f"CREATE TABLE IF NOT EXISTS {_S}.dq_schedule_runs ("
            "  schedule_name TEXT PRIMARY KEY,"
            "  last_run_at   TIMESTAMPTZ,"
            "  next_run_at   TIMESTAMPTZ,"
            "  last_run_id   TEXT,"
            "  status        TEXT,"
            "  updated_at    TIMESTAMPTZ,"
            "  CONSTRAINT chk_dq_schedule_runs_status "
            "    CHECK (status IS NULL OR status IN "
            "      ('pending','success','partial_failure','failed'))"
            ");"
            # The scheduler loop polls "next_run_at <= now() AND status
            # IS NOT 'pending'" every tick; a btree index on
            # next_run_at keeps that scan cheap as the schedule list
            # grows.
            f"CREATE INDEX IF NOT EXISTS idx_dq_schedule_runs_next_run_at "
            f"  ON {_S}.dq_schedule_runs (next_run_at);"
            # ----------------------------------------------------------
            # dq_schedule_configs — live config per schedule.
            # ----------------------------------------------------------
            f"CREATE TABLE IF NOT EXISTS {_S}.dq_schedule_configs ("
            "  schedule_name TEXT PRIMARY KEY,"
            "  config_json   TEXT NOT NULL,"
            "  version       INTEGER NOT NULL,"
            "  created_by    TEXT,"
            "  created_at    TIMESTAMPTZ,"
            "  updated_by    TEXT,"
            "  updated_at    TIMESTAMPTZ"
            ");"
            # ----------------------------------------------------------
            # dq_schedule_configs_history — append-only audit trail.
            # ----------------------------------------------------------
            f"CREATE TABLE IF NOT EXISTS {_S}.dq_schedule_configs_history ("
            "  history_id    BIGSERIAL PRIMARY KEY,"
            "  schedule_name TEXT NOT NULL,"
            "  config_json   TEXT,"
            "  version       INTEGER,"
            "  action        TEXT NOT NULL,"
            "  changed_by    TEXT,"
            "  changed_at    TIMESTAMPTZ"
            ");"
            f"CREATE INDEX IF NOT EXISTS idx_dq_schedule_configs_history_schedule_changed_at "
            f"  ON {_S}.dq_schedule_configs_history (schedule_name, changed_at DESC);"
        ),
    ),
]


_META_TABLE = f"{_S}.dq_migrations"


class PgMigrationRunner:
    """Applies pending Postgres migrations to the Lakebase OLTP schema.

    Mirrors :class:`backend.migrations.MigrationRunner` so the lifespan
    handler can call ``run_all`` against either backend with the same
    contract.  The schema is created (``CREATE SCHEMA IF NOT EXISTS``)
    so the very first deploy doesn't need a separate bootstrap step on
    the Postgres side — Lakebase is provisioned bare with just a
    ``public`` schema by default.
    """

    def __init__(
        self,
        executor: _Executor,
        *,
        migrations: Sequence[PgMigration] | None = None,
    ) -> None:
        """Construct the runner.

        Parameters
        ----------
        executor:
            Postgres executor (PgExecutor in production, a mock in unit
            tests) satisfying :class:`_Executor`.
        migrations:
            Optional migration list. Defaults to the module-level
            :data:`PG_MIGRATIONS` catalogue that production deploys
            use. Exposed as a constructor seam so tests can inject a
            deterministic fake list without monkey-patching the
            module-level constant — monkey-patching couples tests to
            an import path and creates surprise interactions when two
            tests run in the same process.
        """
        self._exec = executor
        self._schema = executor.schema
        # Pre-compute the executor-quoted schema once and feed it to
        # every DDL/DML site (templates, meta-table reference,
        # ``CREATE SCHEMA``). The Delta runner's
        # :class:`MigrationRunner` does the same with backticks; this
        # keeps the two backends symmetric and removes the previous
        # foot-gun where ``_ensure_schema`` was the only quoted site
        # while ``_meta_table`` and the migration templates raw-
        # interpolated the schema name.
        self._schema_q = executor.q(self._schema)
        self._meta_table = _META_TABLE.format(schema=self._schema_q)
        # Take a defensive tuple snapshot so callers can't mutate the
        # runner's view of the catalogue after construction.
        self._migrations: tuple[PgMigration, ...] = tuple(migrations if migrations is not None else PG_MIGRATIONS)

    def run_all(self) -> int:
        self._ensure_schema()
        self._ensure_meta_table()
        applied = self._applied_versions()
        count = 0
        for migration in self._migrations:
            if migration.version in applied:
                logger.debug(
                    "Postgres migration v%d (%s) already applied",
                    migration.version,
                    migration.description,
                )
                continue
            logger.info(
                "Applying Postgres migration v%d: %s",
                migration.version,
                migration.description,
            )
            self._apply(migration)
            count += 1
        return count

    def _ensure_schema(self) -> None:
        # ``self._schema_q`` is already the quoted form (see __init__).
        # Calling :meth:`_Executor.q` rather than hardcoding the ANSI
        # double-quote keeps the dialect contract on the executor.
        self._exec.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema_q}")

    def _ensure_meta_table(self) -> None:
        # ``self._meta_table`` already embeds the quoted schema.
        sql = (
            f"CREATE TABLE IF NOT EXISTS {self._meta_table} ("
            "  version INTEGER PRIMARY KEY,"
            "  description TEXT NOT NULL,"
            "  applied_at TIMESTAMPTZ NOT NULL"
            ")"
        )
        self._exec.execute(sql)

    def _applied_versions(self) -> set[int]:
        rows = self._exec.query(f"SELECT version FROM {self._meta_table} ORDER BY version")
        return {int(row[0]) for row in rows}

    def _apply(self, migration: PgMigration) -> None:
        """Apply *migration* atomically.

        Postgres DDL is transactional (CREATE TABLE / INDEX inside
        BEGIN/COMMIT all roll back together on error — modulo
        ``CREATE INDEX CONCURRENTLY`` which we don't use). We run every
        DDL statement in the migration **and** the ``dq_migrations``
        INSERT inside a single transaction so a half-applied migration
        can never end up with committed DDL but no version row. If any
        statement fails the whole migration rolls back and the next run
        retries cleanly from the beginning.

        Statements are still split on ``;`` and executed one at a time
        through a single cursor so an error message pinpoints the exact
        failing DDL rather than a position inside a multi-kilobyte
        compound string.
        """
        formatted = migration.sql.format(schema=self._schema_q)
        with self._exec.connection() as conn:
            with conn.cursor() as cur:
                for stmt in formatted.split(";"):
                    stmt = stmt.strip()
                    if stmt:
                        run_trusted_sql(cur, stmt)
                # Same transaction: either the whole migration lands
                # *and* its version row is recorded, or nothing does.
                #
                # The template here is trusted (``self._meta_table`` is
                # built via the executor's ``q()`` quoter at __init__),
                # but ``migration.version`` / ``migration.description``
                # are runtime values from a Python object — they MUST
                # NOT be f-string-interpolated even with manual escaping.
                # Routing them through ``run_parameterized_sql`` hands
                # the binding to psycopg / libpq, which keeps the values
                # out of the SQL string entirely. See the docstring on
                # ``backend.pg_executor.run_trusted_sql`` for the full
                # contract.
                run_parameterized_sql(
                    cur,
                    f"INSERT INTO {self._meta_table} (version, description, applied_at) "
                    "VALUES (%s, %s, CURRENT_TIMESTAMP)",
                    (migration.version, migration.description),
                )
            conn.commit()
        logger.info("Postgres migration v%d applied", migration.version)
