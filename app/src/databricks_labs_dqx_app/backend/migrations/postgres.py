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
            # dq_quality_rules — active rule catalog. ``registry_rule_id``/
            # ``registry_version``/``applied_rule_id`` are provenance
            # columns (Phase 3A, see docs/superpowers/specs/2026-07-02-
            # rules-registry-design.md §3.1): when a row was materialized
            # from a Rules Registry application, they point back at the
            # source ``dq_rules`` row, the published version substituted,
            # and the ``dq_applied_rules`` link — all NULL for rules
            # authored directly against a table (unchanged legacy path).
            # ``source='registry'`` marks a materialized row (Phase 3C
            # ``Materializer``); the runner ignores ``source`` entirely so
            # this is purely provenance for the UI/audit trail.
            # ----------------------------------------------------------
            f"CREATE TABLE IF NOT EXISTS {_S}.dq_quality_rules ("
            "  rule_id           TEXT PRIMARY KEY,"
            "  table_fqn         TEXT NOT NULL,"
            '  "check"           JSONB NOT NULL,'
            "  version           INTEGER NOT NULL,"
            "  status            TEXT NOT NULL,"
            "  source            TEXT NOT NULL,"
            "  registry_rule_id  TEXT,"
            "  registry_version  INTEGER,"
            "  applied_rule_id   TEXT,"
            "  created_by TEXT,"
            "  created_at TIMESTAMPTZ,"
            "  updated_by TEXT,"
            "  updated_at TIMESTAMPTZ,"
            "  CONSTRAINT chk_dq_quality_rules_status "
            "    CHECK (status IN ('draft','pending_approval','approved','rejected')),"
            "  CONSTRAINT chk_dq_quality_rules_source "
            "    CHECK (source IN ('ui','sql','profiler','import','ai','registry'))"
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
            # ----------------------------------------------------------
            # dq_rules — Rules Registry: table-agnostic, versioned rule
            # templates (the authoring/governance layer). Descriptive
            # metadata (name, description, dimension, severity) is NOT
            # a column — it lives as reserved TAG keys inside
            # ``user_metadata`` (see ``label_definitions``/Phase 1),
            # same as arbitrary free-text tags. ``rule_id`` is a
            # hex-string id generated in Python (``uuid4().hex[:16]``,
            # matching ``dq_quality_rules.rule_id`` / ``dq_comments.comment_id``)
            # stored as TEXT rather than the native ``UUID`` type, so all
            # entity ids share one representation across the schema.
            # ----------------------------------------------------------
            f"CREATE TABLE IF NOT EXISTS {_S}.dq_rules ("
            "  rule_id       TEXT PRIMARY KEY,"
            "  mode          TEXT NOT NULL,"
            "  status        TEXT NOT NULL,"
            "  version       INTEGER NOT NULL DEFAULT 0,"
            "  polarity      TEXT,"
            "  author_kind   TEXT,"
            "  definition    JSONB NOT NULL,"
            "  user_metadata JSONB,"
            "  fingerprint   TEXT,"
            "  steward       TEXT,"
            "  is_builtin    BOOLEAN NOT NULL DEFAULT FALSE,"
            "  source        TEXT,"
            "  created_by    TEXT,"
            "  created_at    TIMESTAMPTZ,"
            "  updated_by    TEXT,"
            "  updated_at    TIMESTAMPTZ,"
            "  CONSTRAINT chk_dq_rules_mode "
            "    CHECK (mode IN ('dqx_native','lowcode','sql')),"
            "  CONSTRAINT chk_dq_rules_status "
            "    CHECK (status IN ('draft','pending_approval','approved','rejected','deprecated')),"
            "  CONSTRAINT chk_dq_rules_polarity "
            "    CHECK (polarity IS NULL OR polarity IN ('pass','fail'))"
            ");"
            # Three read paths dominate: the registry list filtered by
            # status (review queue), fingerprint dedup lookups on
            # create/update, and per-steward filtering.
            f"CREATE INDEX IF NOT EXISTS idx_dq_rules_status ON {_S}.dq_rules (status);"
            f"CREATE INDEX IF NOT EXISTS idx_dq_rules_fingerprint ON {_S}.dq_rules (fingerprint);"
            f"CREATE INDEX IF NOT EXISTS idx_dq_rules_steward ON {_S}.dq_rules (steward);"
            # ----------------------------------------------------------
            # dq_rule_versions — frozen snapshot written on every publish
            # of a ``dq_rules`` row (pinnable artifact + audit trail).
            # ``user_metadata`` here is a full frozen copy of the tags at
            # publish time, including the reserved dimension/severity keys.
            # ----------------------------------------------------------
            f"CREATE TABLE IF NOT EXISTS {_S}.dq_rule_versions ("
            "  id            BIGSERIAL PRIMARY KEY,"
            "  rule_id       TEXT NOT NULL,"
            "  version       INTEGER NOT NULL,"
            "  definition    JSONB NOT NULL,"
            "  polarity      TEXT,"
            "  user_metadata JSONB,"
            "  created_by    TEXT,"
            "  created_at    TIMESTAMPTZ,"
            "  CONSTRAINT uq_dq_rule_versions_rule_version UNIQUE (rule_id, version)"
            ");"
            f"CREATE INDEX IF NOT EXISTS idx_dq_rule_versions_rule_id ON {_S}.dq_rule_versions (rule_id);"
            # ----------------------------------------------------------
            # dq_rules_history — append-only audit trail for the
            # registry rule lifecycle (create/update/status transitions/
            # delete), mirroring ``dq_quality_rules_history``'s shape.
            # ----------------------------------------------------------
            f"CREATE TABLE IF NOT EXISTS {_S}.dq_rules_history ("
            "  history_id    BIGSERIAL PRIMARY KEY,"
            "  rule_id       TEXT,"
            "  definition    JSONB,"
            "  version       INTEGER,"
            "  action        TEXT NOT NULL,"
            "  prev_status   TEXT,"
            "  new_status    TEXT,"
            "  changed_by    TEXT,"
            "  changed_at    TIMESTAMPTZ"
            ");"
            f"CREATE INDEX IF NOT EXISTS idx_dq_rules_history_rule_changed_at "
            f"  ON {_S}.dq_rules_history (rule_id, changed_at DESC);"
            # ----------------------------------------------------------
            # dq_monitored_tables — Layer 2: thin binding recording that a
            # table is under active governance (see design spec §3.1/§7).
            # Profiling data itself lives in the existing
            # ``dq_profiling_results`` Delta table; this row just tracks
            # the steward + submit-for-review lifecycle (draft ->
            # pending_approval -> approved/rejected) of the binding.
            # ----------------------------------------------------------
            f"CREATE TABLE IF NOT EXISTS {_S}.dq_monitored_tables ("
            "  binding_id       TEXT PRIMARY KEY,"
            "  table_fqn        TEXT NOT NULL,"
            "  steward          TEXT,"
            "  status           TEXT NOT NULL,"
            "  last_profiled_at TIMESTAMPTZ,"
            "  created_by       TEXT,"
            "  created_at       TIMESTAMPTZ,"
            "  updated_by       TEXT,"
            "  updated_at       TIMESTAMPTZ,"
            "  CONSTRAINT uq_dq_monitored_tables_table_fqn UNIQUE (table_fqn),"
            "  CONSTRAINT chk_dq_monitored_tables_status "
            "    CHECK (status IN ('draft','pending_approval','approved','rejected'))"
            ");"
            f"CREATE INDEX IF NOT EXISTS idx_dq_monitored_tables_status "
            f"  ON {_S}.dq_monitored_tables (status);"
            # ----------------------------------------------------------
            # dq_applied_rules — the LIVE LINK between a published
            # registry rule and a monitored table's column mapping.
            # ``pinned_version`` NULL means "follow latest published"
            # (auto-upgrade); a non-NULL value freezes the applied rule to
            # that ``dq_rule_versions`` snapshot. ``mapping_hash`` is a
            # deterministic hash of ``column_mapping`` (see
            # ``registry_models.compute_mapping_hash``) so the same rule
            # can be applied to the same table with two *different*
            # column mappings (e.g. checking two different columns with
            # the same rule) without violating uniqueness, while an exact
            # duplicate application is rejected. ``binding_id``/``rule_id``
            # are informal references to ``dq_monitored_tables``/
            # ``dq_rules`` (service-enforced, no FK constraint — matching
            # ``dq_rule_versions.rule_id``'s existing convention in this
            # baseline).
            # ----------------------------------------------------------
            f"CREATE TABLE IF NOT EXISTS {_S}.dq_applied_rules ("
            "  id                 TEXT PRIMARY KEY,"
            "  binding_id         TEXT NOT NULL,"
            "  rule_id            TEXT NOT NULL,"
            "  pinned_version     INTEGER,"
            "  severity_override  TEXT,"
            "  column_mapping     JSONB,"
            "  user_metadata      JSONB,"
            "  mapping_hash       TEXT NOT NULL,"
            "  created_by         TEXT,"
            "  created_at         TIMESTAMPTZ,"
            "  CONSTRAINT uq_dq_applied_rules_binding_rule_mapping "
            "    UNIQUE (binding_id, rule_id, mapping_hash)"
            ");"
            f"CREATE INDEX IF NOT EXISTS idx_dq_applied_rules_binding_id "
            f"  ON {_S}.dq_applied_rules (binding_id);"
            f"CREATE INDEX IF NOT EXISTS idx_dq_applied_rules_rule_id "
            f"  ON {_S}.dq_applied_rules (rule_id);"
        ),
    ),
    PgMigration(
        version=2,
        description="Run review status (per-run review label + audit history)",
        sql=(
            # ----------------------------------------------------------
            # dq_run_review_status — one mutable row per run that has
            # been explicitly reviewed. Runs without a row surface the
            # configured default virtually at read-time (see
            # ReviewStatusService.get_effective).
            # ----------------------------------------------------------
            f"CREATE TABLE IF NOT EXISTS {_S}.dq_run_review_status ("
            "  run_id     TEXT PRIMARY KEY,"
            "  status     TEXT NOT NULL,"
            "  updated_by TEXT,"
            "  updated_at TIMESTAMPTZ"
            ");"
            # The Runs History page filters by status across the whole
            # list, so an index on status keeps that scan cheap as the
            # review-status table grows alongside the run history.
            f"CREATE INDEX IF NOT EXISTS idx_dq_run_review_status_status "
            f"  ON {_S}.dq_run_review_status (status);"
            # ----------------------------------------------------------
            # dq_run_review_status_history — append-only audit log.
            # BIGSERIAL gives us a stable display order even if two
            # changes land on the same TIMESTAMPTZ (rare but possible
            # with millisecond resolution + bulk admin tooling).
            # ----------------------------------------------------------
            f"CREATE TABLE IF NOT EXISTS {_S}.dq_run_review_status_history ("
            "  history_id      BIGSERIAL PRIMARY KEY,"
            "  run_id          TEXT NOT NULL,"
            "  status          TEXT NOT NULL,"
            "  previous_status TEXT,"
            "  changed_by      TEXT NOT NULL,"
            "  changed_at      TIMESTAMPTZ NOT NULL"
            ");"
            f"CREATE INDEX IF NOT EXISTS idx_dq_run_review_status_history_run_changed_at "
            f"  ON {_S}.dq_run_review_status_history (run_id, changed_at DESC);"
        ),
    ),
    PgMigration(
        version=3,
        description="Role mappings audit history (dq_role_mappings_history)",
        sql=(
            # ----------------------------------------------------------
            # dq_role_mappings_history — append-only audit log for
            # changes to dq_role_mappings. Mirrors the Delta v7 shape;
            # see the corresponding _V7_ROLE_MAPPINGS_HISTORY comment in
            # ``backend.migrations.__init__`` for the rationale.
            #
            # BIGSERIAL gives us a stable display order even if two
            # admin actions land on the same TIMESTAMPTZ (rare but
            # possible with millisecond resolution + bulk tooling).
            # ----------------------------------------------------------
            f"CREATE TABLE IF NOT EXISTS {_S}.dq_role_mappings_history ("
            "  history_id BIGSERIAL PRIMARY KEY,"
            "  role       TEXT NOT NULL,"
            "  group_name TEXT NOT NULL,"
            "  action     TEXT NOT NULL,"
            "  changed_by TEXT,"
            "  changed_at TIMESTAMPTZ NOT NULL"
            ");"
            # Two read patterns: full-history-for-mapping (compliance
            # answer "show me every change to Approver→group_x") and
            # recent-activity (admin Settings page "last 50 changes"). A
            # single composite covers the first; the second is served by
            # the second index on changed_at alone.
            f"CREATE INDEX IF NOT EXISTS idx_dq_role_mappings_history_role_group_changed_at "
            f"  ON {_S}.dq_role_mappings_history (role, group_name, changed_at DESC);"
            f"CREATE INDEX IF NOT EXISTS idx_dq_role_mappings_history_changed_at "
            f"  ON {_S}.dq_role_mappings_history (changed_at DESC);"
        ),
    ),
    PgMigration(
        version=4,
        description="Rule embeddings corpus (dq_rule_embeddings) — Rules Registry Phase 4B",
        sql=(
            # ----------------------------------------------------------
            # dq_rule_embeddings — one row per published registry rule,
            # holding the normalized text blob (see
            # ``services.rule_embeddings.build_rule_embed_text``) and its
            # embedding vector. Populated best-effort on rule publish
            # (``routes.v1.registry_rules.approve_registry_rule``) and by
            # the ``RuleEmbeddingsService.backfill`` helper. Entirely
            # optional infrastructure: the table always exists, but stays
            # empty on any deploy with no ``embedding_endpoint_name``
            # configured (see ``AppSettingsService`` Phase 4B/4C settings).
            #
            # ``embedding`` is stored as a JSON-encoded array of floats in
            # a portable TEXT column (not a native vector/array type) so
            # the same DDL/read path works unchanged on the Delta OLTP
            # fallback. The Databricks Vector Search index itself is the
            # actual ANN store the mapping suggester queries at runtime
            # (see ``services.rule_retriever.VectorSearchRetriever``) —
            # this table is the source-of-truth corpus a Vector Search
            # "Delta Sync" index would sync from, or that a backfill job
            # can re-embed from without re-deriving ``embed_text``.
            # ----------------------------------------------------------
            f"CREATE TABLE IF NOT EXISTS {_S}.dq_rule_embeddings ("
            "  rule_id      TEXT PRIMARY KEY,"
            "  rule_version INTEGER,"
            "  embed_text   TEXT,"
            "  embedding    TEXT,"
            "  model        TEXT,"
            "  updated_at   TIMESTAMPTZ"
            ");"
        ),
    ),
    PgMigration(
        version=5,
        description="Converge dq_monitored_tables status to the 4-state review set (draft/pending_approval/approved/rejected)",
        sql=(
            # ----------------------------------------------------------
            # Monitored-table status lifecycle converge (P16-H).
            #
            # The v1 baseline above already declares the 4-state CHECK
            # set, so this migration is a NO-OP on fresh installs. It
            # exists purely to converge databases already deployed with
            # the ORIGINAL 2-state constraint (``('draft','published')``)
            # — the v1 baseline is skipped on those DBs because v1 is
            # already recorded in ``dq_migrations``, so editing v1 in
            # place could never reach them. Appending a new version is
            # the only way the runner re-visits an already-migrated DB.
            #
            # Order matters and is safe inside the single transaction the
            # runner wraps every migration in: drop the old constraint
            # first (so the legacy value can be rewritten), rewrite any
            # legacy ``published`` binding to ``approved`` (its lifecycle
            # equivalent — a published table's checks were live), then
            # re-add the constraint with the final 4-state set. On a
            # fresh install this drops+re-adds an identical constraint and
            # rewrites zero rows.
            # ----------------------------------------------------------
            f"ALTER TABLE {_S}.dq_monitored_tables "
            "  DROP CONSTRAINT IF EXISTS chk_dq_monitored_tables_status;"
            f"UPDATE {_S}.dq_monitored_tables SET status = 'approved' WHERE status = 'published';"
            f"ALTER TABLE {_S}.dq_monitored_tables "
            "  ADD CONSTRAINT chk_dq_monitored_tables_status "
            "    CHECK (status IN ('draft','pending_approval','approved','rejected'));"
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
