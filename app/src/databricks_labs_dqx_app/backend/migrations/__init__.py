"""Database migration runner for DQX Studio (Delta).

Migrations are versioned DDL statements applied in order against the
configured Unity Catalog catalog/schema. The runner tracks every
applied version in a ``dq_migrations`` meta-table, so re-starting the
app never re-applies a migration that already succeeded.

Hybrid backend split
--------------------
The schema is delivered in **two parts** so the OLTP-style tables can
optionally live on Lakebase Postgres while the high-volume analytical
tables stay in Delta:

- **v1 — Delta analytical baseline** (always applied). Holds the
  Spark-written tables: ``dq_validation_runs``,
  ``dq_profiling_results``, ``dq_quarantine_records``,
  ``dq_metrics``.
- **v2 — Delta OLTP fallback** (only applied when Lakebase is
  disabled, i.e. ``include_oltp_fallback=True``). Holds the
  FastAPI-served tables: ``dq_app_settings``, ``dq_quality_rules``,
  ``dq_quality_rules_history``, ``dq_role_mappings``, ``dq_comments``,
  ``dq_schedule_configs``, ``dq_schedule_configs_history``,
  ``dq_schedule_runs``.

When Lakebase is enabled the same OLTP tables are created via
:mod:`backend.migrations.postgres` against the Postgres schema and v2
is skipped on the Delta side.

Status casing convention
------------------------
Two status families intentionally use different casing:

- **Run-lifecycle** (``dq_validation_runs.status``,
  ``dq_profiling_results.status``) — UPPERCASE
  (``RUNNING``/``SUCCESS``/``FAILED``/``CANCELED``). These mirror the
  Databricks Jobs SDK ``life_cycle_state`` / ``result_state`` values
  that are passed straight through ``RunStatusOut`` to the frontend.
- **App-domain workflow** (``dq_quality_rules.status``,
  ``dq_schedule_runs.status``) — lowercase. These are pure DQX
  vocabulary (``draft``/``approved``, ``pending``/``partial_failure``)
  with no SDK counterpart.

CHECK constraints enforce the agreed values per domain — see each
table's ``chk_*_status`` constraint below.

Adding a new table or schema change after baseline
--------------------------------------------------
Append a new :class:`Migration` entry with the next monotonically
increasing version number. **Never edit or reorder existing entries.**
For column additions use ``ALTER TABLE ... ADD COLUMN`` (do *not* use
``ADD COLUMN IF NOT EXISTS`` — it is not supported on all Databricks
SQL warehouse versions; ``_apply`` instead catches and tolerates
``COLUMN_ALREADY_EXISTS`` so re-running is safe).

If the change touches an OLTP table, mirror it in
:mod:`backend.migrations.postgres` so Lakebase deployments stay in
sync.

Upgrading an existing dev workspace
-----------------------------------
A workspace that previously ran the legacy migration sequence will have
``dq_migrations`` rows for versions that no longer exist, and tables
whose column types or constraints predate this baseline revision. The
cleanest path is::

    DROP SCHEMA <catalog>.<schema> CASCADE;

then redeploy — the consolidated baseline runs from scratch.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor
from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Migration definitions
# ---------------------------------------------------------------------------

_PLACEHOLDER = "{catalog}.{schema}"


@dataclass(frozen=True)
class Migration:
    version: int
    description: str
    sql_template: str


# Order is significant. Never change or remove existing entries — only
# append new ones.
#
# v1 is the consolidated baseline. Each table is defined at its final
# shape with liquid clustering, primary keys, and CHECK constraints
# inlined. Revisions to the baseline are allowed (and encouraged) until
# the app ships externally; existing dev workspaces upgrade by
# ``DROP SCHEMA … CASCADE`` and re-running migrations from scratch.
#
# Notes on column choices:
# - PRIMARY KEY constraints are informational (``NOT ENFORCED RELY``);
#   Delta uses them for query optimization, lineage, AI/BI tooling.
# - CHECK constraints ARE enforced — picking the right value-set on
#   day one is cheap; loosening later is just an ALTER TABLE.
# - ``VARIANT`` (DBR 15.3+ / serverless) replaces ad-hoc JSON-in-string
#   for the largest blob columns (``dq_quality_rules.check``,
#   ``dq_quarantine_records.row_data``/``errors``).
# - Run-lifecycle ``status`` columns use UPPERCASE values to mirror the
#   Databricks Jobs SDK; app-domain ``status`` columns use lowercase.
# v1 always runs against Delta and only contains the high-volume
# analytical tables that the Spark task runner writes.  Keeping these
# in Delta lets AI/BI dashboards consume them directly via SQL
# warehouse without round-tripping through Postgres.
_V1_ANALYTICAL_BASELINE = (
    # Profiler runs — one row per profile job. Mutable lifecycle
    # (RUNNING → SUCCESS/FAILED/CANCELED). Status values mirror
    # Databricks Jobs SDK convention.
    #
    # NOTE: Delta only allows PRIMARY KEY and FOREIGN KEY constraints
    # inline in CREATE TABLE — every CHECK constraint must be added
    # via a separate ALTER TABLE … ADD CONSTRAINT statement after the
    # table exists. The migration runner swallows
    # ``DELTA_CONSTRAINT_ALREADY_EXISTS`` so re-runs are idempotent.
    f"CREATE TABLE IF NOT EXISTS {_PLACEHOLDER}.dq_profiling_results ("
    "  run_id STRING NOT NULL,"
    "  requesting_user STRING,"
    "  source_table_fqn STRING NOT NULL,"
    "  view_fqn STRING,"
    "  sample_limit INT,"
    "  rows_profiled INT,"
    "  columns_profiled INT,"
    "  duration_seconds DOUBLE,"
    "  summary_json STRING,"
    "  generated_rules_json STRING,"
    "  status STRING NOT NULL,"
    "  error_message STRING,"
    "  canceled_by STRING,"
    "  updated_at TIMESTAMP,"
    "  job_run_id BIGINT,"
    "  rule_set_fingerprint STRING,"
    "  created_at TIMESTAMP"
    ") CLUSTER BY (source_table_fqn, run_id, created_at);"
    f"ALTER TABLE {_PLACEHOLDER}.dq_profiling_results "
    f"  ADD CONSTRAINT chk_dq_profiling_results_status "
    f"  CHECK (status IN ('RUNNING','SUCCESS','FAILED','CANCELED'));"
    #
    # Validation (dryrun + scheduled) runs — one row per run, mutable
    # lifecycle.  Joins to ``dq_metrics`` on
    # ``(run_id, rule_set_fingerprint)``.
    f"CREATE TABLE IF NOT EXISTS {_PLACEHOLDER}.dq_validation_runs ("
    "  run_id STRING NOT NULL,"
    "  requesting_user STRING,"
    "  source_table_fqn STRING NOT NULL,"
    "  view_fqn STRING,"
    "  checks_json STRING,"
    "  sample_size INT,"
    "  total_rows INT,"
    "  valid_rows INT,"
    "  invalid_rows INT,"
    "  error_rows INT,"
    "  warning_rows INT,"
    "  error_summary_json STRING,"
    "  sample_invalid_json STRING,"
    "  status STRING NOT NULL,"
    "  error_message STRING,"
    "  canceled_by STRING,"
    "  updated_at TIMESTAMP,"
    "  run_type STRING,"
    "  job_run_id BIGINT,"
    "  rule_set_fingerprint STRING,"
    "  created_at TIMESTAMP"
    ") CLUSTER BY (source_table_fqn, run_id, created_at);"
    f"ALTER TABLE {_PLACEHOLDER}.dq_validation_runs "
    f"  ADD CONSTRAINT chk_dq_validation_runs_status "
    f"  CHECK (status IN ('RUNNING','SUCCESS','FAILED','CANCELED'));"
    f"ALTER TABLE {_PLACEHOLDER}.dq_validation_runs "
    f"  ADD CONSTRAINT chk_dq_validation_runs_run_type "
    f"  CHECK (run_type IS NULL OR run_type IN ('dryrun','scheduled','preview'));"
    #
    # Quarantined invalid rows captured during validation.  ``row_data``
    # and ``errors`` are VARIANT for native JSON predicate pushdown and
    # ~10x compression vs. STRING.
    f"CREATE TABLE IF NOT EXISTS {_PLACEHOLDER}.dq_quarantine_records ("
    "  quarantine_id STRING NOT NULL,"
    "  run_id STRING NOT NULL,"
    "  source_table_fqn STRING NOT NULL,"
    "  requesting_user STRING,"
    "  row_data VARIANT,"
    "  errors VARIANT,"
    "  warnings VARIANT,"
    "  created_at TIMESTAMP,"
    "  CONSTRAINT pk_dq_quarantine_records PRIMARY KEY (quarantine_id) RELY"
    ") CLUSTER BY (run_id);"
    #
    # Long-format observability events written by DQMetricsObserver.
    # Schema mirrors the public DQX OBSERVATION_TABLE_SCHEMA so AI/BI
    # dashboard templates targeting the spec drop straight in.
    f"CREATE TABLE IF NOT EXISTS {_PLACEHOLDER}.dq_metrics ("
    "  run_id STRING NOT NULL,"
    "  run_name STRING,"
    "  input_location STRING,"
    "  output_location STRING,"
    "  quarantine_location STRING,"
    "  checks_location STRING,"
    "  rule_set_fingerprint STRING,"
    "  metric_name STRING NOT NULL,"
    "  metric_value STRING,"
    "  run_time TIMESTAMP NOT NULL,"
    "  error_column_name STRING,"
    "  warning_column_name STRING,"
    "  user_metadata MAP<STRING, STRING>"
    ") CLUSTER BY (input_location, run_id, run_time)"
)


# v2 is the Delta-only OLTP fallback. It is **only** applied when
# Lakebase is disabled (``include_oltp_fallback=True`` in
# :meth:`MigrationRunner.run_all`). When Lakebase is enabled, the same
# tables are created via :mod:`backend.migrations.postgres` against the
# Postgres backend.
_V2_OLTP_FALLBACK = (
    # Settings — single-row-per-key key/value store (workspace config,
    # label catalog, custom metrics, timezone, ...).
    f"CREATE TABLE IF NOT EXISTS {_PLACEHOLDER}.dq_app_settings ("
    "  setting_key STRING NOT NULL,"
    "  setting_value STRING,"
    "  updated_at TIMESTAMP,"
    "  updated_by STRING,"
    "  CONSTRAINT pk_dq_app_settings PRIMARY KEY (setting_key) RELY"
    ") CLUSTER BY (setting_key);"
    #
    # Active rule catalog. ``rule_id`` is a per-check stable identifier;
    # each row holds exactly ONE check serialized as a VARIANT object
    # (no array wrapper). ``source`` records which authoring path
    # produced the rule.
    f"CREATE TABLE IF NOT EXISTS {_PLACEHOLDER}.dq_quality_rules ("
    "  rule_id STRING NOT NULL,"
    "  table_fqn STRING NOT NULL,"
    "  `check` VARIANT NOT NULL,"
    "  version INT NOT NULL,"
    "  status STRING NOT NULL,"
    "  source STRING NOT NULL,"
    "  created_by STRING,"
    "  created_at TIMESTAMP,"
    "  updated_by STRING,"
    "  updated_at TIMESTAMP,"
    "  CONSTRAINT pk_dq_quality_rules PRIMARY KEY (rule_id) RELY"
    ") CLUSTER BY (table_fqn, status, rule_id);"
    f"ALTER TABLE {_PLACEHOLDER}.dq_quality_rules "
    f"  ADD CONSTRAINT chk_dq_quality_rules_status "
    f"  CHECK (status IN ('draft','pending_approval','approved','rejected'));"
    f"ALTER TABLE {_PLACEHOLDER}.dq_quality_rules "
    f"  ADD CONSTRAINT chk_dq_quality_rules_source "
    f"  CHECK (source IN ('ui','sql','profiler','import','ai'));"
    #
    # Append-only audit trail for rule changes. Carries the post-state
    # ``check`` payload on every row plus an explicit
    # ``prev_status``/``new_status`` pair for status transitions.
    f"CREATE TABLE IF NOT EXISTS {_PLACEHOLDER}.dq_quality_rules_history ("
    "  rule_id STRING,"
    "  table_fqn STRING NOT NULL,"
    "  `check` VARIANT,"
    "  version INT,"
    "  source STRING,"
    "  action STRING NOT NULL,"
    "  prev_status STRING,"
    "  new_status STRING,"
    "  changed_by STRING,"
    "  changed_at TIMESTAMP"
    ") CLUSTER BY (table_fqn, changed_at);"
    #
    # RBAC: maps app roles (admin/rule_approver/rule_author/viewer/
    # runner) to Databricks workspace groups. Tiny table — no
    # clustering needed.
    f"CREATE TABLE IF NOT EXISTS {_PLACEHOLDER}.dq_role_mappings ("
    "  role STRING NOT NULL,"
    "  group_name STRING NOT NULL,"
    "  created_by STRING,"
    "  created_at TIMESTAMP,"
    "  updated_by STRING,"
    "  updated_at TIMESTAMP,"
    "  CONSTRAINT pk_dq_role_mappings PRIMARY KEY (role, group_name) RELY"
    ");"
    f"ALTER TABLE {_PLACEHOLDER}.dq_role_mappings "
    f"  ADD CONSTRAINT chk_dq_role_mappings_role "
    f"  CHECK (role IN ('admin','rule_approver','rule_author','viewer','runner'));"
    #
    # Per-entity comment threads (rules, runs, profiles, ...).
    f"CREATE TABLE IF NOT EXISTS {_PLACEHOLDER}.dq_comments ("
    "  comment_id STRING NOT NULL,"
    "  entity_type STRING NOT NULL,"
    "  entity_id STRING NOT NULL,"
    "  user_email STRING NOT NULL,"
    "  comment STRING NOT NULL,"
    "  created_at TIMESTAMP,"
    "  CONSTRAINT pk_dq_comments PRIMARY KEY (comment_id) RELY"
    ") CLUSTER BY (entity_type, entity_id);"
    f"ALTER TABLE {_PLACEHOLDER}.dq_comments "
    f"  ADD CONSTRAINT chk_dq_comments_entity_type "
    f"  CHECK (entity_type IN ('run','rule'));"
    #
    # Scheduler bookkeeping: last/next run pointer per schedule.
    # ``status`` is app-domain (lowercase).
    f"CREATE TABLE IF NOT EXISTS {_PLACEHOLDER}.dq_schedule_runs ("
    "  schedule_name STRING NOT NULL,"
    "  last_run_at TIMESTAMP,"
    "  next_run_at TIMESTAMP,"
    "  last_run_id STRING,"
    "  status STRING,"
    "  updated_at TIMESTAMP,"
    "  CONSTRAINT pk_dq_schedule_runs PRIMARY KEY (schedule_name) RELY"
    ") CLUSTER BY (schedule_name);"
    f"ALTER TABLE {_PLACEHOLDER}.dq_schedule_runs "
    f"  ADD CONSTRAINT chk_dq_schedule_runs_status "
    f"  CHECK (status IS NULL OR status IN ('pending','success','partial_failure','failed'));"
    #
    # Per-schedule live config (cron/interval, scope filters).
    f"CREATE TABLE IF NOT EXISTS {_PLACEHOLDER}.dq_schedule_configs ("
    "  schedule_name STRING NOT NULL,"
    "  config_json STRING NOT NULL,"
    "  version INT NOT NULL,"
    "  created_by STRING,"
    "  created_at TIMESTAMP,"
    "  updated_by STRING,"
    "  updated_at TIMESTAMP,"
    "  CONSTRAINT pk_dq_schedule_configs PRIMARY KEY (schedule_name) RELY"
    ") CLUSTER BY (schedule_name);"
    #
    # Append-only audit trail for schedule changes.
    f"CREATE TABLE IF NOT EXISTS {_PLACEHOLDER}.dq_schedule_configs_history ("
    "  schedule_name STRING NOT NULL,"
    "  config_json STRING,"
    "  version INT,"
    "  action STRING NOT NULL,"
    "  changed_by STRING,"
    "  changed_at TIMESTAMP"
    ") CLUSTER BY (schedule_name, changed_at)"
)


# Backfills ``warning_rows`` on workspaces deployed before v1 added it.
# On fresh deploys ``_apply`` swallows the ``COLUMN_ALREADY_EXISTS`` error
# per the column-addition rule documented at the top of this module.
_V3_VALIDATION_RUNS_WARNING_ROWS = f"ALTER TABLE {_PLACEHOLDER}.dq_validation_runs " f"  ADD COLUMN warning_rows INT"


# Quarantine rows that fail only warning-level checks would otherwise
# show an empty ``errors`` column in the UI. We mirror DQX's row-level
# ``_warnings`` map into its own VARIANT so warnings can be rendered
# alongside errors in the dry-run sample table.
_V4_QUARANTINE_WARNINGS = f"ALTER TABLE {_PLACEHOLDER}.dq_quarantine_records " f"  ADD COLUMN warnings VARIANT"


# ``invalid_rows`` (set from ``invalid_df.count()``) conflated "rows that
# failed any check" with "rows with errors" — and could over-count when
# certain DQX checks fan out internally. ``error_rows`` is the
# authoritative count from the DQX observer (``error_row_count``), so the
# UI now surfaces it as the primary "Errors" stat. ``invalid_rows`` is
# kept for backwards compatibility but no longer drives the UI.
_V5_VALIDATION_RUNS_ERROR_ROWS = f"ALTER TABLE {_PLACEHOLDER}.dq_validation_runs " f"  ADD COLUMN error_rows INT"


# Run review status — per-run review label set by business / SA reviewers
# from the Runs detail page. The allowed value list is admin-managed in
# ``dq_app_settings.run_review_statuses`` so there's no CHECK constraint
# on ``status``; the service validates against the live list before INSERT.
#
# Two tables intentionally:
# - ``dq_run_review_status`` is mutable (one row per run that has been
#   reviewed; absent rows surface the configured default virtually).
# - ``dq_run_review_status_history`` is append-only so we can show
#   "X changed status from Pending to Acknowledged on Tue" on the run
#   detail page and answer compliance questions. Same shape as
#   ``dq_quality_rules_history`` — no PK column on Delta (rows are
#   ordered by ``changed_at`` for display).
#
# Marked ``oltp_fallback=True`` because both tables are OLTP-shaped
# (single-key lookup, frequent mutation) and live in Lakebase when
# enabled; this migration only runs against Delta when Lakebase is off.
_V6_RUN_REVIEW_STATUS = (
    f"CREATE TABLE IF NOT EXISTS {_PLACEHOLDER}.dq_run_review_status ("
    "  run_id     STRING NOT NULL,"
    "  status     STRING NOT NULL,"
    "  updated_by STRING,"
    "  updated_at TIMESTAMP,"
    "  CONSTRAINT pk_dq_run_review_status PRIMARY KEY (run_id) RELY"
    ") CLUSTER BY (run_id);"
    f"CREATE TABLE IF NOT EXISTS {_PLACEHOLDER}.dq_run_review_status_history ("
    "  run_id          STRING NOT NULL,"
    "  status          STRING NOT NULL,"
    "  previous_status STRING,"
    "  changed_by      STRING NOT NULL,"
    "  changed_at      TIMESTAMP NOT NULL"
    ") CLUSTER BY (run_id, changed_at)"
)


# OLTP fallback migration is identified by ``oltp_fallback=True`` so
# the runner can skip it when Lakebase is enabled. Keeping the flag on
# the migration itself (rather than e.g. a hard-coded version number)
# makes it easy to add follow-up Delta-only OLTP migrations later
# without re-discovering the rule.
@dataclass(frozen=True)
class DeltaMigration(Migration):
    """Migration variant that knows whether it carries OLTP fallback DDL.

    A subclass (rather than a flag on :class:`Migration`) keeps
    backwards compatibility for any callers that still hand-build
    ``Migration`` instances and don't care about the flag.
    """

    oltp_fallback: bool = False


MIGRATIONS: list[Migration] = [
    DeltaMigration(
        version=1,
        description="Delta analytical baseline (validation, profiling, quarantine, metrics)",
        sql_template=_V1_ANALYTICAL_BASELINE,
        oltp_fallback=False,
    ),
    DeltaMigration(
        version=2,
        description="Delta OLTP fallback (rules, app settings, RBAC, schedules) — used only when Lakebase is disabled",
        sql_template=_V2_OLTP_FALLBACK,
        oltp_fallback=True,
    ),
    DeltaMigration(
        version=3,
        description="Add warning_rows column to dq_validation_runs (backfill for pre-v3 deploys)",
        sql_template=_V3_VALIDATION_RUNS_WARNING_ROWS,
        oltp_fallback=False,
    ),
    DeltaMigration(
        version=4,
        description="Add warnings VARIANT column to dq_quarantine_records (mirror DQX _warnings map)",
        sql_template=_V4_QUARANTINE_WARNINGS,
        oltp_fallback=False,
    ),
    DeltaMigration(
        version=5,
        description="Add error_rows column to dq_validation_runs (DQX error_row_count, replaces invalid_rows for UI)",
        sql_template=_V5_VALIDATION_RUNS_ERROR_ROWS,
        oltp_fallback=False,
    ),
    DeltaMigration(
        version=6,
        description="Run review status (per-run review label + audit history) — used only when Lakebase is disabled",
        sql_template=_V6_RUN_REVIEW_STATUS,
        oltp_fallback=True,
    ),
]

# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

_META_TABLE = f"{_PLACEHOLDER}.dq_migrations"


class MigrationRunner:
    """Applies pending database migrations against the DQX Studio schema.

    All DDL is executed via the Databricks Statement Execution API using
    the app's service-principal credentials, *not* the OBO user token.

    Usage::

        runner = MigrationRunner(sql=sql_executor)
        applied = runner.run_all()
        # applied == number of migrations just executed (0 if already up to date)
    """

    def __init__(self, sql: SqlExecutor) -> None:
        self._sql = sql
        self._catalog = sql.catalog
        self._schema = sql.schema
        self._meta_table = _META_TABLE.format(catalog=sql.catalog, schema=sql.schema)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run_all(self, *, include_oltp_fallback: bool = True) -> int:
        """Ensure the schema exists and apply all pending Delta migrations.

        Parameters
        ----------
        include_oltp_fallback:
            When ``True`` (legacy mode, no Lakebase) all migrations
            run including the OLTP fallback DDL (v2 in the baseline).
            When ``False`` (Lakebase enabled) migrations marked with
            ``oltp_fallback=True`` are skipped — the same tables are
            created in Postgres via :class:`PgMigrationRunner` instead.

            Skipped migrations are *not* recorded as applied, so
            disabling Lakebase later will cause them to run on the
            next deploy and create the Delta-side tables on demand.

        Returns:
            The number of migrations applied in this invocation.
        """
        self._ensure_schema()
        self._ensure_meta_table()
        applied_versions = self._applied_versions()

        count = 0
        for migration in MIGRATIONS:
            if migration.version in applied_versions:
                logger.debug(
                    "Migration v%d (%s) already applied, skipping",
                    migration.version,
                    migration.description,
                )
                continue

            if not include_oltp_fallback and isinstance(migration, DeltaMigration) and migration.oltp_fallback:
                logger.info(
                    "Skipping Delta OLTP fallback migration v%d "
                    "(Lakebase enabled — these tables live in Postgres): %s",
                    migration.version,
                    migration.description,
                )
                continue

            logger.info(
                "Applying migration v%d: %s",
                migration.version,
                migration.description,
            )
            self._apply(migration)
            count += 1

        return count

    def status(self) -> list[dict[str, object]]:
        """Return the list of all migrations with their applied status.

        Returns:
            A list of dicts with keys ``version``, ``description``,
            ``applied``, and ``applied_at`` (ISO string or ``None``).
        """
        self._ensure_schema()
        self._ensure_meta_table()
        applied_at = self._applied_at_map()
        return [
            {
                "version": m.version,
                "description": m.description,
                "applied": m.version in applied_at,
                "applied_at": applied_at.get(m.version),
            }
            for m in MIGRATIONS
        ]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _ensure_schema(self) -> None:
        """Create the catalog schema if it doesn't exist.

        Must use the bootstrap executor (catalog-only context) because the
        schema does not exist yet and passing ``schema=<name>`` to the
        statement-execution API would cause it to fail before the DDL runs.
        """
        sql = f"CREATE SCHEMA IF NOT EXISTS {self._catalog}.{self._schema}"
        self._sql.execute_no_schema(sql)
        logger.debug("Ensured schema exists: %s.%s", self._catalog, self._schema)

    def _ensure_meta_table(self) -> None:
        """Create the migration tracking table if it doesn't exist."""
        sql = (
            f"CREATE TABLE IF NOT EXISTS {self._meta_table} ("
            "  version INT NOT NULL,"
            "  description STRING NOT NULL,"
            "  applied_at TIMESTAMP NOT NULL"
            ")"
        )
        self._sql.execute(sql)
        logger.debug("Ensured migrations meta-table exists: %s", self._meta_table)

    def _applied_versions(self) -> set[int]:
        return set(self._applied_at_map().keys())

    def _applied_at_map(self) -> dict[int, str]:
        sql = f"SELECT version, CAST(applied_at AS STRING) FROM {self._meta_table} ORDER BY version"
        rows = self._sql.query(sql)
        return {int(row[0]): row[1] for row in rows}

    # Errors that mean "the desired state is already in place" — safe
    # to swallow so that re-running a migration is a no-op. The
    # baseline statement uses ``CREATE TABLE IF NOT EXISTS``, but if a
    # future migration appends an ``ALTER TABLE … ADD COLUMN`` or
    # ``CLUSTER BY`` and the workspace already has it, we don't want
    # the whole migration to abort.
    _IDEMPOTENT_ERROR_FRAGMENTS = (
        "COLUMN_ALREADY_EXISTS",
        # Databricks Delta surfaces an ``ADD COLUMN`` that targets an
        # already-present column as ``FIELD_ALREADY_EXISTS`` (singular).
        # Older versions used the plural ``FIELDS_ALREADY_EXISTS``;
        # keep both to defend against runtime wording drift across
        # workspaces / DBR versions.
        "FIELD_ALREADY_EXISTS",
        "FIELDS_ALREADY_EXISTS",
        "TABLE_OR_VIEW_ALREADY_EXISTS",
        "TABLE_ALREADY_EXISTS",
        "already has liquid clustering defined",
        # CHECK constraints are added via ``ALTER TABLE … ADD CONSTRAINT``
        # in a separate statement after CREATE TABLE (Delta only allows
        # PK/FK inline). When a previous migration attempt got past
        # CREATE TABLE but failed before recording the version, the
        # next run sees the constraint and emits this error — safe to
        # swallow because the desired state is already in place. The
        # second fragment guards against future error-message tweaks
        # since Databricks has used both the SQLSTATE-prefixed code and
        # plain English wording at different times.
        "DELTA_CONSTRAINT_ALREADY_EXISTS",
        "constraint already exists",
    )

    def _apply(self, migration: Migration) -> None:
        formatted = migration.sql_template.format(catalog=self._catalog, schema=self._schema)
        for stmt in formatted.split(";"):
            stmt = stmt.strip()
            if stmt:
                try:
                    self._sql.execute(stmt)
                except Exception as exc:
                    if any(frag in str(exc) for frag in self._IDEMPOTENT_ERROR_FRAGMENTS):
                        logger.warning(
                            "Migration v%d DDL already applied (safe to skip): %s",
                            migration.version,
                            exc,
                        )
                    else:
                        raise

        escaped_desc = escape_sql_string(migration.description)
        record_sql = (
            f"INSERT INTO {self._meta_table} (version, description, applied_at) "
            f"VALUES ({migration.version}, '{escaped_desc}', current_timestamp())"
        )
        self._sql.execute(record_sql)
        logger.info(
            "Migration v%d applied successfully: %s",
            migration.version,
            migration.description,
        )
