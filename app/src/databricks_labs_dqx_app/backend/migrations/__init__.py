"""Database migration runner for DQX Studio.

Migrations are versioned DDL statements applied in order against the
configured catalog/schema. The runner tracks every applied version in a
``dq_migrations`` meta-table, so re-starting the app never re-applies a
migration that already succeeded.

Schema baseline
---------------
The app has not shipped to external users, so the schema is delivered
as a **single consolidated baseline** rather than 36 incremental
migrations. Every table is created at its final shape, with liquid
clustering inlined into the ``CREATE TABLE`` statement.

Adding a new table or schema change after baseline
--------------------------------------------------
Append a new :class:`Migration` entry with the next monotonically
increasing version number. **Never edit or reorder existing entries.**
For column additions use ``ALTER TABLE ... ADD COLUMN`` (do *not* use
``ADD COLUMN IF NOT EXISTS`` — it is not supported on all Databricks
SQL warehouse versions; ``_apply`` instead catches and tolerates
``COLUMN_ALREADY_EXISTS`` so re-running is safe).

Example::

    Migration(
        version=2,
        description="Add description column to dq_role_mappings",
        sql_template=(
            "ALTER TABLE {catalog}.{schema}.dq_role_mappings "
            "ADD COLUMN description STRING"
        ),
    )

Upgrading an existing dev workspace
-----------------------------------
A workspace that previously ran the legacy v1–v36 sequence will have
``dq_migrations`` rows for versions that no longer exist. The cleanest
path is::

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
# v1 is the consolidated baseline created on 2026-05-03 by collapsing
# the original v1–v36 incremental sequence. Each table is defined at
# its final shape with liquid clustering inlined; the legacy
# wide-format ``dq_metrics`` (renamed to ``dq_metrics_v1_legacy`` in
# the original v32) is dropped from the baseline because it has no
# clean-install consumer.
MIGRATIONS: list[Migration] = [
    Migration(
        version=1,
        description="Baseline schema — all DQX Studio tables at their final shape",
        sql_template=(
            # Settings — single-row-per-key key/value store (workspace
            # config, label catalog, custom metrics, timezone, ...).
            f"CREATE TABLE IF NOT EXISTS {_PLACEHOLDER}.dq_app_settings ("
            "  setting_key STRING NOT NULL,"
            "  setting_value STRING,"
            "  updated_at TIMESTAMP,"
            "  updated_by STRING"
            ") CLUSTER BY (setting_key);"
            #
            # Active rule catalog. ``rule_id`` is a per-check stable
            # identifier; ``source`` records which authoring path
            # produced the rule (single-table, sql, profiler, import).
            f"CREATE TABLE IF NOT EXISTS {_PLACEHOLDER}.dq_quality_rules ("
            "  table_fqn STRING NOT NULL,"
            "  checks STRING NOT NULL,"
            "  version INT,"
            "  status STRING,"
            "  source STRING,"
            "  rule_id STRING,"
            "  created_by STRING,"
            "  created_at TIMESTAMP,"
            "  updated_by STRING,"
            "  updated_at TIMESTAMP"
            ") CLUSTER BY (table_fqn, status);"
            #
            # Append-only audit trail for rule changes.
            f"CREATE TABLE IF NOT EXISTS {_PLACEHOLDER}.dq_quality_rules_history ("
            "  table_fqn STRING NOT NULL,"
            "  checks STRING,"
            "  version INT,"
            "  source STRING,"
            "  rule_id STRING,"
            "  action STRING NOT NULL,"
            "  changed_by STRING,"
            "  changed_at TIMESTAMP"
            ") CLUSTER BY (table_fqn, changed_at);"
            #
            # Profiler runs — one row per profile job. Mutable
            # lifecycle (RUNNING → SUCCESS/FAILED/CANCELED).
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
            "  status STRING,"
            "  error_message STRING,"
            "  canceled_by STRING,"
            "  updated_at STRING,"
            "  job_run_id BIGINT,"
            "  rule_set_fingerprint STRING,"
            "  created_at STRING"
            ") CLUSTER BY (source_table_fqn, created_at);"
            #
            # Validation (dryrun + scheduled) runs — one row per run,
            # mutable lifecycle. Joins to ``dq_metrics`` on
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
            "  error_summary_json STRING,"
            "  sample_invalid_json STRING,"
            "  status STRING,"
            "  error_message STRING,"
            "  canceled_by STRING,"
            "  updated_at STRING,"
            "  run_type STRING,"
            "  job_run_id BIGINT,"
            "  rule_set_fingerprint STRING,"
            "  created_at STRING"
            ") CLUSTER BY (source_table_fqn, created_at);"
            #
            # RBAC: maps app roles (ADMIN/RULE_APPROVER/RULE_AUTHOR/
            # VIEWER/RUNNER) to Databricks workspace groups.
            f"CREATE TABLE IF NOT EXISTS {_PLACEHOLDER}.dq_role_mappings ("
            "  role STRING NOT NULL,"
            "  group_name STRING NOT NULL,"
            "  created_by STRING,"
            "  created_at TIMESTAMP,"
            "  updated_by STRING,"
            "  updated_at TIMESTAMP"
            ") CLUSTER BY (role);"
            #
            # Per-entity comment threads (rules, runs, profiles, ...).
            f"CREATE TABLE IF NOT EXISTS {_PLACEHOLDER}.dq_comments ("
            "  comment_id STRING NOT NULL,"
            "  entity_type STRING NOT NULL,"
            "  entity_id STRING NOT NULL,"
            "  user_email STRING NOT NULL,"
            "  comment STRING NOT NULL,"
            "  created_at TIMESTAMP"
            ") CLUSTER BY (entity_type, entity_id);"
            #
            # Quarantined invalid rows captured during validation.
            f"CREATE TABLE IF NOT EXISTS {_PLACEHOLDER}.dq_quarantine_records ("
            "  quarantine_id STRING NOT NULL,"
            "  run_id STRING NOT NULL,"
            "  source_table_fqn STRING NOT NULL,"
            "  requesting_user STRING,"
            "  row_data STRING,"
            "  errors STRING,"
            "  created_at STRING"
            ") CLUSTER BY (run_id, source_table_fqn);"
            #
            # Long-format observability events written by
            # DQMetricsObserver. Schema mirrors the public DQX
            # OBSERVATION_TABLE_SCHEMA so AI/BI dashboard templates
            # targeting the spec drop straight in.
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
            ") CLUSTER BY (input_location, run_time);"
            #
            # Scheduler bookkeeping: last/next run pointer per schedule.
            f"CREATE TABLE IF NOT EXISTS {_PLACEHOLDER}.dq_schedule_runs ("
            "  schedule_name STRING NOT NULL,"
            "  last_run_at TIMESTAMP,"
            "  next_run_at TIMESTAMP,"
            "  last_run_id STRING,"
            "  status STRING"
            ");"
            #
            # Per-schedule live config (cron/interval, scope filters).
            f"CREATE TABLE IF NOT EXISTS {_PLACEHOLDER}.dq_schedule_configs ("
            "  schedule_name STRING NOT NULL,"
            "  config_json STRING NOT NULL,"
            "  version INT,"
            "  created_by STRING,"
            "  created_at TIMESTAMP,"
            "  updated_by STRING,"
            "  updated_at TIMESTAMP"
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
        ),
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

    def run_all(self) -> int:
        """Ensure the schema exists and apply all pending migrations.

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
        "FIELDS_ALREADY_EXISTS",
        "TABLE_OR_VIEW_ALREADY_EXISTS",
        "TABLE_ALREADY_EXISTS",
        "already has liquid clustering defined",
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
