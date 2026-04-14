"""Database migration runner for DQX App.

Migrations are versioned DDL statements applied in order against the
configured catalog/schema.  The runner tracks every applied version
in a ``dq_migrations`` meta-table, so re-starting the app never
re-applies a migration that already succeeded.

Adding a new table or schema change
------------------------------------
Append a new :class:`Migration` entry to ``MIGRATIONS``.  Never edit
or reorder existing entries — only append.  The version number must be
strictly monotonically increasing.

Example::

    Migration(
        version=3,
        description="Add name column to dq_app_settings",
        sql_template=(
            "ALTER TABLE {catalog}.{schema}.dq_app_settings "
            "ADD COLUMN IF NOT EXISTS name STRING"
        ),
    )
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Disposition, Format, StatementState

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


# Order is significant.  Never change or remove existing entries.
MIGRATIONS: list[Migration] = [
    Migration(
        version=1,
        description="Create dq_app_settings table",
        sql_template=(
            f"CREATE TABLE IF NOT EXISTS {_PLACEHOLDER}.dq_app_settings ("
            "  setting_key STRING NOT NULL,"
            "  setting_value STRING,"
            "  updated_at TIMESTAMP,"
            "  updated_by STRING"
            ")"
        ),
    ),
    Migration(
        version=2,
        description="Create dq_quality_rules table",
        sql_template=(
            f"CREATE TABLE IF NOT EXISTS {_PLACEHOLDER}.dq_quality_rules ("
            "  table_fqn STRING NOT NULL,"
            "  checks STRING NOT NULL,"
            "  version INT,"
            "  status STRING,"
            "  created_by STRING,"
            "  created_at TIMESTAMP,"
            "  updated_by STRING,"
            "  updated_at TIMESTAMP"
            ")"
        ),
    ),
    Migration(
        version=3,
        description="Create dq_profiling_results table",
        sql_template=(
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
            "  created_at STRING"
            ")"
        ),
    ),
    Migration(
        version=4,
        description="Create dq_validation_runs table",
        sql_template=(
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
            "  created_at STRING"
            ")"
        ),
    ),
    Migration(
        version=5,
        description="Liquid cluster dq_quality_rules by table_fqn and status",
        sql_template=(f"ALTER TABLE {_PLACEHOLDER}.dq_quality_rules " "CLUSTER BY (table_fqn, status)"),
    ),
    Migration(
        version=6,
        description="Liquid cluster dq_profiling_results by source_table_fqn and created_at",
        sql_template=(f"ALTER TABLE {_PLACEHOLDER}.dq_profiling_results " "CLUSTER BY (source_table_fqn, created_at)"),
    ),
    Migration(
        version=7,
        description="Liquid cluster dq_validation_runs by source_table_fqn and created_at",
        sql_template=(f"ALTER TABLE {_PLACEHOLDER}.dq_validation_runs " "CLUSTER BY (source_table_fqn, created_at)"),
    ),
    Migration(
        version=8,
        description="Liquid cluster dq_app_settings by setting_key",
        sql_template=(f"ALTER TABLE {_PLACEHOLDER}.dq_app_settings " "CLUSTER BY (setting_key)"),
    ),
    Migration(
        version=9,
        description="Create dq_role_mappings table for RBAC",
        sql_template=(
            f"CREATE TABLE IF NOT EXISTS {_PLACEHOLDER}.dq_role_mappings ("
            "  role STRING NOT NULL,"
            "  group_name STRING NOT NULL,"
            "  created_by STRING,"
            "  created_at TIMESTAMP,"
            "  updated_by STRING,"
            "  updated_at TIMESTAMP"
            ")"
        ),
    ),
    Migration(
        version=10,
        description="Liquid cluster dq_role_mappings by role",
        sql_template=(f"ALTER TABLE {_PLACEHOLDER}.dq_role_mappings " "CLUSTER BY (role)"),
    ),
    Migration(
        version=11,
        description="Create dq_comments table",
        sql_template=(
            f"CREATE TABLE IF NOT EXISTS {_PLACEHOLDER}.dq_comments ("
            "  comment_id STRING NOT NULL,"
            "  entity_type STRING NOT NULL,"
            "  entity_id STRING NOT NULL,"
            "  user_email STRING NOT NULL,"
            "  comment STRING NOT NULL,"
            "  created_at TIMESTAMP"
            ")"
        ),
    ),
    Migration(
        version=12,
        description="Liquid cluster dq_comments by entity_type and entity_id",
        sql_template=(f"ALTER TABLE {_PLACEHOLDER}.dq_comments " "CLUSTER BY (entity_type, entity_id)"),
    ),
    Migration(
        version=13,
        description="Add canceled_by and updated_at audit columns to dq_validation_runs",
        sql_template=(
            f"ALTER TABLE {_PLACEHOLDER}.dq_validation_runs " "ADD COLUMNS (canceled_by STRING, updated_at STRING)"
        ),
    ),
    Migration(
        version=14,
        description="Add canceled_by and updated_at audit columns to dq_profiling_results",
        sql_template=(
            f"ALTER TABLE {_PLACEHOLDER}.dq_profiling_results " "ADD COLUMNS (canceled_by STRING, updated_at STRING)"
        ),
    ),
    Migration(
        version=15,
        description="Create dq_quarantine_records table",
        sql_template=(
            f"CREATE TABLE IF NOT EXISTS {_PLACEHOLDER}.dq_quarantine_records ("
            "  quarantine_id STRING NOT NULL,"
            "  run_id STRING NOT NULL,"
            "  source_table_fqn STRING NOT NULL,"
            "  requesting_user STRING,"
            "  row_data STRING,"
            "  errors STRING,"
            "  created_at STRING"
            ")"
        ),
    ),
    Migration(
        version=16,
        description="Liquid cluster dq_quarantine_records by run_id and source_table_fqn",
        sql_template=(f"ALTER TABLE {_PLACEHOLDER}.dq_quarantine_records " "CLUSTER BY (run_id, source_table_fqn)"),
    ),
    Migration(
        version=17,
        description="Create dq_metrics table for quality trend tracking",
        sql_template=(
            f"CREATE TABLE IF NOT EXISTS {_PLACEHOLDER}.dq_metrics ("
            "  metric_id STRING NOT NULL,"
            "  run_id STRING NOT NULL,"
            "  source_table_fqn STRING NOT NULL,"
            "  run_type STRING,"
            "  total_rows INT,"
            "  valid_rows INT,"
            "  invalid_rows INT,"
            "  pass_rate DOUBLE,"
            "  error_breakdown STRING,"
            "  requesting_user STRING,"
            "  created_at STRING"
            ")"
        ),
    ),
    Migration(
        version=18,
        description="Liquid cluster dq_metrics by source_table_fqn and created_at",
        sql_template=(f"ALTER TABLE {_PLACEHOLDER}.dq_metrics " "CLUSTER BY (source_table_fqn, created_at)"),
    ),
    Migration(
        version=19,
        description="Add source column to dq_quality_rules",
        sql_template=(f"ALTER TABLE {_PLACEHOLDER}.dq_quality_rules " "ADD COLUMNS (source STRING)"),
    ),
    Migration(
        version=20,
        description="Create dq_quality_rules_history table",
        sql_template=(
            f"CREATE TABLE IF NOT EXISTS {_PLACEHOLDER}.dq_quality_rules_history ("
            "  table_fqn STRING NOT NULL,"
            "  checks STRING,"
            "  version INT,"
            "  source STRING,"
            "  action STRING NOT NULL,"
            "  changed_by STRING,"
            "  changed_at TIMESTAMP"
            ")"
        ),
    ),
    Migration(
        version=21,
        description="Liquid cluster dq_quality_rules_history by table_fqn and changed_at",
        sql_template=(f"ALTER TABLE {_PLACEHOLDER}.dq_quality_rules_history " "CLUSTER BY (table_fqn, changed_at)"),
    ),
    Migration(
        version=22,
        description="Create dq_schedule_runs table for tracking scheduled run triggers",
        sql_template=(
            f"CREATE TABLE IF NOT EXISTS {_PLACEHOLDER}.dq_schedule_runs ("
            "  schedule_name STRING NOT NULL,"
            "  last_run_at TIMESTAMP,"
            "  next_run_at TIMESTAMP,"
            "  last_run_id STRING,"
            "  status STRING"
            ")"
        ),
    ),
    Migration(
        version=23,
        description="Create dq_schedule_configs table for separate per-schedule storage",
        sql_template=(
            f"CREATE TABLE IF NOT EXISTS {_PLACEHOLDER}.dq_schedule_configs ("
            "  schedule_name STRING NOT NULL,"
            "  config_json STRING NOT NULL,"
            "  version INT,"
            "  created_by STRING,"
            "  created_at TIMESTAMP,"
            "  updated_by STRING,"
            "  updated_at TIMESTAMP"
            ")"
        ),
    ),
    Migration(
        version=24,
        description="Liquid cluster dq_schedule_configs by schedule_name",
        sql_template=(f"ALTER TABLE {_PLACEHOLDER}.dq_schedule_configs " "CLUSTER BY (schedule_name)"),
    ),
    Migration(
        version=25,
        description="Create dq_schedule_configs_history table for change tracking",
        sql_template=(
            f"CREATE TABLE IF NOT EXISTS {_PLACEHOLDER}.dq_schedule_configs_history ("
            "  schedule_name STRING NOT NULL,"
            "  config_json STRING,"
            "  version INT,"
            "  action STRING NOT NULL,"
            "  changed_by STRING,"
            "  changed_at TIMESTAMP"
            ")"
        ),
    ),
    Migration(
        version=26,
        description="Liquid cluster dq_schedule_configs_history by schedule_name and changed_at",
        sql_template=(
            f"ALTER TABLE {_PLACEHOLDER}.dq_schedule_configs_history " "CLUSTER BY (schedule_name, changed_at)"
        ),
    ),
    Migration(
        version=27,
        description="Add rule_id column to dq_quality_rules for per-check granularity",
        sql_template=(f"ALTER TABLE {_PLACEHOLDER}.dq_quality_rules " "ADD COLUMNS (rule_id STRING)"),
    ),
    Migration(
        version=28,
        description="Add rule_id column to dq_quality_rules_history",
        sql_template=(f"ALTER TABLE {_PLACEHOLDER}.dq_quality_rules_history " "ADD COLUMNS (rule_id STRING)"),
    ),
    Migration(
        version=29,
        description="Add run_type column to dq_validation_runs to distinguish dryrun vs scheduled",
        sql_template=(f"ALTER TABLE {_PLACEHOLDER}.dq_validation_runs " "ADD COLUMNS (run_type STRING)"),
    ),
]

# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

_META_TABLE = f"{_PLACEHOLDER}.dq_migrations"


class MigrationRunner:
    """Applies pending database migrations against the DQX App schema.

    All DDL is executed via the Databricks Statement Execution API using
    the app's service-principal credentials, *not* the OBO user token.

    Usage::

        runner = MigrationRunner(ws=rt.ws, warehouse_id=wh_id,
                                 catalog="dqx", schema="dqx_app")
        applied = runner.run_all()
        # applied == number of migrations just executed (0 if already up to date)
    """

    def __init__(
        self,
        ws: WorkspaceClient,
        warehouse_id: str,
        catalog: str,
        schema: str,
    ) -> None:
        self._ws = ws
        self._warehouse_id = warehouse_id
        self._catalog = catalog
        self._schema = schema
        self._meta_table = _META_TABLE.format(catalog=catalog, schema=schema)

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
        self._execute_bootstrap(sql)
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
        self._execute(sql)
        logger.debug("Ensured migrations meta-table exists: %s", self._meta_table)

    def _applied_versions(self) -> set[int]:
        return set(self._applied_at_map().keys())

    def _applied_at_map(self) -> dict[int, str]:
        sql = f"SELECT version, CAST(applied_at AS STRING) FROM {self._meta_table} ORDER BY version"
        rows = self._query(sql)
        return {int(row[0]): row[1] for row in rows}

    def _apply(self, migration: Migration) -> None:
        sql = migration.sql_template.format(catalog=self._catalog, schema=self._schema)
        self._execute(sql)

        record_sql = (
            f"INSERT INTO {self._meta_table} (version, description, applied_at) "
            f"VALUES ({migration.version}, '{migration.description.replace(chr(39), chr(39)*2)}', current_timestamp())"
        )
        self._execute(record_sql)
        logger.info(
            "Migration v%d applied successfully: %s",
            migration.version,
            migration.description,
        )

    def _execute_bootstrap(self, sql: str) -> None:
        """Execute DDL with catalog-only context (no schema).

        Used for statements that must run before the target schema exists,
        e.g. ``CREATE SCHEMA``.  All table references in ``sql`` must be
        fully-qualified (catalog.schema.table).
        """
        resp = self._ws.statement_execution.execute_statement(
            warehouse_id=self._warehouse_id,
            statement=sql,
            catalog=self._catalog,
            disposition=Disposition.INLINE,
            format=Format.JSON_ARRAY,
        )
        if resp.status and resp.status.state == StatementState.FAILED:
            msg = resp.status.error.message if resp.status.error else "Unknown error"
            raise RuntimeError(f"Migration SQL failed: {msg}\nSQL: {sql}")

    def _execute(self, sql: str) -> None:
        resp = self._ws.statement_execution.execute_statement(
            warehouse_id=self._warehouse_id,
            statement=sql,
            catalog=self._catalog,
            schema=self._schema,
            disposition=Disposition.INLINE,
            format=Format.JSON_ARRAY,
        )
        if resp.status and resp.status.state == StatementState.FAILED:
            msg = resp.status.error.message if resp.status.error else "Unknown error"
            raise RuntimeError(f"Migration SQL failed: {msg}\nSQL: {sql}")

    def _query(self, sql: str) -> list[list[str]]:
        resp = self._ws.statement_execution.execute_statement(
            warehouse_id=self._warehouse_id,
            statement=sql,
            catalog=self._catalog,
            schema=self._schema,
            disposition=Disposition.INLINE,
            format=Format.JSON_ARRAY,
        )
        if resp.status and resp.status.state == StatementState.FAILED:
            msg = resp.status.error.message if resp.status.error else "Unknown error"
            raise RuntimeError(f"Migration query failed: {msg}\nSQL: {sql}")

        if resp.result and resp.result.data_array:
            return resp.result.data_array
        return []
