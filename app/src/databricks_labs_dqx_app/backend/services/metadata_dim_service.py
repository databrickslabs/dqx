"""Materialize rule + monitored-table metadata dims for the Genie space.

Two SP-owned UC Delta tables in the app's main schema, FULL-REFRESH
materialized from the Lakebase Rules Registry so the Ask-Genie space can
answer authoring/ownership questions ("who owns this rule/table", "what is
this rule's description", "which tables are in draft") WITHOUT reaching
Postgres directly (Genie can only query UC objects) and without exposing
any row-level / quarantine data — the same aggregates-only posture as the
score views in :mod:`backend.services.score_view_service`.

- *dim_dq_rules* — one row per registry rule (any status), sourced from
  :meth:`RegistryService.list_rules` (no filter = all, capped at 2000). The
  descriptive columns (*name* / *description* / *dimension* /
  *default_severity*) are the rule's OWN reserved ``user_metadata`` tags —
  the RAW default the rule was authored with, never resolved against any
  run's applied severity. *default_severity* is deliberately named to keep
  it distinct from the APPLIED/effective ``severity`` carried on the score
  views (``v_dq_check_attribution`` / ``v_dq_check_results`` /
  ``mv_dq_scores``), which is what a check actually ran with post
  ``severity_override``.
- *dim_dq_monitored_tables* — one row per monitored-table binding (any
  status), sourced from :meth:`MonitoredTableService.list_monitored_tables`
  (no filter = all): the binding's FQN, owner, review status, schedule,
  and version.

Write pattern (full refresh, idempotent): ``CREATE OR REPLACE TABLE`` first
(establishing the empty schema even when there are zero rows), then — only
when rows exist — bounded ``INSERT INTO ... VALUES (...), (...)`` batches
with bound values. Table FQNs are backtick-quoted per part via
:func:`quote_object_fqn` so a hyphenated catalog stays parseable. Both
writes go through the SP ``SqlExecutor`` (``sp_sql``).

Not best-effort internally: :meth:`refresh` lets exceptions propagate to
the cached startup and Genie-message refresh coordinator, whose callers are
best-effort — mirroring how
:class:`ScoreViewService.ensure_views` raises and ``_ensure_score_views``
catches.
"""

import logging

from databricks_labs_dqx_app.backend.registry_models import (
    MonitoredTable,
    RegistryRule,
    get_rule_description,
    get_rule_dimension,
    get_rule_name,
    get_rule_severity,
)
from databricks_labs_dqx_app.backend.services.monitored_table_service import MonitoredTableService
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor, SqlParameterValue
from databricks_labs_dqx_app.backend.sql_utils import quote_object_fqn

logger = logging.getLogger(__name__)

DIM_RULES_TABLE_NAME = "dim_dq_rules"
DIM_MONITORED_TABLES_TABLE_NAME = "dim_dq_monitored_tables"
_INSERT_BATCH_SIZE = 50

# Column DDL for the two dims. Kept as module constants (mirroring the
# view-name constants in ``score_view_service``) so the CREATE-OR-REPLACE and
# the tests share one source of truth for the schema.
_RULES_COLUMNS_DDL = (
    "rule_id STRING, name STRING, description STRING, dimension STRING, "
    "default_severity STRING, mode STRING, status STRING, is_builtin BOOLEAN, "
    "owner STRING, version INT, created_at TIMESTAMP, updated_at TIMESTAMP"
)
_MONITORED_TABLES_COLUMNS_DDL = (
    "binding_id STRING, table_fqn STRING, owner STRING, status STRING, "
    "schedule_cron STRING, version INT, created_at TIMESTAMP, updated_at TIMESTAMP"
)


class MetadataDimService:
    """Full-refresh materializer for the rule + monitored-table metadata dims."""

    def __init__(
        self,
        sp_sql: SqlExecutor,
        registry: RegistryService,
        monitored_tables: MonitoredTableService,
        genie_schema: str,
    ) -> None:
        self._sql = sp_sql
        self._registry = registry
        self._monitored_tables = monitored_tables
        self._catalog = sp_sql.catalog
        self._schema = sp_sql.schema
        self._genie_schema = genie_schema

    def refresh(self) -> None:
        """Full-refresh both dims from the registry (SP credentials).

        Each dim is dropped-and-recreated (``CREATE OR REPLACE TABLE``) and
        repopulated in one ``INSERT``. Raises on failure — the caller decides
        whether that is fatal (it is best-effort both at startup and on the
        start of a Genie conversation).
        """
        self._refresh_rules()
        self._refresh_monitored_tables()

    def _refresh_rules(self) -> None:
        fqn = quote_object_fqn(self._catalog, self._genie_schema, DIM_RULES_TABLE_NAME)
        self._sql.execute(f"CREATE OR REPLACE TABLE {fqn} ({_RULES_COLUMNS_DDL})")
        rules = self._registry.list_rules()
        if not rules:
            logger.info("Refreshed %s with %d rule(s)", DIM_RULES_TABLE_NAME, 0)
            return
        self._insert_rows(fqn, [self._rule_values(rule) for rule in rules])
        logger.info("Refreshed %s with %d rule(s)", DIM_RULES_TABLE_NAME, len(rules))

    def _refresh_monitored_tables(self) -> None:
        fqn = quote_object_fqn(self._catalog, self._genie_schema, DIM_MONITORED_TABLES_TABLE_NAME)
        self._sql.execute(f"CREATE OR REPLACE TABLE {fqn} ({_MONITORED_TABLES_COLUMNS_DDL})")
        summaries = self._monitored_tables.list_monitored_tables()
        if not summaries:
            logger.info("Refreshed %s with %d table(s)", DIM_MONITORED_TABLES_TABLE_NAME, 0)
            return
        self._insert_rows(fqn, [self._table_values(summary.table) for summary in summaries])
        logger.info("Refreshed %s with %d table(s)", DIM_MONITORED_TABLES_TABLE_NAME, len(summaries))

    def _rule_values(self, rule: RegistryRule) -> dict[str, SqlParameterValue]:
        """Bound values for *rule*, in ``_RULES_COLUMNS_DDL`` order.

        The descriptive columns read the rule's OWN reserved
        ``user_metadata`` tags via the registry_models helpers — the raw
        default, never resolved against any run's applied severity.
        """
        metadata = rule.user_metadata
        return {
            "rule_id": rule.rule_id,
            "name": get_rule_name(metadata),
            "description": get_rule_description(metadata),
            "dimension": get_rule_dimension(metadata),
            "default_severity": get_rule_severity(metadata),
            "mode": rule.mode,
            "status": rule.status,
            "is_builtin": rule.is_builtin,
            "owner": rule.owner,
            "version": rule.version,
            "created_at": rule.created_at.isoformat() if rule.created_at else None,
            "updated_at": rule.updated_at.isoformat() if rule.updated_at else None,
        }

    def _table_values(self, table: MonitoredTable) -> dict[str, SqlParameterValue]:
        """Bound values for *table*, in ``_MONITORED_TABLES_COLUMNS_DDL`` order."""
        return {
            "binding_id": table.binding_id,
            "table_fqn": table.table_fqn,
            "owner": table.owner,
            "status": table.status,
            "schedule_cron": table.schedule_cron,
            "version": table.version,
            "created_at": table.created_at.isoformat() if table.created_at else None,
            "updated_at": table.updated_at.isoformat() if table.updated_at else None,
        }

    def _bind_rows(self, rows: list[dict[str, SqlParameterValue]]) -> tuple[str, dict[str, SqlParameterValue]]:
        """Build one VALUES clause with distinct markers for every cell."""
        parameters: dict[str, SqlParameterValue] = {}
        tuples: list[str] = []
        for index, row in enumerate(rows):
            cells: list[str] = []
            for column, value in row.items():
                name = f"{column}_{index}"
                parameters[name] = value
                marker = self._sql.param(name)
                cells.append(f"CAST({marker} AS TIMESTAMP)" if column in {"created_at", "updated_at"} else marker)
            tuples.append("(" + ", ".join(cells) + ")")
        return ", ".join(tuples), parameters

    def _insert_rows(self, fqn: str, rows: list[dict[str, SqlParameterValue]]) -> None:
        """Insert metadata in bounded batches for the Statement Execution API."""
        for start in range(0, len(rows), _INSERT_BATCH_SIZE):
            values, parameters = self._bind_rows(rows[start : start + _INSERT_BATCH_SIZE])
            self._sql.execute(f"INSERT INTO {fqn} VALUES {values}", parameters=parameters)
