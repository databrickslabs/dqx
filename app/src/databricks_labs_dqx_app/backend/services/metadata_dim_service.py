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
  (no filter = all): the binding's FQN, steward, review status, schedule,
  and version.

Write pattern (full refresh, idempotent): ``CREATE OR REPLACE TABLE`` first
(establishing the empty schema even when there are zero rows), then — only
when rows exist — a single ``INSERT INTO ... VALUES (...), (...)`` built
from escaped literals. Table FQNs are backtick-quoted per part via
:func:`quote_object_fqn` so a hyphenated catalog stays parseable. Both
writes go through the SP ``SqlExecutor`` (``sp_sql``).

Not best-effort internally: :meth:`refresh` lets exceptions propagate to
its callers (``app._ensure_metadata_dims`` at startup and the scheduler's
hourly tick), both of which are best-effort — mirroring how
:class:`ScoreViewService.ensure_views` raises and ``_ensure_score_views``
catches.
"""

from __future__ import annotations

import logging
from datetime import datetime

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
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor
from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string, quote_object_fqn

logger = logging.getLogger(__name__)

DIM_RULES_TABLE_NAME = "dim_dq_rules"
DIM_MONITORED_TABLES_TABLE_NAME = "dim_dq_monitored_tables"

# Column DDL for the two dims. Kept as module constants (mirroring the
# view-name constants in ``score_view_service``) so the CREATE-OR-REPLACE and
# the tests share one source of truth for the schema.
_RULES_COLUMNS_DDL = (
    "rule_id STRING, name STRING, description STRING, dimension STRING, "
    "default_severity STRING, mode STRING, status STRING, is_builtin BOOLEAN, "
    "steward STRING, version INT, created_at TIMESTAMP, updated_at TIMESTAMP"
)
_MONITORED_TABLES_COLUMNS_DDL = (
    "binding_id STRING, table_fqn STRING, steward STRING, status STRING, "
    "schedule_cron STRING, version INT, created_at TIMESTAMP, updated_at TIMESTAMP"
)


class MetadataDimService:
    """Full-refresh materializer for the rule + monitored-table metadata dims."""

    def __init__(
        self,
        sp_sql: SqlExecutor,
        registry: RegistryService,
        monitored_tables: MonitoredTableService,
    ) -> None:
        self._sql = sp_sql
        self._registry = registry
        self._monitored_tables = monitored_tables
        self._catalog = sp_sql.catalog
        self._schema = sp_sql.schema

    def refresh(self) -> None:
        """Full-refresh both dims from the registry (SP credentials).

        Each dim is dropped-and-recreated (``CREATE OR REPLACE TABLE``) and
        repopulated in one ``INSERT``. Raises on failure — the caller decides
        whether that is fatal (it is best-effort both at startup and on the
        scheduler tick).
        """
        self._refresh_rules()
        self._refresh_monitored_tables()

    def _refresh_rules(self) -> None:
        fqn = quote_object_fqn(self._catalog, self._schema, DIM_RULES_TABLE_NAME)
        self._sql.execute(f"CREATE OR REPLACE TABLE {fqn} ({_RULES_COLUMNS_DDL})")
        rules = self._registry.list_rules()
        if not rules:
            logger.info("Refreshed %s with %d rule(s)", DIM_RULES_TABLE_NAME, 0)
            return
        values = ", ".join(self._rule_values(rule) for rule in rules)
        self._sql.execute(f"INSERT INTO {fqn} VALUES {values}")
        logger.info("Refreshed %s with %d rule(s)", DIM_RULES_TABLE_NAME, len(rules))

    def _refresh_monitored_tables(self) -> None:
        fqn = quote_object_fqn(self._catalog, self._schema, DIM_MONITORED_TABLES_TABLE_NAME)
        self._sql.execute(f"CREATE OR REPLACE TABLE {fqn} ({_MONITORED_TABLES_COLUMNS_DDL})")
        summaries = self._monitored_tables.list_monitored_tables()
        if not summaries:
            logger.info("Refreshed %s with %d table(s)", DIM_MONITORED_TABLES_TABLE_NAME, 0)
            return
        values = ", ".join(self._table_values(summary.table) for summary in summaries)
        self._sql.execute(f"INSERT INTO {fqn} VALUES {values}")
        logger.info("Refreshed %s with %d table(s)", DIM_MONITORED_TABLES_TABLE_NAME, len(summaries))

    def _rule_values(self, rule: RegistryRule) -> str:
        """One ``(...)`` VALUES tuple for *rule*, columns in ``_RULES_COLUMNS_DDL`` order.

        The descriptive columns read the rule's OWN reserved
        ``user_metadata`` tags via the registry_models helpers — the raw
        default, never resolved against any run's applied severity.
        """
        metadata = rule.user_metadata
        cells = [
            self._str_lit(rule.rule_id),
            self._str_lit(get_rule_name(metadata)),
            self._str_lit(get_rule_description(metadata)),
            self._str_lit(get_rule_dimension(metadata)),
            self._str_lit(get_rule_severity(metadata)),
            self._str_lit(rule.mode),
            self._str_lit(rule.status),
            self._bool_lit(rule.is_builtin),
            self._str_lit(rule.steward),
            self._int_lit(rule.version),
            self._ts_lit(rule.created_at),
            self._ts_lit(rule.updated_at),
        ]
        return "(" + ", ".join(cells) + ")"

    def _table_values(self, table: MonitoredTable) -> str:
        """One ``(...)`` VALUES tuple for *table*, columns in ``_MONITORED_TABLES_COLUMNS_DDL`` order."""
        cells = [
            self._str_lit(table.binding_id),
            self._str_lit(table.table_fqn),
            self._str_lit(table.steward),
            self._str_lit(table.status),
            self._str_lit(table.schedule_cron),
            self._int_lit(table.version),
            self._ts_lit(table.created_at),
            self._ts_lit(table.updated_at),
        ]
        return "(" + ", ".join(cells) + ")"

    @staticmethod
    def _str_lit(value: str | None) -> str:
        """Single-quoted, escaped string literal, or the ``NULL`` literal for None."""
        return "NULL" if value is None else f"'{escape_sql_string(value)}'"

    @staticmethod
    def _int_lit(value: int | None) -> str:
        """Bare integer literal, or the ``NULL`` literal for None."""
        return "NULL" if value is None else str(int(value))

    @staticmethod
    def _bool_lit(value: bool) -> str:
        """``TRUE`` / ``FALSE`` literal."""
        return "TRUE" if value else "FALSE"

    @staticmethod
    def _ts_lit(value: datetime | None) -> str:
        """Delta ``TIMESTAMP'<iso>'`` literal, or the ``NULL`` literal for None."""
        return "NULL" if value is None else f"TIMESTAMP'{value.isoformat()}'"
