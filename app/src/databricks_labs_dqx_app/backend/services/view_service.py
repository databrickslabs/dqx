"""View management service for creating/dropping temporary views.

Uses the **OBO-authenticated** SqlExecutor so that view creation
inherits the user's table permissions.
"""

from __future__ import annotations

import logging
from uuid import uuid4

from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor

logger = logging.getLogger(__name__)

_tmp_schema_ready = False

# Map Unity Catalog ColumnTypeName values to SQL CAST targets for VALUES views.
# Anything not listed falls back to STRING.
_UC_TYPE_TO_SQL: dict[str, str] = {
    "LONG": "BIGINT",
    "INT": "INT",
    "SHORT": "SMALLINT",
    "BYTE": "TINYINT",
    "DOUBLE": "DOUBLE",
    "FLOAT": "FLOAT",
    "DECIMAL": "DECIMAL",
    "BOOLEAN": "BOOLEAN",
    "DATE": "DATE",
    "TIMESTAMP": "TIMESTAMP",
    "TIMESTAMP_NTZ": "TIMESTAMP_NTZ",
}


def mark_tmp_schema_ready() -> None:
    """Called at startup after the SP has ensured the tmp schema exists."""
    global _tmp_schema_ready
    _tmp_schema_ready = True


def reset_tmp_schema_ready() -> None:
    """Reset the flag — only used in tests."""
    global _tmp_schema_ready
    _tmp_schema_ready = False


class ViewService:
    """Create and drop temporary views via the SQL Statement Execution API."""

    def __init__(self, sql: SqlExecutor, sp_sql: SqlExecutor | None = None) -> None:
        self._sql = sql
        self._sp_sql = sp_sql

    def _ensure_schema(self) -> None:
        """Ensure the tmp schema exists. Uses SP credentials for DDL if available."""
        global _tmp_schema_ready
        if _tmp_schema_ready:
            return
        cat = self._sql.catalog.replace("`", "")
        schema = self._sql.schema.replace("`", "")
        if self._sp_sql is None:
            raise RuntimeError(
                f"Tmp schema `{cat}`.`{schema}` has not been created yet and no "
                f"service principal is available. Check app startup logs."
            )
        try:
            self._sp_sql.execute_no_schema(f"CREATE SCHEMA IF NOT EXISTS `{cat}`.`{schema}`")
            _tmp_schema_ready = True
        except Exception as e:
            raise RuntimeError(
                f"Cannot create tmp schema `{cat}`.`{schema}` via service principal. " f"Original error: {e}"
            ) from e

    def create_view(self, source_table_fqn: str, sample_limit: int | None = None) -> str:
        """Create a temporary view over *source_table_fqn*.

        Returns the fully qualified view name.  The view is created using
        the caller's OBO token so that the user's table permissions are
        enforced.
        """
        from databricks_labs_dqx_app.backend.sql_utils import quote_fqn, validate_fqn

        validate_fqn(source_table_fqn)
        self._ensure_schema()

        view_id = uuid4().hex[:12]
        view_name = f"{self._sql.catalog}.{self._sql.schema}.tmp_view_{view_id}"
        quoted_source = quote_fqn(source_table_fqn)
        quoted_view = quote_fqn(view_name)
        limit_clause = f" LIMIT {int(sample_limit)}" if sample_limit else ""
        sql = f"CREATE OR REPLACE VIEW {quoted_view} AS SELECT * FROM {quoted_source}{limit_clause}"

        logger.info("Creating view %s from %s", view_name, source_table_fqn)
        self._sql.execute(sql)

        grant_sql = f"GRANT SELECT ON VIEW {quoted_view} TO `account users`"
        try:
            self._sql.execute(grant_sql)
            logger.info("Granted SELECT on %s to account users", view_name)
        except Exception as e:
            logger.error(
                "GRANT SELECT failed on %s: %s — the background job (running as SP) "
                "will not be able to read this view. Check that the user has GRANT "
                "privileges on schema %s.%s",
                view_name,
                e,
                self._sql.catalog,
                self._sql.schema,
            )
            raise RuntimeError(
                f"Cannot grant SELECT on temporary view to account users. "
                f"Ensure the user has ownership or GRANT privilege on "
                f"schema {self._sql.catalog}.{self._sql.schema}: {e}"
            ) from e

        if not self._view_exists(view_name):
            raise RuntimeError(f"View creation succeeded but view not found: {view_name}")

        logger.info("View created and verified: %s", view_name)
        return view_name

    def _view_exists(self, view_fqn: str) -> bool:
        """Check if a view exists in Unity Catalog."""
        from databricks_labs_dqx_app.backend.sql_utils import quote_fqn

        sql = f"DESCRIBE TABLE {quote_fqn(view_fqn)}"
        try:
            self._sql.execute(sql)
            return True
        except Exception as e:
            logger.warning("View existence check failed for %s: %s", view_fqn, e)
            return False

    def create_view_from_sql(self, sql_query: str) -> str:
        """Create a temporary view whose body is an arbitrary SQL query.

        Used for cross-table SQL checks where the query itself returns
        the violation rows.  Returns the fully qualified view name.
        """
        from databricks.labs.dqx.utils import is_sql_query_safe
        from databricks.labs.dqx.errors import UnsafeSqlQueryError

        if not is_sql_query_safe(sql_query):
            raise UnsafeSqlQueryError(
                "The SQL query contains prohibited statements and cannot be used to create a view."
            )

        from databricks_labs_dqx_app.backend.sql_utils import quote_fqn

        self._ensure_schema()

        view_id = uuid4().hex[:12]
        view_name = f"{self._sql.catalog}.{self._sql.schema}.tmp_view_{view_id}"
        quoted_view = quote_fqn(view_name)
        sql = f"CREATE OR REPLACE VIEW {quoted_view} AS {sql_query}"

        logger.info("Creating SQL-check view %s", view_name)
        self._sql.execute(sql)

        grant_sql = f"GRANT SELECT ON VIEW {quoted_view} TO `account users`"
        try:
            self._sql.execute(grant_sql)
            logger.info("Granted SELECT on %s to account users", view_name)
        except Exception as e:
            logger.error(
                "GRANT SELECT failed on SQL-check view %s: %s — the background job "
                "will not be able to read this view.",
                view_name,
                e,
            )
            raise RuntimeError(
                f"Cannot grant SELECT on temporary view to account users. "
                f"Ensure the user has ownership or GRANT privilege on "
                f"schema {self._sql.catalog}.{self._sql.schema}: {e}"
            ) from e

        if not self._view_exists(view_name):
            raise RuntimeError(f"View creation succeeded but view not found: {view_name}")

        logger.info("SQL-check view created and verified: %s", view_name)
        return view_name

    def create_view_from_rows(
        self,
        columns: list[str],
        rows: list[dict[str, str | None]],
        col_types: dict[str, str],
    ) -> str:
        """Create a temporary view over inline preview rows using a VALUES clause.

        All values arrive as strings from the preview API; each column is
        TRY_CAST to its Unity Catalog type so DQX numeric/date checks still work.
        Returns the fully qualified view name.
        """
        from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string, quote_fqn

        self._ensure_schema()

        view_id = uuid4().hex[:12]
        view_name = f"{self._sql.catalog}.{self._sql.schema}.tmp_view_{view_id}"
        quoted_view = quote_fqn(view_name)

        pos_aliases = [f"_c{i}" for i in range(len(columns))]

        def _literal(val: str | None) -> str:
            if val is None:
                return "NULL"
            return f"'{escape_sql_string(val)}'"

        def _row_sql(row: dict[str, str | None]) -> str:
            return "(" + ", ".join(_literal(row.get(col)) for col in columns) + ")"

        rows_sql = ",\n    ".join(_row_sql(r) for r in rows)

        select_parts: list[str] = []
        for i, col in enumerate(columns):
            pos = pos_aliases[i]
            uc_type = col_types.get(col, "STRING").upper()
            sql_type = _UC_TYPE_TO_SQL.get(uc_type, "STRING")
            quoted_col = "`" + col.replace("`", "``") + "`"
            if sql_type == "STRING":
                select_parts.append(f"CAST({pos} AS STRING) AS {quoted_col}")
            else:
                select_parts.append(f"TRY_CAST({pos} AS {sql_type}) AS {quoted_col}")

        aliases_csv = ", ".join(pos_aliases)
        select_csv = ", ".join(select_parts)

        sql = (
            f"CREATE OR REPLACE VIEW {quoted_view} AS\n"
            f"SELECT {select_csv}\n"
            f"FROM (\n  VALUES\n    {rows_sql}\n) AS _t({aliases_csv})"
        )

        logger.info("Creating VALUES view %s (%d rows, %d cols)", view_name, len(rows), len(columns))
        self._sql.execute(sql)

        grant_sql = f"GRANT SELECT ON VIEW {quoted_view} TO `account users`"
        try:
            self._sql.execute(grant_sql)
        except Exception as e:
            logger.error("GRANT SELECT failed on VALUES view %s: %s", view_name, e)
            raise RuntimeError(
                f"Cannot grant SELECT on VALUES view to account users. "
                f"Ensure the user has ownership or GRANT privilege on "
                f"schema {self._sql.catalog}.{self._sql.schema}: {e}"
            ) from e

        if not self._view_exists(view_name):
            raise RuntimeError(f"VALUES view creation succeeded but view not found: {view_name}")

        logger.info("VALUES view created and verified: %s", view_name)
        return view_name

    def drop_view(self, view_fqn: str) -> None:
        """Drop a temporary view.  Best-effort -- logs warnings on failure."""
        from databricks_labs_dqx_app.backend.sql_utils import quote_fqn

        sql = f"DROP VIEW IF EXISTS {quote_fqn(view_fqn)}"
        try:
            self._sql.execute(sql)
            logger.info("Dropped view %s", view_fqn)
        except Exception:
            logger.warning("Failed to drop view %s", view_fqn, exc_info=True)
