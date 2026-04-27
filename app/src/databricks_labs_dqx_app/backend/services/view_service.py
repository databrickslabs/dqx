"""View management service for creating/dropping temporary views.

Uses the **OBO-authenticated** SqlExecutor so that view creation
inherits the user's table permissions.
"""

from __future__ import annotations

import logging
from uuid import uuid4

from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor

logger = logging.getLogger(__name__)


class ViewService:
    """Create and drop temporary views via the SQL Statement Execution API."""

    def __init__(self, sql: SqlExecutor) -> None:
        self._sql = sql
        self._schema_ensured = False

    def _ensure_schema(self) -> None:
        """Create the tmp schema if it doesn't exist."""
        if self._schema_ensured:
            return
        cat = self._sql.catalog.replace("`", "")
        schema = self._sql.schema.replace("`", "")
        sql = f"CREATE SCHEMA IF NOT EXISTS `{cat}`.`{schema}`"
        logger.info("Ensuring tmp schema exists: %s.%s", self._sql.catalog, self._sql.schema)
        self._sql.execute_no_schema(sql)
        self._schema_ensured = True
        logger.info("Tmp schema ensured: %s.%s", self._sql.catalog, self._sql.schema)

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
            logger.warning("Failed to grant SELECT on %s: %s (job may fail if running as different user)", view_name, e)

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
            logger.warning("Failed to grant SELECT on %s: %s", view_name, e)

        if not self._view_exists(view_name):
            raise RuntimeError(f"View creation succeeded but view not found: {view_name}")

        logger.info("SQL-check view created and verified: %s", view_name)
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
