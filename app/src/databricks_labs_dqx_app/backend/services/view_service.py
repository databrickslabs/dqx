"""View management service for creating/dropping temporary views.

Uses the SQL Statement Execution API with the **OBO-authenticated**
WorkspaceClient so that view creation inherits the user's table permissions.
"""

from __future__ import annotations

import logging
import time
from uuid import uuid4

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Disposition, Format, StatementState

logger = logging.getLogger(__name__)

# Terminal states for SQL statement execution
_TERMINAL_STATES = {StatementState.SUCCEEDED, StatementState.FAILED, StatementState.CANCELED, StatementState.CLOSED}


class ViewService:
    """Create and drop temporary views via the SQL Statement Execution API."""

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
        self._schema_ensured = False

    def _ensure_schema(self) -> None:
        """Create the tmp schema if it doesn't exist.

        Uses catalog-only context (no schema) because the schema may not exist yet.
        """
        if self._schema_ensured:
            return
        cat = self._catalog.replace("`", "")
        schema = self._schema.replace("`", "")
        sql = f"CREATE SCHEMA IF NOT EXISTS `{cat}`.`{schema}`"
        logger.info("Ensuring tmp schema exists: %s.%s", self._catalog, self._schema)

        resp = self._ws.statement_execution.execute_statement(
            warehouse_id=self._warehouse_id,
            statement=sql,
            catalog=self._catalog,
            disposition=Disposition.INLINE,
            format=Format.JSON_ARRAY,
            wait_timeout="30s",
        )

        state = resp.status.state if resp.status else None
        statement_id = resp.statement_id

        # Poll if warehouse is still starting
        if state not in _TERMINAL_STATES and statement_id:
            logger.info("Schema creation statement %s in state %s, polling...", statement_id, state)
            state = self._wait_for_completion(statement_id, timeout_seconds=120)

        if state == StatementState.FAILED:
            msg = resp.status.error.message if resp.status and resp.status.error else "Unknown error"
            raise RuntimeError(f"Failed to create tmp schema: {msg}")
        elif state != StatementState.SUCCEEDED:
            raise RuntimeError(f"Schema creation ended in unexpected state: {state}")

        self._schema_ensured = True
        logger.info("Tmp schema ensured: %s.%s", self._catalog, self._schema)

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
        view_name = f"{self._catalog}.{self._schema}.tmp_view_{view_id}"
        quoted_source = quote_fqn(source_table_fqn)
        quoted_view = quote_fqn(view_name)
        limit_clause = f" LIMIT {int(sample_limit)}" if sample_limit else ""
        sql = f"CREATE OR REPLACE VIEW {quoted_view} AS SELECT * FROM {quoted_source}{limit_clause}"

        logger.info("Creating view %s from %s", view_name, source_table_fqn)
        self._execute(sql)

        # Grant ALL PRIVILEGES on the view to all account users so the job can read it
        # The job runs as the bundle deployer, not the OBO user who owns the view
        grant_sql = f"GRANT SELECT ON VIEW {quoted_view} TO `account users`"
        try:
            self._execute(grant_sql)
            logger.info("Granted SELECT on %s to account users", view_name)
        except Exception as e:
            logger.warning("Failed to grant SELECT on %s: %s (job may fail if running as different user)", view_name, e)

        # Verify the view was created successfully
        if not self._view_exists(view_name):
            raise RuntimeError(f"View creation succeeded but view not found: {view_name}")

        logger.info("View created and verified: %s", view_name)
        return view_name

    def _view_exists(self, view_fqn: str) -> bool:
        """Check if a view exists in Unity Catalog."""
        from databricks_labs_dqx_app.backend.sql_utils import quote_fqn

        sql = f"DESCRIBE TABLE {quote_fqn(view_fqn)}"
        try:
            resp = self._ws.statement_execution.execute_statement(
                warehouse_id=self._warehouse_id,
                statement=sql,
                catalog=self._catalog,
                schema=self._schema,
                disposition=Disposition.INLINE,
                format=Format.JSON_ARRAY,
                wait_timeout="30s",
            )

            state = resp.status.state if resp.status else None
            statement_id = resp.statement_id

            # Poll if needed
            if state not in _TERMINAL_STATES and statement_id:
                state = self._wait_for_completion(statement_id, timeout_seconds=60)

            exists = state == StatementState.SUCCEEDED
            logger.info("View existence check for %s: %s", view_fqn, "exists" if exists else "not found")
            return exists
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
        view_name = f"{self._catalog}.{self._schema}.tmp_view_{view_id}"
        quoted_view = quote_fqn(view_name)
        sql = f"CREATE OR REPLACE VIEW {quoted_view} AS {sql_query}"

        logger.info("Creating SQL-check view %s", view_name)
        self._execute(sql)

        grant_sql = f"GRANT SELECT ON VIEW {quoted_view} TO `account users`"
        try:
            self._execute(grant_sql)
            logger.info("Granted SELECT on %s to account users", view_name)
        except Exception as e:
            logger.warning("Failed to grant SELECT on %s: %s", view_name, e)

        if not self._view_exists(view_name):
            raise RuntimeError(f"View creation succeeded but view not found: {view_name}")

        logger.info("SQL-check view created and verified: %s", view_name)
        return view_name

    def drop_view(self, view_fqn: str) -> None:
        """Drop a temporary view.  Best-effort — logs warnings on failure."""
        from databricks_labs_dqx_app.backend.sql_utils import quote_fqn

        sql = f"DROP VIEW IF EXISTS {quote_fqn(view_fqn)}"
        try:
            self._execute(sql)
            logger.info("Dropped view %s", view_fqn)
        except Exception:
            logger.warning("Failed to drop view %s", view_fqn, exc_info=True)

    def _execute(self, sql: str, timeout_seconds: int = 120) -> None:
        """Execute SQL and wait for completion, polling if warehouse is starting."""
        logger.info("Executing SQL: %s", sql[:200])

        resp = self._ws.statement_execution.execute_statement(
            warehouse_id=self._warehouse_id,
            statement=sql,
            catalog=self._catalog,
            schema=self._schema,
            disposition=Disposition.INLINE,
            format=Format.JSON_ARRAY,
            wait_timeout="30s",
        )

        if not resp.status:
            raise RuntimeError(f"SQL statement returned no status\nSQL: {sql}")

        state = resp.status.state
        statement_id = resp.statement_id

        # If statement is still running (warehouse starting up), poll for completion
        if state not in _TERMINAL_STATES and statement_id:
            logger.info("Statement %s in state %s, polling for completion...", statement_id, state)
            state = self._wait_for_completion(statement_id, timeout_seconds)

        # Handle terminal states
        if state == StatementState.SUCCEEDED:
            logger.info("SQL statement succeeded")
            return
        elif state == StatementState.FAILED:
            msg = resp.status.error.message if resp.status.error else "Unknown error"
            raise RuntimeError(f"SQL statement failed: {msg}\nSQL: {sql}")
        elif state == StatementState.CANCELED:
            raise RuntimeError(f"SQL statement was canceled\nSQL: {sql}")
        elif state == StatementState.CLOSED:
            raise RuntimeError(f"SQL statement was closed unexpectedly\nSQL: {sql}")
        else:
            raise RuntimeError(f"SQL statement ended in unexpected state {state}\nSQL: {sql}")

    def _wait_for_completion(self, statement_id: str, timeout_seconds: int) -> StatementState:
        """Poll statement status until it reaches a terminal state."""
        start_time = time.time()
        poll_interval = 2.0

        while time.time() - start_time < timeout_seconds:
            status = self._ws.statement_execution.get_statement(statement_id)
            state = status.status.state if status.status else None

            if state in _TERMINAL_STATES:
                logger.info("Statement %s completed with state: %s", statement_id, state)
                return state

            logger.debug("Statement %s still in state %s, waiting...", statement_id, state)
            time.sleep(poll_interval)

        raise RuntimeError(f"SQL statement {statement_id} timed out after {timeout_seconds}s")
