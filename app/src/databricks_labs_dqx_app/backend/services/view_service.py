"""View management service for creating/dropping temporary views.

Uses the SQL Statement Execution API with the **OBO-authenticated**
WorkspaceClient so that view creation inherits the user's table permissions.
"""

from __future__ import annotations

import logging
from uuid import uuid4

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Disposition, Format, StatementState

logger = logging.getLogger(__name__)


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

    def create_view(self, source_table_fqn: str, sample_limit: int | None = None) -> str:
        """Create a temporary view over *source_table_fqn*.

        Returns the fully qualified view name.  The view is created using
        the caller's OBO token so that the user's table permissions are
        enforced.
        """
        view_id = uuid4().hex[:12]
        view_name = f"{self._catalog}.{self._schema}.tmp_view_{view_id}"
        limit_clause = f" LIMIT {sample_limit}" if sample_limit else ""
        sql = f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM {source_table_fqn}{limit_clause}"

        logger.info("Creating view %s from %s", view_name, source_table_fqn)
        self._execute(sql)
        return view_name

    def drop_view(self, view_fqn: str) -> None:
        """Drop a temporary view.  Best-effort — logs warnings on failure."""
        sql = f"DROP VIEW IF EXISTS {view_fqn}"
        try:
            self._execute(sql)
            logger.info("Dropped view %s", view_fqn)
        except Exception:
            logger.warning("Failed to drop view %s", view_fqn, exc_info=True)

    def _execute(self, sql: str) -> None:
        resp = self._ws.statement_execution.execute_statement(
            warehouse_id=self._warehouse_id,
            statement=sql,
            catalog=self._catalog,
            schema=self._schema,
            disposition=Disposition.INLINE,
            format=Format.JSON_ARRAY,
            wait_timeout="50s",
        )
        if resp.status and resp.status.state == StatementState.FAILED:
            msg = resp.status.error.message if resp.status.error else "Unknown error"
            raise RuntimeError(f"SQL statement failed: {msg}\nSQL: {sql}")
