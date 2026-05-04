"""Centralised SQL Statement Execution API wrapper.

Every service that needs to run SQL against a Databricks SQL Warehouse
should accept a ``SqlExecutor`` instance instead of constructing its own
``execute_statement`` calls.  This eliminates boilerplate duplication and
makes services testable via ``create_autospec(SqlExecutor)``.
"""

from __future__ import annotations

import logging
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Disposition, Format, StatementState

logger = logging.getLogger(__name__)

_TERMINAL_STATES = {StatementState.SUCCEEDED, StatementState.FAILED, StatementState.CANCELED, StatementState.CLOSED}


class SqlExecutor:
    """Thin wrapper around the Databricks Statement Execution API.

    Provides ``execute``, ``query``, ``query_dicts``, and
    ``execute_no_schema`` covering all usage patterns in the DQX App
    backend.
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

    @property
    def warehouse_id(self) -> str:
        return self._warehouse_id

    @property
    def catalog(self) -> str:
        return self._catalog

    @property
    def schema(self) -> str:
        return self._schema

    def execute(self, sql: str, *, timeout_seconds: int = 120) -> None:
        """Execute a SQL statement that does not return rows.

        Polls for completion when the warehouse is cold-starting.
        Raises ``RuntimeError`` on failure.
        """
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
        sid = resp.statement_id

        if state not in _TERMINAL_STATES and sid:
            state = self._wait_for_completion(sid, timeout_seconds)

        if state == StatementState.SUCCEEDED:
            return
        if state == StatementState.FAILED:
            msg = resp.status.error.message if resp.status.error else "Unknown error"
            raise RuntimeError(f"SQL execution failed: {msg}\nSQL: {sql}")
        raise RuntimeError(f"SQL statement ended in unexpected state {state}\nSQL: {sql}")

    def execute_no_schema(self, sql: str) -> None:
        """Execute DDL with catalog-only context (no default schema).

        Used for ``CREATE SCHEMA`` and other bootstrap DDL that must run
        before the target schema exists.
        """
        resp = self._ws.statement_execution.execute_statement(
            warehouse_id=self._warehouse_id,
            statement=sql,
            catalog=self._catalog,
            disposition=Disposition.INLINE,
            format=Format.JSON_ARRAY,
            wait_timeout="30s",
        )
        if not resp.status:
            raise RuntimeError(f"SQL statement returned no status\nSQL: {sql}")

        state = resp.status.state
        sid = resp.statement_id

        if state not in _TERMINAL_STATES and sid:
            state = self._wait_for_completion(sid, 120)

        if state == StatementState.SUCCEEDED:
            return
        if state == StatementState.FAILED:
            msg = resp.status.error.message if resp.status.error else "Unknown error"
            raise RuntimeError(f"SQL execution failed: {msg}\nSQL: {sql}")
        raise RuntimeError(f"SQL statement ended in unexpected state {state}\nSQL: {sql}")

    def query(self, sql: str, *, timeout_seconds: int = 120) -> list[list[str]]:
        """Execute a SQL query and return rows as lists of strings.

        Polls for completion when the warehouse is cold-starting.
        """
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
            raise RuntimeError(f"SQL query returned no status\nSQL: {sql}")

        state = resp.status.state
        sid = resp.statement_id

        if state not in _TERMINAL_STATES and sid:
            state = self._wait_for_completion(sid, timeout_seconds)
            if state == StatementState.SUCCEEDED and sid:
                resp = self._ws.statement_execution.get_statement(sid)

        if state == StatementState.FAILED:
            msg = resp.status.error.message if resp.status and resp.status.error else "Unknown error"
            raise RuntimeError(f"SQL query failed: {msg}\nSQL: {sql}")
        if state != StatementState.SUCCEEDED:
            raise RuntimeError(f"SQL query ended in unexpected state {state}\nSQL: {sql}")

        if resp.result and resp.result.data_array:
            return resp.result.data_array
        return []

    def query_dicts(self, sql: str, *, timeout_seconds: int = 120) -> list[dict[str, str | None]]:
        """Execute a SQL query and return rows as column-name-keyed dicts.

        Column names are extracted from the response manifest.
        """
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
            raise RuntimeError(f"SQL query returned no status\nSQL: {sql}")

        state = resp.status.state
        sid = resp.statement_id

        if state not in _TERMINAL_STATES and sid:
            state = self._wait_for_completion(sid, timeout_seconds)
            if state == StatementState.SUCCEEDED and sid:
                resp = self._ws.statement_execution.get_statement(sid)

        if state == StatementState.FAILED:
            msg = resp.status.error.message if resp.status and resp.status.error else "Unknown error"
            raise RuntimeError(f"SQL query failed: {msg}\nSQL: {sql}")
        if state != StatementState.SUCCEEDED:
            raise RuntimeError(f"SQL query ended in unexpected state {state}\nSQL: {sql}")

        if not resp.result or not resp.result.data_array:
            return []

        columns = [
            col.name or ""
            for col in ((resp.manifest.schema.columns if resp.manifest and resp.manifest.schema else None) or [])
        ]
        return [dict(zip(columns, row)) for row in resp.result.data_array]

    def _wait_for_completion(self, statement_id: str, timeout_seconds: int) -> StatementState:
        """Poll statement status until it reaches a terminal state."""
        start = time.time()
        poll_interval = 2.0
        while time.time() - start < timeout_seconds:
            status = self._ws.statement_execution.get_statement(statement_id)
            state = status.status.state if status.status else None
            if state in _TERMINAL_STATES:
                return state  # type: ignore[return-value]
            time.sleep(poll_interval)
        raise RuntimeError(f"SQL statement {statement_id} timed out after {timeout_seconds}s")
