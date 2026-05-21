"""Centralised SQL Statement Execution API wrapper.

Every service that needs to run SQL against a Databricks SQL Warehouse
should accept a ``SqlExecutor`` instance instead of constructing its own
``execute_statement`` calls.  This eliminates boilerplate duplication and
makes services testable via ``create_autospec(SqlExecutor)``.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Disposition, Format, StatementState

from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

logger = logging.getLogger(__name__)

_TERMINAL_STATES = {StatementState.SUCCEEDED, StatementState.FAILED, StatementState.CANCELED, StatementState.CLOSED}


# Sentinel value for raw SQL expressions in upsert() — anything wrapped with
# ``RawSql("current_timestamp()")`` is interpolated verbatim instead of being
# escaped as a string literal.
class RawSql:
    """Marker that wraps a raw SQL expression so :meth:`SqlExecutor.upsert` does not escape it."""

    __slots__ = ("expr",)

    def __init__(self, expr: str) -> None:
        self.expr = expr


def _render_value(value: Any) -> str:
    """Convert a Python value to a SQL literal or raw expression.

    Strings are wrapped in single quotes and ANSI-escaped (`'` → `''`).
    ``None`` becomes ``NULL``. Booleans are rendered as ``TRUE``/``FALSE``.
    Numerics pass through. :class:`RawSql` is returned verbatim — this is
    how callers inject ``current_timestamp()``, ``parse_json('...')``, etc.
    """
    if isinstance(value, RawSql):
        return value.expr
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return repr(value)
    return f"'{escape_sql_string(str(value))}'"


class SqlExecutor:
    """Thin wrapper around the Databricks Statement Execution API.

    Provides ``execute``, ``query``, ``query_dicts``, and
    ``execute_no_schema`` covering all usage patterns in the DQX Studio
    backend.

    The class also exposes a tiny dialect surface (:attr:`dialect`,
    :meth:`q`, :meth:`json_literal_expr`) so that services which can
    target either Delta or Lakebase Postgres can share the same SQL
    while staying portable.  See :class:`PgExecutor` for the Postgres
    implementation.
    """

    # Cheap identifier for routing decisions in shared service code.
    dialect: str = "delta"

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

    # ------------------------------------------------------------------
    # Dialect helpers — kept identical-named on every executor so callers
    # can hand-write portable SQL without an "if dialect" branch.
    # ------------------------------------------------------------------
    def fqn(self, table: str) -> str:
        """Return the fully-qualified path for *table* on this backend.

        Delta uses three-part names (``catalog.schema.table``) because
        Unity Catalog organises tables under a catalog. Postgres has
        only one catalog per connection so :class:`PgExecutor.fqn` drops
        the catalog component and returns ``schema.table``. Services
        that just need an addressable table identifier should call this
        instead of inlining ``f"{sql.catalog}.{sql.schema}.{table}"`` —
        the dialect branch lives here once instead of being repeated at
        every call site.
        """
        return f"{self._catalog}.{self._schema}.{table}"

    def q(self, identifier: str) -> str:
        """Quote an identifier for this dialect.

        Delta uses backticks (``` `check` ```); Postgres uses double
        quotes (``"check"``).  Always quote columns/tables that share
        a name with a reserved word — ``check``, ``user``, ``order``…
        """
        return f"`{identifier}`"

    def json_literal_expr(self, json_str: str) -> str:
        """Return the SQL expression that turns *json_str* into a JSON value.

        Delta uses :func:`parse_json`; Postgres uses ``::jsonb``. The
        returned expression is safe to inline into a larger statement
        as it already includes the proper escaping.
        """
        return f"parse_json('{escape_sql_string(json_str)}')"

    def ts_text(self, col: str) -> str:
        """Project a timestamp column as an ISO-formatted string.

        Delta wraps the column in ``CAST(... AS STRING)`` because the
        Statement Execution API otherwise returns timestamps as their
        Spark string repr (which uses a space separator instead of
        ``T``).  On Postgres the column is selected verbatim and the
        :class:`PgExecutor` row converter ISO-formats it on the way
        out.
        """
        return f"CAST({col} AS STRING)"

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

    def upsert(
        self,
        table: str,
        key_cols: dict[str, Any],
        value_cols: dict[str, Any],
        *,
        timeout_seconds: int = 120,
    ) -> None:
        """Idempotently insert-or-update one row identified by *key_cols*.

        Centralises the ``MERGE INTO ... USING (SELECT)`` pattern that
        otherwise gets duplicated across every service.  *key_cols* are
        the natural-key columns used in the ON clause; *value_cols* are
        the non-key columns set on update / inserted on miss.

        Each value can be one of:
        - ``str`` / ``int`` / ``float`` / ``bool`` / ``None`` — bound as
          a SQL literal (strings are ANSI-escaped).
        - :class:`RawSql` — interpolated verbatim, e.g.
          ``RawSql("current_timestamp()")``.

        The full row written on insert is the union of *key_cols* and
        *value_cols*.  Updates only touch *value_cols* (the keys are
        immutable by definition of the merge predicate).
        """
        if not key_cols:
            raise ValueError("upsert requires at least one key column")

        on_clause = " AND ".join(f"target.{k} = source.{k}" for k in key_cols)
        source_select = ", ".join(f"{_render_value(v)} AS {k}" for k, v in key_cols.items())
        update_set = ", ".join(f"{k} = {_render_value(v)}" for k, v in value_cols.items())
        all_cols = list(key_cols.keys()) + list(value_cols.keys())
        all_vals = [_render_value(v) for v in list(key_cols.values()) + list(value_cols.values())]
        insert_cols = ", ".join(all_cols)
        insert_vals = ", ".join(all_vals)

        sql = (
            f"MERGE INTO {table} AS target "
            f"USING (SELECT {source_select}) AS source ON {on_clause} "
            f"WHEN MATCHED THEN UPDATE SET {update_set} "
            f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
        )
        self.execute(sql, timeout_seconds=timeout_seconds)

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
