"""Centralised SQL Statement Execution API wrapper.

Every service that needs to run SQL against a Databricks SQL Warehouse
should accept a ``SqlExecutor`` instance instead of constructing its own
``execute_statement`` calls.  This eliminates boilerplate duplication and
makes services testable via ``create_autospec(SqlExecutor)``.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Protocol, runtime_checkable

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Disposition, Format, StatementState

from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# OLTP executor protocol — the shared surface SqlExecutor and PgExecutor
# both implement.
# ---------------------------------------------------------------------------


@runtime_checkable
class OltpExecutorProtocol(Protocol):
    """Structural surface shared by Delta (:class:`SqlExecutor`) and Postgres
    (``backend.pg_executor.PgExecutor``) executors used by OLTP services.

    Why this exists
    ---------------
    The previous design used ``Union[SqlExecutor, PgExecutor]`` plus
    runtime ``cast(SqlExecutor, pg)`` calls at every OLTP boundary so
    services with ``sql: SqlExecutor`` signatures kept typechecking
    even though a :class:`PgExecutor` flowed through them at runtime.
    The casts silently muted the type-checker for every Postgres path
    — a method removed from :class:`PgExecutor` (or renamed only on
    one side) wouldn't surface until production.

    A structural Protocol replaces both:

    - Services and the scheduler annotate their executor parameter as
      :class:`OltpExecutorProtocol`. basedpyright then verifies every
      ``self._sql.foo()`` call exists on the Protocol — and therefore
      on both concrete classes — without any cast.
    - :func:`backend.dependencies.get_sp_oltp_executor` returns the
      Protocol so the FastAPI ``Depends`` graph stays type-honest.
    - :class:`backend.services.scheduler_service.SchedulerService`
      stores it as ``self._oltp_sql: OltpExecutorProtocol`` and drops
      the ``cast(SqlExecutor, oltp_sql)`` line that previously hid the
      Postgres path from type-checking.

    What the surface includes
    -------------------------
    The Protocol intentionally captures only what OLTP services use.
    Warehouse-specific affordances (``warehouse_id``, ``connection()``,
    ``close()``) stay on the concrete classes — services that need
    them keep their concrete-class annotations.

    ``@runtime_checkable``
    ----------------------
    Enables ``isinstance(executor, OltpExecutorProtocol)`` for tests
    and adapters. Cheap to add; note that the runtime check verifies
    method *presence* only, not signature compatibility.
    """

    # Routing identifier. ``"delta"`` on :class:`SqlExecutor`,
    # ``"postgres"`` on :class:`PgExecutor`. Services that need
    # dialect-specific SQL branch on this.
    dialect: str

    @property
    def catalog(self) -> str:
        """The catalog/database the executor operates against.

        Delta: Unity Catalog name. Postgres: the logical database
        (``PgExecutor.catalog`` aliases ``database`` so portable code
        works on both backends).
        """
        ...

    @property
    def schema(self) -> str:
        """The schema the executor operates against."""
        ...

    def fqn(self, table: str) -> str:
        """Build a backend-specific fully-qualified path for *table*.

        Three parts on Delta (``catalog.schema.table``), two on
        Postgres (``schema.table``). Always prefer this over
        hand-rolled ``f"{x.catalog}.{x.schema}.{t}"`` so the dialect
        branch stays in one place.
        """
        ...

    def q(self, identifier: str) -> str:
        """Dialect-aware identifier quoter (backticks on Delta, ANSI ``"`` on Postgres).

        Doubles the internal quoting character so reserved words and
        identifiers containing the dialect's quote character (e.g.
        a hyphenated UC catalog like ``team-data``) survive
        interpolation into DDL.
        """
        ...

    def json_literal_expr(self, json_str: str) -> str:
        """Render *json_str* as a backend-native JSON literal expression.

        Delta wraps with :func:`parse_json`; Postgres uses ``::jsonb``.
        Always returns a safe-to-inline expression (escaping baked in).
        """
        ...

    def ts_text(self, col: str) -> str:
        """Project a timestamp column as an ISO-string-safe expression.

        Delta wraps in ``CAST(... AS STRING)``; Postgres returns the
        column verbatim because :func:`backend.pg_executor._to_text`
        ISO-formats it during row coercion.
        """
        ...

    def execute(self, sql: str, *, timeout_seconds: int = 120) -> None:
        """Run a non-returning statement and commit it."""

    def execute_no_schema(self, sql: str) -> None:
        """Run DDL with catalog-only context (no default schema).

        Used for bootstrap DDL that must run before the target schema
        exists. On Postgres this delegates to :meth:`execute`.
        """

    def query(self, sql: str, *, timeout_seconds: int = 120) -> list[list[str]]:
        """Run a query and return rows as lists of stringified cells.

        Cells are strings (or ``None`` for NULL) to mirror the
        Statement Execution API's ``JSON_ARRAY`` serialisation, so
        services that ``json.loads`` JSON columns work on both
        backends without dialect branches.
        """
        ...

    def query_dicts(self, sql: str, *, timeout_seconds: int = 120) -> list[dict[str, str | None]]:
        """Like :meth:`query` but rows are column-name-keyed dicts."""
        ...

    def upsert(
        self,
        table: str,
        key_cols: dict[str, Any],
        value_cols: dict[str, Any],
        *,
        timeout_seconds: int = 120,
    ) -> None:
        """Idempotently insert-or-update a single row keyed by *key_cols*.

        Delta uses ``MERGE INTO``; Postgres uses
        ``INSERT ... ON CONFLICT ... DO UPDATE``. The signature is
        identical so services don't need a dialect branch.
        """

    def upsert_with_audit(
        self,
        table: str,
        key_cols: dict[str, Any],
        value_cols: dict[str, Any],
        *,
        preserve_created: bool = True,
        increment_on_update: str | None = None,
        timeout_seconds: int = 120,
    ) -> None:
        """Audit-aware variant of :meth:`upsert` for tables with audit columns.

        Exists so service code stops branching on ``self._sql.dialect``
        to write ``MERGE INTO ...`` (Delta) or ``INSERT ... ON CONFLICT
        ... DO UPDATE`` (Postgres) by hand. The two patterns are
        equivalent — both upsert with audit semantics — so the dialect
        choice lives on the executor, not on every caller.

        Behaves like :meth:`upsert` (single-row INSERT-or-UPDATE keyed
        by *key_cols*) with two added knobs:

        - ``preserve_created`` (default True): any column in
          *value_cols* whose name starts with ``created_`` is set on
          INSERT but NOT touched on UPDATE. Mirrors the universal
          audit convention: ``created_by`` and ``created_at`` are
          immutable; ``updated_by`` / ``updated_at`` move on every
          write.
        - ``increment_on_update``: a column name. The column's
          *initial value* on INSERT comes from *value_cols* (use
          ``{"version": 1}`` for the conventional starting point);
          on UPDATE the executor rewrites that column to ``existing
          + 1`` using the dialect-appropriate self-reference syntax
          (``target.col + 1`` on Delta MERGE, bare ``col + 1`` on
          Postgres ON CONFLICT). Use for monotonic version counters
          on history-tracked rows.
        """

    def select_json_text(self, col: str) -> str:
        """Project a JSON-typed column as JSON-text on this backend.

        Delta's VARIANT column comes back as a non-string object via
        the JSON_ARRAY response format, so callers that want to
        ``json.loads`` need a ``to_json(col)`` wrapper. Postgres'
        JSONB cells are coerced to JSON strings by
        :func:`backend.pg_executor._to_text` during row conversion,
        so the same projection just selects the column verbatim.

        Always prefer this over hand-writing ``to_json(col)`` —
        scattering the wrapper across service code reintroduces
        the dialect branch the executor is supposed to absorb.
        """
        ...

    def interval_days_expr(self, days: int) -> str:
        """Render an INTERVAL literal equivalent to *days* days.

        Composes into DATE/TIMESTAMP arithmetic — e.g.
        ``CURRENT_TIMESTAMP - executor.interval_days_expr(90)`` is
        "ninety days ago" on both backends.

        Delta speaks ``INTERVAL 90 DAY`` (no quotes, uppercase
        singular); Postgres speaks ``INTERVAL '90 days'`` (single-
        quoted literal, lowercase plural). The two are mutually
        unparseable — Postgres throws ``syntax error at or near
        "DAY"`` on the Delta form, and Delta throws on the quoted
        form — so callers must never hand-build either.
        """
        ...


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
        quotes (``"check"``).  Always quote columns/tables/catalogs/
        schemas that share a name with a reserved word (``check``,
        ``user``, ``order``…) **or** that contain characters outside
        ``[A-Za-z0-9_]`` — Databricks catalog names in particular are
        commonly hyphenated (``prod-east``, ``team-data-platform``)
        and would otherwise emit a parse error when interpolated raw
        into DDL.

        Internal backticks in *identifier* are doubled per Databricks
        SQL convention so a name like ``` weird`name ``` becomes
        ``` `weird``name` ``` rather than producing two adjacent
        terminators. This mirrors :meth:`PgExecutor.q` which doubles
        internal double-quotes for the same reason.
        """
        return "`" + identifier.replace("`", "``") + "`"

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

        # Every column-name position is routed through :meth:`q` so a
        # future audit column whose name is a Delta reserved word
        # (``check``, ``order``, ``group``, …) survives interpolation
        # into the MERGE. Mirrors the same quoting strategy that
        # :meth:`PgExecutor.upsert` already applies on the Postgres
        # side so the two backends stay symmetric — without this the
        # Postgres path accepts a reserved-word column and Delta
        # raises a parse error.
        on_clause = " AND ".join(f"target.{self.q(k)} = source.{self.q(k)}" for k in key_cols)
        source_select = ", ".join(f"{_render_value(v)} AS {self.q(k)}" for k, v in key_cols.items())
        update_set = ", ".join(f"{self.q(k)} = {_render_value(v)}" for k, v in value_cols.items())
        all_cols = list(key_cols.keys()) + list(value_cols.keys())
        all_vals = [_render_value(v) for v in list(key_cols.values()) + list(value_cols.values())]
        insert_cols = ", ".join(self.q(c) for c in all_cols)
        insert_vals = ", ".join(all_vals)

        sql = (
            f"MERGE INTO {table} AS target "
            f"USING (SELECT {source_select}) AS source ON {on_clause} "
            f"WHEN MATCHED THEN UPDATE SET {update_set} "
            f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
        )
        self.execute(sql, timeout_seconds=timeout_seconds)

    def upsert_with_audit(
        self,
        table: str,
        key_cols: dict[str, Any],
        value_cols: dict[str, Any],
        *,
        preserve_created: bool = True,
        increment_on_update: str | None = None,
        timeout_seconds: int = 120,
    ) -> None:
        """Delta MERGE INTO with audit semantics.

        See :meth:`OltpExecutorProtocol.upsert_with_audit` for the
        contract. ``target`` is the alias used in the MERGE so the
        increment self-reference renders as ``target.<col> + 1``.
        """
        if not key_cols:
            raise ValueError("upsert_with_audit requires at least one key column")
        if increment_on_update is not None and increment_on_update not in value_cols:
            raise ValueError(
                f"increment_on_update={increment_on_update!r} must be present in value_cols "
                "with its initial INSERT value (e.g. {'version': 1})"
            )

        # Every column-name position is routed through :meth:`q` so a
        # future audit column whose name is a Delta reserved word
        # (``check``, ``order``, ``group``, …) survives interpolation
        # into the MERGE. Mirrors the same quoting strategy that
        # :meth:`PgExecutor.upsert_with_audit` already applies on the
        # Postgres side so the two backends stay symmetric — without
        # this the Postgres path accepts a reserved-word audit column
        # and Delta raises a parse error.
        on_clause = " AND ".join(f"target.{self.q(k)} = source.{self.q(k)}" for k in key_cols)
        source_select = ", ".join(f"{_render_value(v)} AS {self.q(k)}" for k, v in key_cols.items())

        # UPDATE SET excludes created_* columns when preserve_created;
        # the increment column (if any) gets the dialect-specific
        # self-reference form instead of the literal value.
        update_pairs: list[str] = []
        for col, val in value_cols.items():
            if preserve_created and col.startswith("created_"):
                continue
            qcol = self.q(col)
            if col == increment_on_update:
                # Delta MERGE — qualify with the ``target`` alias to
                # disambiguate from the source row.
                update_pairs.append(f"{qcol} = target.{qcol} + 1")
            else:
                update_pairs.append(f"{qcol} = {_render_value(val)}")
        update_set = ", ".join(update_pairs)

        all_cols = list(key_cols.keys()) + list(value_cols.keys())
        all_vals = [_render_value(v) for v in list(key_cols.values()) + list(value_cols.values())]
        insert_cols = ", ".join(self.q(c) for c in all_cols)
        insert_vals = ", ".join(all_vals)

        sql = (
            f"MERGE INTO {table} AS target "
            f"USING (SELECT {source_select}) AS source ON {on_clause} "
            f"WHEN MATCHED THEN UPDATE SET {update_set} "
            f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
        )
        self.execute(sql, timeout_seconds=timeout_seconds)

    def select_json_text(self, col: str) -> str:
        """Delta needs ``to_json`` to serialise VARIANT through JSON_ARRAY."""
        return f"to_json({col})"

    def interval_days_expr(self, days: int) -> str:
        """Delta interval literal — bare integer, uppercase ``DAY`` (singular)."""
        return f"INTERVAL {int(days)} DAY"

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
