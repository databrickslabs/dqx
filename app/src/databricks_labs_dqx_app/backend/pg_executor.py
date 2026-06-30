"""Lakebase Postgres executor.

Mirrors the public surface of :class:`SqlExecutor` so that services can
target either Delta (via SQL warehouse) or Lakebase (via psycopg) with
the same call signatures.

Why a separate class instead of a generic SQLAlchemy abstraction?
We need extremely tight control over:

- **OAuth token refresh.** Lakebase tokens expire after one hour. A
  background daemon thread refreshes the password every
  ``DQX_LAKEBASE_TOKEN_REFRESH_MINUTES`` minutes and the connection
  pool's ``configure`` callback hands new connections the latest
  password. Existing connections in the pool are recycled when they
  exceed ``max_lifetime``.
- **String-typed result rows.** The legacy :class:`SqlExecutor` returns
  ``list[list[str]]`` because the Statement Execution API serialises
  cells via ``Format.JSON_ARRAY``.  Service code expects to call
  ``json.loads`` on JSON columns and to receive ISO-string timestamps.
  We coerce psycopg's natively-typed results to that shape so existing
  services work unchanged.
- **Dialect helpers.** :meth:`q` and :meth:`json_literal_expr` produce
  Postgres-flavoured identifiers/literals so portable service SQL
  doesn't need a dialect branch.
"""

from __future__ import annotations

import json
import logging
import os
import random
import threading
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

from databricks.sdk import WorkspaceClient
from psycopg import Connection, Cursor
from psycopg_pool import ConnectionPool

# ``run_trusted_sql`` / ``run_parameterized_sql`` live in the
# psycopg-free :mod:`backend.pg_cursor_helpers` module so callers
# that ONLY need the trust-boundary wrappers (notably
# :mod:`backend.migrations.postgres`) can be imported in environments
# that don't have psycopg installed. We re-export them here so existing
# imports of the form ``from backend.pg_executor import run_trusted_sql``
# keep working — the executor is the "real" psycopg consumer and the
# natural place to look for them.
from databricks_labs_dqx_app.backend.pg_cursor_helpers import (
    run_parameterized_sql,
    run_trusted_sql,
)
from databricks_labs_dqx_app.backend.sql_executor import RawSql, _render_value
from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

logger = logging.getLogger(__name__)

# Re-exports so ``from backend.pg_executor import run_trusted_sql`` keeps
# working after the helpers moved to ``backend.pg_cursor_helpers``.
__all__ = [
    "PgExecutor",
    "build_pg_executor",
    "run_parameterized_sql",
    "run_trusted_sql",
]


class _TokenHolder:
    """Thread-safe container for the rotating Lakebase OAuth token.

    The connection pool's ``configure`` callback (called when each new
    physical connection is opened) reads :attr:`token` so the *next*
    connection always picks up the latest credential.  Existing
    connections continue to work — Postgres only validates the
    password during the SCRAM handshake, not on every query.
    """

    def __init__(self, token: str) -> None:
        self._token = token
        self._lock = threading.Lock()

    @property
    def token(self) -> str:
        with self._lock:
            return self._token

    @token.setter
    def token(self, value: str) -> None:
        with self._lock:
            self._token = value


def _generate_token(ws: WorkspaceClient, instance_name: str) -> str:
    """Generate a fresh Lakebase OAuth token (1-hour TTL)."""
    cred = ws.database.generate_database_credential(
        request_id=str(uuid.uuid4()),
        instance_names=[instance_name],
    )
    if not cred.token:
        raise RuntimeError(f"Lakebase credential response had no token (instance={instance_name})")
    return cred.token


def _to_text(value: Any) -> str | None:
    """Coerce a psycopg cell value to Delta-compatible string output.

    - ``None`` stays ``None``.
    - ``dict``/``list`` → compact JSON (matching Delta's ``to_json``).
    - ``datetime``/``date`` → ISO 8601.
    - ``bool`` → ``"true"``/``"false"`` (lowercase, JSON-style).
    - ``Decimal``/``int``/``float`` → ``str(value)``.
    - Everything else → ``str(value)``.
    """
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value, separators=(",", ":"))
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        # Binary columns are not used in any OLTP table today; surface
        # them as hex if a future migration introduces one.
        return bytes(value).hex()
    return str(value)


def _pg_render_value(value: Any) -> str:
    """Postgres-flavoured literal renderer for :meth:`PgExecutor.upsert`.

    Behaves identically to :func:`backend.sql_executor._render_value`
    except that:

    - :class:`RawSql("current_timestamp()")` is rewritten to
      ``CURRENT_TIMESTAMP`` because Postgres rejects the parenthesised
      Spark SQL form.  Other ``RawSql`` payloads pass through verbatim
      so callers can still inject Postgres-specific helpers like
      ``now()`` or ``::jsonb`` casts.
    - ``bool`` renders as ``TRUE``/``FALSE`` which Postgres accepts.
    """
    if isinstance(value, RawSql):
        expr = value.expr.strip()
        # Common Spark idiom that doesn't parse in Postgres — translate.
        if expr.lower() in {"current_timestamp()", "now()"}:
            return "CURRENT_TIMESTAMP"
        return value.expr
    return _render_value(value)


class PgExecutor:
    """Drop-in :class:`SqlExecutor` replacement backed by Lakebase Postgres.

    Constructed at app startup when ``conf.lakebase_enabled`` is true;
    the lifespan handler kicks off the token-refresh thread and runs
    the Postgres migrations once before traffic arrives.
    """

    dialect: str = "postgres"

    # Alias applied to the conflict target in :meth:`upsert_with_audit` so a
    # self-referencing increment can address the existing row without a
    # schema-qualified reference (which Postgres rejects in DO UPDATE SET).
    # Deliberately prefixed to avoid colliding with any real relation name.
    _UPSERT_TARGET_ALIAS: str = "dqx_upsert_target"

    def __init__(
        self,
        *,
        ws: WorkspaceClient,
        instance_name: str,
        database: str,
        schema: str,
        username: str,
        host: str,
        port: int = 5432,
        token_refresh_minutes: int = 50,
        token_refresh_retry_seconds: int = 10,
        token_refresh_retry_jitter: float = 0.3,
        token_refresh_max_failures: int = 12,
        pool_min_size: int = 1,
        pool_max_size: int = 10,
    ) -> None:
        self._ws = ws
        self._instance_name = instance_name
        self._database = database
        self._schema = schema
        self._username = username
        self._host = host
        self._port = port
        self._token_refresh_seconds = max(60, token_refresh_minutes * 60)
        # Retry tuning: clamp to sensible floors so a mis-configured
        # env var can't degenerate into a busy-loop or an unbounded
        # failure window. ``retry_seconds`` floors at 1s (anything
        # below that is effectively a tight loop against the SDK);
        # ``jitter`` clamps to ``[0, 1]`` so the spread can't go
        # negative or larger than ±100%; ``max_failures`` floors at
        # 1 so we always escalate on at least one failure rather than
        # spinning forever.
        self._token_refresh_retry_seconds = max(1, int(token_refresh_retry_seconds))
        self._token_refresh_retry_jitter = max(0.0, min(1.0, float(token_refresh_retry_jitter)))
        self._token_refresh_max_failures = max(1, int(token_refresh_max_failures))

        # Bootstrap the first token before the pool starts so the very
        # first connection has valid credentials. The bootstrap mint
        # counts as a successful refresh — the metric / counter start
        # in the "healthy" state, not a transient "never refreshed"
        # one that would confuse a health endpoint at t=0.
        self._token_holder = _TokenHolder(_generate_token(ws, instance_name))
        self._last_successful_refresh_at: datetime | None = datetime.now(timezone.utc)
        self._consecutive_refresh_failures: int = 0

        # ``kwargs`` is the dict the pool hands to ``Connection.connect``
        # every time it opens a new physical connection.  Mutating
        # ``password`` on token refresh means subsequent connects pick
        # up the fresh credential without restarting the pool. The
        # ``options`` flag sets the Postgres ``search_path`` so
        # unqualified table references resolve to the app schema.
        self._connect_kwargs: dict[str, Any] = {
            "host": host,
            "port": port,
            "dbname": database,
            "user": username,
            "password": self._token_holder.token,
            "sslmode": "require",
            "options": f"-c search_path={schema}",
        }

        # ``max_lifetime`` recycles connections every 50 minutes which
        # ensures we never hand out a connection authenticated with a
        # near-expired token. ``check`` runs ``SELECT 1`` on idle pool
        # members so a server-side disconnect doesn't poison the pool.
        self._pool: ConnectionPool = ConnectionPool(
            conninfo="",
            min_size=pool_min_size,
            max_size=pool_max_size,
            max_lifetime=self._token_refresh_seconds,
            check=ConnectionPool.check_connection,
            open=False,  # opened explicitly below so failures surface eagerly
            kwargs=self._connect_kwargs,
            timeout=30.0,
            name="dqx-lakebase",
        )
        self._pool.open(wait=True, timeout=30.0)
        logger.info(
            "Lakebase connection pool open (host=%s db=%s schema=%s user=%s)",
            host,
            database,
            schema,
            username,
        )

        self._stop = threading.Event()
        self._refresher = threading.Thread(
            target=self._token_refresh_loop,
            name="dqx-lakebase-token-refresh",
            daemon=True,
        )
        self._refresher.start()

    # ------------------------------------------------------------------
    # Public API mirrors SqlExecutor
    # ------------------------------------------------------------------

    @property
    def warehouse_id(self) -> str:
        # Kept for type compatibility with ``SqlExecutor``; Lakebase
        # has no warehouse concept.
        return ""

    @property
    def catalog(self) -> str:
        # Postgres has no Unity Catalog; return the database name so
        # callers that build fully-qualified identifiers still get a
        # 3-part name (``database.schema.table``) on Postgres.
        return self._database

    @property
    def schema(self) -> str:
        return self._schema

    @property
    def database(self) -> str:
        return self._database

    # ------------------------------------------------------------------
    # Token-refresh observability
    # ------------------------------------------------------------------
    # These two properties surface the refresh daemon's state so
    # operators can wire a health endpoint / metric exporter to it
    # WITHOUT poking at private attributes. The refresh loop updates
    # them under no extra lock — they are coarse-grained gauges, not
    # transactional state, so a single torn read at the boundary is
    # harmless (worst case: a health check sees a stale value for
    # one scrape interval and corrects on the next).

    @property
    def last_successful_refresh_at(self) -> datetime | None:
        """Wall-clock time of the last successful token refresh, UTC.

        Set to ``datetime.now(UTC)`` in :meth:`__init__` because the
        bootstrap token mint IS a successful refresh from the pool's
        perspective. Operators monitoring this metric should alert
        if ``now() - last_successful_refresh_at`` exceeds
        ``token_refresh_minutes`` by a meaningful margin (e.g. 2x)
        — that is the leading indicator of the silent-pool-drain
        failure mode this metric exists to prevent.
        """
        return self._last_successful_refresh_at

    @property
    def consecutive_refresh_failures(self) -> int:
        """Failed-refresh streak since the last success.

        Reset to 0 on every successful refresh. Reaching
        ``token_refresh_max_failures`` triggers the escalation path
        in :meth:`_token_refresh_loop` (process exit so the
        supervisor restarts the worker).
        """
        return self._consecutive_refresh_failures

    def fqn(self, table: str) -> str:
        """Return the schema-qualified path for *table*.

        Postgres only has one catalog per connection so we return
        ``schema.table``; :meth:`SqlExecutor.fqn` returns three parts.
        See :meth:`SqlExecutor.fqn` for the parity contract.
        """
        return f"{self._schema}.{table}"

    def q(self, identifier: str) -> str:
        """Quote a Postgres identifier (ANSI double quotes, doubled internal ``"``)."""
        return '"' + identifier.replace('"', '""') + '"'

    def json_literal_expr(self, json_str: str) -> str:
        """Return a Postgres expression that yields a JSONB value for *json_str*."""
        return f"'{escape_sql_string(json_str)}'::jsonb"

    def ts_text(self, col: str) -> str:
        """Project a timestamp column as a string.

        Postgres TIMESTAMPTZ values are converted to ISO strings by
        :func:`_to_text` when the row leaves the cursor, so we just
        select the column verbatim and let the row-level coercion do
        the work.  This keeps service SQL portable: callers always
        write ``executor.ts_text('created_at')`` regardless of dialect.
        """
        return col

    @contextmanager
    def connection(self) -> Iterator[Connection]:
        """Yield a pooled connection for multi-statement transactional work.

        Use this when several statements MUST be atomic — e.g. DDL + a
        bookkeeping INSERT that record their joint application. The
        caller is responsible for ``conn.commit()``; if the ``with``
        block exits without committing (exception or otherwise) the
        connection is rolled back and returned to the pool by psycopg-
        pool. Single one-shot statements should keep using
        :meth:`execute`, which commits per call.

        **Statement timeouts are the caller's responsibility here.**
        Unlike :meth:`execute` / :meth:`query` / :meth:`query_dicts`,
        this method does NOT auto-emit ``SET LOCAL statement_timeout``
        because callers in this branch are running multi-statement
        units of work — e.g. the migration runner applying a series
        of DDLs whose appropriate timeout depends on the work being
        done (a 30s app query vs. a 10-minute index rebuild). Callers
        that want a guard should issue ``SET LOCAL statement_timeout
        = <ms>`` themselves inside the transaction (routed through
        :func:`run_trusted_sql` for the LiteralString cast).
        """
        with self._pool.connection() as conn:
            yield conn

    @staticmethod
    def _apply_statement_timeout(cur: Cursor[Any], timeout_seconds: int) -> None:
        """Cap the next statement on ``cur`` at ``timeout_seconds`` via
        Postgres' ``statement_timeout`` GUC.

        Without this, a misbehaving query (accidental cross join, missing
        predicate, a server-side function that hangs on a lock) would pin
        the pool connection forever and eventually starve the pool —
        every subsequent caller would block on ``ConnectionPool.getconn``
        instead of failing fast. The legacy :class:`SqlExecutor` got this
        for free via warehouse-side polling deadlines; Postgres needs us
        to opt in explicitly per statement.

        Why ``SET LOCAL`` rather than ``SET``:
            ``SET LOCAL`` scopes the GUC to the current transaction and
            is reset on COMMIT / ROLLBACK. Pool connections are reused
            across callers, so a plain ``SET`` would silently leak the
            most recent caller's timeout into the next caller's
            statements — a 1-second timeout set by a healthcheck would
            kill a 60-second migration on the next checkout. ``SET
            LOCAL`` is the only correct primitive for a pooled
            executor.

        psycopg3 starts an implicit transaction on the first statement
        issued through a cursor (autocommit defaults to ``False``), so
        firing this helper as the first ``cur.execute`` call BEFORE
        the user SQL puts both statements in the same transaction and
        the ``LOCAL`` scope covers the user SQL correctly.

        The ``timeout_seconds * 1000`` is built from Python ``int``
        arithmetic — no user input flows in — which is why
        :func:`run_trusted_sql` can safely cast it to
        :class:`LiteralString`.
        """
        ms = max(1, int(timeout_seconds) * 1000)
        run_trusted_sql(cur, f"SET LOCAL statement_timeout = {ms}")

    def execute(self, sql: str, *, timeout_seconds: int = 120) -> None:
        """Run a single non-result-returning statement and commit it.

        ``timeout_seconds`` is enforced via
        :meth:`_apply_statement_timeout` — a runaway query gets
        cancelled rather than pinning a pool connection forever.
        """
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                self._apply_statement_timeout(cur, timeout_seconds)
                run_trusted_sql(cur, sql)
            conn.commit()

    def execute_no_schema(self, sql: str) -> None:
        """Parity stub. Postgres has no per-statement catalog/schema context.

        Schemas are created via ``CREATE SCHEMA IF NOT EXISTS`` like any
        other DDL, so we simply delegate to :meth:`execute`.
        """
        self.execute(sql)

    def query(self, sql: str, *, timeout_seconds: int = 120) -> list[list[str]]:
        """Run a query and return rows as lists of strings (Delta-compatible).

        The return type is annotated as ``list[list[str]]`` to mirror
        :meth:`SqlExecutor.query`, but at runtime NULL cells surface
        as ``None`` (just like the JSON_ARRAY response format used by
        the Statement Execution API).  Services already handle both
        — e.g. ``int(row[0]) if row[0] else 0`` — so we keep the
        Optional shape rather than coercing NULL to ``""``.

        ``timeout_seconds`` is enforced via
        :meth:`_apply_statement_timeout`.
        """
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                self._apply_statement_timeout(cur, timeout_seconds)
                run_trusted_sql(cur, sql)
                rows = cur.fetchall()
        return [[_to_text(cell) for cell in row] for row in rows]  # pyright: ignore[reportReturnType]

    def query_dicts(self, sql: str, *, timeout_seconds: int = 120) -> list[dict[str, str | None]]:
        """Run a query and return rows as ``{column: stringified value}`` dicts.

        ``timeout_seconds`` is enforced via
        :meth:`_apply_statement_timeout`.
        """
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                self._apply_statement_timeout(cur, timeout_seconds)
                run_trusted_sql(cur, sql)
                rows = cur.fetchall()
                cols = [d.name for d in (cur.description or [])]
        return [{col: _to_text(cell) for col, cell in zip(cols, row)} for row in rows]

    def upsert(
        self,
        table: str,
        key_cols: dict[str, Any],
        value_cols: dict[str, Any],
        *,
        timeout_seconds: int = 120,
    ) -> None:
        """Postgres ``INSERT ... ON CONFLICT ... DO UPDATE`` upsert.

        Keeps the same call shape as :meth:`SqlExecutor.upsert` so a
        service can switch backends without code changes.  The natural
        key composing ``key_cols`` MUST have a UNIQUE/PRIMARY KEY index
        in the migration.
        """
        if not key_cols:
            raise ValueError("upsert requires at least one key column")

        all_cols = list(key_cols.keys()) + list(value_cols.keys())
        all_vals = [_pg_render_value(v) for v in list(key_cols.values()) + list(value_cols.values())]

        # Natural-key columns get quoted via q() so reserved words like
        # ``check`` survive. Service-provided keys are already validated
        # in higher layers, but using q() also makes them dialect-safe.
        quoted_cols = [self.q(c) for c in all_cols]
        quoted_keys = [self.q(c) for c in key_cols]
        update_set = ", ".join(f"{self.q(c)} = EXCLUDED.{self.q(c)}" for c in value_cols)

        if value_cols:
            conflict_clause = f"ON CONFLICT ({', '.join(quoted_keys)}) DO UPDATE SET {update_set}"
        else:
            # Pure existence check — keys-only row, no update payload.
            conflict_clause = f"ON CONFLICT ({', '.join(quoted_keys)}) DO NOTHING"

        sql = f"INSERT INTO {table} ({', '.join(quoted_cols)}) " f"VALUES ({', '.join(all_vals)}) " f"{conflict_clause}"
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
        """Postgres ``INSERT ... ON CONFLICT ... DO UPDATE`` with audit semantics.

        See :meth:`backend.sql_executor.OltpExecutorProtocol.upsert_with_audit`
        for the contract. The increment self-reference must point at the
        *existing* row (not ``EXCLUDED``, which carries the proposed
        value). A bare column name (``"col" = "col" + 1``) is ambiguous
        when the same column also appears in ``EXCLUDED``, but a
        schema-qualified reference (``"dq"."tbl"."col"``) is **not** a
        valid existing-row reference inside ``DO UPDATE SET`` — Postgres
        resolves it as a FROM-clause entry and errors with "invalid
        reference to FROM-clause entry for table ...". We therefore alias
        the conflict target (``INSERT INTO <fqn> AS <alias>``, supported
        since the ON CONFLICT feature shipped in PG 9.5) and reference the
        alias, which resolves unambiguously to the existing row. Aliasing
        also sidesteps having to recover the bare relation name from the
        already-quoted FQN string.
        """
        if not key_cols:
            raise ValueError("upsert_with_audit requires at least one key column")
        if increment_on_update is not None and increment_on_update not in value_cols:
            raise ValueError(
                f"increment_on_update={increment_on_update!r} must be present in value_cols "
                "with its initial INSERT value (e.g. {'version': 1})"
            )

        all_cols = list(key_cols.keys()) + list(value_cols.keys())
        all_vals = [_pg_render_value(v) for v in list(key_cols.values()) + list(value_cols.values())]
        quoted_cols = [self.q(c) for c in all_cols]
        quoted_keys = [self.q(c) for c in key_cols]
        alias = self.q(self._UPSERT_TARGET_ALIAS)

        # DO UPDATE SET excludes created_* columns when preserve_created;
        # the increment column (if any) references the aliased conflict
        # target so it resolves to the existing row rather than EXCLUDED.
        update_pairs: list[str] = []
        needs_alias = False
        for col, val in value_cols.items():
            if preserve_created and col.startswith("created_"):
                continue
            qcol = self.q(col)
            if col == increment_on_update:
                # Alias-qualified reference resolves unambiguously to the
                # existing row — even when ``EXCLUDED`` also exposes the
                # same column name (which is always the case for
                # ``increment_on_update`` since it has to be in
                # ``value_cols``).
                update_pairs.append(f"{qcol} = {alias}.{qcol} + 1")
                needs_alias = True
            else:
                update_pairs.append(f"{qcol} = {_pg_render_value(val)}")

        if update_pairs:
            conflict_clause = f"ON CONFLICT ({', '.join(quoted_keys)}) DO UPDATE SET {', '.join(update_pairs)}"
        else:
            # All value_cols were created_* — INSERT-only with
            # existence semantics. Rare but valid (e.g. a pure
            # "create-if-missing" audit row).
            conflict_clause = f"ON CONFLICT ({', '.join(quoted_keys)}) DO NOTHING"

        # Only alias the target when a self-reference needs it, so the
        # non-increment path keeps its existing rendered shape.
        target = f"{table} AS {alias}" if needs_alias else table
        sql = (
            f"INSERT INTO {target} ({', '.join(quoted_cols)}) " f"VALUES ({', '.join(all_vals)}) " f"{conflict_clause}"
        )
        self.execute(sql, timeout_seconds=timeout_seconds)

    def select_json_text(self, col: str) -> str:
        """Postgres JSONB cells are coerced to text by :func:`_to_text` already."""
        return col

    def interval_days_expr(self, days: int) -> str:
        """Postgres interval literal — single-quoted, lowercase ``days`` (plural)."""
        return f"INTERVAL '{int(days)} days'"

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        self._stop.set()
        try:
            self._pool.close()
        except Exception:
            # Best-effort lifespan teardown. close() can fail for a wide
            # variety of reasons (pool-worker shutdown timing, libpq
            # socket close errors, in-flight queries that the pool
            # decides to abort) and the resilience contract here is
            # "this MUST NOT propagate" — we're already tearing the pool
            # down, and a raise would leak Postgres connections on every
            # restart flap and could hang the worker. Logged for
            # observability; ruff BLE001 is intentionally not enforced
            # here per the policy block in ``pyproject.toml``.
            logger.warning("Error closing Lakebase pool", exc_info=True)

    def _next_wait_seconds(self) -> float:
        """Pick the next sleep duration before the refresh loop wakes.

        Two modes:

        * **Healthy** (``consecutive_refresh_failures == 0``): wait the
          full scheduled interval (``token_refresh_seconds``) so we
          don't burn SDK quota on a working token.
        * **Failing** (counter > 0): wait the *retry* interval, jittered
          by ±``token_refresh_retry_jitter`` so multiple workers
          recovering from the same transient SDK blip don't thunder-
          herd against the credential endpoint on the same second.

        Why the distinction matters: the pool's ``max_lifetime`` is
        the same as ``token_refresh_seconds`` (50 min by default), so
        retrying only at the scheduled cadence after a failure would
        give the pool a 50-60 minute window to drain silently before
        the next refresh attempt. Retrying at 10s on failure keeps
        us responsive to transient failures and gives the escalation
        path (``max_failures``) a sane budget to fire inside.
        """
        if self._consecutive_refresh_failures == 0:
            return float(self._token_refresh_seconds)
        spread = self._token_refresh_retry_seconds * self._token_refresh_retry_jitter
        # Lower bound is 0 in principle but never below 0.1s; without
        # the floor a jitter-of-1.0 + retry-of-1s could degenerate
        # into a busy-loop on the few microseconds either side of 0.
        return max(
            0.1,
            random.uniform(  # noqa: S311 — jitter, not crypto
                self._token_refresh_retry_seconds - spread,
                self._token_refresh_retry_seconds + spread,
            ),
        )

    def _escalate_refresh_failure(self) -> None:
        """Last-resort hand-off to the process supervisor.

        Called when ``consecutive_refresh_failures`` reaches
        ``token_refresh_max_failures``. The default budget (12 ×
        ~10s ≈ 2 minutes) leaves the pool with comfortable headroom
        before ``max_lifetime`` would otherwise start expiring
        connections, so escalating loudly here is strictly better
        than continuing to retry silently while the pool drains.

        We log CRITICAL with full context and then exit the process
        via :func:`os._exit` (not ``sys.exit``: that raises
        :class:`SystemExit`, which a daemon thread swallows without
        affecting the main process). The supervisor — uvicorn in
        local dev, the Databricks Apps runtime in production — will
        restart the worker and the new process will mint a fresh
        token at startup, recovering automatically once the
        underlying SDK / network / OAuth issue clears.
        """
        logger.critical(
            "Lakebase token refresh failed %d times in a row; exiting so the "
            "supervisor restarts the worker (instance=%s, last_success=%s). "
            "Continuing would silently drain the pool when ``max_lifetime`` "
            "starts recycling connections without a valid replacement token.",
            self._consecutive_refresh_failures,
            self._instance_name,
            self._last_successful_refresh_at.isoformat() if self._last_successful_refresh_at else None,
        )
        # ``os._exit`` skips atexit / signal handlers so it cannot
        # deadlock on a shutdown path that itself depends on a
        # working Lakebase pool. The supervisor sees a non-zero exit
        # and restarts us — that's the contract.
        os._exit(1)  # noqa: PLW1514 - deliberate hard-exit; see docstring

    def _token_refresh_loop(self) -> None:
        """Background thread that rotates the Lakebase OAuth token.

        Updates both the shared :class:`_TokenHolder` and the pool's
        ``kwargs`` dict so the very next physical connect uses the
        fresh credential. Existing connections keep working until
        ``max_lifetime`` recycles them — Postgres only validates the
        password during the SCRAM handshake, not on every query.

        Sleep cadence is dynamic — see :meth:`_next_wait_seconds`:
        after a success we wait the full scheduled interval; after a
        failure we wait the short retry interval (jittered) so we
        recover from transient failures inside the pool's
        ``max_lifetime`` window rather than letting the pool drain.

        After ``token_refresh_max_failures`` consecutive failures we
        hand off to :meth:`_escalate_refresh_failure` instead of
        continuing to retry — losing the refresh loop is a process-
        level health event, not a transient warning.
        """
        while not self._stop.is_set():
            if self._stop.wait(self._next_wait_seconds()):
                return
            try:
                fresh = _generate_token(self._ws, self._instance_name)
                self._token_holder.token = fresh
                # Mutating the same dict the pool was constructed with
                # is the supported way to inject rotating credentials
                # (psycopg-pool re-reads ``kwargs`` on every connect).
                self._connect_kwargs["password"] = fresh
                self._last_successful_refresh_at = datetime.now(timezone.utc)
                self._consecutive_refresh_failures = 0
                logger.info("Lakebase OAuth token refreshed")
            except Exception:
                # Background-thread resilience: this daemon MUST NOT die.
                # Token issuance can fail for SDK reasons (Databricks API
                # rate limit, transient OAuth blip), network reasons
                # (DNS, transient connectivity), or workspace-side reasons
                # (the SP's permissions getting briefly invalidated mid-
                # rotate). If any of those killed the loop, the pool's
                # tokens would silently expire and every subsequent
                # connect would fail with confusing SCRAM errors. The
                # broad catch is the resilience contract — narrowing to
                # (DatabricksError, requests.RequestException, OSError)
                # cannot enumerate every plausible failure mode, and the
                # cost of missing one is a silently-dead refresher. Ruff
                # BLE001 is intentionally not enforced here per the
                # policy block in ``pyproject.toml``.
                self._consecutive_refresh_failures += 1
                logger.warning(
                    "Failed to refresh Lakebase token (attempt %d/%d); will retry",
                    self._consecutive_refresh_failures,
                    self._token_refresh_max_failures,
                    exc_info=True,
                )
                if self._consecutive_refresh_failures >= self._token_refresh_max_failures:
                    self._escalate_refresh_failure()
                    return  # only reached if _escalate is patched in tests


# ---------------------------------------------------------------------------
# Construction helper
# ---------------------------------------------------------------------------


def build_pg_executor(
    ws: WorkspaceClient,
    *,
    instance_name: str,
    database: str,
    schema: str,
    token_refresh_minutes: int = 50,
    token_refresh_retry_seconds: int = 10,
    token_refresh_retry_jitter: float = 0.3,
    token_refresh_max_failures: int = 12,
    pool_min_size: int = 1,
    pool_max_size: int = 10,
) -> PgExecutor:
    """Construct a :class:`PgExecutor` from a Databricks workspace client.

    Resolves the instance's read/write DNS endpoint and the calling
    identity's username (service principal in production, real user
    locally) before opening the pool. The token-refresh tuning
    kwargs are passed straight through to :class:`PgExecutor` — see
    its :meth:`__init__` and :meth:`_token_refresh_loop` for the
    full back-off / escalation contract.
    """
    instance = ws.database.get_database_instance(name=instance_name)
    host = instance.read_write_dns
    if not host:
        raise RuntimeError(
            f"Lakebase instance {instance_name!r} has no read_write_dns. " "Is it provisioned and running?"
        )

    me = ws.current_user.me()
    username = me.user_name or me.id or ""
    if not username:
        raise RuntimeError("Could not determine workspace identity for Lakebase connection")

    return PgExecutor(
        ws=ws,
        instance_name=instance_name,
        database=database,
        schema=schema,
        username=username,
        host=host,
        token_refresh_minutes=token_refresh_minutes,
        token_refresh_retry_seconds=token_refresh_retry_seconds,
        token_refresh_retry_jitter=token_refresh_retry_jitter,
        token_refresh_max_failures=token_refresh_max_failures,
        pool_min_size=pool_min_size,
        pool_max_size=pool_max_size,
    )
