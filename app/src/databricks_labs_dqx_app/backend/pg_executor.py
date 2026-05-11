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
import threading
import time
import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from databricks.sdk import WorkspaceClient
from psycopg_pool import ConnectionPool

from databricks_labs_dqx_app.backend.sql_executor import RawSql, _render_value
from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

logger = logging.getLogger(__name__)


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

        # Bootstrap the first token before the pool starts so the very
        # first connection has valid credentials.
        self._token_holder = _TokenHolder(_generate_token(ws, instance_name))

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

    def execute(self, sql: str, *, timeout_seconds: int = 120) -> None:  # noqa: ARG002 - parity with SqlExecutor
        """Run a non-result-returning statement."""
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                # psycopg accepts ``str`` at runtime (``Query`` is a
                # union of bytes/str/Composable/SQL); the published
                # stubs only declare the ``Template`` overload so we
                # silence basedpyright here.
                cur.execute(sql)  # pyright: ignore[reportCallIssue, reportArgumentType]
            conn.commit()

    def execute_no_schema(self, sql: str) -> None:
        """Parity stub. Postgres has no per-statement catalog/schema context.

        Schemas are created via ``CREATE SCHEMA IF NOT EXISTS`` like any
        other DDL, so we simply delegate to :meth:`execute`.
        """
        self.execute(sql)

    def query(self, sql: str, *, timeout_seconds: int = 120) -> list[list[str]]:  # noqa: ARG002
        """Run a query and return rows as lists of strings (Delta-compatible).

        The return type is annotated as ``list[list[str]]`` to mirror
        :meth:`SqlExecutor.query`, but at runtime NULL cells surface
        as ``None`` (just like the JSON_ARRAY response format used by
        the Statement Execution API).  Services already handle both
        — e.g. ``int(row[0]) if row[0] else 0`` — so we keep the
        Optional shape rather than coercing NULL to ``""``.
        """
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)  # pyright: ignore[reportCallIssue, reportArgumentType]
                rows = cur.fetchall()
        return [[_to_text(cell) for cell in row] for row in rows]  # pyright: ignore[reportReturnType]

    def query_dicts(self, sql: str, *, timeout_seconds: int = 120) -> list[dict[str, str | None]]:  # noqa: ARG002
        """Run a query and return rows as ``{column: stringified value}`` dicts."""
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)  # pyright: ignore[reportCallIssue, reportArgumentType]
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

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        self._stop.set()
        try:
            self._pool.close()
        except Exception:  # noqa: BLE001 - best-effort shutdown
            logger.warning("Error closing Lakebase pool", exc_info=True)

    def _token_refresh_loop(self) -> None:
        """Background thread that rotates the Lakebase OAuth token.

        Runs every ``token_refresh_minutes`` and updates both the
        shared :class:`_TokenHolder` and the pool's ``kwargs`` dict so
        the very next physical connect uses the fresh credential.
        Existing connections keep working until ``max_lifetime``
        recycles them — Postgres only validates the password during
        the SCRAM handshake, not on every query.
        """
        while not self._stop.is_set():
            # Wait first so we don't immediately re-issue a token after
            # init.
            if self._stop.wait(self._token_refresh_seconds):
                return
            try:
                fresh = _generate_token(self._ws, self._instance_name)
                self._token_holder.token = fresh
                # Mutating the same dict the pool was constructed with
                # is the supported way to inject rotating credentials
                # (psycopg-pool re-reads ``kwargs`` on every connect).
                self._connect_kwargs["password"] = fresh
                logger.info("Lakebase OAuth token refreshed")
            except Exception:  # noqa: BLE001
                # Don't crash the app on a transient SDK failure — the
                # next iteration retries and existing connections keep
                # working until the previous token expires.
                logger.warning("Failed to refresh Lakebase token; will retry", exc_info=True)
                # Back off a bit so we don't tight-loop on persistent
                # failures.
                time.sleep(60)


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
    pool_min_size: int = 1,
    pool_max_size: int = 10,
) -> PgExecutor:
    """Construct a :class:`PgExecutor` from a Databricks workspace client.

    Resolves the instance's read/write DNS endpoint and the calling
    identity's username (service principal in production, real user
    locally) before opening the pool.
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
        pool_min_size=pool_min_size,
        pool_max_size=pool_max_size,
    )
