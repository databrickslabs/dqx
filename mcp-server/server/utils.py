"""
Utility functions for the DQX MCP server.

Key patterns:
- Pure ASGI middleware for OBO (not BaseHTTPMiddleware — avoids streaming timeouts)
- Extracts user identity from Databricks Apps proxy headers
- User OBO token creates temp views (UC governance) and runs direct SQL
- App SP submits wheel-task jobs (run as a dedicated runner SP) that read through definer's-rights views
- Async job pattern: submit returns run_id, get_run_result fetches output
- auth_type="pat" to avoid conflict with auto-injected SP env vars
"""

import contextvars
import json
import logging
import os
import re
import sys
import time
import uuid
from typing import Any

from starlette.types import ASGIApp, Message, Receive, Scope, Send

logger = logging.getLogger(__name__)


def sanitize_for_log(value: object) -> str:
    """Strip CR/LF from a value before logging to prevent log injection (CWE-117).

    User-supplied values (table names, view names) may contain newlines or carriage
    returns that could forge log entries or corrupt log pipelines. Replace them with
    spaces before interpolating into a log message.

    Args:
        value: Any value to be logged.

    Returns:
        String form of *value* with newlines and carriage returns replaced by spaces.
    """
    return str(value).replace("\n", " ").replace("\r", " ")


# ── Logging configuration ────────────────────────────────────────────

# Per-request correlation id (set by OBOAuthMiddleware), so log lines from a single
# request — across the tool handler, SQL, and job submission — can be traced together
# in the Databricks Apps log stream, which is the only place these logs surface.
_request_id_var: contextvars.ContextVar[str | None] = contextvars.ContextVar("request_id", default=None)

# Third-party loggers that are noisy at INFO and would otherwise bury the server's own logs.
_NOISY_LOGGERS = ("databricks.sdk", "httpx", "httpcore", "urllib3", "py4j")

_LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] [req=%(request_id)s user=%(user)s] %(message)s"

_logging_configured = False


class RequestContextFilter(logging.Filter):
    """Inject the per-request correlation id and calling user into every log record.

    Attached to the root handler so *all* records (including third-party ones) carry
    ``request_id`` and ``user`` fields, defaulting to ``"-"`` outside a request. The
    user email is sanitized (CWE-117) since it originates from a request header.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = _request_id_var.get(None) or "-"
        email = _user_email_var.get(None)
        record.user = sanitize_for_log(email) if email else "-"
        return True


def configure_logging() -> None:
    """Configure root logging for the MCP server. Idempotent and entry-point agnostic.

    Safe to call from any entry point (``server/main.py``, a direct ``uvicorn`` invocation,
    or tests). Emits to stdout (where Databricks Apps collects logs), honors the
    ``DQX_MCP_LOG_LEVEL`` env var (default ``INFO``), tags every line with the request id and
    calling user, and quiets noisy third-party loggers so the server's own logs stand out.
    """
    global _logging_configured
    if _logging_configured:
        return

    level_name = os.environ.get("DQX_MCP_LOG_LEVEL", "INFO").upper()
    level = logging.getLevelName(level_name)
    if not isinstance(level, int):
        level = logging.INFO

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(_LOG_FORMAT))
    handler.addFilter(RequestContextFilter())

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)

    for name in _NOISY_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING)

    _logging_configured = True


# ── OBO Auth via contextvars ──────────────────────────────────────────

# Store user identity per-request from Databricks Apps proxy headers
_user_token_var: contextvars.ContextVar[tuple[str, str] | None] = contextvars.ContextVar("user_token", default=None)
_user_email_var: contextvars.ContextVar[str | None] = contextvars.ContextVar("user_email", default=None)

# Service principal client singleton (fallback when no OBO token)
_sp_client = None


class OBOAuthMiddleware:
    """Pure ASGI middleware for on-behalf-of authentication.

    Extracts user identity from Databricks Apps proxy headers:
    - X-Forwarded-Access-Token: user's OBO token (creates temp views + enforces the caller's
      UC permissions before the app SP submits the runner job)
    - X-Forwarded-Email: user's email (grant principal + log context)

    Also establishes a per-request correlation id (honoring an inbound ``X-Request-Id``,
    otherwise generated) and logs one line per request with status and duration, so a
    request can be traced end-to-end in the Databricks Apps log stream. All request
    context is reset on the way out so it never leaks across requests on a reused worker.

    Using pure ASGI (not BaseHTTPMiddleware) is critical — BaseHTTPMiddleware
    buffers response bodies which causes MCP streaming timeouts.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return

        headers = dict(scope.get("headers", []))
        user_token = headers.get(b"x-forwarded-access-token", b"").decode() or None
        user_email = headers.get(b"x-forwarded-email", b"").decode() or None
        # Correlate logs across the request; honor an upstream trace id if the proxy set one.
        incoming_id = headers.get(b"x-request-id", b"").decode().strip()
        request_id = (incoming_id or uuid.uuid4().hex)[:32]

        host = os.environ.get("DATABRICKS_HOST", "")
        token_tok = _user_token_var.set((host, user_token) if user_token else None)
        email_tok = _user_email_var.set(user_email)
        request_tok = _request_id_var.set(request_id)

        status_holder = {"code": 0}

        async def send_wrapper(message: Message) -> None:
            if message.get("type") == "http.response.start":
                status_holder["code"] = message.get("status", 0)
            await send(message)

        method = scope.get("method", "-")
        path = sanitize_for_log(scope.get("path", "-"))
        # Health probes hit "/" constantly; log them at DEBUG so they don't drown real traffic.
        log_at = logger.debug if path == "/" else logger.info
        started = time.monotonic()
        try:
            await self.app(scope, receive, send_wrapper)
        except Exception:
            elapsed_ms = int((time.monotonic() - started) * 1000)
            logger.exception(f"request error: {method} {path} after {elapsed_ms}ms")
            raise
        else:
            elapsed_ms = int((time.monotonic() - started) * 1000)
            log_at(f"request: {method} {path} status={status_holder['code']} {elapsed_ms}ms")
        finally:
            _user_token_var.reset(token_tok)
            _user_email_var.reset(email_tok)
            _request_id_var.reset(request_tok)


def get_obo_client():
    """Get a WorkspaceClient authenticated with the user's OBO token.

    Used for operations that must run as the user (SQL queries, view creation)
    to enforce Unity Catalog governance.

    Raises:
        RuntimeError: If no OBO token is available in the current request context.
    """
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.config import Config

    token_info = _user_token_var.get(None)
    if token_info is None:
        raise RuntimeError(
            "No OBO token available. This operation requires a user context (X-Forwarded-Access-Token header)."
        )

    host, token = token_info
    cfg = Config(host=host, token=token, auth_type="pat")
    return WorkspaceClient(config=cfg)


def get_user_email() -> str | None:
    """The calling user's email (from the X-Forwarded-Email OBO header), or None.

    Used as the principal to grant on MCP-created tables so the user can read/manage the outputs
    outside the MCP. None when there is no user context (e.g. a non-OBO/service-principal call),
    in which case the runner simply skips the grant.
    """
    return _user_email_var.get(None)


def _get_sp_client():
    """Get the app's service principal WorkspaceClient.

    The SP is used for job submission. UC governance is enforced before this
    call via temporary views created with the user's OBO token.
    """
    from databricks.sdk import WorkspaceClient

    global _sp_client
    if _sp_client is None:
        _sp_client = WorkspaceClient()
    return _sp_client


def get_sp_client_for_telemetry():
    """The app SP client, for telemetry's User-Agent signal only.

    A named public accessor so ``server.telemetry`` does not reach into a private helper. Uses the
    app's own identity rather than the caller's OBO client deliberately: the signal records which
    tools this *deployment* exposes and uses, so it needs no user context, and it keeps telemetry off
    the caller's credentials entirely.
    """
    return _get_sp_client()


# ── SQL helpers (OBO) ────────────────────────────────────────────────


def get_warehouse_id(ws: Any) -> str:
    """Auto-discover a SQL warehouse the user has access to.

    Picks the first running or available warehouse. The user's OBO token
    has 'sql' scope so it can list warehouses they have access to.

    Args:
        ws: WorkspaceClient (OBO or SP).

    Returns:
        Warehouse ID string.

    Raises:
        RuntimeError: If no warehouses are available.
    """
    warehouses = list(ws.warehouses.list())
    if not warehouses:
        raise RuntimeError("No SQL warehouses available. Check workspace permissions.")

    # Prefer a running warehouse to avoid startup wait
    for wh in warehouses:
        if wh.state and wh.state.value == "RUNNING":
            logger.debug(f"Using running warehouse: {wh.name} ({wh.id})")
            return wh.id

    # Fall back to first available
    wh = warehouses[0]
    logger.debug(f"Using warehouse: {wh.name} ({wh.id})")
    return wh.id


# A statement still PENDING/RUNNING after the execute wait window is polled to completion rather
# than treated as a failure (a cold-start warehouse or a very wide/partitioned table can exceed it).
_SQL_POLL_TIMEOUT_SECONDS = 120
_SQL_POLL_INTERVAL_SECONDS = 2.0


def _statement_state(result: Any) -> str:
    """Normalized statement lifecycle state string (e.g. 'SUCCEEDED', 'RUNNING', 'FAILED')."""
    state = result.status.state if result.status else None
    if state is None:
        return "UNKNOWN"
    return str(getattr(state, "value", state))


def execute_sql(ws: Any, query: str, warehouse_id: str, parameters: list[Any] | None = None) -> list[dict[str, Any]]:
    """Execute a SQL query using the Databricks SQL Statement API.

    Args:
        ws: WorkspaceClient (OBO or SP).
        query: SQL query string.
        warehouse_id: SQL warehouse ID to execute against.
        parameters: Optional bound parameters for *:name* markers in *query*. Prefer these over
            interpolating a value into the statement — the server binds them, so no quoting or
            escaping is needed and a value can never alter the statement's structure. Identifiers
            (catalog/schema/table names) still have to be interpolated, since markers may only
            stand in for values; validate those with *_validate_sql_identifier*.

    Returns:
        List of row dicts.

    Raises:
        RuntimeError: If the query fails.
    """
    result = ws.statement_execution.execute_statement(
        statement=query,
        warehouse_id=warehouse_id,
        wait_timeout="30s",
        # `is not None`, not truthiness: an explicit empty list means "the caller supplied bindings",
        # and silently dropping the kwarg would hide that rather than honour it.
        **({"parameters": parameters} if parameters is not None else {}),
    )

    # on_wait_timeout defaults to CONTINUE, so a slow statement can still be PENDING/RUNNING when the
    # 30s wait elapses. Poll to completion instead of misreporting a healthy-but-slow query as failed.
    deadline = time.monotonic() + _SQL_POLL_TIMEOUT_SECONDS
    while _statement_state(result) in ("PENDING", "RUNNING") and result.statement_id:
        if time.monotonic() > deadline:
            raise RuntimeError(f"SQL query did not complete within {_SQL_POLL_TIMEOUT_SECONDS}s")
        time.sleep(_SQL_POLL_INTERVAL_SECONDS)
        result = ws.statement_execution.get_statement(result.statement_id)

    state = _statement_state(result)
    if state != "SUCCEEDED":
        # status can be None for some error/edge states (_statement_state handles that → "UNKNOWN"),
        # so guard it here too rather than dereferencing .error and raising an opaque AttributeError.
        err = result.status.error if result.status else None
        error_msg = (getattr(err, "message", None) or str(err)) if err else state
        raise RuntimeError(f"SQL query failed: {error_msg}")

    columns = [col.name for col in result.manifest.schema.columns]
    rows: list[dict[str, Any]] = []

    # The result set is chunked: execute_statement returns only the first chunk (result.result) and,
    # for large results, a next_chunk_index pointing at the next one. Follow the chain to the end so
    # callers (sweep_stale_views listing every temp view; get_table_schema on a wide table) see the
    # full result rather than a silently truncated first page. Reading only the first chunk would
    # leave orphan views un-swept and drop columns off a wide DESCRIBE.
    chunk = result.result
    while chunk is not None:
        for row_data in chunk.data_array or []:
            rows.append(dict(zip(columns, row_data)))
        next_index = chunk.next_chunk_index
        if next_index is None:
            break
        chunk = ws.statement_execution.get_statement_result_chunk_n(result.statement_id, next_index)
    return rows


_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_]+$")


def _validate_sql_identifier(name: str, label: str) -> str:
    """Validate and backtick-quote a SQL identifier to prevent injection.

    Args:
        name: Raw identifier (catalog, schema, or table name part).
        label: Human-readable label for error messages (e.g. 'catalog').

    Returns:
        Backtick-quoted identifier safe for SQL interpolation.

    Raises:
        ValueError: If *name* contains characters outside ``[A-Za-z0-9_]``.
    """
    if not _SAFE_IDENTIFIER_RE.match(name):
        raise ValueError(f"Invalid {label}: '{name}'. Only alphanumeric characters and underscores are allowed.")
    return f"`{name}`"


def validate_output_name(name: str) -> str:
    """Validate a caller-supplied output object name as a bare SQL identifier.

    Outputs (save_checks / apply_checks_and_save_to_table) are written to the caller's private,
    per-user MCP schema, so the caller supplies only the table *name* — never a catalog/schema.
    Reject anything that isn't a plain identifier (blocks FQNs, file paths, and SQL injection).
    The name is interpolated UNQUOTED into the output FQN (OutputConfig.location, spark.table), so it
    must also not start with a digit — an unquoted digit-leading identifier is a SQL parse error.

    Raises:
        ValueError: If *name* is empty, starts with a digit, or contains characters outside
            ``[A-Za-z0-9_]``.
    """
    if not name or not _SAFE_IDENTIFIER_RE.match(name) or name[0].isdigit():
        raise ValueError(
            f"Invalid output name '{name}'. Provide a bare table name that starts with a letter or "
            f"underscore (letters, digits, underscores) — outputs are written to your private MCP "
            f"schema, not an arbitrary catalog.schema.table."
        )
    return name


_VALID_WRITE_MODES = ("append", "overwrite")


def validate_write_mode(mode: str) -> str:
    """Validate a table write mode up front so an unsupported value fails clearly, not silently.

    Raises:
        ValueError: If *mode* is not one of ``append`` / ``overwrite``.
    """
    if mode not in _VALID_WRITE_MODES:
        raise ValueError(f"Invalid mode '{mode}'. Must be one of {list(_VALID_WRITE_MODES)}.")
    return mode


def validate_and_quote_table_name(table_name: str) -> str:
    """Validate a fully qualified table name and return a backtick-quoted version.

    Args:
        table_name: Fully qualified table name (catalog.schema.table).

    Returns:
        Backtick-quoted table name safe for SQL interpolation.

    Raises:
        ValueError: If table_name is not fully qualified or contains unsafe characters.
    """
    parts = table_name.split(".")
    if len(parts) != 3:
        raise ValueError(f"Table name '{table_name}' must be fully qualified (catalog.schema.table)")

    quoted = [_validate_sql_identifier(p, label) for p, label in zip(parts, ("catalog", "schema", "table"))]
    return ".".join(quoted)


def create_temp_view(
    ws: Any,
    table_name: str,
    catalog: str,
    schema: str,
    warehouse_id: str,
) -> str:
    """Create a temporary view over a table using the user's OBO credentials.

    The view creation enforces UC governance — if the user can't read the source
    table, the CREATE VIEW fails. The view uses definer's rights (UC default),
    so the SP can read through it using the creator's permissions.

    Args:
        ws: WorkspaceClient (OBO — user's identity).
        table_name: Fully qualified source table (catalog.schema.table).
        catalog: Catalog for the temp view.
        schema: Schema for the temp view.
        warehouse_id: SQL warehouse ID.

    Returns:
        Fully qualified view name (catalog.schema.v_{uuid}).

    Raises:
        ValueError: If table_name is not fully qualified or contains unsafe characters.
        RuntimeError: If view creation fails (e.g., user lacks SELECT on source table).
    """
    import time
    import uuid

    safe_source = validate_and_quote_table_name(table_name)
    view_catalog = _validate_sql_identifier(catalog, "view catalog")
    view_schema = _validate_sql_identifier(schema, "view schema")

    # Encode the creation epoch in the name (v_<epoch>_<uuid>) so the sweeper can drop
    # stale views by age. The UUID keeps it unique; the whole name stays within the
    # identifier-safety charset.
    view_basename = f"v_{int(time.time())}_{uuid.uuid4().hex[:12]}"
    view_name = _validate_sql_identifier(view_basename, "view name")
    view_fqn = f"{catalog}.{schema}.{view_basename}"

    logger.info(f"Creating temp view {sanitize_for_log(view_fqn)} over {sanitize_for_log(table_name)}")
    execute_sql(
        ws,
        f"CREATE VIEW {view_catalog}.{view_schema}.{view_name} AS SELECT * FROM {safe_source}",
        warehouse_id=warehouse_id,
    )
    return view_fqn


def drop_view(ws: Any, view_fqn: str, warehouse_id: str) -> None:
    """Drop a temporary view. Logs errors but does not raise.

    Args:
        ws: WorkspaceClient (SP or OBO).
        view_fqn: Fully qualified view name to drop.
        warehouse_id: SQL warehouse ID.
    """
    parts = view_fqn.split(".")
    if len(parts) != 3:
        logger.warning(f"Invalid view name '{sanitize_for_log(view_fqn)}', skipping drop")
        return

    quoted_parts = []
    for part in parts:
        if not _SAFE_IDENTIFIER_RE.match(part):
            logger.warning(f"Invalid identifier in view name '{sanitize_for_log(view_fqn)}', skipping drop")
            return
        quoted_parts.append(f"`{part}`")

    safe_fqn = ".".join(quoted_parts)
    logger.info(f"Dropping temp view {sanitize_for_log(view_fqn)}")
    try:
        execute_sql(ws, f"DROP VIEW IF EXISTS {safe_fqn}", warehouse_id=warehouse_id)
    except Exception:
        logger.warning(f"Failed to drop temp view {sanitize_for_log(view_fqn)}", exc_info=True)


# ── Caller-permission enforcement (OBO) ──────────────────────────────
#
# The runner job runs as a dedicated runner service principal. For *source-table* reads it reads
# through a definer's-rights view created with the caller's OBO token, so UC governance holds.
#
# Reads of caller-supplied files (load_checks file backend, generate_rules_from_contract) do NOT go
# through that view, so verify_obo_read_access confirms the caller can read the path — as the caller,
# via their OBO client — BEFORE the SP job is submitted.
#
# Writes need no such pre-check: save_checks / apply_checks_and_save_to_table no longer take a
# caller-supplied destination. Outputs go to the caller's own SP-owned per-user schema
# (dqx_mcp_<user>, created + granted by the runner), so the SP always writes where it is guaranteed
# to have permission and users are isolated from each other. (An earlier verify_obo_write_access
# pre-check was removed: it checked the *caller's* perms while the *SP* did the write — false
# assurance — and classified UC errors by brittle string-matching.)


def classify_location(location: str) -> str:
    """Classify a checks/contract location the same way DQX's storage factory does.

    Returns one of ``"table"`` (a ``catalog.schema.table`` name), ``"volume"`` (a
    ``/Volumes/...`` path), or ``"workspace"`` (any other ``/...`` path).
    """
    if location.startswith("/Volumes/"):
        return "volume"
    if location.startswith("/"):
        return "workspace"
    return "table"


def verify_obo_read_access(ws: Any, location: str) -> None:
    """Verify the calling user can read *location* (file backends), as the caller via OBO.

    Tables are governed via a definer's-rights temp view (see :func:`create_temp_view`), so this
    only covers UC-volume and workspace-file paths. Raises if the caller cannot access the path.

    Raises:
        PermissionError: If the caller cannot read the path (or it does not exist).
    """
    kind = classify_location(location)
    try:
        if kind == "volume":
            ws.files.get_metadata(location)
        elif kind == "workspace":
            ws.workspace.get_status(location)
        else:
            # Tables are enforced separately via an OBO temp view; nothing to do here.
            return
    except Exception as e:
        raise PermissionError(
            f"You do not have access to {sanitize_for_log(location)} (or it does not exist): {e}"
        ) from e


def read_file_via_obo(ws: Any, location: str) -> bytes:
    """Read a file's raw bytes as the calling user (OBO).

    Supports the two file backends the tools accept: a UC-volume path (``/Volumes/...``) via the
    Files API, and a workspace file (any other ``/...`` path) via the Workspace export API. Tables
    are not files and are rejected.

    Raises:
        ValueError: If *location* is a table name (not a file path).
    """
    kind = classify_location(location)
    if kind == "volume":
        contents = ws.files.download(location).contents
        return contents.read() if contents is not None else b""
    if kind == "workspace":
        import base64

        from databricks.sdk.service.workspace import ExportFormat

        exported = ws.workspace.export(location, format=ExportFormat.AUTO)
        return base64.b64decode(exported.content) if exported.content else b""
    raise ValueError(f"{sanitize_for_log(location)} is a table name, not a file path.")


def stage_bytes_to_results_volume(content: bytes, suffix: str = "") -> str:
    """Write *content* to the results volume via the app SP and return the ``/Volumes/...`` path.

    Used to stage a caller-supplied payload (e.g. a data contract) somewhere the **runner SP** can
    read it: the runner SP has ``READ VOLUME`` on the results volume but no access to arbitrary
    caller Workspace/Volume paths (the OBO read gap). The app SP has ``WRITE VOLUME`` here, so it
    writes the staged copy and the runner reads it back. The stale-file sweeper reaps these by age,
    same as result files.
    """
    import io

    path = f"{_get_results_volume()}/staged_{uuid.uuid4().hex}{suffix}"
    _get_sp_client().files.upload(path, io.BytesIO(content), overwrite=True)
    logger.info(f"Staged {len(content)} bytes to {sanitize_for_log(path)}")
    return path


# ── Temp-view sweeper (backstop cleanup) ─────────────────────────────

# View names are v_<epoch>_<uuid>. The runner job drops its own view in a finally,
# so this sweeper only catches orphans: views whose job never started or was killed
# before cleanup. It runs as the SP, which owns the temp schema.
_VIEW_NAME_RE = re.compile(r"^v_(\d+)_[0-9a-f]+$")
# Overridable so the integration test can exercise the sweepers within a single run: the defaults
# are deliberately long (an orphan is harmless for an hour), which no test can wait out. Only the
# thresholds are configurable — the sweep logic itself is identical in every environment.
_VIEW_TTL_SECONDS = int(os.environ.get("DQX_SWEEP_TTL_SECONDS", "3600"))  # drop views older than 1 hour
_SWEEP_INTERVAL_SECONDS = int(
    os.environ.get("DQX_SWEEP_INTERVAL_SECONDS", "600")
)  # sweep at most once per 10 minutes per replica

# Floor on the view TTL, regardless of configuration. A view is created moments before its job is
# submitted, and the job may sit QUEUED for minutes on a busy workspace before it reads the view —
# so a TTL shorter than the worst-case queue wait would delete a live run's input and fail it with
# TABLE_OR_VIEW_NOT_FOUND. The configured TTL can only ever be raised above this floor, never below:
# an operator (or a test) tuning the threshold must not be able to introduce data loss.
_MIN_VIEW_TTL_SECONDS = 900

# TTL for `staged_*` job inputs on the results volume. Deliberately far longer than any plausible
# queue wait: a queued job has not read its staged input yet, and deleting it fails that run with
# "Contract file not found". 24h means an input is only reclaimed once its job could not possibly
# still be pending, while the volume no longer grows without bound.
_STAGED_TTL_SECONDS = int(os.environ.get("DQX_STAGED_TTL_SECONDS", str(24 * 60 * 60)))

# TTL for `<run_id>.json` result files. Its own knob, deliberately: this used to default to
# _VIEW_TTL_SECONDS, so DQX_SWEEP_TTL_SECONDS — documented as tuning the *view* sweep — silently drove
# result-file deletion too, and the view TTL's 900s safety floor leaked across to files it was never
# reasoned about. Same 3600s default, so behaviour is unchanged; the two thresholds are now
# independent and the constant name no longer misleads.
_RESULT_TTL_SECONDS = int(os.environ.get("DQX_RESULT_TTL_SECONDS", "3600"))
_last_sweep_at = 0.0


def _effective_view_ttl(ttl_seconds: int) -> int:
    """Clamp a requested view TTL to the safe floor, warning when the request is overridden."""
    if ttl_seconds < _MIN_VIEW_TTL_SECONDS:
        logger.warning(
            f"View sweep: requested TTL of {ttl_seconds}s is below the {_MIN_VIEW_TTL_SECONDS}s floor "
            f"(a queued job could still be waiting to read its view); using the floor instead"
        )
        return _MIN_VIEW_TTL_SECONDS
    return ttl_seconds


def sweep_stale_views(
    ws: Any, catalog: str, schema: str, warehouse_id: str, ttl_seconds: int = _VIEW_TTL_SECONDS
) -> int:
    """Drop temp views in *catalog.schema* older than *ttl_seconds*. Best-effort.

    Identifies age from the v_<epoch>_<uuid> name. Returns the number of views dropped.
    Never raises — logs and moves on so cleanup can't break request handling.

    *ttl_seconds* is clamped up to a safe floor: this sweep runs on the job-submission path, so a
    view belonging to a job that is still QUEUED must never be treated as an orphan.
    """
    import time

    # The catalog is a NAME, so it must be interpolated (a parameter marker cannot stand in for an
    # identifier) — hence the charset validation. The schema is compared as a VALUE and is bound as a
    # parameter below, so it never becomes part of the statement's structure.
    safe_catalog = _validate_sql_identifier(catalog, "catalog")
    ttl_seconds = _effective_view_ttl(ttl_seconds)
    now = int(time.time())
    dropped = 0
    try:
        # Query information_schema rather than SHOW VIEWS: `SHOW VIEWS IN <catalog>.<schema>` is
        # rejected outright with CROSS_CATALOG_SCHEMA_REFERENCE_NOT_SUPPORTED ("Run 'USE CATALOG'
        # first"), because SHOW resolves the schema against the session's current catalog only. This
        # runs statelessly against a shared warehouse, so there is no session to set a catalog on —
        # information_schema takes the catalog as the first level of the name and needs no USE.
        # Imported here, not at module scope: this module is imported by the app at startup and
        # keeps SDK types out of its import graph (see the other local `from databricks.sdk`
        # imports).
        from databricks.sdk.service.sql import StatementParameterListItem

        # Compare case-insensitively: Unity Catalog folds identifiers to lower case in
        # information_schema, so an equality against a mixed-case schema name matches nothing and the
        # sweep silently returns 0 — indistinguishable from "no stale views", which is exactly how the
        # previous SHOW VIEWS bug stayed hidden. SHOW resolved the identifier case-insensitively; this
        # preserves that rather than relying on every caller pre-normalising.
        rows = execute_sql(
            ws,
            f"SELECT table_name FROM {safe_catalog}.information_schema.views "
            "WHERE lower(table_schema) = :schema_name",
            warehouse_id=warehouse_id,
            parameters=[StatementParameterListItem(name="schema_name", value=schema.lower(), type="STRING")],
        )
    except Exception:
        logger.warning(f"View sweep: failed to list views in {sanitize_for_log(f'{catalog}.{schema}')}", exc_info=True)
        return 0

    for row in rows:
        view_name = row.get("table_name") or ""
        match = _VIEW_NAME_RE.match(view_name)
        if not match:
            continue
        age = now - int(match.group(1))
        if age > ttl_seconds:
            drop_view(ws, f"{catalog}.{schema}.{view_name}", warehouse_id=warehouse_id)
            dropped += 1
    if dropped:
        logger.info(f"View sweep: dropped {dropped} stale view(s) in {sanitize_for_log(f'{catalog}.{schema}')}")
    return dropped


def sweep_stale_result_files(ws: Any, ttl_seconds: int = _RESULT_TTL_SECONDS) -> int:
    """Delete stale files from the results volume. Best-effort.

    Backstop for result files whose caller never polled get_run_result. Uses each file's
    last-modified time (the runner names files <run_id>.json, which carries no timestamp).
    Never raises — logs and moves on so cleanup can't break request handling.

    Two kinds of file live here and they get different TTLs:

    * ``<run_id>.json`` results — swept at *ttl_seconds*. Once a run is finished its result is only
      waiting to be polled, so the normal TTL applies.
    * ``staged_*`` job inputs written by stage_bytes_to_results_volume (e.g. an inline data contract)
      — swept only after :data:`_STAGED_TTL_SECONDS`, which is far longer than any plausible queue
      wait. A *pending* job has not read its input yet, so deleting one by the normal TTL failed that
      job with "Contract file not found". Excluding them from cleanup entirely was the previous fix
      and it leaked: nothing else reclaims them, so every inline-contract call left a file on the
      volume forever.
    """
    import time

    now = time.time()
    dropped = 0
    try:
        volume = _get_results_volume()
        entries = ws.files.list_directory_contents(volume)
    except Exception:
        logger.warning("Result-file sweep: failed to list results volume", exc_info=True)
        return 0

    for entry in entries:
        if getattr(entry, "is_directory", False):
            continue
        path = entry.path or ""
        name = path.rsplit("/", 1)[-1]
        if name.startswith("staged_"):
            # A queued job may still be waiting to read this; only reclaim long-abandoned inputs.
            threshold = max(ttl_seconds, _STAGED_TTL_SECONDS)
        elif name.endswith(".json"):
            threshold = ttl_seconds
        else:
            continue  # unknown file: not ours to delete
        last_modified = getattr(entry, "last_modified", None)  # epoch millis
        age = now - (last_modified / 1000) if last_modified else 0
        if age > threshold:
            try:
                ws.files.delete(path)
                dropped += 1
            except Exception:
                logger.warning(f"Result-file sweep: failed to delete {sanitize_for_log(path)}", exc_info=True)
    if dropped:
        logger.info(f"Result-file sweep: deleted {dropped} stale result file(s)")
    return dropped


def _maybe_sweep_stale_views() -> None:
    """Run the stale-view and stale-result-file sweeps at most once per interval. Never raises."""
    import time

    global _last_sweep_at
    now = time.time()
    if now - _last_sweep_at < _SWEEP_INTERVAL_SECONDS:
        return
    _last_sweep_at = now

    catalog = os.environ.get("DQX_CATALOG", "")
    schema = os.environ.get("DQX_TMP_SCHEMA", "dqx_mcp_tmp")
    if not catalog:
        return
    try:
        ws = _get_sp_client()
        warehouse_id = get_warehouse_id(ws)
        sweep_stale_views(ws, catalog, schema, warehouse_id)
    except Exception:
        logger.warning("View sweep: skipped due to error", exc_info=True)
    try:
        sweep_stale_result_files(_get_sp_client())
    except Exception:
        logger.warning("Result-file sweep: skipped due to error", exc_info=True)


# ── Jobs API — async submit + poll ───────────────────────────────────


def _get_runner_job_id() -> int:
    """Get the pre-deployed runner job ID from environment."""
    job_id = os.environ.get("DQX_RUNNER_JOB_ID")
    if not job_id:
        raise RuntimeError("DQX_RUNNER_JOB_ID not set. Deploy the bundle first: databricks bundle deploy")
    return int(job_id)


def _get_results_volume() -> str:
    """UC-volume path where the runner writes result files: /Volumes/<catalog>/<schema>/mcp_results.

    The runner (wheel task) writes ``<run_id>.json`` here; the app reads it back via the Files API
    (no SQL warehouse needed). Catalog/schema come from the same env the temp-view config uses.
    """
    catalog = os.environ.get("DQX_CATALOG", "")
    schema = os.environ.get("DQX_TMP_SCHEMA", "dqx_mcp_tmp")
    if not catalog:
        raise RuntimeError("DQX_CATALOG not set. Deploy the bundle first.")
    return f"/Volumes/{catalog}/{schema}/mcp_results"


def submit_job_async(operation: str, params: dict[str, Any]) -> int:
    """Submit a DQX operation and return the run_id immediately (non-blocking).

    Stateless by design: the runner job drops its own temp view (params['view_name'])
    in a finally and echoes params['table_name'] into its result, so no per-run state
    is kept in the server process. This means a restart or a poll landing on a
    different app replica does not leak views or lose context.

    Args:
        operation: The DQX operation name (e.g. 'profile_table', 'run_checks').
        params: Dict of parameters passed to the runner as JSON. For table-backed operations,
            include 'view_name' (dropped by the runner) and 'table_name' (echoed into the result).

    Returns:
        The Databricks job run_id.
    """
    # Opportunistically reap orphaned temp views and stale result files (throttled).
    _maybe_sweep_stale_views()

    ws = _get_sp_client()
    job_id = _get_runner_job_id()

    logger.info(f"Submitting async job {job_id}: operation={operation}")

    # job_parameters (not notebook_params): the runner is a python_wheel_task. results_volume tells
    # the runner where to write <run_id>.json; the app reads it back in get_run_status.
    # requesting_user records the submitting caller on the run so get_run_status can authorize the
    # reader (only the submitter may fetch the result — see the IDOR guard there).
    wait = ws.jobs.run_now(
        job_id=job_id,
        job_parameters={
            "operation": operation,
            "params": json.dumps(params),
            "results_volume": _get_results_volume(),
            "requesting_user": get_user_email() or "",
        },
    )

    run_id = wait.run_id
    logger.info(f"Job submitted: run_id={run_id}")
    return run_id


# How long get_run_status waits for a run to finish before reporting 'running'.
#
# Chosen against two hard limits, not preference: MCP clients and the Databricks Apps front door
# commonly time out an idle HTTP request around 60s, and each in-flight poll holds an anyio worker
# thread (the tools are sync). 30s stays clear of both while covering a large share of runs — the
# job-backed tools take 50–90s end to end, so one submit plus two polls now completes the common
# case instead of the agent giving up on the first one.
#
# Configurable via DQX_RUN_WAIT_SECONDS (the bundle exposes it as an app config value) so an operator
# whose client has a tighter timeout can lower it. Note that lowering it does NOT make a run finish
# sooner — the job takes as long as it takes — it only changes how many HTTP round-trips are spent
# waiting: a 70s job costs 3 calls at 30s and ~470 at 0s. The upper bound is clamped below, because a
# value above the client/proxy timeout converts a working poll into a hung request.
_RUN_WAIT_CEILING_SECONDS = 55.0
_RUN_WAIT_SECONDS = min(float(os.environ.get("DQX_RUN_WAIT_SECONDS", "30")), _RUN_WAIT_CEILING_SECONDS)

# Poll fast at first (a short job should return promptly), then back off so a multi-minute run does
# not cost one control-plane request per second. These bound how soon a *finished* run is noticed, so
# they are what to lower if a test wants tighter latency — up to _RUN_POLL_MAX_SECONDS of the measured
# time is just the gap before the sweep notices a job that has already completed.
_RUN_POLL_INITIAL_SECONDS = float(os.environ.get("DQX_RUN_POLL_INITIAL_SECONDS", "2"))
_RUN_POLL_MAX_SECONDS = float(os.environ.get("DQX_RUN_POLL_MAX_SECONDS", "8"))
_RUN_POLL_BACKOFF = 1.5


def _run_not_found(run_id: int) -> dict[str, Any]:
    """Structured 'not_found' result for an invalid/expired/foreign run_id."""
    return {
        "status": "not_found",
        "run_id": run_id,
        "error": (
            f"No run found for run_id={run_id}. It may be invalid, expired, or from a different "
            f"job. Use the run_id returned by the submit call (profile_table, run_checks, etc.)."
        ),
    }


def get_run_status(run_id: int, *, wait_seconds: float | None = None) -> dict[str, Any]:
    """Check the status of a submitted job run, waiting briefly for it to finish.

    Blocks up to *wait_seconds* (default `_RUN_WAIT_SECONDS`) for the run to reach a terminal
    state, then returns 'completed' (with result), 'failed', or 'running'.

    **Why this waits at all.** An LLM agent has no sleep primitive: told to "call again after a
    short wait", it calls again *immediately*, which looks to its own loop detection like a
    repeated no-progress action — so it stops and asks the user to confirm, mid-workflow, on every
    submitted tool. Observed end-to-end against a real deployment: every single job-backed tool
    stalled with "I kept repeating the same action without making progress". Absorbing the wait
    server-side is what makes the submit→poll pattern usable by an agent.

    The wait is deliberately bounded well below the ~60s that clients and proxies commonly
    time out at, and it returns as soon as the run is terminal, so a fast job stays fast. A longer
    job still needs more than one call — but each call now makes visible progress, which is what
    the agent needs to keep going. Polling is done with a backoff so a slow job costs few requests.

    Args:
        run_id: The Databricks job run_id from a prior submit call.
        wait_seconds: Maximum seconds to wait for a terminal state. 0 polls once and returns.

    Returns:
        Dict with 'status' ('running', 'completed', 'failed', 'not_found') and optionally 'result'.
        'not_found' means the run_id is invalid, expired, or not from this MCP's runner job.
    """
    budget = _RUN_WAIT_SECONDS if wait_seconds is None else max(0.0, wait_seconds)
    deadline = time.monotonic() + budget
    interval = _RUN_POLL_INITIAL_SECONDS
    while True:
        result = _get_run_status_once(run_id, waited_seconds=budget)
        if result["status"] != "running":
            return result
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return result
        time.sleep(min(interval, remaining))
        # Back off so a multi-minute job does not cost one control-plane call per second.
        interval = min(interval * _RUN_POLL_BACKOFF, _RUN_POLL_MAX_SECONDS)


def _get_run_status_once(run_id: int, waited_seconds: float = _RUN_WAIT_SECONDS) -> dict[str, Any]:
    """One non-blocking status check. See get_run_status for the authorization rules.

    *waited_seconds* is the total budget get_run_status is blocking for, reported verbatim in the
    'running' message so the caller is told the real wait — not the module default, which is wrong
    whenever wait_seconds is overridden.
    """
    from databricks.sdk.errors.base import DatabricksError

    ws = _get_sp_client()

    try:
        run = ws.jobs.get_run(run_id)
    except DatabricksError as e:
        # An unknown/expired run_id surfaces as RESOURCE_DOES_NOT_EXIST (404) or, on some
        # API/SDK versions, INVALID_PARAMETER_VALUE (400) "Run ... does not exist". Return a
        # structured not_found so the calling agent fixes the run_id instead of treating an
        # unstructured exception as a job failure.
        error_code = getattr(e, "error_code", "") or ""
        if error_code in ("RESOURCE_DOES_NOT_EXIST", "INVALID_PARAMETER_VALUE") or "does not exist" in str(e).lower():
            return _run_not_found(run_id)
        # A run the app SP cannot see is also not_found, for the same reason the job_id mismatch below
        # is: telling the caller "permission denied" confirms the run EXISTS, which is exactly the
        # disclosure the ownership guard exists to prevent — run ids are guessable integers. The job
        # ACL blocking the read first is defence in depth, not a licence to leak. Logged so an
        # operator can still distinguish a misconfigured ACL from a genuinely stale run_id.
        # Gate on the STRUCTURED code only. A substring match on "permission" also swallowed unrelated
        # failures whose text happens to mention it (e.g. "insufficient permissions to attach
        # warehouse"), telling the run's rightful owner it does not exist and hiding the real error —
        # a debugging dead end. Anything else propagates; the submitter/caller check below is what
        # actually prevents the disclosure, so this branch does not need to be greedy.
        if error_code == "PERMISSION_DENIED":
            logger.warning(f"Denying run {run_id}: the app service principal cannot read it ({error_code})")
            return _run_not_found(run_id)
        raise

    # Guard against polling a run that belongs to some other job the SP can see — only the
    # MCP runner job's runs are valid here. Best-effort: skip the check if the job id is unset.
    try:
        runner_job_id = _get_runner_job_id()
        if run.job_id is not None and run.job_id != runner_job_id:
            return _run_not_found(run_id)
    except RuntimeError:
        pass

    # Authorize the read: a run's result can contain the submitter's governed data (e.g. sampled
    # rows from their source table), so only the user who submitted the run may read it. run_id is a
    # guessable sequential integer and every user has CAN_MANAGE_RUN on the shared runner job, so we
    # bind the run to its submitter via the requesting_user parameter recorded at submit time and
    # reject a mismatch as not_found — without disclosing the result OR the run's existence/status.
    submitter = (next((p.value for p in (run.job_parameters or []) if p.name == "requesting_user"), "") or "").strip()
    caller = (get_user_email() or "").strip()
    # Deny when either side is empty (an unowned run, or a caller with no identity) — never let an
    # empty submitter match an empty caller, which would expose runs to an unauthenticated reader.
    if not submitter or not caller or submitter.lower() != caller.lower():
        logger.warning(f"Denying run {run_id}: submitter/caller mismatch or missing identity")
        return _run_not_found(run_id)

    life_cycle = run.state.life_cycle_state.value if run.state and run.state.life_cycle_state else "UNKNOWN"
    if life_cycle in ("PENDING", "RUNNING", "QUEUED", "BLOCKED"):
        return {
            "status": "running",
            "run_id": run_id,
            # Say explicitly that calling straight back is correct. Without this an agent treats an
            # unchanged 'running' as making no progress and stops to ask the user, because it has no
            # way to sleep between calls — the server already absorbed the wait before returning.
            "message": (
                f"Job is still running (waited {int(waited_seconds)}s). This is expected for a long job. "
                f"Call get_run_result with run_id={run_id} again right away — the call itself waits, so "
                f"repeating it is progress, not a retry loop. Do not ask the user to confirm."
            ),
        }

    # No local cleanup here: the runner job drops its own temp view, and any orphans are
    # reaped by the sweeper. This keeps get_run_status stateless and replica-independent.

    # Check for failure
    result_state = run.state.result_state.value if run.state and run.state.result_state else "UNKNOWN"
    if result_state != "SUCCESS":
        state_msg = (run.state.state_message if run.state else "") or ""
        run_url = run.run_page_url or ""
        # The job-level state_message is generic ("Workload failed, see run output for details").
        # Pull the failed task's actual error (e.g. a PERMISSION_DENIED on a table) so the calling
        # agent gets an actionable reason instead of an opaque failure.
        detail = state_msg
        try:
            task_run_id = run.tasks[0].run_id if run.tasks else run.run_id
            if task_run_id is not None:
                task_err = (ws.jobs.get_run_output(task_run_id).error or "").strip()
                if task_err:
                    detail = f"{state_msg} {task_err}".strip()
        except Exception:
            logger.warning(f"Could not fetch task error for failed run {run_id}", exc_info=True)
        return {
            "status": "failed",
            "run_id": run_id,
            "error": f"Job failed: {detail or 'Unknown error'}. Debug at: {run_url}",
        }

    # Read the result the runner (wheel task) wrote to the results volume, keyed by run id.
    # (Wheel tasks have no notebook_output; the runner writes <run_id>.json via the Files API.)
    result_path = f"{_get_results_volume()}/{run_id}.json"
    try:
        download = ws.files.download(result_path)
        if download.contents is None:
            raise RuntimeError("result file download returned no contents")
        content = download.contents.read()
        result = json.loads(content)
        # table_name is echoed by the runner into the result, so nothing to re-attach here.
        return {"status": "completed", "run_id": run_id, "result": result}
    except Exception as e:
        run_url = run.run_page_url or ""
        return {
            "status": "failed",
            "run_id": run_id,
            "error": f"Run succeeded but its result file could not be read ({sanitize_for_log(result_path)}): "
            f"{e}. Debug at: {run_url}",
        }
