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


def execute_sql(ws: Any, query: str, warehouse_id: str) -> list[dict[str, Any]]:
    """Execute a SQL query using the Databricks SQL Statement API.

    Args:
        ws: WorkspaceClient (OBO or SP).
        query: SQL query string.
        warehouse_id: SQL warehouse ID to execute against.

    Returns:
        List of row dicts.

    Raises:
        RuntimeError: If the query fails.
    """
    result = ws.statement_execution.execute_statement(
        statement=query,
        warehouse_id=warehouse_id,
        wait_timeout="30s",
    )

    state = str(result.status.state.value if hasattr(result.status.state, "value") else result.status.state)
    if state != "SUCCEEDED":
        error_msg = getattr(result.status.error, "message", str(result.status.error)) if result.status.error else state
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

    Raises:
        ValueError: If *name* is empty or contains characters outside ``[A-Za-z0-9_]``.
    """
    if not name or not _SAFE_IDENTIFIER_RE.match(name):
        raise ValueError(
            f"Invalid output name '{name}'. Provide a bare table name (letters, digits, and underscores) — "
            f"outputs are written to your private MCP schema, not an arbitrary catalog.schema.table."
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


def to_local_fuse_path(location: str) -> str:
    """Map a file location to the path the runner's cluster filesystem sees.

    DQX reads a data contract via a local ``open()``, so a workspace path must use the FUSE
    mount prefix (``/Workspace/...``). The Workspace API form (``/Users/...``) does not exist
    on the cluster filesystem. UC-volume paths (``/Volumes/...``) are already FUSE-mounted as-is.

    - ``/Volumes/...``  -> unchanged (mounted at the same path on the cluster)
    - ``/Workspace/...`` -> unchanged (already the FUSE form)
    - any other ``/...`` workspace path (e.g. ``/Users/...``) -> prefixed with ``/Workspace``
    """
    if classify_location(location) == "workspace" and not location.startswith("/Workspace/"):
        return "/Workspace" + location
    return location


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
# before cleanup. It runs as the SP, which owns the temp schema (see setup.py).
_VIEW_NAME_RE = re.compile(r"^v_(\d+)_[0-9a-f]+$")
_VIEW_TTL_SECONDS = 3600  # drop views older than 1 hour
_SWEEP_INTERVAL_SECONDS = 600  # sweep at most once per 10 minutes per replica
_last_sweep_at = 0.0


def sweep_stale_views(
    ws: Any, catalog: str, schema: str, warehouse_id: str, ttl_seconds: int = _VIEW_TTL_SECONDS
) -> int:
    """Drop temp views in *catalog.schema* older than *ttl_seconds*. Best-effort.

    Identifies age from the v_<epoch>_<uuid> name. Returns the number of views dropped.
    Never raises — logs and moves on so cleanup can't break request handling.
    """
    import time

    safe_catalog = _validate_sql_identifier(catalog, "catalog")
    safe_schema = _validate_sql_identifier(schema, "schema")
    now = int(time.time())
    dropped = 0
    try:
        rows = execute_sql(ws, f"SHOW VIEWS IN {safe_catalog}.{safe_schema}", warehouse_id=warehouse_id)
    except Exception:
        logger.warning(f"View sweep: failed to list views in {sanitize_for_log(f'{catalog}.{schema}')}", exc_info=True)
        return 0

    for row in rows:
        view_name = row.get("viewName") or row.get("tableName") or ""
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


def sweep_stale_result_files(ws: Any, ttl_seconds: int = _VIEW_TTL_SECONDS) -> int:
    """Delete result files older than *ttl_seconds* from the results volume. Best-effort.

    Backstop for result files whose caller never polled get_run_result. Uses each file's
    last-modified time (the runner names files <run_id>.json, which carries no timestamp).
    Never raises — logs and moves on so cleanup can't break request handling.
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
        last_modified = getattr(entry, "last_modified", None)  # epoch millis
        age = now - (last_modified / 1000) if last_modified else 0
        if age > ttl_seconds:
            try:
                ws.files.delete(entry.path)
                dropped += 1
            except Exception:
                logger.warning(f"Result-file sweep: failed to delete {sanitize_for_log(entry.path)}", exc_info=True)
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
    schema = os.environ.get("DQX_TMP_SCHEMA", "tmp")
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
    schema = os.environ.get("DQX_TMP_SCHEMA", "tmp")
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
    wait = ws.jobs.run_now(
        job_id=job_id,
        job_parameters={
            "operation": operation,
            "params": json.dumps(params),
            "results_volume": _get_results_volume(),
        },
    )

    run_id = wait.run_id
    logger.info(f"Job submitted: run_id={run_id}")
    return run_id


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


def get_run_status(run_id: int) -> dict[str, Any]:
    """Check the status of a submitted job run with a single, non-blocking poll.

    Performs one status check and returns immediately as 'completed' (with result),
    'failed', or 'running'. When 'running', the caller polls again — the client drives
    the cadence. We deliberately do NOT wait/sleep internally: holding the HTTP
    connection (and an anyio worker thread, since the tools are sync) open for the whole
    job would risk client/proxy timeouts and saturate the thread pool under concurrent
    polls.

    Args:
        run_id: The Databricks job run_id from a prior submit call.

    Returns:
        Dict with 'status' ('running', 'completed', 'failed', 'not_found') and optionally 'result'.
        'not_found' means the run_id is invalid, expired, or not from this MCP's runner job.
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
        raise

    # Guard against polling a run that belongs to some other job the SP can see — only the
    # MCP runner job's runs are valid here. Best-effort: skip the check if the job id is unset.
    try:
        runner_job_id = _get_runner_job_id()
        if run.job_id is not None and run.job_id != runner_job_id:
            return _run_not_found(run_id)
    except RuntimeError:
        pass

    life_cycle = run.state.life_cycle_state.value if run.state and run.state.life_cycle_state else "UNKNOWN"
    if life_cycle in ("PENDING", "RUNNING", "QUEUED", "BLOCKED"):
        return {"status": "running", "run_id": run_id, "message": "Job is still running. Call get_run_result again."}

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
