"""
Utility functions for the DQX MCP server.

Key patterns:
- Pure ASGI middleware for OBO (not BaseHTTPMiddleware — avoids streaming timeouts)
- Extracts user identity from Databricks Apps proxy headers
- User OBO token creates temp views (UC governance) and runs direct SQL
- SP submits notebook jobs that read through definer's-rights views
- Async job pattern: submit returns run_id, get_run_result fetches output
- auth_type="pat" to avoid conflict with auto-injected SP env vars
"""

import contextvars
import json
import logging
import os
import re
from typing import Any

from starlette.types import ASGIApp, Receive, Scope, Send

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


# ── OBO Auth via contextvars ──────────────────────────────────────────

# Store user identity per-request from Databricks Apps proxy headers
_user_token_var: contextvars.ContextVar[tuple[str, str] | None] = contextvars.ContextVar("user_token", default=None)
_user_email_var: contextvars.ContextVar[str | None] = contextvars.ContextVar("user_email", default=None)

# Service principal client singleton (fallback when no OBO token)
_sp_client = None


class OBOAuthMiddleware:
    """Pure ASGI middleware for on-behalf-of authentication.

    Extracts user identity from Databricks Apps proxy headers:
    - X-Forwarded-Access-Token: user's OBO token (used to call run_now() as user)
    - X-Forwarded-Email: user's email (for logging)

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

        if user_token:
            host = os.environ.get("DATABRICKS_HOST", "")
            _user_token_var.set((host, user_token))
        else:
            _user_token_var.set(None)

        _user_email_var.set(user_email)

        await self.app(scope, receive, send)


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
            "No OBO token available. This operation requires a user context " "(X-Forwarded-Access-Token header)."
        )

    host, token = token_info
    cfg = Config(host=host, token=token, auth_type="pat")
    return WorkspaceClient(config=cfg)


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
            logger.info(f"Using running warehouse: {wh.name} ({wh.id})")
            return wh.id

    # Fall back to first available
    wh = warehouses[0]
    logger.info(f"Using warehouse: {wh.name} ({wh.id})")
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
    rows = []
    if result.result and result.result.data_array:
        for row_data in result.result.data_array:
            rows.append(dict(zip(columns, row_data)))
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


# ── Temp-view sweeper (backstop cleanup) ─────────────────────────────

# View names are v_<epoch>_<uuid>. The runner job drops its own view in a finally,
# so this sweeper only catches orphans: views whose job never started or was killed
# before cleanup. It runs as the SP, which owns the temp schema (see setup.py).
_VIEW_NAME_RE = re.compile(r"^v_(\d+)_[0-9a-f]+$")
_VIEW_TTL_SECONDS = 3600  # drop views older than 1 hour
_SWEEP_INTERVAL_SECONDS = 600  # sweep at most once per 10 minutes per replica
_last_sweep_at = 0.0


def sweep_stale_views(ws: Any, catalog: str, schema: str, warehouse_id: str, ttl_seconds: int = _VIEW_TTL_SECONDS) -> int:
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


def _maybe_sweep_stale_views() -> None:
    """Run the stale-view sweep at most once per interval. Never raises."""
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


# ── Jobs API — async submit + poll ───────────────────────────────────


def _get_runner_job_id() -> int:
    """Get the pre-deployed runner job ID from environment."""
    job_id = os.environ.get("DQX_RUNNER_JOB_ID")
    if not job_id:
        raise RuntimeError("DQX_RUNNER_JOB_ID not set. Deploy the bundle first: databricks bundle deploy")
    return int(job_id)


def submit_job_async(operation: str, params: dict[str, Any]) -> int:
    """Submit a DQX operation and return the run_id immediately (non-blocking).

    Stateless by design: the runner job drops its own temp view (params['view_name'])
    in a finally and echoes params['table_name'] into its result, so no per-run state
    is kept in the server process. This means a restart or a poll landing on a
    different app replica does not leak views or lose context.

    Args:
        operation: The DQX operation name (e.g. 'profile_table', 'run_checks').
        params: Dict of parameters to pass to the notebook as JSON. For table-backed
            operations, include 'view_name' (dropped by the runner) and 'table_name'
            (echoed back in the result).

    Returns:
        The Databricks job run_id.
    """
    # Opportunistically reap orphaned temp views (throttled). Backstop for views whose
    # job never started or was killed before its own cleanup ran.
    _maybe_sweep_stale_views()

    ws = _get_sp_client()
    job_id = _get_runner_job_id()

    logger.info(f"Submitting async job {job_id}: operation={operation}")

    wait = ws.jobs.run_now(
        job_id=job_id,
        notebook_params={
            "operation": operation,
            "params": json.dumps(params),
        },
    )

    run_id = wait.run_id
    logger.info(f"Job submitted: run_id={run_id}")
    return run_id


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
        Dict with 'status' ('running', 'completed', 'failed') and optionally 'result'.
    """
    ws = _get_sp_client()

    run = ws.jobs.get_run(run_id)
    life_cycle = run.state.life_cycle_state.value if run.state and run.state.life_cycle_state else "UNKNOWN"
    if life_cycle in ("PENDING", "RUNNING", "QUEUED", "BLOCKED"):
        return {"status": "running", "run_id": run_id, "message": "Job is still running. Call get_run_result again."}

    # No local cleanup here: the runner job drops its own temp view, and any orphans are
    # reaped by the sweeper. This keeps get_run_status stateless and replica-independent.

    # Check for failure
    result_state = run.state.result_state.value if run.state and run.state.result_state else "UNKNOWN"
    if result_state != "SUCCESS":
        error_msg = run.state.state_message if run.state else "Unknown error"
        run_url = run.run_page_url or ""
        return {
            "status": "failed",
            "run_id": run_id,
            "error": f"Job failed: {error_msg}. Debug at: {run_url}",
        }

    # Extract notebook output
    task_run_id = run.tasks[0].run_id if run.tasks else run.run_id
    output = ws.jobs.get_run_output(task_run_id)

    if output.notebook_output and output.notebook_output.result:
        result = json.loads(output.notebook_output.result)
        # table_name is echoed by the runner into the result, so nothing to re-attach here.
        return {"status": "completed", "run_id": run_id, "result": result}

    error_msg = output.error or "No output from notebook"
    run_url = run.run_page_url or ""
    return {
        "status": "failed",
        "run_id": run_id,
        "error": f"No output: {error_msg}. Debug at: {run_url}",
    }
