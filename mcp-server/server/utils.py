"""
Utility functions for the DQX MCP server.

Key patterns:
- Pure ASGI middleware for OBO (not BaseHTTPMiddleware — avoids streaming timeouts)
- Extracts user identity from Databricks Apps proxy headers
- User OBO token creates temp views (UC governance) and runs direct SQL
- SP submits notebook jobs that read through definer's-rights views
- auth_type="pat" to avoid conflict with auto-injected SP env vars
"""

import contextvars
import json
import logging
import os
from datetime import timedelta
from typing import Any

from starlette.types import ASGIApp, Receive, Scope, Send

logger = logging.getLogger(__name__)


# ── OBO Auth via contextvars ──────────────────────────────────────────

# Store user identity per-request from Databricks Apps proxy headers
_user_token_var: contextvars.ContextVar[tuple[str, str] | None] = contextvars.ContextVar(
    "user_token", default=None
)
_user_email_var: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "user_email", default=None
)

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


def get_workspace_client():
    """Get a WorkspaceClient for the current request.

    Returns an OBO client (user's identity) if running in a Databricks App
    with a forwarded token, otherwise falls back to the service principal.
    """
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.config import Config

    token_info = _user_token_var.get(None)
    if token_info is not None:
        host, token = token_info
        cfg = Config(host=host, token=token, auth_type="pat")
        return WorkspaceClient(config=cfg)

    global _sp_client
    if _sp_client is None:
        _sp_client = WorkspaceClient()
    return _sp_client


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
            "No OBO token available. This operation requires a user context "
            "(X-Forwarded-Access-Token header)."
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
        ValueError: If table_name is not fully qualified.
        RuntimeError: If view creation fails (e.g., user lacks SELECT on source table).
    """
    import uuid

    parts = table_name.split(".")
    if len(parts) != 3:
        raise ValueError(
            f"Table name '{table_name}' must be fully qualified (catalog.schema.table)"
        )

    view_id = uuid.uuid4().hex[:12]
    view_fqn = f"{catalog}.{schema}.v_{view_id}"

    logger.info(f"Creating temp view {view_fqn} over {table_name}")
    execute_sql(
        ws,
        f"CREATE VIEW {view_fqn} AS SELECT * FROM {table_name}",
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
    logger.info(f"Dropping temp view {view_fqn}")
    try:
        execute_sql(ws, f"DROP VIEW IF EXISTS {view_fqn}", warehouse_id=warehouse_id)
    except Exception:
        logger.warning(f"Failed to drop temp view {view_fqn}", exc_info=True)


# ── Jobs API — SP submits notebook job ───────────────────────────────

# Cache the notebook path from the pre-deployed job definition
_notebook_path: str | None = None


def _get_notebook_path(ws: Any) -> str:
    """Resolve the runner notebook path from the pre-deployed job definition.

    Reads the job once and caches the notebook path. This avoids hardcoding
    the path, which varies by deployer (e.g. /Workspace/Users/<email>/.bundle/...).
    """
    global _notebook_path
    if _notebook_path is not None:
        return _notebook_path

    job_id = os.environ.get("DQX_RUNNER_JOB_ID")
    if not job_id:
        raise RuntimeError(
            "DQX_RUNNER_JOB_ID not set. Deploy the bundle first: databricks bundle deploy"
        )

    job = ws.jobs.get(int(job_id))
    task = job.settings.tasks[0]
    _notebook_path = task.notebook_task.notebook_path
    logger.info(f"Resolved notebook path from job {job_id}: {_notebook_path}")
    return _notebook_path


def submit_notebook_job(operation: str, params: dict[str, Any]) -> dict[str, Any]:
    """Submit a DQX operation as a notebook job and wait for results.

    The app's service principal submits the job. UC governance is enforced
    before this call via temporary views created with the user's OBO token.

    Args:
        operation: The DQX operation name (e.g. 'profile_table', 'run_checks').
        params: Dict of parameters to pass to the notebook as JSON.

    Returns:
        Parsed JSON dict from dbutils.notebook.exit() output.

    Raises:
        RuntimeError: If the job fails, times out, or no job ID is configured.
    """
    from databricks.sdk.service.jobs import NotebookTask, SubmitTask

    ws = _get_sp_client()
    notebook_path = _get_notebook_path(ws)

    logger.info(f"Submitting notebook job: operation={operation}, notebook={notebook_path}")

    notebook_task = NotebookTask(
        notebook_path=notebook_path,
        base_parameters={
            "operation": operation,
            "params": json.dumps(params),
        },
    )

    task = SubmitTask(
        task_key="dqx_run",
        notebook_task=notebook_task,
    )

    wait = ws.jobs.submit(
        run_name=f"mcp-dqx-{operation}",
        tasks=[task],
    )

    run = wait.result(timeout=timedelta(minutes=10))
    logger.info(f"Job completed: run_id={run.run_id}, url={run.run_page_url}")

    task_run_id = run.tasks[0].run_id if run.tasks else run.run_id
    output = ws.jobs.get_run_output(task_run_id)

    if output.notebook_output and output.notebook_output.result:
        return json.loads(output.notebook_output.result)

    error_msg = output.error or "No output from notebook"
    run_url = run.run_page_url or ""
    raise RuntimeError(
        f"DQX job failed (operation={operation}): {error_msg}. Debug at: {run_url}"
    )


# ── JSON serialization helpers ────────────────────────────────────────


def make_json_safe(value: Any) -> Any:
    """Recursively convert values that are not JSON-serializable (e.g. Decimal, datetime)."""
    import datetime
    from decimal import Decimal

    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: make_json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [make_json_safe(v) for v in value]
    return value
