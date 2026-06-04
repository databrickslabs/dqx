"""
Utility functions for the DQX MCP server.

Key patterns:
- Pure ASGI middleware for OBO (not BaseHTTPMiddleware — avoids streaming timeouts)
- Lazy WorkspaceClient creation per-request using contextvars
- auth_type="pat" to avoid conflict with auto-injected SP env vars
- Jobs API notebook submission for Spark operations (no Databricks Connect)
"""

import contextvars
import json
import logging
import os
from datetime import timedelta
from typing import Any

from starlette.types import ASGIApp, Receive, Scope, Send

logger = logging.getLogger(__name__)

# Default notebook path — matches databricks bundle deploy location
DEFAULT_NOTEBOOK_PATH = "/Workspace/.bundle/mcp-dqx/notebooks/runner"

# ── OBO Auth via contextvars ──────────────────────────────────────────

_user_token_var: contextvars.ContextVar[tuple[str, str] | None] = contextvars.ContextVar(
    "user_token", default=None
)

_sp_client = None


class OBOAuthMiddleware:
    """Pure ASGI middleware for on-behalf-of authentication.

    Extracts X-Forwarded-Access-Token from Databricks Apps proxy headers
    and stores it in a context var. WorkspaceClient is created lazily
    only when get_workspace_client() is called.

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

        if user_token:
            host = os.environ.get("DATABRICKS_HOST", "")
            _user_token_var.set((host, user_token))
        else:
            _user_token_var.set(None)

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


# ── Jobs API notebook submission ─────────────────────────────────────


def submit_notebook_job(operation: str, params: dict[str, Any]) -> dict[str, Any]:
    """Submit a DQX operation as a serverless notebook job and wait for results.

    Uses the OBO WorkspaceClient so the job runs as the logged-in user.
    Falls back to service principal when no OBO token is present.

    Args:
        operation: The DQX operation name (e.g. 'get_table_schema', 'run_checks').
        params: Dict of parameters to pass to the notebook as JSON.

    Returns:
        Parsed JSON dict from dbutils.notebook.exit() output.

    Raises:
        RuntimeError: If the job fails or times out.
    """
    from databricks.sdk.service.jobs import NotebookTask, SubmitTask

    ws = get_workspace_client()
    notebook_path = os.environ.get("DQX_RUNNER_NOTEBOOK_PATH", DEFAULT_NOTEBOOK_PATH)

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

    # Read notebook output from the task run (not the parent run)
    task_run_id = run.tasks[0].run_id if run.tasks else run.run_id
    output = ws.jobs.get_run_output(task_run_id)

    if output.notebook_output and output.notebook_output.result:
        return json.loads(output.notebook_output.result)

    # Job ran but no notebook output — likely a failure
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
