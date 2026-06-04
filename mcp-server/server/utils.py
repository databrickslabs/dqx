"""
Utility functions for the DQX MCP server.

Key patterns:
- Pure ASGI middleware for OBO (not BaseHTTPMiddleware — avoids streaming timeouts)
- Extracts user identity from Databricks Apps proxy headers (X-Forwarded-Email)
- SP submits notebook jobs with run_as=user for OBO (Apps can't grant 'jobs' scope)
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

# Default notebook path — matches databricks bundle deploy location
DEFAULT_NOTEBOOK_PATH = "/Workspace/.bundle/mcp-dqx/notebooks/runner"

# ── OBO Auth via contextvars ──────────────────────────────────────────

# Store user identity per-request from Databricks Apps proxy headers
_user_token_var: contextvars.ContextVar[tuple[str, str] | None] = contextvars.ContextVar(
    "user_token", default=None
)
_user_email_var: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "user_email", default=None
)

# Service principal client singleton
_sp_client = None


class OBOAuthMiddleware:
    """Pure ASGI middleware for on-behalf-of authentication.

    Extracts user identity from Databricks Apps proxy headers:
    - X-Forwarded-Access-Token: user's OBO token
    - X-Forwarded-Email: user's email (used for run_as in job submission)

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


def _get_sp_client():
    """Get the service principal WorkspaceClient (always).

    Used for job submission since the OBO token can't have the 'jobs' scope
    in Databricks Apps. The SP has full API access.
    """
    from databricks.sdk import WorkspaceClient

    global _sp_client
    if _sp_client is None:
        _sp_client = WorkspaceClient()
    return _sp_client


# ── Jobs API notebook submission ─────────────────────────────────────


def submit_notebook_job(operation: str, params: dict[str, Any]) -> dict[str, Any]:
    """Submit a DQX operation as a serverless notebook job and wait for results.

    Uses the app's service principal to submit the job (OBO tokens can't call
    the Jobs API — 'jobs' is not a valid App scope). Passes run_as with the
    user's email so the job runs under the user's identity, preserving
    Unity Catalog governance.

    Falls back to SP identity when no user email is present (e.g., direct API call).

    Args:
        operation: The DQX operation name (e.g. 'get_table_schema', 'run_checks').
        params: Dict of parameters to pass to the notebook as JSON.

    Returns:
        Parsed JSON dict from dbutils.notebook.exit() output.

    Raises:
        RuntimeError: If the job fails or times out.
    """
    from databricks.sdk.service.jobs import JobRunAs, NotebookTask, SubmitTask

    # Always use SP for job submission (OBO token lacks 'jobs' scope)
    ws = _get_sp_client()
    notebook_path = os.environ.get("DQX_RUNNER_NOTEBOOK_PATH", DEFAULT_NOTEBOOK_PATH)
    user_email = _user_email_var.get(None)

    logger.info(
        f"Submitting notebook job: operation={operation}, notebook={notebook_path}, "
        f"run_as={user_email or 'SP (no user)'}"
    )

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

    # run_as user if we have their email, otherwise job runs as SP
    run_as = JobRunAs(user_name=user_email) if user_email else None

    wait = ws.jobs.submit(
        run_name=f"mcp-dqx-{operation}",
        tasks=[task],
        run_as=run_as,
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
