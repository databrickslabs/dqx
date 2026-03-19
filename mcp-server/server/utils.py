"""
Utility functions for the DQX MCP server.

Key patterns applied from biomni-mcp-server:
- Pure ASGI middleware for OBO (not BaseHTTPMiddleware — avoids streaming timeouts)
- Lazy WorkspaceClient creation per-request using contextvars
- auth_type="pat" to avoid conflict with auto-injected SP env vars
"""

import contextvars
import logging
import os
from typing import Any

from starlette.types import ASGIApp, Receive, Scope, Send

logger = logging.getLogger(__name__)

# ── OBO Auth via contextvars ──────────────────────────────────────────

# Store the user token per-request (not the WorkspaceClient — create lazily)
_user_token_var: contextvars.ContextVar[tuple[str, str] | None] = contextvars.ContextVar(
    "user_token", default=None
)

# Service principal client singleton (fallback when no user token)
_sp_client = None


class OBOAuthMiddleware:
    """Pure ASGI middleware for on-behalf-of authentication.

    Extracts X-Forwarded-Access-Token from Databricks Apps proxy headers
    and stores it in a context var. WorkspaceClient is created lazily
    only when get_workspace_client() is called (zero overhead for tools
    that don't need it).

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

    Uses auth_type="pat" to avoid conflict with auto-injected
    DATABRICKS_CLIENT_ID/SECRET env vars in the App container.
    """
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.config import Config

    token_info = _user_token_var.get(None)
    if token_info is not None:
        host, token = token_info
        cfg = Config(host=host, token=token, auth_type="pat")
        return WorkspaceClient(config=cfg)

    # Fallback: service principal (auto-detected from env vars)
    global _sp_client
    if _sp_client is None:
        _sp_client = WorkspaceClient()
    return _sp_client


# ── Spark + DQX component factories ──────────────────────────────────
# Note: Spark session via databricks-connect is shared (serverless).
# The WorkspaceClient used for profiler/generator/engine should be
# per-request for OBO, but Spark itself is connection-pooled.

_spark = None
_profiler = None
_generator = None
_engine = None


def _get_spark():
    global _spark, _profiler, _generator, _engine
    if _spark is not None:
        try:
            _spark.sql("SELECT 1")
        except Exception:
            logger.warning("Spark session expired, creating a new one")
            _spark = None
            _profiler = None
            _generator = None
            _engine = None
    if _spark is None:
        from databricks.connect import DatabricksSession

        _spark = DatabricksSession.builder.serverless(True).getOrCreate()
    return _spark


def _get_profiler():
    from databricks.labs.dqx.profiler.profiler import DQProfiler

    return DQProfiler(workspace_client=get_workspace_client(), spark=_get_spark())


def _get_generator():
    from databricks.labs.dqx.profiler.generator import DQGenerator

    return DQGenerator(workspace_client=get_workspace_client(), spark=_get_spark())


def _get_engine():
    from databricks.labs.dqx.engine import DQEngine

    return DQEngine(workspace_client=get_workspace_client(), spark=_get_spark())


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


def compute_rule_summary(invalid_df) -> list[dict]:
    """Aggregate per-rule error/warning counts from the invalid DataFrame."""
    import pyspark.sql.functions as F

    summary: dict[str, dict[str, int]] = {}

    for col_name in ("_errors", "_warnings"):
        if col_name not in invalid_df.columns:
            continue
        exploded = invalid_df.select(F.explode(F.col(col_name)).alias("item"))
        rows = exploded.groupBy("item.name").count().collect()
        for row in rows:
            rule_name = row["name"] or "unknown"
            if rule_name not in summary:
                summary[rule_name] = {"error_count": 0, "warning_count": 0}
            if col_name == "_errors":
                summary[rule_name]["error_count"] = row["count"]
            else:
                summary[rule_name]["warning_count"] = row["count"]

    return [{"rule_name": name, **counts} for name, counts in summary.items()]
