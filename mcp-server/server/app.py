"""
FastMCP application configuration for the DQX MCP server.

- Use stateless_http=True + json_response=True for Genie Code compatibility
- Add CORSMiddleware for OPTIONS preflight requests from workspace origin
- Use pure ASGI middleware (not BaseHTTPMiddleware) to avoid streaming timeouts
- OBO: extract X-Forwarded-Access-Token, store in contextvars, create per-request client lazily
"""

import logging
import os

from fastmcp import FastMCP
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from .tools import load_tools
from .utils import OBOAuthMiddleware, configure_logging

# Ensure logging is configured even when this module is the entry point (e.g. a direct
# `uvicorn server.app:combined_app` in dev). Idempotent — a no-op once main.py has run.
configure_logging()

logger = logging.getLogger(__name__)

mcp_server = FastMCP(name="mcp-dqx")

load_tools(mcp_server)

# Get the ASGI app — stateless_http + json_response for Genie Code compatibility
combined_app = mcp_server.http_app(
    stateless_http=True,
    json_response=True,
)


# Add health check route
async def health_check(request: Request) -> JSONResponse:
    return JSONResponse({"message": "DQX MCP Server is running", "status": "healthy"})


combined_app.routes.insert(0, Route("/", health_check))

# Add OBO middleware (pure ASGI — no BaseHTTPMiddleware to avoid streaming timeouts)
combined_app.add_middleware(OBOAuthMiddleware)


def _normalize_origin(value: str) -> str | None:
    """Normalize a raw origin/host into a scheme-qualified origin, or None if blank."""
    value = value.strip().rstrip("/")
    if not value:
        return None
    if not value.startswith(("http://", "https://")):
        value = "https://" + value
    return value


def get_allowed_cors_origins(databricks_host: str | None = None, extra_origins: str | None = None) -> list[str]:
    """Build the exact list of browser origins allowed to make credentialed CORS calls.

    A credentialed wildcard over ``*.databricksapps.com`` is unsafe: that domain is
    multi-tenant, so every other customer's app would be an allowed credentialed origin and
    could replay JSON-RPC tool calls. Instead we allow only specific, single-tenant hosts:

    - the **workspace origin** (``DATABRICKS_HOST``) — where the Genie Code browser UI runs and
      from which it issues the cross-origin preflight to this app;
    - any operator-configured origins in ``DQX_MCP_EXTRA_CORS_ORIGINS`` (comma-separated), e.g.
      a second workspace or a ``http://localhost`` dev origin.

    The app's own ``*.databricksapps.com`` URL is **same-origin** for its own page and needs no
    CORS entry. Server-to-server callers (e.g. Claude) are not browsers and are unaffected.
    """
    if databricks_host is None:
        databricks_host = os.environ.get("DATABRICKS_HOST", "")
    if extra_origins is None:
        extra_origins = os.environ.get("DQX_MCP_EXTRA_CORS_ORIGINS", "")

    origins: list[str] = []
    for raw in [databricks_host, *extra_origins.split(",")]:
        origin = _normalize_origin(raw)
        if origin and origin not in origins:
            origins.append(origin)
    return origins


# Exact allow-list (not a regex/wildcard) so allow_credentials=True stays spec-compliant and
# scoped to this tenant. Starlette reflects the matched origin instead of the forbidden "*".
CORS_ALLOWED_ORIGINS = get_allowed_cors_origins()

if CORS_ALLOWED_ORIGINS:
    logger.info(f"CORS: allowing credentialed origins {CORS_ALLOWED_ORIGINS}")
    combined_app.add_middleware(
        CORSMiddleware,
        allow_origins=CORS_ALLOWED_ORIGINS,
        allow_methods=["*"],
        allow_headers=["*"],
        allow_credentials=True,
    )
else:
    # No DATABRICKS_HOST (e.g. unconfigured local run). Skip CORS entirely rather than fall back
    # to a permissive wildcard — browser callers will be blocked until an origin is configured.
    logger.warning(
        "CORS: no allowed origins resolved (DATABRICKS_HOST unset and DQX_MCP_EXTRA_CORS_ORIGINS "
        "empty); browser-based callers will be blocked. Server-to-server callers are unaffected."
    )
