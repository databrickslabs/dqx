"""
FastMCP application configuration for the DQX MCP server.

Key learnings applied from biomni-mcp-server:
- Use stateless_http=True + json_response=True for Genie Code compatibility
- Add CORSMiddleware for OPTIONS preflight requests from workspace origin
- Use pure ASGI middleware (not BaseHTTPMiddleware) to avoid streaming timeouts
- OBO: extract X-Forwarded-Access-Token, store in contextvars, create per-request client lazily
"""

import logging

from fastmcp import FastMCP
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from .tools import load_tools
from .utils import OBOAuthMiddleware

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

# Add CORS middleware — Genie Code sends OPTIONS preflight from workspace origin
combined_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True,
)
