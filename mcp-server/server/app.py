"""
FastAPI application configuration for the DQX MCP server.

Sets up the FastMCP server, loads DQX tools, and creates a combined
FastAPI application that serves both MCP protocol routes and custom endpoints.
"""

from fastapi import FastAPI, Request
from fastmcp import FastMCP

from .tools import load_tools
from .utils import header_store

mcp_server = FastMCP(name="mcp-dqx")

load_tools(mcp_server)

mcp_app = mcp_server.http_app()

app = FastAPI(
    title="DQX MCP Server",
    description="DQX MCP Server - AI agent tools for data quality checks",
    version="0.1.0",
    lifespan=mcp_app.lifespan,
)


@app.get("/", include_in_schema=False)
async def serve_index():
    return {"message": "DQX MCP Server is running", "status": "healthy"}


combined_app = FastAPI(
    title="DQX MCP Server",
    routes=[
        *mcp_app.routes,
        *app.routes,
    ],
    lifespan=mcp_app.lifespan,
)


@combined_app.middleware("http")
async def capture_headers(request: Request, call_next):
    """Middleware to capture request headers for authentication."""
    header_store.set(dict(request.headers))
    return await call_next(request)
