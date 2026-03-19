"""
Main entry point for the DQX MCP server application.

Configured as the entry point in pyproject.toml, so you can run the server
using: uv run mcp-dqx
"""

import logging
import os
import sys

import uvicorn

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    stream=sys.stdout,
    force=True,
)


def main():
    port = int(os.getenv("DATABRICKS_APP_PORT", "8000"))
    uvicorn.run(
        "server.app:combined_app",
        host="0.0.0.0",
        port=port,
    )
