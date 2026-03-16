"""
Main entry point for the DQX MCP server application.

Configured as the entry point in pyproject.toml, so you can run the server
using: uv run mcp-dqx
"""

import argparse

import uvicorn


def main():
    parser = argparse.ArgumentParser(description="Start the DQX MCP server")
    parser.add_argument(
        "--port", type=int, default=8000, help="Port to run the server on (default: 8000)"
    )
    args = parser.parse_args()

    uvicorn.run(
        "server.app:combined_app",
        host="0.0.0.0",
        port=args.port,
    )
