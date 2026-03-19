"""
Main entry point for the DQX MCP server application.
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
    # Add parent dir to path so 'server' package is importable
    parent = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if parent not in sys.path:
        sys.path.insert(0, parent)
    print(f"sys.path[0]={parent}", flush=True)
    print(f"cwd={os.getcwd()}", flush=True)
    print(f"files in parent: {os.listdir(parent)}", flush=True)
    print(f"files in server: {os.listdir(os.path.join(parent, 'server'))}", flush=True)

    from server.app import combined_app

    port = int(os.getenv("DATABRICKS_APP_PORT", "8000"))
    uvicorn.run(
        combined_app,
        host="0.0.0.0",
        port=port,
    )


if __name__ == "__main__":
    main()
