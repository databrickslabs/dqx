"""
Main entry point for the DQX MCP server application.
"""

import logging
import os
import sys

import uvicorn

logger = logging.getLogger(__name__)


def main():
    # Add parent dir to path so 'server' package is importable
    parent = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if parent not in sys.path:
        sys.path.insert(0, parent)

    # Configure logging once 'server' is importable (the script is run as 'server/main.py',
    # so the package is only on sys.path after the insert above).
    from server.utils import configure_logging

    configure_logging()

    logger.debug(f"sys.path[0]={parent}")
    logger.debug(f"cwd={os.getcwd()}")
    logger.debug(f"files in parent: {os.listdir(parent)}")
    logger.debug(f"files in server: {os.listdir(os.path.join(parent, 'server'))}")

    from server.app import combined_app

    port = int(os.getenv("DATABRICKS_APP_PORT", "8000"))
    uvicorn.run(
        combined_app,
        host="0.0.0.0",
        port=port,
    )


if __name__ == "__main__":
    main()
