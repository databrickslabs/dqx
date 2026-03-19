import logging
import os

logger = logging.getLogger(__name__)


class SQLAuthentication:
    """Multi-strategy auth resolver for SQL Warehouse connections.

    Resolution order:
    1. OBO bearer token (passed explicitly)
    2. DATABRICKS_TOKEN environment variable
    3. Raise ValueError
    """

    def __init__(self, bearer: str | None = None) -> None:
        self._bearer = bearer

    @property
    def access_token(self) -> str:
        if self._bearer:
            return self._bearer

        env_token = os.environ.get("DATABRICKS_TOKEN")
        if env_token:
            logger.debug("Using DATABRICKS_TOKEN from environment for SQL authentication")
            return env_token

        raise ValueError(
            "No SQL authentication token available. "
            "Provide an OBO bearer token or set the DATABRICKS_TOKEN environment variable."
        )
