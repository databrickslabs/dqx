import logging
from typing import Callable

from databricks.sdk import WorkspaceClient
from databricks.sdk.config import with_user_agent_extra
from databricks.sdk.errors import DatabricksError


logger = logging.getLogger(__name__)


def trace(ws: WorkspaceClient, key: str, value: str) -> None:
    """
    Trace specific telemetry information in the Databricks workspace by setting user agent extra info.

    Args:
        ws: WorkspaceClient
        key: key to log
        value: value to log
    """
    with_user_agent_extra(key, value)
    try:
        ws.current_user.me()
    except DatabricksError as e:
        # support local execution
        logger.debug(f"Databricks workspace is not available: {e}")


def log_telemetry(key: str, value: str) -> Callable:
    """
    Decorator to automatically log telemetry for method calls.

    Usage: @log_telemetry("telemetry_key", "telemetry_value")

    Args:
        key: Telemetry key to log
        value: Telemetry value to log
    """

    def decorator(func: Callable) -> Callable:
        def wrapper(self, *args, **kwargs):
            if hasattr(self, 'ws'):  # requires workspace client to be set
                trace(self.ws, key, value)
            elif hasattr(self, 'workspace_client'):  # requires workspace client to be set
                trace(self.workspace_client, key, value)
            else:
                raise AttributeError(
                    f"Workspace client not found on {self.__class__.__name__}. "
                    f"Telemetry decorator requires 'ws' or 'workspace_client' attribute. "
                    f"Make sure your class has a workspace client defined."
                )
            return func(self, *args, **kwargs)

        return wrapper

    return decorator
