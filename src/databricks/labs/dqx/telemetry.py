import logging
from collections.abc import Callable
from databricks.sdk import WorkspaceClient
from databricks.sdk.config import with_user_agent_extra
from databricks.sdk.errors import DatabricksError


logger = logging.getLogger(__name__)


def log_telemetry(ws: WorkspaceClient, key: str, value: str) -> None:
    """
    Trace specific telemetry information in the Databricks workspace by setting user agent extra info.

    Args:
        ws: WorkspaceClient
        key: telemetry key to log
        value: telemetry value to log
    """
    with_user_agent_extra(key, value)
    try:
        ws.current_user.me()
    except DatabricksError as e:
        # support local execution
        logger.debug(f"Databricks workspace is not available: {e}")


def telemetry_logger(key: str, value: str) -> Callable:
    """
    Decorator to log telemetry for method calls.

    Usage: @telemetry_logger("telemetry_key", "telemetry_value")

    Args:
        key: Telemetry key to log
        value: Telemetry value to log
    """

    def decorator(func: Callable) -> Callable:
        def wrapper(self, *args, **kwargs):
            """
            Expecting the workspace client be available in the calling class as 'ws' or 'workspace_client' attribute.
            """
            if hasattr(self, 'ws'):  # requires workspace client to be set
                log_telemetry(self.ws, key, value)
            elif hasattr(self, 'workspace_client'):  # requires workspace client to be set
                log_telemetry(self.workspace_client, key, value)
            else:
                raise AttributeError(
                    f"Workspace client not found on {self.__class__.__name__}. "
                    f"Telemetry decorator requires 'ws' or 'workspace_client' attribute. "
                    f"Make sure your class has a workspace client defined."
                )
            return func(self, *args, **kwargs)

        return wrapper

    return decorator
