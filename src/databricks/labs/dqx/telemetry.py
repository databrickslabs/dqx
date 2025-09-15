import logging
from collections.abc import Callable
from databricks.sdk import WorkspaceClient
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
    if not hasattr(ws, "config"):
        # support local execution
        logger.warning("Workspace client is not configured.")
        return

    new_config = ws.config.copy().with_user_agent_extra(key, value)
    logger.debug(f"Added User-Agent extra {key}={value}")

    # Recreate the WorkspaceClient from the same type to preserve type information
    ws = type(ws)(config=new_config)

    try:
        ws.get_workspace_id()
    except DatabricksError as e:
        # support local execution
        logger.debug(f"Databricks workspace is not available: {e}")


def telemetry_logger(key: str, value: str, workspace_client_attr: str = "ws") -> Callable:
    """
    Decorator to log telemetry for method calls.

    Usage:
        @telemetry_logger("telemetry_key", "telemetry_value")  # Uses "ws" attribute for workspace client by default
        @telemetry_logger("telemetry_key", "telemetry_value", "my_ws_client")  # Custom attribute

    Args:
        key: Telemetry key to log
        value: Telemetry value to log
        workspace_client_attr: Name of the workspace client attribute on the class (defaults to "ws")
    """

    def decorator(func: Callable) -> Callable:
        def wrapper(self, *args, **kwargs):
            if hasattr(self, workspace_client_attr):
                workspace_client = getattr(self, workspace_client_attr)
                log_telemetry(workspace_client, key, value)
            else:
                raise AttributeError(
                    f"Workspace client attribute '{workspace_client_attr}' not found on {self.__class__.__name__}. "
                    f"Make sure your class has the specified workspace client attribute."
                )
            return func(self, *args, **kwargs)

        return wrapper

    return decorator
