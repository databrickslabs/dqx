from functools import cached_property

from databricks.labs.dqx.contexts.cli_context import CliContext
from databricks.labs.dqx.utils import get_workspace_client
from databricks.sdk import WorkspaceClient


class WorkspaceContext(CliContext):
    """
    WorkspaceContext class that extends CliContext to provide workspace-specific functionality.
    """

    def __init__(self, ws: WorkspaceClient | None = None, named_parameters: dict[str, str] | None = None):
        super().__init__(named_parameters)
        self._ws = ws

    @cached_property
    def workspace_client(self) -> WorkspaceClient:
        """Returns the WorkspaceClient instance."""
        return self._ws or get_workspace_client()
