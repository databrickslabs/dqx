from functools import cached_property

from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.contexts.application import CliContext


class WorkspaceContext(CliContext):
    """
    WorkspaceContext class that extends CliContext to provide workspace-specific functionality.

    :param ws: The WorkspaceClient instance to use for accessing the workspace.
    :param named_parameters: Optional dictionary of named parameters.
    """

    def __init__(self, ws: WorkspaceClient, named_parameters: dict[str, str] | None = None):
        super().__init__(named_parameters)
        self._ws = ws

    @cached_property
    def workspace_client(self) -> WorkspaceClient:
        """Returns the WorkspaceClient instance."""
        return self._ws
