from functools import cached_property

from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.contexts.cli_context import CliContext


class WorkspaceContext(CliContext):
    """
    WorkspaceContext class that extends CliContext to provide workspace-specific functionality.
    """

    def __init__(
        self, ws: WorkspaceClient, named_parameters: dict[str, str] | None = None, install_folder: str | None = None
    ):
        super().__init__(named_parameters, install_folder)
        self._ws = ws

    @cached_property
    def workspace_client(self) -> WorkspaceClient:
        """Returns the WorkspaceClient instance."""
        return self._ws
