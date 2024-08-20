from functools import cached_property

from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.contexts.application import CliContext


class WorkspaceContext(CliContext):
    def __init__(self, ws: WorkspaceClient, named_parameters: dict[str, str] | None = None):
        super().__init__(named_parameters)
        self._ws = ws

    @cached_property
    def workspace_client(self) -> WorkspaceClient:
        return self._ws
