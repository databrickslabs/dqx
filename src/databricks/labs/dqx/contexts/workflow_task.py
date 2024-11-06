from functools import cached_property
from pathlib import Path

from databricks.labs.blueprint.installation import Installation
from databricks.labs.lsql.backends import RuntimeBackend, SqlBackend
from databricks.sdk import WorkspaceClient, core
from databricks.labs.dqx.contexts.application import GlobalContext
from databricks.labs.dqx.config import WorkspaceConfig
from databricks.labs.dqx.__about__ import __version__


class RuntimeContext(GlobalContext):
    @cached_property
    def _config_path(self) -> Path:
        config = self.named_parameters.get("config")
        if not config:
            raise ValueError("config flag is required")
        return Path(config)

    @cached_property
    def config(self) -> WorkspaceConfig:
        return Installation.load_local(WorkspaceConfig, self._config_path)

    @cached_property
    def connect_config(self) -> core.Config:
        connect = self.config.connect
        assert connect, "connect is required"
        return connect

    @cached_property
    def workspace_client(self) -> WorkspaceClient:
        return WorkspaceClient(config=self.connect_config, product='dqx', product_version=__version__)

    @cached_property
    def sql_backend(self) -> SqlBackend:
        return RuntimeBackend(debug_truncate_bytes=self.connect_config.debug_truncate_bytes)

    @cached_property
    def installation(self) -> Installation:
        install_folder = self._config_path.parent.as_posix().removeprefix("/Workspace")
        return Installation(self.workspace_client, "dqx", install_folder=install_folder)

    @cached_property
    def workspace_id(self) -> int:
        return self.workspace_client.get_workspace_id()

    @cached_property
    def parent_run_id(self) -> int:
        return int(self.named_parameters["parent_run_id"])
