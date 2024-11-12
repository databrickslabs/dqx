from functools import cached_property
from pathlib import Path

from databricks.labs.blueprint.installation import Installation
from databricks.labs.lsql.backends import RuntimeBackend, SqlBackend
from databricks.sdk import WorkspaceClient, core
from databricks.labs.dqx.contexts.application import GlobalContext
from databricks.labs.dqx.config import WorkspaceConfig
from databricks.labs.dqx.__about__ import __version__


class RuntimeContext(GlobalContext):
    """
    Returns the WorkspaceClient instance.

    :return: The WorkspaceClient instance.
    """

    @cached_property
    def _config_path(self) -> Path:
        config = self.named_parameters.get("config")
        if not config:
            raise ValueError("config flag is required")
        return Path(config)

    @cached_property
    def config(self) -> WorkspaceConfig:
        """
        Loads and returns the workspace configuration.

        :return: The WorkspaceConfig instance.
        """
        return Installation.load_local(WorkspaceConfig, self._config_path)

    @cached_property
    def connect_config(self) -> core.Config:
        """
        Returns the connection configuration.

        :return: The core.Config instance.
        :raises AssertionError: If the connect configuration is not provided.
        """
        connect = self.config.connect
        assert connect, "connect is required"
        return connect

    @cached_property
    def workspace_client(self) -> WorkspaceClient:
        """
        Returns the WorkspaceClient instance.

        :return: The WorkspaceClient instance.
        """
        return WorkspaceClient(config=self.connect_config, product='dqx', product_version=__version__)

    @cached_property
    def sql_backend(self) -> SqlBackend:
        """
        Returns the SQL backend for the runtime.

        :return: The SqlBackend instance.
        """
        return RuntimeBackend(debug_truncate_bytes=self.connect_config.debug_truncate_bytes)

    @cached_property
    def installation(self) -> Installation:
        """
        Returns the installation instance for the runtime.

        :return: The Installation instance.
        """
        install_folder = self._config_path.parent.as_posix().removeprefix("/Workspace")
        return Installation(self.workspace_client, "dqx", install_folder=install_folder)

    @cached_property
    def workspace_id(self) -> int:
        """
        Returns the workspace ID.

        :return: The workspace ID as an integer.
        """
        return self.workspace_client.get_workspace_id()

    @cached_property
    def parent_run_id(self) -> int:
        """
        Returns the parent run ID.

        :return: The parent run ID as an integer.
        """
        return int(self.named_parameters["parent_run_id"])
