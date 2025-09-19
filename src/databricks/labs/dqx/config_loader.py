from databricks.labs.blueprint.installation import Installation
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

from databricks.labs.dqx.config import RunConfig, WorkspaceConfig


class RunConfigLoader:
    """
    Class to handle loading of configuration from the installation.
    """

    def __init__(self, workspace_client: WorkspaceClient):
        self.ws = workspace_client

    def load_config(self, assume_user: bool = True, product_name: str = "dqx") -> WorkspaceConfig:
        """
        Load workspace configuration from the installation. The workspace config contains all run configs.

        Args:
            assume_user: if True, assume user installation
            product_name: name of the product
        """
        installation = self.get_installation(assume_user, product_name)
        return installation.load(WorkspaceConfig)

    def load_run_config(
        self,
        run_config_name: str | None,
        install_folder: str | None = None,
        assume_user: bool = True,
        product_name: str = "dqx",
    ) -> RunConfig:
        """
        Load run configuration from the installation.

        Args:
            run_config_name: Name of the run configuration to use.
            install_folder: Custom workspace installation folder. Required if DQX is installed in a custom folder.
            assume_user: Whether to assume a per-user installation when loading the run configuration (True as default, skipped if install_folder is provided).
            product_name: Product/installation identifier used to resolve installation paths for config loading in install_folder is not provided ("dqx" as default).
        """
        installation = self.get_installation(assume_user, product_name, install_folder)
        return self._load_run_config(installation, run_config_name)

    def get_installation(self, assume_user: bool, product_name: str, install_folder: str | None = None) -> Installation:
        """
        Get the installation for the given product name.

        Args:
            assume_user: if True, assume user installation
            product_name: name of the product
            install_folder: optional installation folder
        """

        if install_folder:
            installation = self.get_custom_installation(self.ws, product_name, install_folder)
            return installation

        if assume_user:
            installation = Installation.assume_user_home(self.ws, product_name)
        else:
            installation = Installation.assume_global(self.ws, product_name)

        installation.current(self.ws, product_name, assume_user=assume_user)
        return installation

    @staticmethod
    def get_custom_installation(ws: WorkspaceClient, product_name: str, install_folder: str) -> Installation:
        """
        Creates an Installation instance for a custom installation folder, similar to assume_user_home and assume_global.
        This ensures the custom folder is created in the workspace when the installation is accessed.

        Args:
            ws: Databricks SDK `WorkspaceClient`
            product_name: The product name
            install_folder: The custom installation folder path

        Returns:
            An Installation instance for the custom folder
        """
        try:
            ws.workspace.get_status(install_folder)
        except NotFound:
            ws.workspace.mkdirs(install_folder)

        return Installation(ws, product_name, install_folder=install_folder)

    @staticmethod
    def _load_run_config(installation: Installation, run_config_name: str | None) -> RunConfig:
        """
        Load run configuration from the installation.

        Args:
            installation: the installation object
            run_config_name: name of the run configuration to use
        """
        config = installation.load(WorkspaceConfig)
        return config.get_run_config(run_config_name)
