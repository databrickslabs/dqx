from databricks.labs.blueprint.installation import Installation
from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.config import RunConfig, WorkspaceConfig


class RunConfigLoader:
    """
    Class to handle loading of run configurations from the installation.
    """

    def __init__(self, workspace_client: WorkspaceClient):
        self.ws = workspace_client

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
            run_config_name: name of the run configuration to use
            install_folder: optional installation folder
            assume_user: if True, assume user installation
            product_name: name of the product
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
            installation = Installation(self.ws, product_name, install_folder=install_folder)
            assume_user = False
        elif assume_user:
            installation = Installation.assume_user_home(self.ws, product_name)
        else:
            installation = Installation.assume_global(self.ws, product_name)

        # verify the installation
        installation.current(self.ws, product_name, assume_user=assume_user)
        return installation

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
