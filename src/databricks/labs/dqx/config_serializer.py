from databricks.labs.blueprint.installation import Installation
from databricks.labs.dqx.config import RunConfig, WorkspaceConfig
from databricks.labs.dqx.installer.mixins import InstallationMixin


class ConfigSerializer(InstallationMixin):
    """
    Class to handle loading of configuration from the installation.
    """

    def load_config(
        self, assume_user: bool = True, product_name: str = "dqx", install_folder: str | None = None
    ) -> WorkspaceConfig:
        """
        Load workspace configuration from the installation. The workspace config contains all run configs.

        Args:
            product_name: name of the product
            assume_user: if True, assume user installation
            install_folder: Custom workspace installation folder. Required if DQX is installed in a custom folder.
        """
        installation = self._get_installation(product_name, assume_user, install_folder)
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
            run_config_name: Name of the run configuration to use, e.g. input table or job name.
            product_name: Product/installation identifier used to resolve installation paths (not used if install_folder is provided)
            assume_user: Whether to assume a per-user installation when loading the run configuration (not used if install_folder is provided)
            install_folder: Custom workspace installation folder. Required if DQX is installed in a custom folder.
        """
        installation = self._get_installation(product_name, assume_user, install_folder)
        return self._load_run_config(installation, run_config_name)

    def save_config(
        self,
        config: WorkspaceConfig,
        install_folder: str | None = None,
        assume_user: bool = True,
        product_name: str = "dqx",
    ) -> None:
        """
        Load configuration from the installation.

        Args:
            config: Workspace config object
            product_name: Product/installation identifier used to resolve installation paths (not used if install_folder is provided)
            assume_user: Whether to assume a per-user installation when loading the run configuration (not used if install_folder is provided)
            install_folder: Custom workspace installation folder. Required if DQX is installed in a custom folder.
        """
        installation = self._get_installation(product_name, assume_user, install_folder)
        return installation.save(config)

    @staticmethod
    def _load_run_config(installation: Installation, run_config_name: str | None) -> RunConfig:
        """
        Load run configuration from the installation.

        Args:
            installation: the installation object.
            run_config_name: name of the run configuration to use, e.g. input table or job name.
        """
        config = installation.load(WorkspaceConfig)
        return config.get_run_config(run_config_name)
