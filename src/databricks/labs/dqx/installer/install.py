import logging
import dataclasses
import os
import webbrowser
from typing import Any
from functools import cached_property
from requests.exceptions import ConnectionError as RequestsConnectionError
import databricks

from databricks.labs.blueprint.entrypoint import get_logger, is_in_debug
from databricks.labs.blueprint.installation import Installation, SerdeError
from databricks.labs.blueprint.installer import InstallState
from databricks.labs.blueprint.parallel import ManyError, Threads
from databricks.labs.blueprint.tui import Prompts
from databricks.labs.blueprint.upgrades import Upgrades
from databricks.labs.blueprint.wheels import ProductInfo, WheelsV2
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import with_user_agent_extra
from databricks.sdk.errors import (
    InvalidParameterValue,
    NotFound,
    PermissionDenied,
)

from databricks.labs.dqx.installer.config_provider import ConfigProvider
from databricks.labs.dqx.installer.dashboard_installer import DashboardInstaller
from databricks.labs.dqx.installer.mixins import InstallationMixin
from databricks.labs.dqx.installer.version_checker import VersionChecker
from databricks.labs.dqx.installer.warehouse_installer import WarehouseInstaller
from databricks.labs.dqx.installer.workflow_installer import WorkflowDeployment
from databricks.labs.dqx.workflows_runner import WorkflowsRunner

from databricks.labs.dqx.__about__ import __version__
from databricks.labs.dqx.config import WorkspaceConfig
from databricks.labs.dqx.contexts.workspace_context import WorkspaceContext


logger = logging.getLogger(__name__)
with_user_agent_extra("cmd", "install")


class WorkspaceInstaller(WorkspaceContext, InstallationMixin):
    """
    Installer for DQX workspace. Orchestrates install flow (config, version checks, upgrades, dependency wiring).

    Args:
        environ: Optional dictionary of environment variables.
        ws: The WorkspaceClient instance.
        install_folder: Optional custom workspace folder path for installation.
    """

    def __init__(self, ws: WorkspaceClient, environ: dict[str, str] | None = None, install_folder: str | None = None):
        super().__init__(ws, install_folder=install_folder)
        if not environ:
            environ = dict(os.environ.items())

        self._force_install = environ.get("DQX_FORCE_INSTALL")
        self._install_folder = install_folder

        if "DATABRICKS_RUNTIME_VERSION" in environ:
            msg = "WorkspaceInstaller is not supposed to be executed in Databricks Runtime"
            raise SystemExit(msg)

    @cached_property
    def upgrades(self):
        """
        Returns the Upgrades instance for the product.

        Returns:
            An Upgrades instance.
        """
        return Upgrades(self.product_info, self.installation)

    @cached_property
    def installation(self):
        """
        Returns the current installation for the product.

        Returns:
            An Installation instance.

        Raises:
            NotFound: If the installation is not found.
        """
        if self._install_folder:
            return self._get_installation(
                product_name=self.product_info.product_name(), install_folder=self._install_folder
            )
        try:
            return self.product_info.current_installation(self.workspace_client)
        except NotFound:
            if self._force_install == "global":
                return Installation.assume_global(self.workspace_client, self.product_info.product_name())
            return Installation.assume_user_home(self.workspace_client, self.product_info.product_name())

    def run(
        self,
        default_config: WorkspaceConfig | None = None,
    ) -> WorkspaceConfig:
        """
        Runs the installation process.

        Args:
            default_config: Optional default configuration.

        Returns:
            The final WorkspaceConfig used for the installation.

        Raises:
            ManyError: If multiple errors occur during installation.
            TimeoutError: If a timeout occurs during installation.
        """
        logger.info(f"Installing DQX v{self.product_info.version()}")
        try:
            config = self.configure(default_config)
            tasks = WorkflowsRunner.all(config).tasks()

            workflows_deployment = WorkflowDeployment(
                config,
                self.installation,
                self.install_state,
                self.workspace_client,
                self.wheels,
                self.product_info,
                tasks,
            )

            warehouse_configurator = WarehouseInstaller(self.workspace_client, self.prompts)
            installation_service = InstallationService(
                config,
                self.installation,
                self.install_state,
                self.workspace_client,
                workflows_deployment,
                warehouse_configurator,
                self.prompts,
                self.product_info,
            )
            installation_service.run()
        except ManyError as err:
            if len(err.errs) == 1:
                raise err.errs[0] from None
            raise err
        except TimeoutError as err:
            if isinstance(err.__cause__, RequestsConnectionError):
                logger.warning(
                    f"Cannot connect with {self.workspace_client.config.host} see "
                    f"https://github.com/databrickslabs/dqx#network-connectivity-issues for help: {err}"
                )
            raise err
        return config

    def open_config_in_browser(self, config):
        ws_file_url = self.installation.workspace_link(config.__file__)
        if self.prompts.confirm(f"Open config file in the browser and continue installing? {ws_file_url}"):
            webbrowser.open(ws_file_url)

    def replace_config(self, **changes: Any) -> WorkspaceConfig | None:
        """
        Persist the list of workspaces where UCX is successfully installed in the config
        """
        try:
            config = self.installation.load(WorkspaceConfig)
            new_config = dataclasses.replace(config, **changes)
            self.installation.save(new_config)
        except (PermissionDenied, NotFound, ValueError):
            logger.warning(f"Failed to replace config for {self.workspace_client.config.host}")
            new_config = None
        return new_config

    def configure_warehouse(self) -> str:
        return WarehouseInstaller(self.workspace_client, self.prompts).create()

    def configure(self, default_config: WorkspaceConfig | None = None) -> WorkspaceConfig:
        """
        Configures the workspace.

        Notes:
        * Connection errors are not handled within this configure method.

        Args:
            default_config: Optional default configuration.

        Returns:
            The final WorkspaceConfig used for the installation.

        Raises:
            NotFound: If the previous installation is not found.
            RuntimeWarning: If the existing installation is corrupted.
        """
        try:
            config = self.installation.load(WorkspaceConfig)
            VersionChecker(self.product_info, self.installation, self.prompts).compare_and_prompt_upgrade()
            if self._confirm_force_install():
                return self._configure_new_installation(default_config)
            self._apply_upgrades(config)
            return config
        except NotFound as err:
            logger.debug(f"Cannot find previous installation: {err}")
        except (PermissionDenied, SerdeError, ValueError, AttributeError):
            logger.warning(f"Existing installation at {self.installation.install_folder()} is corrupted. Skipping...")
        return self._configure_new_installation(default_config)

    def _is_testing(self):
        return self.product_info.product_name() != "dqx"

    def _prompt_for_new_installation(self) -> WorkspaceConfig:
        configurator = WarehouseInstaller(self.workspace_client, self.prompts)
        prompter = ConfigProvider(self.prompts, configurator, logger)
        return prompter.prompt_new_installation(self._install_folder)

    def _confirm_force_install(self) -> bool:
        if not self._force_install:
            return False

        msg = "DQX is already installed on this workspace. Do you want to create a new installation?"
        if not self.prompts.confirm(msg):
            raise RuntimeWarning("DQX is already installed, but no confirmation")
        if not self.installation.is_global() and self._force_install == "global":
            # Logic for forced global install over user install
            raise databricks.sdk.errors.NotImplemented("Migration needed. Not implemented yet.")
        if self.installation.is_global() and self._force_install == "user":
            # Logic for forced user install over global install
            self.replace(
                installation=Installation.assume_user_home(self.workspace_client, self.product_info.product_name())
            )
            return True
        return False

    def _apply_upgrades(self, config):
        try:
            self.upgrades.apply(self.workspace_client)
            self.open_config_in_browser(config)
        except (InvalidParameterValue, NotFound) as err:
            logger.warning(f"Installed version is too old: {err}")

    def _configure_new_installation(self, config: WorkspaceConfig | None = None) -> WorkspaceConfig:
        if config is None:
            config = self._prompt_for_new_installation()
        self.installation.save(config)
        self.open_config_in_browser(config)
        return config


class InstallationService:
    """
    Perform operations on a concrete installation instance (create jobs/dashboards, uninstall, cleanup).
    """

    def __init__(
        self,
        config: WorkspaceConfig,
        installation: Installation,
        install_state: InstallState,
        ws: WorkspaceClient,
        workflow_installer: WorkflowDeployment,
        warehouse_configurator: WarehouseInstaller,
        prompts: Prompts,
        product_info: ProductInfo,
    ):
        self._config = config
        self._installation = installation
        self._install_state = install_state
        self._workflow_installer = workflow_installer
        self._warehouse_configurator = warehouse_configurator
        self._ws = ws
        self._prompts = prompts
        self._product_info = product_info
        self._wheels = WheelsV2(self._installation, product_info)

    @classmethod
    def current(cls, ws: WorkspaceClient):
        """
        Creates a current WorkspaceInstallation instance based on the current workspace client.

        Args:
            ws: The WorkspaceClient instance.

        Returns:
            A WorkspaceInstallation instance.
        """
        product_info = ProductInfo.from_class(WorkspaceConfig)
        installation = product_info.current_installation(ws)
        install_state = InstallState.from_installation(installation)
        config = installation.load(WorkspaceConfig)
        prompts = Prompts()
        wheels = WheelsV2(installation, product_info)
        tasks = WorkflowsRunner.all(config).tasks()
        workflow_installer = WorkflowDeployment(config, installation, install_state, ws, wheels, product_info, tasks)
        warehouse_configurator = WarehouseInstaller(ws, prompts)

        return cls(
            config,
            installation,
            install_state,
            ws,
            workflow_installer,
            warehouse_configurator,
            prompts,
            product_info,
        )

    @property
    def config(self):
        """
        Returns the configuration of the workspace installation.

        :return: The WorkspaceConfig instance.
        """
        return self._config

    @property
    def install_folder(self):
        """
        Returns the installation install_folder path.

        :return: The installation install_folder path as a string.
        """
        return self._installation.install_folder()

    def run(self) -> bool:
        """
        Runs the workflow installation.

        :return: True if the installation finished successfully, False otherwise.
        """
        logger.info(f"Installing DQX v{self._product_info.version()}")
        install_tasks = [self._workflow_installer.create_jobs, self._create_dashboard]
        Threads.strict("installing components", install_tasks)
        logger.info("Installation completed successfully!")

        return True

    def uninstall(self):
        """
        Uninstalls DQX from the workspace, including project install_folder, dashboards, and jobs.
        """
        if self._prompts and not self._prompts.confirm(
            "Do you want to uninstall DQX from the workspace? "
            "this would remove dqx project install_folder, dashboards, and jobs"
        ):
            return

        logger.info(f"Deleting DQX v{self._product_info.version()} from {self._ws.config.host}")
        try:
            self._installation.files()  # this also deletes the dashboard
        except NotFound:
            logger.error(f"Check if {self._installation.install_folder()} is present")
            return

        self._workflow_installer.remove_jobs()
        default_run_config = self._config.get_run_config()
        self._warehouse_configurator.remove(default_run_config.warehouse_id)
        self._installation.remove()
        logger.info("Uninstalling DQX complete")

    def _create_dashboard(self) -> None:
        installer = DashboardInstaller(
            self._ws, self._installation, self._install_state, self._product_info, self._config
        )
        Threads.strict("Installing dashboards", list(installer.get_create_dashboard_tasks()))


if __name__ == "__main__":
    logger = get_logger(__file__)
    if is_in_debug():
        logging.getLogger("databricks").setLevel(logging.DEBUG)

    installer_prompts = Prompts()
    custom_folder = installer_prompts.question(
        "Enter a workspace path for DQX installation (leave empty to install in user's home or global directory)",
        default="empty",
        valid_regex=r"^(/.*)?$",
    ).strip()

    custom_install_folder = custom_folder if custom_folder and custom_folder != "empty" else None

    workspace_installer = WorkspaceInstaller(
        WorkspaceClient(product="dqx", product_version=__version__), install_folder=custom_install_folder
    )
    workspace_installer.run()
