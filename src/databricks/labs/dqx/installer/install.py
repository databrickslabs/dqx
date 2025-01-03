import logging
import os
import webbrowser
from functools import cached_property
from requests.exceptions import ConnectionError as RequestsConnectionError
import databricks

from databricks.labs.blueprint.entrypoint import get_logger, is_in_debug
from databricks.labs.blueprint.installation import Installation, SerdeError
from databricks.labs.blueprint.installer import InstallState
from databricks.labs.blueprint.parallel import ManyError, Threads
from databricks.labs.blueprint.tui import Prompts
from databricks.labs.blueprint.upgrades import Upgrades
from databricks.labs.blueprint.wheels import ProductInfo, Version
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import with_user_agent_extra
from databricks.sdk.errors import InvalidParameterValue, NotFound, PermissionDenied
from databricks.labs.dqx.installer.workflows_installer import WorkflowsDeployment
from databricks.labs.dqx.runtime import Workflows

from databricks.labs.dqx.__about__ import __version__
from databricks.labs.dqx.config import WorkspaceConfig, RunConfig
from databricks.labs.dqx.contexts.workspace_cli import WorkspaceContext
from databricks.labs.dqx.utils import extract_major_minor

logger = logging.getLogger(__name__)
with_user_agent_extra("cmd", "install")


class WorkspaceInstaller(WorkspaceContext):
    """
    Installer for DQX workspace.

    :param ws: The WorkspaceClient instance.
    :param environ: Optional dictionary of environment variables.
    """

    def __init__(self, ws: WorkspaceClient, environ: dict[str, str] | None = None):
        super().__init__(ws)
        if not environ:
            environ = dict(os.environ.items())

        self._force_install = environ.get("DQX_FORCE_INSTALL")

        if "DATABRICKS_RUNTIME_VERSION" in environ:
            msg = "WorkspaceInstaller is not supposed to be executed in Databricks Runtime"
            raise SystemExit(msg)

        self._tasks = Workflows.all().tasks()

    @cached_property
    def upgrades(self):
        """
        Returns the Upgrades instance for the product.

        :return: An Upgrades instance.
        """
        return Upgrades(self.product_info, self.installation)

    @cached_property
    def installation(self):
        """
        Returns the current installation for the product.

        :return: An Installation instance.
        :raises NotFound: If the installation is not found.
        """
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

        :param default_config: Optional default configuration.
        :return: The final WorkspaceConfig used for the installation.
        :raises ManyError: If multiple errors occur during installation.
        :raises TimeoutError: If a timeout occurs during installation.
        """
        logger.info(f"Installing DQX v{self.product_info.version()}")
        try:
            config = self.configure(default_config)
            workflows_deployment = WorkflowsDeployment(
                config,
                config.get_run_config().name,
                self.installation,
                self.install_state,
                self.workspace_client,
                self.wheels,
                self.product_info,
                self._tasks,
            )

            workspace_installation = WorkspaceInstallation(
                config,
                self.installation,
                self.install_state,
                self.workspace_client,
                workflows_deployment,
                self.prompts,
                self.product_info,
            )
            workspace_installation.run()
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

    def _is_testing(self):
        return self.product_info.product_name() != "dqx"

    def _prompt_for_new_installation(self) -> WorkspaceConfig:
        logger.info("Please answer a couple of questions to configure DQX")
        log_level = self.prompts.question("Log level", default="INFO").upper()

        input_location = self.prompts.question(
            "Provide location for the input data "
            "as a path or table in the UC fully qualified format `catalog.schema.table`)",
            default="skipped",
            valid_regex=r"^\w.+$",
        )

        input_format = self.prompts.question(
            "Provide format for the input data (e.g. delta, parquet, csv, json)",
            default="delta",
            valid_regex=r"^\w.+$",
        )

        output_table = self.prompts.question(
            "Provide output table in the UC fully qualified format `catalog.schema.table`",
            default="skipped",
            valid_regex=r"^\w.+$",
        )

        quarantine_table = self.prompts.question(
            "Provide quarantined table in the UC fully qualified format `catalog.schema.table` "
            "(use output table if skipped)",
            default="skipped",
            valid_regex=r"^\w.+$",
        )

        if not quarantine_table:
            quarantine_table = output_table

        checks_file = self.prompts.question(
            "Provide filename for data quality rules (checks)", default="checks.yml", valid_regex=r"^\w.+$"
        )

        profile_summary_stats_file = self.prompts.question(
            "Provide filename to store profile summary statistics",
            default="profile_summary_stats.yml",
            valid_regex=r"^\w.+$",
        )

        return WorkspaceConfig(
            log_level=log_level,
            run_configs=[
                RunConfig(
                    input_location=input_location,
                    input_format=input_format,
                    output_table=output_table,
                    quarantine_table=quarantine_table,
                    checks_file=checks_file,
                    profile_summary_stats_file=profile_summary_stats_file,
                )
            ],
        )

    def _compare_remote_local_versions(self):
        try:
            local_version = self.product_info.released_version()
            remote_version = self.installation.load(Version).version
            if extract_major_minor(remote_version) == extract_major_minor(local_version):
                logger.info(f"DQX v{self.product_info.version()} is already installed on this workspace")
                msg = "Do you want to update the existing installation?"
                if not self.prompts.confirm(msg):
                    raise RuntimeWarning(
                        "DQX workspace remote and local install versions are same and no override is requested. "
                        "Exiting..."
                    )
        except NotFound as err:
            logger.warning(f"DQX workspace remote version not found: {err}")

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

    def configure(self, default_config: WorkspaceConfig | None = None) -> WorkspaceConfig:
        """
        Configures the workspace.

        Notes:
        1. Connection errors are not handled within this configure method.

        :param default_config: Optional default configuration.
        :return: The final WorkspaceConfig used for the installation.
        :raises NotFound: If the previous installation is not found.
        :raises RuntimeWarning: If the existing installation is corrupted.
        """
        try:
            config = self.installation.load(WorkspaceConfig)
            self._compare_remote_local_versions()
            if self._confirm_force_install():
                return self._configure_new_installation(default_config)
            self._apply_upgrades(config)
            return config
        except NotFound as err:
            logger.debug(f"Cannot find previous installation: {err}")
        except (PermissionDenied, SerdeError, ValueError, AttributeError):
            logger.warning(f"Existing installation at {self.installation.install_folder()} is corrupted. Skipping...")
        return self._configure_new_installation(default_config)

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

    def open_config_in_browser(self, config):
        ws_file_url = self.installation.workspace_link(config.__file__)
        if self.prompts.confirm(f"Open config file in the browser and continue installing? {ws_file_url}"):
            webbrowser.open(ws_file_url)


class WorkspaceInstallation:
    def __init__(
        self,
        config: WorkspaceConfig,
        installation: Installation,
        install_state: InstallState,
        ws: WorkspaceClient,
        workflows_installer: WorkflowsDeployment,
        prompts: Prompts,
        product_info: ProductInfo,
    ):
        self._config = config
        self._installation = installation
        self._install_state = install_state
        self._workflows_installer = workflows_installer
        self._ws = ws
        self._prompts = prompts
        self._product_info = product_info
        self._wheels = product_info.wheels(ws)

    @classmethod
    def current(cls, ws: WorkspaceClient):
        """
        Creates a current WorkspaceInstallation instance based on the current workspace client.

        :param ws: The WorkspaceClient instance.
        :return: A WorkspaceInstallation instance.
        """
        product_info = ProductInfo.from_class(WorkspaceConfig)
        installation = product_info.current_installation(ws)
        install_state = InstallState.from_installation(installation)
        config = installation.load(WorkspaceConfig)
        run_config_name = config.get_run_config().name
        prompts = Prompts()
        wheels = product_info.wheels(ws)
        tasks = Workflows.all().tasks()
        workflows_installer = WorkflowsDeployment(
            config, run_config_name, installation, install_state, ws, wheels, product_info, tasks
        )

        return cls(
            config,
            installation,
            install_state,
            ws,
            workflows_installer,
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
    def folder(self):
        """
        Returns the installation folder path.

        :return: The installation folder path as a string.
        """
        return self._installation.install_folder()

    def _upload_wheel(self) -> None:
        with self._wheels:
            wheel_path = self._wheels.upload_to_wsfs()
            logger.info(f"Wheel uploaded to /Workspace{wheel_path}")

    def _remove_jobs(self):
        self._workflows_installer.remove_jobs()

    def run(self) -> bool:
        """
        Runs the workflow installation.

        :return: True if the installation finished successfully, False otherwise.
        """
        logger.info(f"Installing DQX v{self._product_info.version()}")
        install_tasks = [self._workflows_installer.create_jobs]
        Threads.strict("installing components", install_tasks)
        logger.info("Installation completed successfully!")

        return True

    def uninstall(self):
        """
        Uninstalls DQX from the workspace, including project folder, dashboards, and jobs.
        """
        if self._prompts and not self._prompts.confirm(
            "Do you want to uninstall DQX from the workspace? this would "
            "remove dqx project folder, dashboards, and jobs"
        ):
            return

        logger.info(f"Deleting DQX v{self._product_info.version()} from {self._ws.config.host}")
        try:
            self._installation.files()
        except NotFound:
            logger.error(f"Check if {self._installation.install_folder()} is present")
            return

        self._remove_jobs()
        self._installation.remove()
        logger.info("Uninstalling DQX complete")


if __name__ == "__main__":
    logger = get_logger(__file__)
    if is_in_debug():
        logging.getLogger("databricks").setLevel(logging.DEBUG)

    workspace_installer = WorkspaceInstaller(WorkspaceClient(product="dqx", product_version=__version__))
    workspace_installer.run()
