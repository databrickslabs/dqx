import logging
import os
import re
import webbrowser
from functools import cached_property

from requests.exceptions import ConnectionError as RequestsConnectionError

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

from databricks.labs.dqx.__about__ import __version__
from databricks.labs.dqx.config import WorkspaceConfig
from databricks.labs.dqx.contexts.workspace_cli import WorkspaceContext

TAG_STEP = "step"
NUM_USER_ATTEMPTS = 10  # number of attempts user gets at answering a question

logger = logging.getLogger(__name__)
with_user_agent_extra("cmd", "install")


def extract_major_minor(version_string):
    match = re.search(r"(\d+\.\d+)", version_string)
    if match:
        return match.group(1)
    return None


class WorkspaceInstaller(WorkspaceContext):
    def __init__(self, ws: WorkspaceClient, environ: dict[str, str] | None = None):
        super().__init__(ws)
        if not environ:
            environ = dict(os.environ.items())

        self._force_install = environ.get("DQX_FORCE_INSTALL")

        if "DATABRICKS_RUNTIME_VERSION" in environ:
            msg = "WorkspaceInstaller is not supposed to be executed in Databricks Runtime"
            raise SystemExit(msg)

    @cached_property
    def upgrades(self):
        return Upgrades(self.product_info, self.installation)

    @cached_property
    def installation(self):
        try:
            return self.product_info.current_installation(self.workspace_client)
        except NotFound:
            if self._force_install == "global":
                return Installation.assume_global(self.workspace_client, self.product_info.product_name())
            return Installation.assume_user_home(self.workspace_client, self.product_info.product_name())

    def run(
        self,
        default_config: WorkspaceConfig | None = None,
        config: WorkspaceConfig | None = None,
    ) -> WorkspaceConfig:
        logger.info(f"Installing DQX v{self.product_info.version()}")
        try:
            if config is None:
                config = self.configure(default_config)
            if self._is_testing():
                return config

            workspace_installation = WorkspaceInstallation(
                config,
                self.installation,
                self.install_state,
                self.workspace_client,
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
            "Provide location for the input data (path or a table)", valid_regex=r"^\w.+$"
        )

        output_location = self.prompts.question(
            "Provide location for the output (path or a table)", default="skip", valid_regex=r"^\w.+$"
        )

        quarantine_location = self.prompts.question(
            "Provide location for the quarantined data (path or a table)", default="skip", valid_regex=r"^\w.+$"
        )

        curated_location = self.prompts.question(
            "Provide location for the curated data (path or a table)", default="skip", valid_regex=r"^\w.+$"
        )

        checks_file = self.prompts.question(
            "Provide name for the filename for the quality rules / checks", default="checks.json", valid_regex=r"^\w.+$"
        )

        profile_summary_stats_file = self.prompts.question(
            "Provide filename for the profile summary statistics", default="checks.json", valid_regex=r"^\w.+$"
        )

        return WorkspaceConfig(
            log_level=log_level,
            input_location=input_location,
            output_location=output_location,
            quarantine_location=quarantine_location,
            curated_location=curated_location,
            checks_file=checks_file,
            profile_summary_stats_file=profile_summary_stats_file,
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
            self.replace(
                installation=Installation.assume_global(self.workspace_client, self.product_info.product_name())
            )
            return True
        if self.installation.is_global() and self._force_install == "user":
            # Logic for forced user install over global install
            self.replace(
                installation=Installation.assume_user_home(self.workspace_client, self.product_info.product_name())
            )
            return True
        return False

    def configure(self, default_config: WorkspaceConfig | None = None) -> WorkspaceConfig:
        """Configure the workspaces

        Notes:
        1. Connection errors are not handled within this configure method.
        """
        try:
            config = self.installation.load(WorkspaceConfig)
            self._compare_remote_local_versions()
            if self._confirm_force_install():
                return self._configure_new_installation(default_config)
            self._apply_upgrades()
            return config
        except NotFound as err:
            logger.debug(f"Cannot find previous installation: {err}")
        except (PermissionDenied, SerdeError, ValueError, AttributeError):
            logger.warning(f"Existing installation at {self.installation.install_folder()} is corrupted. Skipping...")
        return self._configure_new_installation(default_config)

    def _apply_upgrades(self):
        try:
            self.upgrades.apply(self.workspace_client)
        except (InvalidParameterValue, NotFound) as err:
            logger.warning(f"Installed version is too old: {err}")

    def _configure_new_installation(self, config: WorkspaceConfig | None = None) -> WorkspaceConfig:
        if config is None:
            config = self._prompt_for_new_installation()
        self.installation.save(config)
        ws_file_url = self.installation.workspace_link(config.__file__)
        if self.prompts.confirm(f"Open config file in the browser and continue installing? {ws_file_url}"):
            webbrowser.open(ws_file_url)
        return config


class WorkspaceInstallation:
    def __init__(
        self,
        config: WorkspaceConfig,
        installation: Installation,
        install_state: InstallState,
        ws: WorkspaceClient,
        prompts: Prompts,
        product_info: ProductInfo,
    ):
        self._config = config
        self._installation = installation
        self._install_state = install_state
        self._ws = ws
        self._prompts = prompts
        self._product_info = product_info
        self._wheels = product_info.wheels(ws)

    @classmethod
    def current(cls, ws: WorkspaceClient):
        product_info = ProductInfo.from_class(WorkspaceConfig)
        installation = product_info.current_installation(ws)
        install_state = InstallState.from_installation(installation)
        config = installation.load(WorkspaceConfig)
        prompts = Prompts()

        return cls(
            config,
            installation,
            install_state,
            ws,
            prompts,
            product_info,
        )

    @property
    def config(self):
        return self._config

    @property
    def folder(self):
        return self._installation.install_folder()

    def _upload_wheel(self) -> None:
        with self._wheels:
            wheel_path = self._wheels.upload_to_wsfs()
            logger.info(f"Wheel uploaded to /Workspace{wheel_path}")

    def run(self) -> bool:
        """Run workflow installation.
        Returns
            bool :
                True, installation finished. False installation did not finish.
        """
        logger.info(f"Installing DQX v{self._product_info.version()}")
        install_tasks = [self._upload_wheel]
        Threads.strict("installing components", install_tasks)
        logger.info("Installation completed successfully!")

        return True

    def uninstall(self):
        if self._prompts and not self._prompts.confirm(
            "Do you want to uninstall DQX from the workspace too, this would "
            "remove dqx project folder, dashboards, and jobs"
        ):
            return

        logger.info(f"Deleting DQX v{self._product_info.version()} from {self._ws.config.host}")
        try:
            self._installation.files()
        except NotFound:
            logger.error(f"Check if {self._installation.install_folder()} is present")
            return

        self._installation.remove()
        logger.info("Uninstalling DQX complete")


if __name__ == "__main__":
    logger = get_logger(__file__)
    if is_in_debug():
        logging.getLogger("databricks").setLevel(logging.DEBUG)

    workspace_installer = WorkspaceInstaller(WorkspaceClient(product="dqx", product_version=__version__))
    workspace_installer.run()