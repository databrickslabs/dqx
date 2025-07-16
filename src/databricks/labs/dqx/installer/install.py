import json
import re
import logging
import dataclasses
import os
import time
import functools
import glob
import webbrowser
from collections.abc import Callable, Iterable
from typing import Any
from functools import cached_property
from datetime import timedelta
from pathlib import Path
from requests.exceptions import ConnectionError as RequestsConnectionError
import databricks

from databricks.labs.blueprint.entrypoint import get_logger, is_in_debug
from databricks.labs.blueprint.installation import Installation, SerdeError
from databricks.labs.blueprint.installer import InstallState
from databricks.labs.blueprint.parallel import ManyError, Threads
from databricks.labs.blueprint.tui import Prompts
from databricks.labs.blueprint.upgrades import Upgrades
from databricks.labs.blueprint.wheels import ProductInfo, Version, find_project_root
from databricks.labs.lsql.dashboards import DashboardMetadata, Dashboards
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import with_user_agent_extra
from databricks.sdk.service.dashboards import LifecycleState
from databricks.sdk.errors import (
    InvalidParameterValue,
    NotFound,
    PermissionDenied,
    InternalError,
    DeadlineExceeded,
    ResourceAlreadyExists,
    ResourceDoesNotExist,
)
from databricks.labs.dqx.installer.workflows_installer import WorkflowsDeployment
from databricks.labs.dqx.runtime import Workflows
from databricks.sdk.retries import retried
from databricks.sdk.service.sql import (
    CreateWarehouseRequestWarehouseType,
    EndpointInfoWarehouseType,
    SpotInstancePolicy,
)

from databricks.labs.dqx.__about__ import __version__
from databricks.labs.dqx.config import WorkspaceConfig, RunConfig, InputConfig, OutputConfig, ProfilerConfig
from databricks.labs.dqx.contexts.workspace import WorkspaceContext


logger = logging.getLogger(__name__)
with_user_agent_extra("cmd", "install")

WAREHOUSE_PREFIX = "DQX Dashboard"


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
            tasks = Workflows.all(config).tasks()

            workflows_deployment = WorkflowsDeployment(
                config,
                config.get_run_config().name,
                self.installation,
                self.install_state,
                self.workspace_client,
                self.wheels,
                self.product_info,
                tasks,
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

    @staticmethod
    def extract_major_minor(version_string: str):
        """
        Extracts the major and minor version from a version string.

        :param version_string: The version string to extract from.
        :return: The major.minor version as a string, or None if not found.
        """
        match = re.search(r"(\d+\.\d+)", version_string)
        if match:
            return match.group(1)
        return None

    def _is_testing(self):
        return self.product_info.product_name() != "dqx"

    def _prompt_for_new_installation(self) -> WorkspaceConfig:
        logger.info(
            "Please answer a couple of questions to provide default DQX run configuration. "
            "The configuration can also be updated manually after the installation."
        )

        log_level = self.prompts.question("Log level", default="INFO").upper()

        is_streaming = self.prompts.confirm("Should the input data be read using streaming?")
        input_config = self._prompt_input_config_for_new_installation(is_streaming)
        output_config = self._prompt_output_config_for_new_installation(is_streaming)
        quarantine_config = self._prompt_quarantine_config_for_new_installation(is_streaming)

        checks_file = self.prompts.question(
            "Provide filename for storing data quality rules (checks)", default="checks.yml", valid_regex=r"^\w.+$"
        )

        checks_table = self.prompts.question(
            "Provide table for storing checks in the format `catalog.schema.table` or `schema.table`",
            default="skipped",
            valid_regex=r"^([\w]+(?:\.[\w]+){1,2})$",
        )

        profiler_config = self._prompt_profiler_config_for_new_installation()

        profiler_spark_conf = json.loads(
            self.prompts.question(
                "Spark conf to use with the profiler job (e.g. {\"spark.sql.ansi.enabled\": \"true\"})",
                default="{}",
                valid_regex=r"^.*$",
            )
        )

        profiler_override_clusters = json.loads(
            self.prompts.question(
                "Cluster ID to use for the profiler job (e.g. {\"main\": \"<existing-cluster-id>\"})",
                default="{}",
                valid_regex=r"^.*$",
            )
        )

        warehouse_id = self.configure_warehouse()

        return WorkspaceConfig(
            log_level=log_level,
            run_configs=[
                RunConfig(
                    input_config=input_config,
                    output_config=output_config,
                    quarantine_config=quarantine_config,
                    checks_file=checks_file,
                    checks_table=None if checks_table == "skipped" else checks_table,
                    warehouse_id=warehouse_id,
                    profiler_config=profiler_config,
                )
            ],
            profiler_spark_conf=profiler_spark_conf,
            profiler_override_clusters=profiler_override_clusters,
        )

    def _prompt_profiler_config_for_new_installation(self) -> ProfilerConfig:
        profile_summary_stats_file = self.prompts.question(
            "Provide filename for storing profile summary statistics",
            default="profile_summary_stats.yml",
            valid_regex=r"^\w.+$",
        )

        return ProfilerConfig(
            summary_stats_file=profile_summary_stats_file,
        )

    def _prompt_input_config_for_new_installation(self, is_streaming: bool) -> InputConfig | None:
        input_location = self.prompts.question(
            "Provide location for the input data "
            "as a path or table in the format `catalog.schema.table` or `schema.table`",
            default="skipped",
            valid_regex=r"/.+|([\w]+(?:\.[\w]+){1,2})$",
        )

        if input_location != "skipped":
            input_format = self.prompts.question(
                "Provide format for the input data (e.g. delta, parquet, csv, json)",
                default="delta",
                valid_regex=r"^\w.+$",
            )

            input_schema = self.prompts.question(
                "Provide schema for the input data (e.g. col1 int, col2 string)",
                default="skipped",
                valid_regex=r"^\w.+$",
            )

            input_read_options = json.loads(
                self.prompts.question(
                    "Provide additional options for reading the input data (e.g. {\"versionAsOf\": \"0\"})",
                    default="{}",
                    valid_regex=r"^.*$",
                )
            )

            return InputConfig(
                location=input_location,
                format=input_format,
                schema=None if input_schema == "skipped" else input_schema,
                options=input_read_options,
                is_streaming=is_streaming,
            )
        return None

    def _prompt_output_config_for_new_installation(self, is_streaming: bool) -> OutputConfig:
        output_table = self.prompts.question(
            "Provide output table in the format `catalog.schema.table` or `schema.table`",
            valid_regex=r"^([\w]+(?:\.[\w]+){1,2})$",
        )

        output_write_mode = self.prompts.question(
            "Provide write mode for output table (e.g. 'append' or 'overwrite')",
            default="append",
            valid_regex=r"^\w.+",
        )

        output_format = self.prompts.question(
            "Provide format for the output data (e.g. delta, parquet)",
            default="delta",
            valid_regex=r"^\w.+$",
        )

        output_write_options = json.loads(
            self.prompts.question(
                "Provide additional options for writing the output data (e.g. {\"mergeSchema\": \"true\"})",
                default="{}",
                valid_regex=r"^.*$",
            )
        )

        output_trigger_options = {}
        if is_streaming:
            output_trigger_options = json.loads(
                self.prompts.question(
                    "Provide additional options for writing the output data using streaming "
                    "(e.g. {\"availableNow\": true})",
                    default="{}",
                    valid_regex=r"^.*$",
                )
            )

        # output is required for installing the dashboard
        return OutputConfig(
            location=output_table,
            mode=output_write_mode,
            format=output_format,
            options=output_write_options,
            trigger=output_trigger_options,
        )

    def _prompt_quarantine_config_for_new_installation(self, is_streaming: bool) -> OutputConfig | None:
        quarantine_table = self.prompts.question(
            "Provide quarantined table in the format `catalog.schema.table` or `schema.table` "
            "(use output table if skipped)",
            default="skipped",
            valid_regex=r"^([\w]+(?:\.[\w]+){1,2})$",
        )

        if quarantine_table != "skipped":
            quarantine_write_mode = self.prompts.question(
                "Provide write mode for quarantine table (e.g. 'append' or 'overwrite')",
                default="append",
                valid_regex=r"^\w.+",
            )

            quarantine_format = self.prompts.question(
                "Provide format for the quarantine data (e.g. delta, parquet)",
                default="delta",
                valid_regex=r"^\w.+$",
            )

            quarantine_write_options = json.loads(
                self.prompts.question(
                    "Provide additional options for writing the quarantine data (e.g. {\"mergeSchema\": \"true\"})",
                    default="{}",
                    valid_regex=r"^.*$",
                )
            )

            quarantine_trigger_options = {}
            if is_streaming:
                quarantine_trigger_options = json.loads(
                    self.prompts.question(
                        "Provide additional options for writing the quarantine data using streaming "
                        "(e.g. {\"availableNow\": true})",
                        default="{}",
                        valid_regex=r"^.*$",
                    )
                )

            return OutputConfig(
                location=quarantine_table,
                mode=quarantine_write_mode,
                format=quarantine_format,
                options=quarantine_write_options,
                trigger=quarantine_trigger_options,
            )
        return None

    def _compare_remote_local_versions(self):
        try:
            local_version = self.product_info.released_version()
            remote_version = self.installation.load(Version).version
            if self.extract_major_minor(remote_version) == self.extract_major_minor(local_version):
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
        def warehouse_type(_):
            return _.warehouse_type.value if not _.enable_serverless_compute else "SERVERLESS"

        pro_warehouses = {" [Create new PRO or SERVERLESS SQL warehouse ] ": "create_new"} | {
            f"{_.name} ({_.id}, {warehouse_type(_)}, {_.state.value})": _.id
            for _ in self.workspace_client.warehouses.list()
            if _.warehouse_type == EndpointInfoWarehouseType.PRO
        }

        warehouse_id = self.prompts.choice_from_dict(
            "Select PRO or SERVERLESS SQL warehouse to run data quality dashboards on", pro_warehouses
        )
        if warehouse_id == "create_new":
            new_warehouse = self.workspace_client.warehouses.create(
                name=f"{WAREHOUSE_PREFIX} {time.time_ns()}",
                spot_instance_policy=SpotInstancePolicy.COST_OPTIMIZED,
                warehouse_type=CreateWarehouseRequestWarehouseType.PRO,
                cluster_size="Small",
                max_num_clusters=1,
            )
            warehouse_id = new_warehouse.id
        return warehouse_id


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
        tasks = Workflows.all(config).tasks()
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
        install_tasks = [self._workflows_installer.create_jobs, self._create_dq_dashboard]
        Threads.strict("installing components", install_tasks)
        logger.info("Installation completed successfully!")

        return True

    def _create_dq_dashboard(self) -> None:
        Threads.strict("Installing dashboards", list(self._get_create_dq_dashboard_tasks()))

    def _get_create_dq_dashboard_tasks(self) -> Iterable[Callable[[], None]]:
        """Get the tasks to create Lakeview dashboards from the SQL queries in the queries subfolders"""

        logger.info("Creating dashboards...")
        dashboard_folder_remote = f"{self._installation.install_folder()}/dashboards"
        try:
            self._ws.workspace.mkdirs(dashboard_folder_remote)
        except ResourceAlreadyExists:
            pass
        queries_folder = find_project_root(__file__) / "src/databricks/labs/dqx/queries"

        logger.debug(f"DQ Dashboard Query Folder is {queries_folder}")
        for step_folder in queries_folder.iterdir():
            if not step_folder.is_dir():
                continue
            logger.debug(f"Reading step folder {step_folder}...")
            for dashboard_folder in step_folder.iterdir():
                if not dashboard_folder.is_dir():
                    continue
                task = functools.partial(
                    self._create_dashboard,
                    dashboard_folder,
                    parent_path=dashboard_folder_remote,
                )
                yield task

    def _handle_existing_dashboard(self, dashboard_id: str, display_name: str, parent_path: str) -> str | None:
        """Handle an existing dashboard

        This method handles the following scenarios:
        - dashboard exists and needs to be updated
        - dashboard is trashed and needs to be recreated
        - dashboard reference is invalid and the dashboard needs to be recreated

        Returns
            str | None :
                The dashboard id. If None, the dashboard will be recreated.
        """
        try:
            dashboard = self._ws.lakeview.get(dashboard_id)
            if dashboard.lifecycle_state is None:
                raise NotFound(f"Dashboard life cycle state: {display_name} ({dashboard_id})")
            if dashboard.lifecycle_state == LifecycleState.TRASHED:
                logger.info(f"Recreating trashed dashboard: {display_name} ({dashboard_id})")
                return None  # Recreate the dashboard if it is trashed (manually)
        except (NotFound, InvalidParameterValue):
            logger.info(f"Recovering invalid dashboard: {display_name} ({dashboard_id})")
            try:
                dashboard_path = f"{parent_path}/{display_name}.lvdash.json"
                self._ws.workspace.delete(dashboard_path)  # Cannot recreate dashboard if file still exists
                logger.debug(f"Deleted dangling dashboard {display_name} ({dashboard_id}): {dashboard_path}")
            except NotFound:
                pass
            return None  # Recreate the dashboard if it's reference is corrupted (manually)
        return dashboard_id  # Update the existing dashboard

    @staticmethod
    def _resolve_table_name_in_queries(src_tbl_name: str, replaced_tbl_name: str, folder: Path) -> bool:
        """Replaces table name variable in all .sql files
        This method iterate through the dashboard folder, and replaces fully qualified tables in *.sql files

        Returns
            True : If the variable name is replaced across .sql files, otherwise False
        """
        logger.debug("Preparing .sql files for DQX Dashboard")
        dyn_sql_files = glob.glob(os.path.join(folder, "*.sql"))
        try:
            for sql_file in dyn_sql_files:
                sql_file_path = Path(sql_file)
                dq_sql_query = sql_file_path.read_text(encoding="utf-8")
                dq_sql_query_ref = dq_sql_query.replace(src_tbl_name, replaced_tbl_name)
                logger.debug(dq_sql_query_ref)
                sql_file_path.write_text(dq_sql_query_ref, encoding="utf-8")
            return True
        except Exception as e:
            err_msg = f"Error during parsing input table name into .sql files: {e}"
            logger.error(err_msg)
            # Review this - Gracefully handling this internal variable replace operation
            return False

    # InternalError and DeadlineExceeded are retried because of Lakeview internal issues
    # These issues have been reported to and are resolved by the Lakeview team.
    # Keeping the retry for resilience.
    @retried(on=[InternalError, DeadlineExceeded], timeout=timedelta(minutes=4))
    def _create_dashboard(self, folder: Path, *, parent_path: str) -> None:
        """Create a lakeview dashboard from the SQL queries in the folder"""
        logger.info(f"Reading dashboard assets from {folder}...")

        run_config = self.config.get_run_config()
        if run_config.quarantine_config:
            dq_table = run_config.quarantine_config.location.lower()
            logger.info(f"Using '{dq_table}' quarantine table as the source table for the dashboard...")
        else:
            dq_table = run_config.output_config.location.lower()
            logger.info(f"Using '{dq_table}' output table as the source table for the dashboard...")

        src_table_name = "$catalog.schema.table"
        if self._resolve_table_name_in_queries(src_tbl_name=src_table_name, replaced_tbl_name=dq_table, folder=folder):
            metadata = DashboardMetadata.from_path(folder)
            logger.debug(f"Dashboard Metadata retrieved is {metadata}")

            metadata.display_name = f"DQX_{folder.parent.stem.title()}_{folder.stem.title()}"
            reference = f"{folder.parent.stem}_{folder.stem}".lower()
            dashboard_id = self._install_state.dashboards.get(reference)
            logger.debug(f"dashboard id retrieved is {dashboard_id}")

            logger.info(f"Installing '{metadata.display_name}' dashboard in '{parent_path}'")
            if dashboard_id is not None:
                dashboard_id = self._handle_existing_dashboard(dashboard_id, metadata.display_name, parent_path)
            dashboard = Dashboards(self._ws).create_dashboard(
                metadata,
                parent_path=parent_path,
                dashboard_id=dashboard_id,
                warehouse_id=run_config.warehouse_id,
                publish=True,
            )
            assert dashboard.dashboard_id is not None
            self._install_state.dashboards[reference] = dashboard.dashboard_id

        # Revert Back SQL queries to placeholder format
        self._resolve_table_name_in_queries(src_tbl_name=dq_table, replaced_tbl_name=src_table_name, folder=folder)

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
        self._remove_warehouse()
        self._installation.remove()
        logger.info("Uninstalling DQX complete")

    def _remove_warehouse(self):
        warehouse_id = self._config.get_run_config().warehouse_id

        try:
            warehouse_name = self._ws.warehouses.get(warehouse_id).name
            if warehouse_name.startswith(WAREHOUSE_PREFIX):
                logger.info(f"Deleting {warehouse_name}.")
                self._ws.warehouses.delete(id=warehouse_id)
        except InvalidParameterValue:
            logger.error("Error accessing warehouse details")
        except ResourceDoesNotExist as e:
            logger.warning(f"Warehouse with id {warehouse_id} does not exist anymore: {e}")


if __name__ == "__main__":
    logger = get_logger(__file__)
    if is_in_debug():
        logging.getLogger("databricks").setLevel(logging.DEBUG)

    workspace_installer = WorkspaceInstaller(WorkspaceClient(product="dqx", product_version=__version__))
    workspace_installer.run()
