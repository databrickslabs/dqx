import functools
import logging
import os
import glob
from collections.abc import Callable, Iterable
from datetime import timedelta
from pathlib import Path
from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.installer import InstallState
from databricks.labs.blueprint.wheels import ProductInfo, find_project_root
from databricks.labs.lsql.dashboards import DashboardMetadata, Dashboards
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import LifecycleState
from databricks.sdk.errors import (
    InvalidParameterValue,
    NotFound,
    InternalError,
    DeadlineExceeded,
    ResourceAlreadyExists,
)
from databricks.sdk.retries import retried
from databricks.labs.dqx.config import WorkspaceConfig


logger = logging.getLogger(__name__)


class DashboardInstaller:
    """
    Creates or updates Lakeview dashboards from bundled SQL queries.
    """

    def __init__(
        self,
        ws: WorkspaceClient,
        installation: Installation,
        install_state: InstallState,
        product_info: ProductInfo,
        config: WorkspaceConfig,
    ) -> None:
        self._ws = ws
        self._installation = installation
        self._install_state = install_state
        self._product_info = product_info
        self._config = config

    def get_create_dashboard_tasks(self) -> Iterable[Callable[[], None]]:
        """
        Returns a generator of tasks to create dashboards from bundled SQL queries.

        Each task is a callable that, when executed, will create a dashboard in the workspace.
        The tasks are created based on the SQL files found in the bundled queries directory.
        The tasks will handle the creation of the dashboard, including resolving table names.
        """
        logger.info("Creating dashboards...")
        dashboard_folder_remote = f"{self._installation.install_folder()}/dashboards"
        try:
            self._ws.workspace.mkdirs(dashboard_folder_remote)
        except ResourceAlreadyExists:
            pass

        queries_folder = find_project_root(__file__) / "src/databricks/labs/dqx/queries"
        logger.debug(f"Dashboard Query Folder is {queries_folder}")
        for step_folder in queries_folder.iterdir():
            if not step_folder.is_dir():
                continue
            logger.debug(f"Reading step install folder {step_folder}...")
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

        Args:
            dashboard_id: The ID of the existing dashboard
            display_name: The display name of the dashboard
            parent_path: The parent path where the dashboard is located

        Returns:
            The dashboard ID if it is valid, otherwise None

        Raises:
            NotFound: If the dashboard is not found
            InvalidParameterValue: If the dashboard ID is invalid
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
        This method iterates through the dashboard install_folder, and replaces fully qualified tables in *.sql files

        Args:
            src_tbl_name: The source table name to be replaced
            replaced_tbl_name: The table name to replace the source table name with
            folder: The install_folder containing the SQL files

        Returns:
            True if the operation was successful, False otherwise
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
        """Create a lakeview dashboard from the SQL queries in the install_folder"""
        logger.info(f"Reading dashboard assets from {folder}...")

        run_config = self._config.get_run_config()
        if run_config.quarantine_config:
            dq_table = run_config.quarantine_config.location.lower()
            logger.info(f"Using '{dq_table}' quarantine table as the source table for the dashboard...")
        else:
            assert run_config.output_config  # output config is always required
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

        # Revert back SQL queries to placeholder format regardless of success
        self._resolve_table_name_in_queries(src_tbl_name=dq_table, replaced_tbl_name=src_table_name, folder=folder)
