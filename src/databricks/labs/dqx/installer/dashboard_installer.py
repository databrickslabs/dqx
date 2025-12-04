import functools
import logging
import json
from typing import Any
from collections.abc import Callable, Iterable
from datetime import timedelta
from pathlib import Path
from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.installer import InstallState
from databricks.labs.blueprint.wheels import ProductInfo, find_project_root
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import LifecycleState, Dashboard
from databricks.sdk.errors import (
    InvalidParameterValue,
    NotFound,
    InternalError,
    DeadlineExceeded,
    ResourceAlreadyExists,
)
from databricks.sdk.retries import retried
from databricks.labs.dqx.config import WorkspaceConfig, RunConfig

logger = logging.getLogger(__name__)


class DashboardMetadata:
    """Creates Dashboard Metadata from exported Lakeview dashboard (.lvdash.json)"""

    def __init__(
        self,
        display_name: str,
        dashboard_def: dict[str, Any],
    ):
        self.display_name = display_name
        self.dashboard_def = dashboard_def

    @classmethod
    def from_path(cls, file: Path) -> "DashboardMetadata":
        """Load dashboard metadata from exported Lakeview dashboard file.

        Expected structure:
        dashboard_folder/
        └── dashboard_name.lvdash.json

        Args:
            file: The path to the .lvdash.json file.

        Returns:
            A DashboardMetadata instance populated with the display name and
            dashboard definition from the exported Lakeview dashboard.
        """
        logger.info(f"Loading exported Lakeview dashboard: {file.name}")

        with open(file, 'r', encoding="utf-8") as f:
            dashboard_data = json.load(f)

        display_name = dashboard_data.get("displayName", file.stem)

        return cls(
            display_name=display_name,
            dashboard_def=dashboard_data,
        )


class DashboardInstaller:
    """
    Creates or updates Lakeview dashboards from exported .lvdash.json files.
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
        Returns a generator of tasks to create dashboards from exported Lakeview dashboards.

        Each task is a callable that, when executed, will create a dashboard in the workspace.
        The tasks are created based on the .lvdash.json files found in the dashboard directory.
        """
        logger.info("Creating dashboards...")
        dashboard_folder_remote = f"{self._installation.install_folder()}/dashboards"
        try:
            self._ws.workspace.mkdirs(dashboard_folder_remote)
        except ResourceAlreadyExists:
            pass

        dashboard_folder = find_project_root(__file__) / "src/databricks/labs/dqx/dashboards"
        logger.debug(f"Dashboard Query Folder is {dashboard_folder}")
        for step_file in dashboard_folder.iterdir():
            if not step_file.is_file():
                continue
            logger.debug(f"Reading dashboard definition from {step_file}...")
            task = functools.partial(
                self._create_dashboard,
                step_file,
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
    def _resolve_table_name_in_dashboard(
        src_tbl_name: str, replaced_tbl_name: str, dashboard_def: dict[str, Any]
    ) -> dict[str, Any]:
        """Replaces table name variable in dashboard definition

        This method replaces the placeholder table name with the actual table name
        in all queries within the dashboard definition.

        Args:
            src_tbl_name: The source table name to be replaced (placeholder)
            replaced_tbl_name: The table name to replace the source table name with
            dashboard_def: The dashboard definition containing datasets with queries

        Returns:
            Updated dashboard definition with replaced table names
        """
        logger.debug(f"Replacing '{src_tbl_name}' with '{replaced_tbl_name}' in dashboard queries")

        # Deep copy to avoid modifying original
        updated_def = json.loads(json.dumps(dashboard_def))

        # Replace table names in all datasets
        for dataset in updated_def.get("datasets", []):
            # Handle queryLines array (exported Lakeview format)
            if "queryLines" in dataset and isinstance(dataset["queryLines"], list):
                dataset["queryLines"] = [
                    line.replace(src_tbl_name, replaced_tbl_name) for line in dataset["queryLines"]
                ]
            # Handle query string (alternative format)
            elif "query" in dataset and isinstance(dataset["query"], str):
                dataset["query"] = dataset["query"].replace(src_tbl_name, replaced_tbl_name)

        return updated_def

    @retried(on=[InternalError, DeadlineExceeded], timeout=timedelta(minutes=4))
    def _create_dashboard(self, file: Path, *, parent_path: str) -> None:
        """
        Create a lakeview dashboard from the exported .lvdash.json file.

        Args:
            file: Path to the .lvdash.json file
            parent_path: Parent path where the dashboard will be created
        """
        logger.info(f"Reading dashboard from {file}...")

        run_config = self._config.get_run_config()
        if run_config.quarantine_config:
            dq_table = run_config.quarantine_config.location.lower()
            logger.info(f"Using '{dq_table}' quarantine table as the source table for the dashboard...")
        else:
            assert run_config.output_config  # output config is always required
            dq_table = run_config.output_config.location.lower()
            logger.info(f"Using '{dq_table}' output table as the source table for the dashboard...")

        try:
            self._create_dashboard_from_metadata(file, parent_path, run_config, dq_table)
        except Exception as e:
            logger.error(f"Failed to create dashboard from {file}: {e}", exc_info=True)
            raise

    def _create_dashboard_from_metadata(
        self, file: Path, parent_path: str, run_config: RunConfig, dq_table: str
    ) -> None:
        """
        Create dashboard using exported Lakeview metadata.

        Args:
            file: Path to the .lvdash.json file
            parent_path: Parent path where the dashboard will be created
            run_config: Run configuration containing warehouse settings
            dq_table: The actual table name to use in queries
        """
        # Load metadata
        metadata = self._prepare_dashboard_metadata(file)
        reference = file.stem.lower()
        dashboard_id = self._install_state.dashboards.get(reference)

        # Handle existing dashboard if needed
        if dashboard_id is not None:
            dashboard_id = self._handle_existing_dashboard(dashboard_id, metadata.display_name, parent_path)

        # Replace source table name in dashboard definition
        src_table_name = "$catalog.schema.table"
        updated_dashboard_def = self._resolve_table_name_in_dashboard(
            src_tbl_name=src_table_name, replaced_tbl_name=dq_table, dashboard_def=metadata.dashboard_def
        )

        # Create or update the dashboard
        dashboard = self._create_or_update_dashboard(
            dashboard_id=dashboard_id,
            metadata=metadata,
            parent_path=parent_path,
            run_config=run_config,
            dashboard_def=updated_dashboard_def,
        )

        # Publish the dashboard
        self._publish_dashboard(dashboard, metadata.display_name)

        # Save the dashboard reference
        assert dashboard.dashboard_id is not None
        self._install_state.dashboards[reference] = dashboard.dashboard_id
        logger.info(f"Successfully installed dashboard: {metadata.display_name} ({dashboard.dashboard_id})")

    def _prepare_dashboard_metadata(self, file: Path) -> DashboardMetadata:
        """
        Load and prepare dashboard metadata from dashboard file.

        Args:
            file: Path to the .lvdash.json file

        Returns:
            Prepared dashboard metadata with formatted display name
        """
        metadata = DashboardMetadata.from_path(file)
        logger.debug(f"Dashboard Metadata retrieved: {metadata.display_name}")
        stem = file.stem.title()
        if stem.lower().endswith(".lvdash"):
            stem = stem[: -len(".lvdash")]
        metadata.display_name = f"DQX_{stem}"
        return metadata

    def _create_or_update_dashboard(
        self,
        dashboard_id: str | None,
        metadata: DashboardMetadata,
        parent_path: str,
        run_config: RunConfig,
        dashboard_def: dict[str, Any],
    ) -> Dashboard:
        """
        Create a new dashboard or update an existing one.

        Args:
            dashboard_id: Existing dashboard ID if updating, None if creating new
            metadata: Dashboard metadata configuration
            parent_path: Parent path where the dashboard will be created
            run_config: Run configuration containing warehouse settings
            dashboard_def: The dashboard definition with resolved table names

        Returns:
            Created or updated Dashboard object
        """
        if dashboard_id is None:
            logger.info(f"Creating new dashboard: {metadata.display_name}")
            return self._ws.lakeview.create(
                dashboard=Dashboard(
                    display_name=metadata.display_name,
                    parent_path=parent_path,
                    warehouse_id=run_config.warehouse_id,
                    serialized_dashboard=json.dumps(dashboard_def),
                )
            )
        logger.info(f"Updating existing dashboard: {metadata.display_name} ({dashboard_id})")
        return self._ws.lakeview.update(
            dashboard_id=dashboard_id,
            dashboard=Dashboard(
                display_name=metadata.display_name,
                serialized_dashboard=json.dumps(dashboard_def),
            ),
        )

    def _publish_dashboard(self, dashboard: Dashboard, display_name: str) -> None:
        """
        Publish a dashboard and handle publication failures gracefully.

        Args:
            dashboard: Dashboard object to publish
            display_name: Display name for logging purposes
        """
        if dashboard.dashboard_id:
            try:
                self._ws.lakeview.publish(dashboard.dashboard_id)
                logger.info(f"Published dashboard: {display_name}")
            except Exception as e:
                logger.warning(f"Failed to publish dashboard: {e}")
