import functools
import logging
import os
import glob
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
    """Creates Dashboard Metadata from dashboard.json and SQL queries"""

    def __init__(
        self,
        display_name: str,
        queries: dict[str, str],
        layout: list[dict[str, Any]],
    ):
        self.display_name = display_name
        self.queries = queries
        self.layout = layout

    @classmethod
    def from_path(cls, folder: Path) -> "DashboardMetadata":
        """Load dashboard metadata from a folder structure.

        This method processes the dashboard directory and extracts all information
        required to construct a DashboardMetadata object. It performs the following:
        - loads dashboard configuration from `dashboard.json`
        - reads all `.sql` files as dashboard queries
        - skips empty SQL files and logs a warning

        Expected structure:
        dashboard_folder/
        ├── dashboard.json  (layout and configuration)
        ├── query1.sql
        ├── query2.sql
        └── ...

        Args:
            folder: The path to the dashboard folder containing configuration
                and SQL query files.

        Returns:
            A DashboardMetadata instance populated with the display name, layout,
            and SQL queries defined in the folder.

        Raises:
            FileNotFoundError: If no valid dashboard.json file is found.
        """

        # Load dashboard configuration
        config_file = folder / "dashboard.json"
        if not config_file.exists():
            raise FileNotFoundError(f"dashboard.json not found in {folder}")

        with open(config_file, 'r', encoding="utf8") as f:
            config = json.load(f)

        # Load SQL queries
        queries = {}
        for sql_file in folder.glob("*.sql"):
            query_name = sql_file.stem
            query_text = sql_file.read_text(encoding="utf-8")

            # Skip empty queries
            if query_text.strip():
                queries[query_name] = query_text
            else:
                logger.warning(f"Skipping empty SQL file: {sql_file}")

        return cls(
            display_name=config.get("display_name", folder.name),
            queries=queries,
            layout=config.get("layout", []),
        )


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

    @retried(on=[InternalError, DeadlineExceeded], timeout=timedelta(minutes=4))
    def _create_dashboard(self, folder: Path, *, parent_path: str) -> None:
        """
        Create a lakeview dashboard from the SQL queries in the folder.

        Args:
            folder: Path to the folder containing dashboard assets
            parent_path: Parent path where the dashboard will be created
        """
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
        if not self._resolve_table_name_in_queries(
            src_tbl_name=src_table_name, replaced_tbl_name=dq_table, folder=folder
        ):
            logger.error(f"Failed to resolve table names in {folder}")
            return

        try:
            self._create_dashboard_from_metadata(folder, parent_path, run_config)
        except Exception as e:
            logger.error(f"Failed to create dashboard from {folder}: {e}", exc_info=True)
            raise
        finally:
            # Revert back SQL queries to placeholder format regardless of success
            self._resolve_table_name_in_queries(src_tbl_name=dq_table, replaced_tbl_name=src_table_name, folder=folder)

    def _create_dashboard_from_metadata(self, folder: Path, parent_path: str, run_config: RunConfig) -> None:
        """
        Create dashboard using prepared metadata.

        Args:
            folder: Path to the folder containing dashboard assets
            parent_path: Parent path where the dashboard will be created
            run_config: Run configuration containing warehouse settings
        """
        # Load and prepare metadata
        metadata = self._prepare_dashboard_metadata(folder)
        reference = f"{folder.parent.stem}_{folder.stem}".lower()
        dashboard_id = self._install_state.dashboards.get(reference)

        # Handle existing dashboard if needed
        if dashboard_id is not None:
            dashboard_id = self._handle_existing_dashboard(dashboard_id, metadata.display_name, parent_path)

        # Create or update the dashboard
        dashboard = self._create_or_update_dashboard(
            dashboard_id=dashboard_id,
            metadata=metadata,
            parent_path=parent_path,
            run_config=run_config,
        )

        # Publish the dashboard
        self._publish_dashboard(dashboard, metadata.display_name)

        # Save the dashboard reference
        assert dashboard.dashboard_id is not None
        self._install_state.dashboards[reference] = dashboard.dashboard_id
        logger.info(f"Successfully installed dashboard: {metadata.display_name} ({dashboard.dashboard_id})")

    def _prepare_dashboard_metadata(self, folder: Path) -> DashboardMetadata:
        """
        Load and prepare dashboard metadata from folder.

        Args:
            folder: Path to the folder containing dashboard assets

        Returns:
            Prepared dashboard metadata with formatted display name
        """
        metadata = DashboardMetadata.from_path(folder)
        logger.debug(f"Dashboard Metadata retrieved: {metadata.display_name}")
        metadata.display_name = f"DQX_{folder.parent.stem.title()}_{folder.stem.title()}"
        return metadata

    def _create_or_update_dashboard(
        self, dashboard_id: str | None, metadata: DashboardMetadata, parent_path: str, run_config: RunConfig
    ) -> Dashboard:
        """
        Create a new dashboard or update an existing one.

        Args:
            dashboard_id: Existing dashboard ID if updating, None if creating new
            metadata: Dashboard metadata configuration
            parent_path: Parent path where the dashboard will be created
            run_config: Run configuration containing warehouse settings

        Returns:
            Created or updated Dashboard object
        """
        dashboard_def = self._build_dashboard_definition(metadata=metadata)

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

    def _build_dashboard_definition(self, metadata: DashboardMetadata) -> dict[str, Any]:
        """
        Build a native Lakeview dashboard definition from metadata.

        Args:
            metadata: Dashboard metadata containing queries and layout

        Returns:
            Dictionary representing the dashboard definition
        """
        # Build datasets from queries
        datasets = []
        for query_name, query_text in metadata.queries.items():
            # Skip empty queries
            if not query_text.strip():
                logger.warning(f"Skipping empty query: {query_name}")
                continue

            datasets.append(
                {"name": query_name, "displayName": query_name.replace("_", " ").title(), "query": query_text}
            )

        # Build page layout
        page_layout = []
        for idx, layout_item in enumerate(metadata.layout):
            widget_name = layout_item.get("name", f"widget_{idx}")
            query_name = layout_item.get("query", "")

            # Skip widgets with empty or missing queries
            if not query_name or query_name not in metadata.queries:
                logger.warning(f"Skipping widget {widget_name}: query '{query_name}' not found")
                continue

            # Build widget with dataset reference
            position = layout_item.get("position", {})

            widget = {
                "name": widget_name,
                "queries": [
                    {"name": query_name, "query": {"datasetName": query_name, "fields": [], "disaggregated": False}}
                ],
                "spec": self._build_widget_spec(layout_item),
            }

            page_layout.append(
                {
                    "widget": widget,
                    "position": {
                        "x": position.get("x", (idx % 4) * 3),
                        "y": position.get("y", (idx // 4) * 2),
                        "width": position.get("width", 6),
                        "height": position.get("height", 4),
                    },
                }
            )

        # Build complete dashboard definition
        dashboard_def = {
            "pages": [
                {
                    "name": "main_page",
                    "displayName": "Main",
                    "layout": page_layout,
                }
            ],
            "datasets": datasets,
        }

        return dashboard_def

    def _build_widget_spec(self, layout_item: dict[str, Any]) -> dict[str, Any]:
        """
        Build widget specification from layout item.

        Args:
            layout_item: Layout item configuration

        Returns:
            Widget specification dictionary
        """
        widget_type = layout_item.get("type", "table")

        spec = {
            "version": 3,
            "widgetType": widget_type,
            "frame": {
                "showTitle": True,
                "title": layout_item.get("title", ""),
            },
        }

        # Add encodings based on widget type
        encodings = layout_item.get("encodings", {})
        if encodings:
            spec["encodings"] = encodings

        return spec
