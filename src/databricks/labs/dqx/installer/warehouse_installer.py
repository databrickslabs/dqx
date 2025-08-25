import logging
import time
from typing import Any
from databricks.labs.blueprint.tui import Prompts
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import (
    InvalidParameterValue,
    ResourceDoesNotExist,
)
from databricks.sdk.service.sql import (
    CreateWarehouseRequestWarehouseType,
    SpotInstancePolicy,
    EndpointInfoWarehouseType,
)


logger = logging.getLogger(__name__)
WAREHOUSE_PREFIX = "DQX Dashboard"


class WarehouseInstaller:
    """Configures or selects a SQL warehouse used by the dashboards.
    Encapsulates all interactions with the Databricks SQL Warehouses API.
    """

    def __init__(self, workspace_client: WorkspaceClient, prompts: Prompts):
        self._ws = workspace_client
        self._prompts = prompts

    def create(self) -> str:
        """
        Select an existing PRO or SERVERLESS warehouse or create a new PRO warehouse.
        """

        def warehouse_type(endpoint: Any) -> str:
            return endpoint.warehouse_type.value if not endpoint.enable_serverless_compute else "SERVERLESS"

        pro_warehouses = {" [Create new PRO or SERVERLESS SQL warehouse ] ": "create_new"} | {
            f"{_.name} ({_.id}, {warehouse_type(_)}, {_.state.value})": _.id
            for _ in self._ws.warehouses.list()
            if _.warehouse_type == EndpointInfoWarehouseType.PRO
        }

        warehouse_id = self._prompts.choice_from_dict(
            "Select PRO or SERVERLESS SQL warehouse to run data quality dashboards on", pro_warehouses
        )
        if warehouse_id == "create_new":
            new_warehouse = self._ws.warehouses.create(
                name=f"{WAREHOUSE_PREFIX} {time.time_ns()}",
                spot_instance_policy=SpotInstancePolicy.COST_OPTIMIZED,
                warehouse_type=CreateWarehouseRequestWarehouseType.PRO,
                cluster_size="Small",
                max_num_clusters=1,
            )
            warehouse_id = new_warehouse.id
        return warehouse_id

    def remove(self, warehouse_id: str):
        try:
            warehouse_name = self._ws.warehouses.get(warehouse_id).name
            if warehouse_name and warehouse_name.startswith(WAREHOUSE_PREFIX):
                logger.info(f"Deleting {warehouse_name}.")
                self._ws.warehouses.delete(id=warehouse_id)
        except InvalidParameterValue:
            logger.error("Error accessing warehouse details")
        except ResourceDoesNotExist as e:
            logger.warning(f"Warehouse with id {warehouse_id} does not exist anymore: {e}")
