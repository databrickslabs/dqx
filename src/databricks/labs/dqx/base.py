import abc
from typing import final
from functools import cached_property
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.__about__ import __version__


class DQEngineBase(abc.ABC):
    def __init__(self, workspace_client: WorkspaceClient):
        self._workspace_client = workspace_client

    @cached_property
    def ws(self) -> WorkspaceClient:
        """
        Cached property to verify and return the workspace client.
        """
        return self._verify_workspace_client(self._workspace_client)

    @staticmethod
    @final
    def _verify_workspace_client(ws: WorkspaceClient) -> WorkspaceClient:
        """
        Verifies the Databricks workspace client configuration.
        """
        # Using reflection to set right value for _product_info as dqx for telemetry
        product_info = getattr(ws.config, '_product_info')
        if product_info[0] != "dqx":
            setattr(ws.config, '_product_info', ('dqx', __version__))
        # make sure Unity Catalog is accessible in the current Databricks workspace
        ws.catalogs.list()
        return ws
