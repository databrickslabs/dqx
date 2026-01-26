from typing import Annotated

from databricks.connect import DatabricksSession
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from fastapi import Depends, Header


def get_obo_ws(
    token: Annotated[str | None, Header(alias="X-Forwarded-Access-Token")] = None,
) -> WorkspaceClient:
    """
    Returns a Databricks Workspace client with authentication behalf of user.
    If the request contains an X-Forwarded-Access-Token header, on behalf of user authentication is used.

    Example usage:
    @api.get("/items/")
    async def read_items(obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)]):
        # do something with the obo_ws
        ...
    """

    if not token:
        raise ValueError("OBO token is not provided in the header X-Forwarded-Access-Token")

    return WorkspaceClient(token=token, auth_type="pat")  # set pat explicitly to avoid issues with SP client


def get_dqx_engine(obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)]) -> DQEngine:
    spark = DatabricksSession.builder.getOrCreate()
    return DQEngine(workspace_client=obo_ws, spark=spark)
