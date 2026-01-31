from typing import Annotated

from databricks.connect import DatabricksSession
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from fastapi import Depends, Header, HTTPException, status

from .logger import logger


def get_obo_ws(
    token: Annotated[str | None, Header(alias="X-Forwarded-Access-Token")] = None,
) -> WorkspaceClient:
    """
    Create a Databricks WorkspaceClient using On-Behalf-Of (OBO) authentication.

    When a Databricks App runs on the platform, the X-Forwarded-Access-Token header
    is automatically injected with the logged-in user's access token. This function
    extracts that token and creates a WorkspaceClient that performs all operations
    with the user's identity and permissions.

    This dependency allows FastAPI routes to:
    - Access workspace resources (notebooks, clusters, jobs, etc.) as the user
    - Respect user permissions (users only see what they have access to)
    - Generate proper audit logs attributing actions to the correct user

    Args:
        token: User's access token from the X-Forwarded-Access-Token header.
               Automatically provided by Databricks when the app runs on the platform.

    Returns:
        WorkspaceClient: Configured with the user's token for OBO operations.

    Raises:
        HTTPException: 401 Unauthorized if the X-Forwarded-Access-Token header is not present.

    Example usage:
        @router.get("/current-user")
        def get_current_user(ws: Annotated[WorkspaceClient, Depends(get_obo_ws)]):
            user = ws.current_user.me()
            return {"user_name": user.user_name, "email": user.emails[0].value}
    """

    if not token:
        logger.warning("OBO token is not provided in the header X-Forwarded-Access-Token")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required. Please refresh the page or contact your administrator.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return WorkspaceClient(token=token, auth_type="pat")  # set pat explicitly to avoid issues with SP client


def get_dqx_engine(obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)]) -> DQEngine:
    """
    Create a DQEngine instance with OBO authentication and Spark session.

    This dependency combines:
    - WorkspaceClient with user's identity (via get_obo_ws)
    - DatabricksSession for Spark operations

    The DQEngine can then execute data quality checks and operations
    on behalf of the logged-in user.

    Args:
        obo_ws: WorkspaceClient with OBO authentication (injected by FastAPI).

    Returns:
        DQEngine: Configured for data quality operations with user context.

    Example usage:
        @router.post("/run-quality-check")
        def run_check(engine: Annotated[DQEngine, Depends(get_dqx_engine)]):
            result = engine.run_checks(...)
            return {"status": "success", "results": result}
    """
    spark = DatabricksSession.builder.getOrCreate()
    return DQEngine(workspace_client=obo_ws, spark=spark)
