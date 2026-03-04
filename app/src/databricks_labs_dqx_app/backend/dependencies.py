import os
from contextlib import contextmanager
from typing import Annotated

from databricks.labs.dqx.config import LLMModelConfig
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.table_manager import TableManager, SDKTableDataProvider
from databricks.sdk import WorkspaceClient
from fastapi import Depends, Header, HTTPException, status

from .logger import logger


@contextmanager
def _without_oauth_env_vars():
    """
    Temporarily remove OAuth environment variables to avoid conflicts with obo token auth.

    Restores them after the context exits so other services can use them if needed.
    """
    oauth_vars = ["DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET"]
    saved_values = {}

    # Save and temporarily remove OAuth env vars
    for var in oauth_vars:
        if var in os.environ:
            saved_values[var] = os.environ[var]
            del os.environ[var]
            logger.debug(f"Temporarily removed {var} for OBO Spark authentication")

    try:
        yield
    finally:
        # Restore OAuth env vars for other services (DSPy/litellm)
        for var, value in saved_values.items():
            os.environ[var] = value
            logger.debug(f"Restored {var} for LLM authentication")


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
        logger.warning("OBO token is not provided in the header X-Forwarded-Access-Token for Spark session")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required. Please refresh the page or contact your administrator.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return WorkspaceClient(token=token, auth_type="pat")  # set pat explicitly to avoid issues with SP client



def get_engine(
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> DQEngine:
    """
    Create a DQEngine instance with OBO authentication.

    Used for checks load/save operations against workspace files. These operations
    use the Databricks SDK (WorkspaceClient) and do not require an active Spark session.
    DQEngine will create a Spark session lazily only if a Spark-backed storage config
    (e.g. TableChecksStorageConfig) is used.

    Args:
        obo_ws: WorkspaceClient with OBO authentication (injected by FastAPI).

    Returns:
        DQEngine: Configured for data quality operations with user context.
    """
    return DQEngine(workspace_client=obo_ws)


def get_generator(
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    token: Annotated[str | None, Header(alias="X-Forwarded-Access-Token")] = None,
) -> DQGenerator:
    """
    Create a DQGenerator instance using SDK-based table access (no Spark required).

    Uses SDKTableDataProvider backed by WorkspaceClient.tables.get() instead of
    Spark Connect, removing the need for serverless compute and the associated
    OAuth scopes for AI-assisted check generation.

    The LLM model is configured to use the OBO token for authentication, ensuring
    LLM API calls run with the user's identity.

    Args:
        obo_ws: WorkspaceClient with OBO authentication (injected by FastAPI).
        token: User's OBO token for LLM authentication (injected by FastAPI).

    Returns:
        DQGenerator: Configured for AI-assisted rules generation with user context.
    """
    if not token:
        logger.warning("OBO token is not provided in the header X-Forwarded-Access-Token")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required. Please refresh the page or contact your administrator.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    host = os.environ.get("DATABRICKS_HOST", "")
    if host:  # DBX App
        llm_model_config = LLMModelConfig(api_key=token)
    else:  # Local development
        logger.info("DATABRICKS_HOST not set, using default LLM configuration")
        llm_model_config = LLMModelConfig()

    table_manager = TableManager(repository=SDKTableDataProvider(workspace_client=obo_ws))
    return DQGenerator(workspace_client=obo_ws, llm_model_config=llm_model_config, table_manager=table_manager)
