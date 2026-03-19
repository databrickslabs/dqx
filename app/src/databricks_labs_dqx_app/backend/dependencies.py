import os
from contextlib import contextmanager
from typing import Annotated

from databricks.connect import DatabricksSession
from databricks.labs.dqx.config import LLMModelConfig
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from fastapi import Depends, Header, HTTPException, status
from pyspark.sql import SparkSession

from .common.authentication.sql import SQLAuthentication
from .common.connectors.sql import SQLConnector
from .config import conf, get_sql_warehouse_path
from .logger import logger
from .migrations import MigrationRunner
from .runtime import rt
from .services.app_settings_service import AppSettingsService
from .services.discovery import DiscoveryService
from .services.rules_catalog_service import RulesCatalogService


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


def get_spark(
    token: Annotated[str | None, Header(alias="X-Forwarded-Access-Token")] = None,
) -> SparkSession:
    """
    Create a Databricks Spark Connect session with OBO authentication on serverless compute.

    This follows the Databricks Apps pattern for using OBO tokens with serverless compute.
    Works in both production (Databricks Apps) and local development environments.

    Args:
        token: User's access token from the X-Forwarded-Access-Token header.
               Automatically provided by Databricks when the app runs on the platform.

    Returns:
        SparkSession: A Databricks Spark Connect session configured with OBO token.

    Raises:
        HTTPException: 401 Unauthorized if the X-Forwarded-Access-Token header is not present.
    """
    if not token:
        logger.warning("OBO token is not provided in the header X-Forwarded-Access-Token for Spark session")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required. Please refresh the page or contact your administrator.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Get Databricks host from environment
    host = os.environ.get("DATABRICKS_HOST")
    if not host:
        logger.info("DATABRICKS_HOST not set, using default configuration for local development")
        return DatabricksSession.builder.token(token).getOrCreate()

    # Temporarily remove OAuth env vars to avoid multi-auth conflicts
    with _without_oauth_env_vars():
        logger.info(f"Creating Spark session with OBO token on serverless compute for host: {host}")
        session = (
            DatabricksSession.builder.host(host)
            .token(token)  # Use the forwarded OBO access token
            .serverless()
            .getOrCreate()
        )
    return session


def get_migration_runner() -> MigrationRunner:
    """Create a MigrationRunner using app (SP) credentials.

    Used at startup to ensure the database schema is current before any
    request is served.  All DDL runs as the app service principal.
    """
    wh_id = os.environ.get("DATABRICKS_WAREHOUSE_ID") or os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID", "")
    return MigrationRunner(
        ws=rt.ws,
        warehouse_id=wh_id,
        catalog=conf.catalog,
        schema=conf.schema_name,
    )


def get_app_settings_service() -> AppSettingsService:
    """Create an AppSettingsService using app (SP) credentials.

    Config is managed centrally by admins and stored in a Delta table,
    not per-user workspace files.  All operations use the app's service
    principal, not the calling user's OBO token.
    """
    wh_id = os.environ.get("DATABRICKS_WAREHOUSE_ID") or os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID", "")
    return AppSettingsService(
        ws=rt.ws,
        warehouse_id=wh_id,
        catalog=conf.catalog,
        schema=conf.schema_name,
    )


def get_engine(
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)], spark: Annotated[SparkSession, Depends(get_spark)]
) -> DQEngine:
    """
    Create a DQEngine instance with OBO authentication and Spark session.

    This dependency combines:
    - WorkspaceClient with user's identity (via get_obo_ws)
    - SparkSession for Spark operations (via get_spark)

    The DQEngine can then execute data quality checks and operations
    on behalf of the logged-in user.

    Args:
        obo_ws: WorkspaceClient with OBO authentication (injected by FastAPI).
        spark: SparkSession for data operations (injected by FastAPI).

    Returns:
        DQEngine: Configured for data quality operations with user context.

    Example usage:
        @router.post("/run-quality-check")
        def run_check(engine: Annotated[DQEngine, Depends(get_engine)]):
            result = engine.run_checks(...)
            return {"status": "success", "results": result}
    """
    return DQEngine(workspace_client=obo_ws, spark=spark)


def get_generator(
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    spark: Annotated[SparkSession, Depends(get_spark)],
    token: Annotated[str | None, Header(alias="X-Forwarded-Access-Token")] = None,
) -> DQGenerator:
    """
    Create a DQGenerator instance with OBO authentication and Spark session.

    This dependency provides an AI-assisted data quality rules generator that
    can create checks from natural language descriptions on behalf of the
    logged-in user. The Spark session is used for data profiling and analysis.

    The LLM model is configured to use the OBO token for authentication, ensuring
    that LLM API calls also run with the user's identity.

    Args:
        obo_ws: WorkspaceClient with OBO authentication (injected by FastAPI).
        spark: SparkSession for data operations (injected by FastAPI).
        token: User's OBO token for LLM authentication (injected by FastAPI).

    Returns:
        DQGenerator: Configured for AI-assisted rules generation with user context.

    Example usage:
        @router.post("/generate-checks")
        def generate_checks(generator: Annotated[DQGenerator, Depends(get_generator)], user_input: str):
            checks = generator.generate_dq_rules_ai_assisted(user_input=user_input)
            return {"checks": checks}
    """
    if not token:
        logger.warning("OBO token is not provided in the header X-Forwarded-Access-Token for Spark session")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required. Please refresh the page or contact your administrator.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    host = os.environ.get("DATABRICKS_HOST", "")
    if host:  # DBX App
        llm_model_config = LLMModelConfig(
            api_key=token,  # Configure LLM to use OBO token for authentication
        )
    else:  # Local development
        logger.info("DATABRICKS_HOST not set, using default configuration for LLM")
        llm_model_config = LLMModelConfig()

    return DQGenerator(workspace_client=obo_ws, spark=spark, llm_model_config=llm_model_config)


def get_rules_catalog_service() -> RulesCatalogService:
    """Create a RulesCatalogService using app (SP) credentials.

    Rules are stored centrally in a Delta table.  All operations use the
    app's service principal, not the calling user's OBO token.
    """
    wh_id = os.environ.get("DATABRICKS_WAREHOUSE_ID") or os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID", "")
    return RulesCatalogService(
        ws=rt.ws,
        warehouse_id=wh_id,
        catalog=conf.catalog,
        schema=conf.schema_name,
    )


def get_discovery_service(
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> DiscoveryService:
    """Create a DiscoveryService using the OBO-authenticated WorkspaceClient."""
    return DiscoveryService(obo_ws)


def get_sql_connector(
    token: Annotated[str | None, Header(alias="X-Forwarded-Access-Token")] = None,
) -> SQLConnector:
    """Create a SQLConnector using the OBO token and configured SQL warehouse."""
    auth = SQLAuthentication(bearer=token)
    host = os.environ.get("DATABRICKS_HOST", "")
    if not host:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="DATABRICKS_HOST is not configured.",
        )
    http_path = get_sql_warehouse_path()
    return SQLConnector(
        access_token=auth.access_token,
        server_hostname=host,
        http_path=http_path,
    )
