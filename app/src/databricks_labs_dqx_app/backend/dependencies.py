from __future__ import annotations

import os
from typing import TYPE_CHECKING, Annotated

if TYPE_CHECKING:
    from .common.connectors.sql import SQLConnector

from databricks.labs.dqx.config import LLMModelConfig
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.table_manager import SDKTableDataProvider, TableManager
from databricks.sdk import WorkspaceClient
from fastapi import Depends, Header, HTTPException, status

from .common.authentication.sql import SQLAuthentication
from .config import conf, get_sql_warehouse_path
from .logger import logger
from .migrations import MigrationRunner
from .runtime import rt
from .services.app_settings_service import AppSettingsService
from .services.discovery import DiscoveryService
from .services.job_service import JobService
from .services.rules_catalog_service import RulesCatalogService
from .services.view_service import ViewService


def get_obo_ws(
    token: Annotated[str | None, Header(alias="X-Forwarded-Access-Token")] = None,
) -> WorkspaceClient:
    """Create a Databricks WorkspaceClient using On-Behalf-Of (OBO) authentication.

    When a Databricks App runs on the platform, the X-Forwarded-Access-Token header
    is automatically injected with the logged-in user's access token.
    """
    if not token:
        logger.warning("OBO token is not provided in the header X-Forwarded-Access-Token")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required. Please refresh the page or contact your administrator.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return WorkspaceClient(token=token, auth_type="pat")  # set pat explicitly to avoid issues with SP client


def _get_warehouse_id() -> str:
    return os.environ.get("DATABRICKS_WAREHOUSE_ID") or os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID") or ""


def get_migration_runner() -> MigrationRunner:
    """Create a MigrationRunner using app (SP) credentials."""
    return MigrationRunner(
        ws=rt.ws,
        warehouse_id=_get_warehouse_id(),
        catalog=conf.catalog,
        schema=conf.schema_name,
    )


def get_app_settings_service() -> AppSettingsService:
    """Create an AppSettingsService using app (SP) credentials."""
    return AppSettingsService(
        ws=rt.ws,
        warehouse_id=_get_warehouse_id(),
        catalog=conf.catalog,
        schema=conf.schema_name,
    )


def get_generator(
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    token: Annotated[str | None, Header(alias="X-Forwarded-Access-Token")] = None,
) -> DQGenerator:
    """Create a DQGenerator instance with OBO authentication (no Spark).

    Uses the workspace client for table metadata via SDK REST API.
    The LLM model is configured to use the OBO token for authentication.
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
        llm_model_config = LLMModelConfig(
            api_key=token,  # Configure LLM to use OBO token for authentication
        )
    else:  # Local development
        logger.info("DATABRICKS_HOST not set, using default configuration for LLM")
        llm_model_config = LLMModelConfig()

    table_manager = TableManager(repository=SDKTableDataProvider(obo_ws))
    return DQGenerator(
        workspace_client=obo_ws,
        llm_model_config=llm_model_config,
        table_manager=table_manager,
    )


def get_rules_catalog_service() -> RulesCatalogService:
    """Create a RulesCatalogService using app (SP) credentials."""
    return RulesCatalogService(
        ws=rt.ws,
        warehouse_id=_get_warehouse_id(),
        catalog=conf.catalog,
        schema=conf.schema_name,
    )


def get_discovery_service(
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> DiscoveryService:
    """Create a DiscoveryService using the OBO-authenticated WorkspaceClient."""
    return DiscoveryService(obo_ws)


def get_view_service(
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> ViewService:
    """Create a ViewService using the OBO-authenticated WorkspaceClient.

    View creation uses the user's token so that table permissions are enforced.
    """
    return ViewService(
        ws=obo_ws,
        warehouse_id=_get_warehouse_id(),
        catalog=conf.catalog,
        schema=conf.schema_name,
    )


def get_job_service() -> JobService:
    """Create a JobService using app (SP) credentials.

    Job submission and polling run as the app's service principal.
    """
    return JobService(
        ws=rt.ws,
        job_id=conf.job_id,
        catalog=conf.catalog,
        schema=conf.schema_name,
        warehouse_id=_get_warehouse_id(),
    )


def get_sql_connector(
    token: Annotated[str | None, Header(alias="X-Forwarded-Access-Token")] = None,
) -> SQLConnector:
    """Create a SQLConnector using the OBO token and configured SQL warehouse."""
    from .common.connectors.sql import SQLConnector

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
