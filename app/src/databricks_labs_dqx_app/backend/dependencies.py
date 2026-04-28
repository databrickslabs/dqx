from __future__ import annotations

import asyncio
import hashlib
import os
from collections.abc import Callable
from typing import TYPE_CHECKING, Annotated, Any

if TYPE_CHECKING:
    from .common.connectors.sql import SQLConnector

from databricks.labs.dqx.checks_validator import ChecksValidationStatus
from databricks.sdk import WorkspaceClient
from fastapi import Depends, Header, HTTPException, status

from .cache import app_cache
from .common.authentication.sql import SQLAuthentication
from .common.authorization import UserRole, get_user_email
from .config import AppConfig, conf, get_sql_warehouse_path
from .logger import logger
from .migrations import MigrationRunner
from .runtime import rt
from .services.ai_rules_service import AiRulesService
from .services.app_settings_service import AppSettingsService
from .services.discovery import DiscoveryService
from .services.job_service import JobService
from .services.role_service import RoleService
from .services.rules_catalog_service import RulesCatalogService
from .services.comments_service import CommentsService
from .services.schedule_config_service import ScheduleConfigService
from .services.view_service import ViewService
from .sql_executor import SqlExecutor

_SP_TTL = 45 * 60  # 45 minutes
_OBO_TTL = 45 * 60  # 45 minutes


# ---------------------------------------------------------------------------
# Service-principal WorkspaceClient — cached for 45 minutes
# ---------------------------------------------------------------------------


@app_cache.cached("auth:sp", ttl=_SP_TTL)
async def get_sp_ws() -> WorkspaceClient:
    """Return the app's service-principal WorkspaceClient, cached for 45 min."""
    return WorkspaceClient()


# ---------------------------------------------------------------------------
# OBO WorkspaceClient — cached per token hash for 45 minutes
# ---------------------------------------------------------------------------


@app_cache.cached("auth:obo:{token_hash}", ttl=_OBO_TTL)
async def _create_obo_ws(token_hash: str, token: str) -> WorkspaceClient:  # noqa: ARG001
    return WorkspaceClient(token=token, auth_type="pat")


async def get_obo_ws(
    token: Annotated[str | None, Header(alias="X-Forwarded-Access-Token")] = None,
) -> WorkspaceClient:
    """Return a WorkspaceClient for the logged-in user (OBO), cached for 45 min.

    When a Databricks App runs on the platform, the X-Forwarded-Access-Token
    header is automatically injected with the logged-in user's access token.
    The token is hashed before use as a cache key so raw tokens are never
    stored in the key space.
    """
    if not token:
        logger.warning("OBO token is not provided in the header X-Forwarded-Access-Token")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required. Please refresh the page or contact your administrator.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token_hash = hashlib.sha256(token.encode()).hexdigest()
    return await _create_obo_ws(token_hash, token)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_warehouse_id() -> str:
    return os.environ.get("DATABRICKS_WAREHOUSE_ID") or os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID") or ""


# ---------------------------------------------------------------------------
# SqlExecutor factories
# ---------------------------------------------------------------------------


async def get_sp_sql_executor(
    sp_ws: Annotated[WorkspaceClient, Depends(get_sp_ws)],
) -> SqlExecutor:
    """SqlExecutor using the app's service-principal credentials (main schema)."""
    return SqlExecutor(
        ws=sp_ws,
        warehouse_id=_get_warehouse_id(),
        catalog=conf.catalog,
        schema=conf.schema_name,
    )


async def get_obo_sql_executor(
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> SqlExecutor:
    """SqlExecutor using the caller's OBO credentials (tmp schema)."""
    return SqlExecutor(
        ws=obo_ws,
        warehouse_id=_get_warehouse_id(),
        catalog=conf.catalog,
        schema=conf.tmp_schema_name,
    )


# ---------------------------------------------------------------------------
# Service factories
# ---------------------------------------------------------------------------


async def get_migration_runner(
    sql: Annotated[SqlExecutor, Depends(get_sp_sql_executor)],
) -> MigrationRunner:
    """Create a MigrationRunner using app (SP) credentials."""
    return MigrationRunner(sql=sql)


async def get_app_settings_service(
    sql: Annotated[SqlExecutor, Depends(get_sp_sql_executor)],
) -> AppSettingsService:
    """Create an AppSettingsService using app (SP) credentials."""
    return AppSettingsService(sql=sql)


async def get_role_service(
    sql: Annotated[SqlExecutor, Depends(get_sp_sql_executor)],
) -> RoleService:
    """Create a RoleService using app (SP) credentials."""
    return RoleService(sql=sql)


async def get_ai_rules_service(
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    sp_ws: Annotated[WorkspaceClient, Depends(get_sp_ws)],
) -> AiRulesService:
    """Create an AiRulesService with split authentication.

    Schema lookups use the OBO client (user's UC permissions).
    LLM calls use the SP client (service principal has the serving scope OBO tokens lack).
    """
    return AiRulesService(obo_ws=obo_ws, sp_ws=sp_ws)


async def get_rules_catalog_service(
    sql: Annotated[SqlExecutor, Depends(get_sp_sql_executor)],
) -> RulesCatalogService:
    """Create a RulesCatalogService using app (SP) credentials."""
    return RulesCatalogService(sql=sql)


async def get_discovery_service(
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> DiscoveryService:
    """Create a DiscoveryService using the OBO-authenticated WorkspaceClient."""
    me = await asyncio.to_thread(obo_ws.current_user.me)
    user_id = me.user_name or me.id or "unknown"
    return DiscoveryService(ws=obo_ws, user_id=user_id)


async def get_view_service(
    sql: Annotated[SqlExecutor, Depends(get_obo_sql_executor)],
    sp_sql: Annotated[SqlExecutor, Depends(get_sp_sql_executor)],
) -> ViewService:
    """Create a ViewService with split auth.

    View creation uses the user's OBO token so that table permissions are
    enforced.  Schema DDL uses the SP executor so that users don't need
    catalog-level CREATE SCHEMA privileges.
    """
    return ViewService(sql=sql, sp_sql=sp_sql)


async def get_comments_service(
    sql: Annotated[SqlExecutor, Depends(get_sp_sql_executor)],
) -> CommentsService:
    """Create a CommentsService using app (SP) credentials."""
    return CommentsService(sql=sql)


async def get_schedule_config_service(
    sql: Annotated[SqlExecutor, Depends(get_sp_sql_executor)],
) -> ScheduleConfigService:
    """Create a ScheduleConfigService using app (SP) credentials."""
    return ScheduleConfigService(sql=sql)


def get_conf() -> AppConfig:
    """Return the app configuration singleton."""
    return conf


def get_check_validator() -> Callable[[list[Any]], ChecksValidationStatus]:
    """Return DQEngine.validate_checks for injection into route handlers."""
    from databricks.labs.dqx.engine import DQEngine

    return DQEngine.validate_checks


async def get_job_service(
    sp_ws: Annotated[WorkspaceClient, Depends(get_sp_ws)],
    sql: Annotated[SqlExecutor, Depends(get_sp_sql_executor)],
) -> JobService:
    """Create a JobService using app (SP) credentials.

    Job submission and polling run as the app's service principal.
    """
    return JobService(ws=sp_ws, job_id=conf.job_id, sql=sql)


async def get_sql_connector(
    token: Annotated[str | None, Header(alias="X-Forwarded-Access-Token")] = None,
) -> "SQLConnector":
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


# ---------------------------------------------------------------------------
# Role-based authorization
# ---------------------------------------------------------------------------


async def get_user_role(
    email: Annotated[str, Depends(get_user_email)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    role_svc: Annotated[RoleService, Depends(get_role_service)],
) -> UserRole:
    """Resolve the user's role based on Databricks group membership.

    Resolution priority:
    1. Bootstrap admin group from DQX_ADMIN_GROUP env var
    2. Groups mapped to roles in dq_role_mappings table
    3. Default to VIEWER if no mappings match
    """
    try:
        user = await asyncio.to_thread(obo_ws.current_user.me)
        user_groups = [g.display for g in (user.groups or []) if g.display]
        logger.debug(f"Resolving role for {email} with groups: {user_groups}")

        role = role_svc.resolve_role(user_groups, conf.admin_group)
        logger.debug(f"Resolved role for {email}: {role.value}")
        return role
    except Exception as e:
        logger.error(f"Error resolving role for {email}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Role resolution is temporarily unavailable. Please try again later.",
        ) from e


def require_role(*roles: UserRole):
    """Dependency factory that rejects requests from users without one of the listed roles."""

    async def _check(role: Annotated[UserRole, Depends(get_user_role)]) -> UserRole:
        if role not in roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Requires one of {[r.value for r in roles]}, but user has '{role.value}'.",
            )
        return role

    return Depends(_check)


CurrentUserRole = Annotated[UserRole, Depends(get_user_role)]


async def get_user_catalog_names(
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> frozenset[str]:
    """Return the set of catalog names the current user can access (via OBO)."""
    catalogs = await asyncio.to_thread(lambda: list(obo_ws.catalogs.list()))
    return frozenset(c.name for c in catalogs if c.name)


# Re-export rt for any remaining usages during transition
__all__ = [
    "get_sp_ws",
    "get_obo_ws",
    "get_sp_sql_executor",
    "get_obo_sql_executor",
    "get_conf",
    "get_check_validator",
    "get_migration_runner",
    "get_app_settings_service",
    "get_role_service",
    "get_ai_rules_service",
    "get_rules_catalog_service",
    "get_discovery_service",
    "get_view_service",
    "get_job_service",
    "get_sql_connector",
    "get_user_role",
    "get_comments_service",
    "get_schedule_config_service",
    "require_role",
    "CurrentUserRole",
    "get_user_catalog_names",
    "rt",
]
