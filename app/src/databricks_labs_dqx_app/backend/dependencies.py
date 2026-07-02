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
from .services.ai_gateway import AIGateway
from .services.ai_rules_service import AiRulesService
from .services.app_settings_service import AppSettingsService
from .services.contract_rules_service import ContractRulesService
from .services.discovery import DiscoveryService
from .services.job_service import JobService
from .services.role_service import RoleService
from .services.registry_service import RegistryService
from .services.monitored_table_service import MonitoredTableService
from .services.apply_rules_service import ApplyRulesService
from .services.materializer import Materializer
from .services.rules_catalog_service import RulesCatalogService
from .services.comments_service import CommentsService
from .services.review_status_service import ReviewStatusService
from .services.schedule_config_service import ScheduleConfigService
from .services.view_service import ViewService
from .sql_executor import OltpExecutorProtocol, SqlExecutor

# Process-wide OLTP executor. Constructed once at app startup by
# ``app.lifespan`` and reused across all requests so the psycopg pool
# isn't rebuilt per call. ``None`` means Lakebase is not configured and
# the legacy Delta executor handles OLTP traffic instead.
#
# Annotated with :class:`OltpExecutorProtocol` so neither this module
# nor downstream callers need to import :class:`PgExecutor` directly
# — the Protocol is the only contract the OLTP plumbing depends on.
# Avoids dragging psycopg into the import graph for Delta-only
# deployments.
_pg_executor: OltpExecutorProtocol | None = None


def set_oltp_executor(executor: OltpExecutorProtocol | None) -> None:
    """Register (or clear) the process-wide OLTP executor.

    Called from :func:`backend.app.lifespan` after the connection pool
    is open.  Keeping this in module state (rather than passing it
    through every request) lets the FastAPI ``Depends`` graph stay
    request-local while still sharing the pool.
    """
    global _pg_executor
    _pg_executor = executor


def get_oltp_executor() -> OltpExecutorProtocol | None:
    """Return the registered OLTP executor or ``None`` if Lakebase is off."""
    return _pg_executor


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

    Local-dev fallback: the platform always injects the header and authenticates
    the app via DATABRICKS_CLIENT_ID/SECRET — it never has a CLI profile or PAT.
    So when the header is absent but a profile/token is configured (loaded from
    app/.env for local dev), we are running locally and fall back to the SDK
    default auth (that same profile) so OBO endpoints work without a header. This
    cannot trigger in production, where no profile/PAT is present.
    """
    if not token:
        profile = os.environ.get("DATABRICKS_CONFIG_PROFILE")
        if profile or os.environ.get("DATABRICKS_TOKEN"):
            logger.warning(
                "X-Forwarded-Access-Token missing — falling back to local default auth "
                "(profile=%s) for OBO. Local dev only: runs as your CLI identity, not an "
                "arbitrary end user.",
                profile or "(env)",
            )
            return WorkspaceClient()  # default auth chain: app/.env profile / token
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


async def get_sp_oltp_executor(
    sp_sql: Annotated[SqlExecutor, Depends(get_sp_sql_executor)],
) -> OltpExecutorProtocol:
    """Return the executor that owns the OLTP tables.

    When Lakebase is configured the lifespan handler registers a
    :class:`backend.pg_executor.PgExecutor` via :func:`set_oltp_executor`
    and we hand it back to every OLTP service.  Otherwise we fall back
    to the legacy Delta executor (``get_sp_sql_executor``) so existing
    deployments keep working with no code changes on their side.

    The return type is :class:`OltpExecutorProtocol` so every
    downstream service annotation type-checks against the structural
    surface shared by both executors. No ``cast`` is needed: both
    :class:`SqlExecutor` and :class:`PgExecutor` structurally satisfy
    the Protocol, so the type-checker validates every ``.execute()``,
    ``.query()`` etc. call against a single source of truth instead
    of being muted by a Delta-class cast around the Postgres path.
    """
    pg = get_oltp_executor()
    if pg is None:
        return sp_sql
    return pg


# ---------------------------------------------------------------------------
# Service factories
# ---------------------------------------------------------------------------


async def get_migration_runner(
    sql: Annotated[SqlExecutor, Depends(get_sp_sql_executor)],
) -> MigrationRunner:
    """Create the Delta MigrationRunner using app (SP) credentials.

    The Postgres :class:`PgMigrationRunner` is constructed separately
    in :func:`backend.app.lifespan` because it needs the running
    :class:`PgExecutor`, not a SQL warehouse executor.
    """
    return MigrationRunner(sql=sql)


async def get_app_settings_service(
    sql: Annotated[OltpExecutorProtocol, Depends(get_sp_oltp_executor)],
) -> AppSettingsService:
    """Create an AppSettingsService routed at the OLTP executor."""
    return AppSettingsService(sql=sql)


async def get_role_service(
    sql: Annotated[OltpExecutorProtocol, Depends(get_sp_oltp_executor)],
) -> RoleService:
    """Create a RoleService routed at the OLTP executor."""
    return RoleService(sql=sql)


async def get_ai_gateway(
    sp_ws: Annotated[WorkspaceClient, Depends(get_sp_ws)],
    app_settings: Annotated[AppSettingsService, Depends(get_app_settings_service)],
) -> AIGateway:
    """Create an AIGateway using the app's service-principal credentials.

    The service principal has the Foundation Model serving scope that OBO tokens lack, so
    every AIGateway call (kill-switch, rate limit, audit — see services/ai_gateway.py) runs
    as the SP, gated by role checks (``require_role``) at the route layer instead of by UC
    permissions on the serving endpoint itself.
    """
    return AIGateway(sp_ws=sp_ws, app_settings=app_settings)


async def get_ai_rules_service(
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    sp_ws: Annotated[WorkspaceClient, Depends(get_sp_ws)],
    gateway: Annotated[AIGateway, Depends(get_ai_gateway)],
) -> AiRulesService:
    """Create an AiRulesService with split authentication.

    Schema lookups use the OBO client (user's UC permissions). The legacy ChatDatabricks leg
    (contract importer) uses the SP client directly; the new AIGateway-backed purpose calls
    (generate_rule/suggest_field/generate_checks_via_gateway) go through the injected gateway.
    """
    return AiRulesService(obo_ws=obo_ws, sp_ws=sp_ws, gateway=gateway)


async def get_contract_rules_service(
    sp_ws: Annotated[WorkspaceClient, Depends(get_sp_ws)],
    ai_service: Annotated[AiRulesService, Depends(get_ai_rules_service)],
) -> ContractRulesService:
    """Create a ContractRulesService.

    Contract parsing is local and the generator doesn't touch UC, so we
    use the SP client — same pattern as the AI generator's LLM call leg.
    The AI service is injected so natural-language (``type: text``) quality
    expectations can be converted through the same ChatDatabricks leg the
    AI-Assisted Generation page uses (DQX's own text path needs dspy + Spark,
    which the app container lacks).
    """
    return ContractRulesService(sp_ws=sp_ws, ai_service=ai_service)


async def get_rules_catalog_service(
    sql: Annotated[OltpExecutorProtocol, Depends(get_sp_oltp_executor)],
) -> RulesCatalogService:
    """Create a RulesCatalogService routed at the OLTP executor."""
    return RulesCatalogService(sql=sql)


async def get_registry_service(
    sql: Annotated[OltpExecutorProtocol, Depends(get_sp_oltp_executor)],
) -> RegistryService:
    """Create a RegistryService routed at the OLTP executor."""
    return RegistryService(sql=sql)


async def get_monitored_table_service(
    sql: Annotated[OltpExecutorProtocol, Depends(get_sp_oltp_executor)],
    profiling_sql: Annotated[SqlExecutor, Depends(get_sp_sql_executor)],
) -> MonitoredTableService:
    """Create a MonitoredTableService.

    The OLTP tables (``dq_monitored_tables``/``dq_applied_rules``) are routed
    at the OLTP executor (Lakebase or Delta fallback); the profiling READ
    path always targets the Delta ``dq_profiling_results`` table via the SP
    SQL executor, since that table is written by the profiler job
    regardless of whether Lakebase is enabled.
    """
    return MonitoredTableService(sql=sql, profiling_sql=profiling_sql)


async def get_apply_rules_service(
    sql: Annotated[OltpExecutorProtocol, Depends(get_sp_oltp_executor)],
    registry: Annotated[RegistryService, Depends(get_registry_service)],
) -> ApplyRulesService:
    """Create an ApplyRulesService routed at the OLTP executor."""
    return ApplyRulesService(sql=sql, registry=registry)


async def get_materializer(
    sql: Annotated[OltpExecutorProtocol, Depends(get_sp_oltp_executor)],
    registry: Annotated[RegistryService, Depends(get_registry_service)],
    monitored_tables: Annotated[MonitoredTableService, Depends(get_monitored_table_service)],
    app_settings: Annotated[AppSettingsService, Depends(get_app_settings_service)],
) -> Materializer:
    """Create a Materializer wired to the registry, monitored-table, and settings services."""
    return Materializer(sql=sql, registry=registry, monitored_tables=monitored_tables, app_settings=app_settings)


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
    sql: Annotated[OltpExecutorProtocol, Depends(get_sp_oltp_executor)],
) -> CommentsService:
    """Create a CommentsService routed at the OLTP executor."""
    return CommentsService(sql=sql)


async def get_review_status_service(
    sql: Annotated[OltpExecutorProtocol, Depends(get_sp_oltp_executor)],
    settings: Annotated[AppSettingsService, Depends(get_app_settings_service)],
) -> ReviewStatusService:
    """Create a ReviewStatusService routed at the OLTP executor.

    Takes the same ``AppSettingsService`` we use everywhere else as a
    transitive dep so the catalogue of allowed status values comes from
    the same singleton (and same cache) as the Configuration page reads.
    """
    return ReviewStatusService(sql=sql, settings=settings)


async def get_schedule_config_service(
    sql: Annotated[OltpExecutorProtocol, Depends(get_sp_oltp_executor)],
) -> ScheduleConfigService:
    """Create a ScheduleConfigService routed at the OLTP executor."""
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

    On transient failures (SCIM hiccup, role-mapping table unavailable) we
    degrade gracefully to VIEWER instead of locking every user out with a 503.
    Read-only endpoints stay reachable; privileged endpoints return 403 until
    the upstream recovers, which is a clearer signal than service-unavailable.
    """
    try:
        user = await asyncio.to_thread(obo_ws.current_user.me)
        user_groups = [g.display for g in (user.groups or []) if g.display]
        logger.debug(f"Resolving role for {email} with groups: {user_groups}")

        role = role_svc.resolve_role(user_groups, conf.admin_group)
        logger.debug(f"Resolved role for {email}: {role.value}")
        return role
    except Exception as e:
        logger.warning(
            f"Role resolution failed for {email}, falling back to VIEWER: {e}",
            exc_info=True,
        )
        return UserRole.VIEWER


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


# ---------------------------------------------------------------------------
# Runner role — orthogonal to the primary-role hierarchy
# ---------------------------------------------------------------------------


async def get_user_runner_flag(
    email: Annotated[str, Depends(get_user_email)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    role_svc: Annotated[RoleService, Depends(get_role_service)],
) -> bool:
    """Return True iff the caller holds the orthogonal RUNNER role.

    Admins are implicit runners (handled inside ``has_runner_role``).
    On transient failures we degrade to ``False`` rather than 5xx so a
    SCIM hiccup doesn't break the whole UI; gated endpoints will then
    return 403 with a clearer message until the upstream recovers.
    """
    try:
        user = await asyncio.to_thread(obo_ws.current_user.me)
        user_groups = [g.display for g in (user.groups or []) if g.display]
        return role_svc.has_runner_role(user_groups, conf.admin_group)
    except Exception as exc:
        logger.warning(
            f"Runner-flag resolution failed for {email}, falling back to False: {exc}",
            exc_info=True,
        )
        return False


CurrentUserRunner = Annotated[bool, Depends(get_user_runner_flag)]


def require_runner():
    """Dependency that rejects callers who can't see the Run Rules page.

    Implements the user-facing rule: admins always pass; non-admins must
    be members of a group mapped to ``UserRole.RUNNER``. Other roles
    (author/approver/viewer) by themselves do **not** grant runner
    access — they need an explicit RUNNER mapping.
    """

    async def _check(
        role: Annotated[UserRole, Depends(get_user_role)],
        is_runner: Annotated[bool, Depends(get_user_runner_flag)],
    ) -> bool:
        if role == UserRole.ADMIN or is_runner:
            return True
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Requires the 'runner' role. Ask an admin to assign your group to the Runner role.",
        )

    return Depends(_check)


async def get_user_catalog_names(
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> frozenset[str]:
    """Return the set of catalog names the current user can access (via OBO).

    Intentionally **not** cached: this list drives authorization filtering on
    list endpoints (rules, dry-run runs/results), so a stale entry could leak
    rows for a catalog whose grant was just revoked. The underlying OBO
    ``WorkspaceClient`` is already cached, so each call only re-issues the
    UC ``catalogs.list`` request, not the full auth handshake.
    """
    catalogs = await asyncio.to_thread(lambda: list(obo_ws.catalogs.list()))
    return frozenset(c.name for c in catalogs if c.name)


# Re-export rt for any remaining usages during transition
__all__ = [
    "get_sp_ws",
    "get_obo_ws",
    "get_sp_sql_executor",
    "get_obo_sql_executor",
    "get_sp_oltp_executor",
    "get_oltp_executor",
    "set_oltp_executor",
    "get_conf",
    "get_check_validator",
    "get_migration_runner",
    "get_app_settings_service",
    "get_role_service",
    "get_ai_gateway",
    "get_ai_rules_service",
    "get_contract_rules_service",
    "get_rules_catalog_service",
    "get_registry_service",
    "get_monitored_table_service",
    "get_apply_rules_service",
    "get_materializer",
    "get_discovery_service",
    "get_view_service",
    "get_job_service",
    "get_sql_connector",
    "get_user_role",
    "get_comments_service",
    "get_review_status_service",
    "get_schedule_config_service",
    "require_role",
    "require_runner",
    "CurrentUserRole",
    "CurrentUserRunner",
    "get_user_runner_flag",
    "get_user_catalog_names",
    "rt",
]
