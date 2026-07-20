from __future__ import annotations

import asyncio
import hashlib
import os
from collections.abc import Callable
from typing import TYPE_CHECKING, Annotated, Any

if TYPE_CHECKING:
    from .common.connectors.sql import SQLConnector
    from .demo.seed_service import DemoSeedService

from databricks.labs.dqx.checks_validator import ChecksValidationStatus
from databricks.sdk import WorkspaceClient
from fastapi import Depends, Header, HTTPException, status

from .cache import app_cache
from .common.authentication.sql import SQLAuthentication
from .common.authorization import UserRole, get_user_email
from .config import AppConfig, conf, get_sql_warehouse_path
from .demo.manifest import SOURCE_SCHEMA as DEMO_SOURCE_SCHEMA
from .demo.status import DemoStatusStore
from .logger import logger
from .migrations import MigrationRunner
from .runtime import rt
from .services.ai_gateway import AIGateway
from .services.ai_rules_service import AiRulesService
from .services.app_settings_service import AppSettingsService
from .services.contract_rules_service import ContractRulesService
from .services.database_reset_service import DatabaseResetService
from .services.discovery import DiscoveryService
from .services.draft_run_gate_service import DraftRunGateService
from .services.job_service import JobService
from .services.role_service import RoleService
from .services.permissions_service import PermissionsService
from .services.registry_service import RegistryService
from .services.monitored_table_service import MonitoredTableService
from .services.apply_rules_service import ApplyRulesService
from .services.pending_application_service import PendingApplicationService
from .services.materializer import Materializer
from .services.monitored_table_versions import MonitoredTableVersionService
from .services.run_sets import RunSetService
from .services.binding_run_service import BindingRunService
from .services.data_product_service import DataProductService
from .services.export_service import ExportService
from .services.entitlement_service import EntitlementService
from .services.rule_embeddings import RuleEmbeddingsService
from .services.score_cache_service import ScoreCacheService
from .services.profiling_suggestion_service import ProfilingSuggestionService
from .services.rule_retriever import CosineRuleRetriever, RuleRetriever
from .services.rule_suggester import RuleSuggester
from .services.rules_catalog_service import RulesCatalogService
from .services.comments_service import CommentsService
from .services.compute_service import ComputeService, resolve_warehouse_id
from .services.rule_test_service import RuleTestService
from .services.table_data_service import TableDataService
from .services.review_status_service import ReviewStatusService
from .services.schedule_config_service import ScheduleConfigService
from .services.tag_mapping_service import ColumnInfo
from .services.tag_reconcile_service import TagReconcileService
from .services.tag_suggestion_service import TagSuggestionService
from .services.vector_store import VectorStoreProvisioner
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
_CATALOG_TTL = 30  # seconds — see get_user_catalog_names for the revocation trade-off


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


async def get_database_reset_service(
    delta_sql: Annotated[SqlExecutor, Depends(get_sp_sql_executor)],
    oltp_sql: Annotated[OltpExecutorProtocol, Depends(get_sp_oltp_executor)],
    sp_ws: Annotated[WorkspaceClient, Depends(get_sp_ws)],
    app_settings: Annotated[AppSettingsService, Depends(get_app_settings_service)],
) -> DatabaseResetService:
    """Create a DatabaseResetService over the Delta + OLTP executors.

    Analytical tables clear through the Delta (SP) executor; OLTP tables
    through whichever executor owns them (Postgres when Lakebase is enabled,
    else the same Delta executor).

    A best-effort Genie re-provision callable is threaded in: the reset wipes
    ``dq_app_settings`` (which holds ``dq_genie_space_id`` /
    ``dq_genie_space_status``), and ``ensure_dq_genie_space`` is otherwise only
    called at app startup — so without re-provisioning here the UI sits on
    "Setting up Genie…" after any reset. The callable mirrors the app-lifespan
    wiring (SP identity, bound warehouse, configured catalog/schema); it is
    ``None`` when no warehouse is bound so the reset simply skips the step.
    """
    return DatabaseResetService(
        delta_sql=delta_sql,
        oltp_sql=oltp_sql,
        app_settings=app_settings,
        genie_reprovision=_build_genie_reprovision(sp_ws, app_settings),
    )


def _build_genie_reprovision(sp_ws: WorkspaceClient, app_settings: AppSettingsService) -> "Callable[[], object] | None":
    """Build the zero-arg Genie re-provision callable, or None when unavailable.

    Mirrors ``backend.app._ensure_genie_space``: requires a bound SQL warehouse
    to attach a freshly-created space to, and resolves the SP's parent folder
    (falling back to ``/Shared``). ``ensure_dq_genie_space`` is itself idempotent
    and never raises out of its own body; the callable is invoked best-effort by
    the reset service, which records (never re-raises) any failure.
    """
    warehouse_id = _get_warehouse_id()
    if not warehouse_id:
        return None

    from .services.genie_space_service import ensure_dq_genie_space

    def _reprovision() -> object:
        try:
            parent_path = f"/Users/{sp_ws.current_user.me().user_name}"
        except Exception:
            # Best-effort: the parent folder is cosmetic — fall back to a
            # location every workspace has rather than skip provisioning.
            parent_path = "/Shared"
        return ensure_dq_genie_space(
            settings=app_settings,
            ws=sp_ws,
            warehouse_id=warehouse_id,
            parent_path=parent_path,
            catalog=conf.catalog,
            schema=conf.schema_name,
        )

    return _reprovision


async def get_permissions_service(
    sql: Annotated[OltpExecutorProtocol, Depends(get_sp_oltp_executor)],
    app_settings: Annotated[AppSettingsService, Depends(get_app_settings_service)],
) -> PermissionsService:
    """Create a PermissionsService (object-grant CRUD + enforcement)."""
    return PermissionsService(sql=sql, app_settings=app_settings)


async def get_ai_gateway(
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    app_settings: Annotated[AppSettingsService, Depends(get_app_settings_service)],
) -> AIGateway:
    """Create an AIGateway using the caller's OBO credentials.

    Every AIGateway call (kill-switch, rate limit, audit — see services/ai_gateway.py) runs
    as the calling user via their OBO token, so the serving-endpoint call is subject to the
    user's own UC permissions on the endpoint, not the app's service principal's. Role checks
    (``require_role``) at the route layer remain the app-level gate on top of that; the SP
    itself no longer needs (and should not be granted) query access to AI serving endpoints.
    """
    return AIGateway(user_ws=obo_ws, app_settings=app_settings)


async def get_ai_rules_service(
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    gateway: Annotated[AIGateway, Depends(get_ai_gateway)],
) -> AiRulesService:
    """Create an AiRulesService, entirely OBO-authenticated.

    Schema lookups and both LLM legs (the legacy ChatDatabricks path used by the contract
    importer, and the AIGateway-backed purpose calls — generate_rule/suggest_field/
    generate_checks_via_gateway) run as the calling user, so every model invocation and UC
    read triggered by an AI-assisted request is subject to that user's own permissions.
    """
    return AiRulesService(obo_ws=obo_ws, gateway=gateway)


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
    app_settings: Annotated[AppSettingsService, Depends(get_app_settings_service)],
) -> ApplyRulesService:
    """Create an ApplyRulesService routed at the OLTP executor."""
    return ApplyRulesService(sql=sql, registry=registry, app_settings=app_settings)


async def get_pending_application_service(
    sql: Annotated[OltpExecutorProtocol, Depends(get_sp_oltp_executor)],
) -> PendingApplicationService:
    """Create a PendingApplicationService routed at the OLTP executor.

    Backs the Bulk Contract Import Phase 2 store (``dq_pending_applications``):
    the execute step records applications for rules that land
    ``pending_approval``, and ``_publish_registry_rule`` drains them into real
    ``dq_applied_rules`` links on the rule's approval.
    """
    return PendingApplicationService(sql=sql)


async def get_materializer(
    sql: Annotated[OltpExecutorProtocol, Depends(get_sp_oltp_executor)],
    registry: Annotated[RegistryService, Depends(get_registry_service)],
    monitored_tables: Annotated[MonitoredTableService, Depends(get_monitored_table_service)],
    app_settings: Annotated[AppSettingsService, Depends(get_app_settings_service)],
) -> Materializer:
    """Create a Materializer wired to the registry, monitored-table, and settings services."""
    return Materializer(sql=sql, registry=registry, monitored_tables=monitored_tables, app_settings=app_settings)


async def get_monitored_table_version_service(
    sql: Annotated[OltpExecutorProtocol, Depends(get_sp_oltp_executor)],
    monitored_tables: Annotated[MonitoredTableService, Depends(get_monitored_table_service)],
    rules_catalog: Annotated[RulesCatalogService, Depends(get_rules_catalog_service)],
    materializer: Annotated[Materializer, Depends(get_materializer)],
) -> MonitoredTableVersionService:
    """Create a MonitoredTableVersionService routed at the OLTP executor.

    Reads the binding's approved rows via ``RulesCatalogService`` and the
    applied-rule linkage via ``MonitoredTableService`` to freeze/re-freeze the
    reference snapshots in ``dq_monitored_table_versions``; the ``Materializer``
    reconstructs the runner payload on demand from the registry
    (``dq_rule_versions``) when :meth:`MonitoredTableVersionService.get_checks`
    is called.
    """
    return MonitoredTableVersionService(
        sql=sql,
        monitored_tables=monitored_tables,
        rules_catalog=rules_catalog,
        materializer=materializer,
    )


def _build_column_reader(
    ws: WorkspaceClient,
    sql: SqlExecutor,
) -> Callable[[str], list[ColumnInfo]]:
    """Build the column reader the tag-reconcile / tag-suggestion services consume.

    Reads each column's NAME and TYPE from ``ws.tables.get`` (the resolver's
    family filter needs ``type_name``) and each column's TAGS from
    ``<catalog>.information_schema.column_tags`` via *sql* — the reliable source
    for column governed tags (see :func:`read_column_tags`). A missing/failed
    ``tables.get`` degrades to ``[]`` for that table so one unreadable table
    never aborts a sweep; a tag-read failure degrades to no tags (columns still
    returned with their types). Never raises.

    Generic over the ``(ws, sql)`` auth pair: the reconcile path passes the SP
    client + SP warehouse executor; the suggestions path passes the OBO client +
    OBO warehouse executor so it respects the calling user's Unity Catalog
    permissions.
    """
    from .services.discovery import read_column_tags

    def read_columns(table_fqn: str) -> list[ColumnInfo]:
        try:
            table_info = ws.tables.get(full_name=table_fqn)
        except Exception:
            logger.warning(f"Failed to read columns for {table_fqn}")
            return []
        tags_by_column = read_column_tags(sql, table_fqn)
        columns: list[ColumnInfo] = []
        for col in table_info.columns or []:
            col_name = col.name or ""
            columns.append(
                ColumnInfo(
                    name=col_name,
                    type_name=(col.type_name.value if col.type_name else ""),
                    tags=tags_by_column.get(col_name, []),
                )
            )
        return columns

    return read_columns


async def get_tag_reconcile_service(
    registry: Annotated[RegistryService, Depends(get_registry_service)],
    monitored_tables: Annotated[MonitoredTableService, Depends(get_monitored_table_service)],
    apply_rules: Annotated[ApplyRulesService, Depends(get_apply_rules_service)],
    app_settings: Annotated[AppSettingsService, Depends(get_app_settings_service)],
    sp_ws: Annotated[WorkspaceClient, Depends(get_sp_ws)],
    sp_sql: Annotated[SqlExecutor, Depends(get_sp_sql_executor)],
) -> TagReconcileService:
    """Create the apply-on-tag orchestrator wired to SP-authed collaborators.

    Every collaborator routes at the SP OLTP/Delta executors (via the shared
    providers) because reconcile runs without a user context; the column reader
    uses the SP :class:`WorkspaceClient` for column names/types and the SP
    warehouse :class:`SqlExecutor` to read column tags from
    ``information_schema.column_tags``, for the same reason.
    """
    return TagReconcileService(
        registry=registry,
        monitored_tables=monitored_tables,
        apply_rules=apply_rules,
        app_settings=app_settings,
        read_columns=_build_column_reader(sp_ws, sp_sql),
    )


async def get_tag_suggestion_service(
    registry: Annotated[RegistryService, Depends(get_registry_service)],
    monitored_tables: Annotated[MonitoredTableService, Depends(get_monitored_table_service)],
    apply_rules: Annotated[ApplyRulesService, Depends(get_apply_rules_service)],
    app_settings: Annotated[AppSettingsService, Depends(get_app_settings_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    obo_sql: Annotated[SqlExecutor, Depends(get_obo_sql_executor)],
) -> TagSuggestionService:
    """Create the apply-on-tag tag-matcher (suggestions AND auto-apply).

    User-facing, so the column reader is built over the OBO
    :class:`WorkspaceClient` + OBO warehouse :class:`SqlExecutor` (via the same
    generic ``_build_column_reader`` factory) — so tag matching respects the
    calling user's Unity Catalog permissions. This is deliberate and
    load-bearing: the app service principal has no grant on user catalogs, so an
    SP-authed read of ``information_schema.column_tags`` returns nothing; running
    the match OBO is what makes auto-apply work.

    When ``tag_auto_apply`` is off the matches surface as suggestions
    (:meth:`~TagSuggestionService.suggest`); when on they auto-attach
    (:meth:`~TagSuggestionService.apply_matches`, from the register / open-table
    hooks). The applied-row WRITE still goes through the SP OLTP executor (via
    ``get_apply_rules_service``), which owns the app's schema.
    """
    return TagSuggestionService(
        registry=registry,
        monitored_tables=monitored_tables,
        apply_rules=apply_rules,
        app_settings=app_settings,
        read_columns=_build_column_reader(obo_ws, obo_sql),
    )


async def get_rule_embeddings_service(
    sql: Annotated[OltpExecutorProtocol, Depends(get_sp_oltp_executor)],
    sp_ws: Annotated[WorkspaceClient, Depends(get_sp_ws)],
    app_settings: Annotated[AppSettingsService, Depends(get_app_settings_service)],
) -> RuleEmbeddingsService:
    """Create a RuleEmbeddingsService routed at the OLTP executor + SP credentials.

    The SP client is used (not OBO) because embedding calls hit a serving
    endpoint, mirroring the ``AIGateway`` split-auth rationale.
    """
    return RuleEmbeddingsService(sql=sql, sp_ws=sp_ws, app_settings=app_settings)


async def get_rule_retriever(
    embeddings: Annotated[RuleEmbeddingsService, Depends(get_rule_embeddings_service)],
) -> RuleRetriever:
    """Create the production in-app cosine RuleRetriever (design spec §8 swappable seam).

    Cosine-over-the-OLTP-corpus (mirroring dqlake) is the default: it has no
    Vector Search index / provisioning-readiness dependency on the retrieval
    path, so suggestions work as soon as rules are embedded. See
    ``services.rule_retriever.CosineRuleRetriever``.
    """
    return CosineRuleRetriever(embeddings=embeddings)


async def get_vector_store_provisioner(
    sp_ws: Annotated[WorkspaceClient, Depends(get_sp_ws)],
    app_settings: Annotated[AppSettingsService, Depends(get_app_settings_service)],
    embeddings: Annotated[RuleEmbeddingsService, Depends(get_rule_embeddings_service)],
    registry: Annotated[RegistryService, Depends(get_registry_service)],
) -> VectorStoreProvisioner:
    """Create the idempotent, best-effort Vector Search endpoint/index provisioner."""
    return VectorStoreProvisioner(sp_ws=sp_ws, app_settings=app_settings, embeddings=embeddings, registry=registry)


async def get_discovery_service(
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    obo_sql: Annotated[SqlExecutor, Depends(get_obo_sql_executor)],
) -> DiscoveryService:
    """Create a DiscoveryService using the OBO-authenticated WorkspaceClient.

    Runs on-behalf-of the calling user so Unity Catalog browsing and governed
    tag-policy discovery respect their Unity Catalog permissions. The OBO
    :class:`SqlExecutor` is threaded in so ``get_table_tags`` can source column
    tags from ``information_schema.column_tags`` (the reliable source) under the
    caller's Unity Catalog permissions.
    """
    me = await asyncio.to_thread(obo_ws.current_user.me)
    user_id = me.user_name or me.id or "unknown"
    return DiscoveryService(ws=obo_ws, user_id=user_id, sql=obo_sql)


async def get_rule_suggester(
    monitored_tables: Annotated[MonitoredTableService, Depends(get_monitored_table_service)],
    registry: Annotated[RegistryService, Depends(get_registry_service)],
    apply_rules: Annotated[ApplyRulesService, Depends(get_apply_rules_service)],
    retriever: Annotated[RuleRetriever, Depends(get_rule_retriever)],
    ai_gateway: Annotated[AIGateway, Depends(get_ai_gateway)],
    discovery: Annotated[DiscoveryService, Depends(get_discovery_service)],
) -> RuleSuggester:
    """Create a RuleSuggester wired to the registry/monitored-table/apply-rules services + AI gateway.

    The OBO-scoped ``discovery`` service resolves the target table's live UC
    columns (name/type/family/comment) for matching, so suggestions work even
    for a table that has never been profiled in the app — mirroring dqlake.
    """
    return RuleSuggester(
        monitored_tables=monitored_tables,
        registry=registry,
        apply_rules=apply_rules,
        retriever=retriever,
        ai_gateway=ai_gateway,
        discovery=discovery,
    )


async def get_profiling_suggestion_service(
    monitored_tables: Annotated[MonitoredTableService, Depends(get_monitored_table_service)],
    registry: Annotated[RegistryService, Depends(get_registry_service)],
    apply_rules: Annotated[ApplyRulesService, Depends(get_apply_rules_service)],
) -> ProfilingSuggestionService:
    """Create a ProfilingSuggestionService for the Profile page's profiler suggestions (B2-82).

    Listing suggestions is side-effect-free; applying one resolves-or-creates +
    approves the registry rule (via ``RegistryService.match_or_create_approved_rule``)
    and binds it through ``ApplyRulesService``.
    """
    return ProfilingSuggestionService(
        monitored_tables=monitored_tables,
        registry=registry,
        apply_rules=apply_rules,
    )


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


async def get_compute_service(
    sp_ws: Annotated[WorkspaceClient, Depends(get_sp_ws)],
    app_settings: Annotated[AppSettingsService, Depends(get_app_settings_service)],
) -> ComputeService:
    """Create a ComputeService (SP-scoped listing + warehouse access checks, P22-B)."""
    return ComputeService(sp_ws=sp_ws, app_settings=app_settings)


async def get_preview_sql_executor(
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    app_settings: Annotated[AppSettingsService, Depends(get_app_settings_service)],
) -> SqlExecutor:
    """OBO SqlExecutor for View Data ad-hoc reads, honouring the configured warehouse.

    The admin-configured SQL warehouse (``dq_app_settings``) wins; otherwise we
    fall back to the bundle-bound ``DATABRICKS_WAREHOUSE_ID`` env var (today's
    behaviour). Runs as the caller so Unity Catalog permissions are enforced.
    """
    return SqlExecutor(
        ws=obo_ws,
        warehouse_id=resolve_warehouse_id(app_settings),
        catalog=conf.catalog,
        schema=conf.tmp_schema_name,
    )


async def get_table_data_service(
    sql: Annotated[SqlExecutor, Depends(get_preview_sql_executor)],
    gateway: Annotated[AIGateway, Depends(get_ai_gateway)],
) -> TableDataService:
    """Create a TableDataService for the monitored-table View Data tab (P22-B)."""
    return TableDataService(sql=sql, ai_gateway=gateway)


async def get_rule_test_service(
    sql: Annotated[SqlExecutor, Depends(get_preview_sql_executor)],
    gateway: Annotated[AIGateway, Depends(get_ai_gateway)],
) -> RuleTestService:
    """Create a RuleTestService for the Rules Registry Test tab (P22-E).

    Shares the View Data OBO warehouse executor seam (``get_preview_sql_executor``
    → configured warehouse, caller's UC perms) and the AI gateway used for
    test-data generation.
    """
    return RuleTestService(sql=sql, ai_gateway=gateway)


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
    app_settings: Annotated[AppSettingsService, Depends(get_app_settings_service)],
) -> JobService:
    """Create a JobService using app (SP) credentials.

    Job submission and polling run as the app's service principal. The
    admin-configured SQL warehouse (``dq_app_settings``) is resolved here and
    threaded into the submitted run so the task runner's temp-view cleanup path
    honours it (env fallback when unset).
    """
    return JobService(
        ws=sp_ws,
        job_id=conf.job_id,
        sql=sql,
        warehouse_id=resolve_warehouse_id(app_settings),
        wheels_volume=conf.wheels_volume,
    )


async def get_run_set_service(
    sql: Annotated[OltpExecutorProtocol, Depends(get_sp_oltp_executor)],
    validation_sql: Annotated[SqlExecutor, Depends(get_sp_sql_executor)],
) -> RunSetService:
    """Create a RunSetService.

    ``dq_run_sets``/``dq_run_set_members`` are routed at the OLTP executor;
    ``dq_validation_runs`` (joined in Python for aggregated status) always
    lives in Delta regardless of whether Lakebase is enabled.
    """
    return RunSetService(oltp_sql=sql, validation_sql=validation_sql)


async def get_draft_run_gate_service(
    validation_sql: Annotated[SqlExecutor, Depends(get_sp_sql_executor)],
) -> DraftRunGateService:
    """Create a DraftRunGateService (issue B2-12).

    ``dq_validation_runs`` is always Delta (written by the runner job), so the
    gate reads off the SP Delta executor regardless of whether the OLTP tables
    live in Lakebase.
    """
    return DraftRunGateService(validation_sql=validation_sql)


async def get_binding_run_service(
    monitored_tables: Annotated[MonitoredTableService, Depends(get_monitored_table_service)],
    version_service: Annotated[MonitoredTableVersionService, Depends(get_monitored_table_version_service)],
    materializer: Annotated[Materializer, Depends(get_materializer)],
    view_service: Annotated[ViewService, Depends(get_view_service)],
    job_service: Annotated[JobService, Depends(get_job_service)],
    run_set_service: Annotated[RunSetService, Depends(get_run_set_service)],
    settings_service: Annotated[AppSettingsService, Depends(get_app_settings_service)],
    sp_sql: Annotated[SqlExecutor, Depends(get_sp_sql_executor)],
) -> BindingRunService:
    """Create a BindingRunService wired to the existing dryrun submission path.

    Copies the exact call sequence
    ``routes/v1/dryrun.py:batch_run_from_catalog`` uses (view creation,
    ``JobService.submit_run``, ``record_dryrun_started``) — see the module
    docstring on ``services/binding_run_service.py``.
    """
    return BindingRunService(
        monitored_tables=monitored_tables,
        version_service=version_service,
        materializer=materializer,
        view_service=view_service,
        job_service=job_service,
        run_set_service=run_set_service,
        settings_service=settings_service,
        runs_table=sp_sql.fqn("dq_validation_runs"),
    )


async def get_score_cache_service(
    oltp: Annotated[OltpExecutorProtocol, Depends(get_sp_oltp_executor)],
    warehouse_sql: Annotated[SqlExecutor, Depends(get_sp_sql_executor)],
) -> ScoreCacheService:
    """Create a ScoreCacheService (P3.4 Lakebase score cache).

    The cache table (plus the product-membership lookups the derived
    scopes need) lives on the OLTP executor; the batched published-score
    recompute reads the ``mv_dq_scores`` metric view via the SP warehouse
    executor. SP-side by design — the cache is shared/global and
    viewer-independent; catalog filtering happens at read time on the
    list endpoints.
    """
    return ScoreCacheService(oltp=oltp, warehouse_sql=warehouse_sql)


async def get_entitlement_service(
    sp_sql: Annotated[SqlExecutor, Depends(get_sp_sql_executor)],
) -> EntitlementService:
    """Create an EntitlementService over the SP warehouse executor (P4.1).

    SP-side by design: the entitlement table and the gated failing-rows
    view are SP-owned UC objects. The caller's OBO executor (for the
    self-verification probes) is passed per call, never stored.
    """
    return EntitlementService(sql=sp_sql)


async def get_data_product_service(
    sql: Annotated[OltpExecutorProtocol, Depends(get_sp_oltp_executor)],
    monitored_tables: Annotated[MonitoredTableService, Depends(get_monitored_table_service)],
    run_set_service: Annotated[RunSetService, Depends(get_run_set_service)],
    binding_run_service: Annotated[BindingRunService, Depends(get_binding_run_service)],
    version_service: Annotated[MonitoredTableVersionService, Depends(get_monitored_table_version_service)],
    app_settings: Annotated[AppSettingsService, Depends(get_app_settings_service)],
) -> DataProductService:
    """Create a DataProductService routed at the OLTP executor.

    Reuses the monitored-table listing (for per-member rules/checks counts
    and live status), the run-set service (for the shared run set + last-run
    lookups), ``BindingRunService`` (for per-member run submission), and the
    version service (so version-pinned members report their frozen snapshot's
    counts rather than the binding's live counts).
    """
    return DataProductService(
        sql=sql,
        monitored_tables=monitored_tables,
        run_set_service=run_set_service,
        binding_run_service=binding_run_service,
        version_service=version_service,
        app_settings=app_settings,
    )


async def get_export_service(
    registry: Annotated[RegistryService, Depends(get_registry_service)],
    app_settings: Annotated[AppSettingsService, Depends(get_app_settings_service)],
    materializer: Annotated[Materializer, Depends(get_materializer)],
    monitored_tables: Annotated[MonitoredTableService, Depends(get_monitored_table_service)],
    data_products: Annotated[DataProductService, Depends(get_data_product_service)],
) -> ExportService:
    """Create an ExportService wired to the registry / table / product services + materializer."""
    return ExportService(
        registry=registry,
        app_settings=app_settings,
        materializer=materializer,
        monitored_tables=monitored_tables,
        data_products=data_products,
    )


async def get_demo_status_store(
    app_settings: Annotated[AppSettingsService, Depends(get_app_settings_service)],
) -> DemoStatusStore:
    """Create the settings-backed store for the long-running demo-seed job status."""
    return DemoStatusStore(app_settings)


async def get_demo_seed_service(
    sp_ws: Annotated[WorkspaceClient, Depends(get_sp_ws)],
    sp_sql: Annotated[SqlExecutor, Depends(get_sp_sql_executor)],
    oltp: Annotated[OltpExecutorProtocol, Depends(get_sp_oltp_executor)],
    registry: Annotated[RegistryService, Depends(get_registry_service)],
    monitored_tables: Annotated[MonitoredTableService, Depends(get_monitored_table_service)],
    apply_rules: Annotated[ApplyRulesService, Depends(get_apply_rules_service)],
    materializer: Annotated[Materializer, Depends(get_materializer)],
    rules_catalog: Annotated[RulesCatalogService, Depends(get_rules_catalog_service)],
    version_service: Annotated[MonitoredTableVersionService, Depends(get_monitored_table_version_service)],
    data_products: Annotated[DataProductService, Depends(get_data_product_service)],
    score_cache: Annotated[ScoreCacheService, Depends(get_score_cache_service)],
    job_service: Annotated[JobService, Depends(get_job_service)],
    run_set_service: Annotated[RunSetService, Depends(get_run_set_service)],
    app_settings: Annotated[AppSettingsService, Depends(get_app_settings_service)],
    status: Annotated[DemoStatusStore, Depends(get_demo_status_store)],
    reset_service: Annotated[DatabaseResetService, Depends(get_database_reset_service)],
    embeddings: Annotated[RuleEmbeddingsService, Depends(get_rule_embeddings_service)],
) -> "DemoSeedService":
    """Assemble the demo-seed orchestrator with an SP-only service graph.

    The demo seed runs on a background daemon thread for ~30min with no request
    context, so every collaborator must be service-principal-authenticated —
    there is no user OBO token to fall back on. Two SP-only decisions are
    load-bearing:

    * ``demo_sql`` is a fresh :class:`SqlExecutor` bound to the demo source
      schema (:data:`~backend.demo.manifest.SOURCE_SCHEMA`), where the seeded
      e-commerce tables live, rather than the app's own ``dqx_studio`` schema.
    * The :class:`BindingRunService` is built with a :class:`ViewService` whose
      ``sql`` AND ``sp_sql`` slots are BOTH the SP executor — unlike the OBO
      ``get_view_service`` used on request paths — so background runs create
      their temp views with no user token.
    """
    from .demo.seed_service import DemoSeedService

    warehouse_id = _get_warehouse_id()
    demo_sql = SqlExecutor(
        ws=sp_ws,
        warehouse_id=warehouse_id,
        catalog=conf.catalog,
        schema=DEMO_SOURCE_SCHEMA,
    )
    sp_view = ViewService(sql=sp_sql, sp_sql=sp_sql)
    # Profiler temp views for the demo profiling phase are created on the tmp
    # schema (like the request-path ViewService), but as the SP — the seed runs
    # on a background thread with no OBO token.
    profiler_view = ViewService(
        sql=SqlExecutor(
            ws=sp_ws,
            warehouse_id=warehouse_id,
            catalog=conf.catalog,
            schema=conf.tmp_schema_name,
        ),
        sp_sql=sp_sql,
    )
    binding_run = BindingRunService(
        monitored_tables=monitored_tables,
        version_service=version_service,
        materializer=materializer,
        view_service=sp_view,
        job_service=job_service,
        run_set_service=run_set_service,
        settings_service=app_settings,
        runs_table=sp_sql.fqn("dq_validation_runs"),
    )
    return DemoSeedService(
        demo_sql=demo_sql,
        app_sql=sp_sql,
        oltp=oltp,
        sp_ws=sp_ws,
        registry=registry,
        monitored_tables=monitored_tables,
        apply_rules=apply_rules,
        materializer=materializer,
        rules_catalog=rules_catalog,
        version_service=version_service,
        data_products=data_products,
        binding_run=binding_run,
        score_cache=score_cache,
        status=status,
        reset_service=reset_service,
        embeddings=embeddings,
        job_service=job_service,
        profiler_view=profiler_view,
        catalog=conf.catalog,
    )


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
        # Mappings match by string equality on the stored ``group_name`` column,
        # which the Entitlements UI also uses for USER-level entitlements (it
        # stores the picked user's display name / username there). So resolution
        # matches against the user's own identity strings AS WELL AS their group
        # memberships — otherwise a user-level entitlement would never take
        # effect. Include display name, userName and email to cover whichever
        # form the principal picker persisted.
        principals = [g.display for g in (user.groups or []) if g.display]
        for ident in (user.display_name, user.user_name, email):
            if ident and ident not in principals:
                principals.append(ident)
        logger.debug(f"Resolving role for {email} with principals: {principals}")

        role = role_svc.resolve_role(principals, conf.admin_group)
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


async def get_current_principal_ids(
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> frozenset[str]:
    """Resolve the caller's principal identity set for object-grant matching.

    Returns the caller's own SCIM id plus every group they belong to — by
    both id (``ComplexValue.value``) and display name — so a grant to a
    group (stored by SCIM id from the principal picker) matches regardless
    of how the group was referenced. One ``me()`` call; degrades to an
    empty set on failure (object grants then fall back to baseline + role
    bypass, never a hard 5xx).
    """
    try:
        me = await asyncio.to_thread(obo_ws.current_user.me)
        ids: set[str] = set()
        if me.id:
            ids.add(me.id)
        for g in me.groups or []:
            if g.value:
                ids.add(g.value)
            if g.display:
                ids.add(g.display)
        return frozenset(ids)
    except Exception as exc:
        logger.warning(f"Principal-id resolution failed, falling back to empty set: {exc}", exc_info=True)
        return frozenset()


CurrentPrincipalIds = Annotated[frozenset[str], Depends(get_current_principal_ids)]


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
        # Include the user's own identity strings alongside group memberships so
        # a USER-level RUNNER entitlement resolves too (see get_user_role).
        principals = [g.display for g in (user.groups or []) if g.display]
        for ident in (user.display_name, user.user_name, email):
            if ident and ident not in principals:
                principals.append(ident)
        return role_svc.has_runner_role(principals, conf.admin_group)
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


async def _fetch_catalog_names(obo_ws: WorkspaceClient) -> frozenset[str]:
    """Issue the UC ``catalogs.list`` call via the caller's OBO client."""
    catalogs = await asyncio.to_thread(lambda: list(obo_ws.catalogs.list()))
    return frozenset(c.name for c in catalogs if c.name)


@app_cache.cached("auth:catalogs:{token_hash}", ttl=_CATALOG_TTL)
async def _list_user_catalog_names(token_hash: str, obo_ws: WorkspaceClient) -> frozenset[str]:  # noqa: ARG001
    return await _fetch_catalog_names(obo_ws)


async def get_user_catalog_names(
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    token: Annotated[str | None, Header(alias="X-Forwarded-Access-Token")] = None,
) -> frozenset[str]:
    """Return the set of catalog names the current user can access (via OBO).

    Cached per token hash for a short TTL (*_CATALOG_TTL*, 30 s), mirroring
    the *_create_obo_ws* idiom. This list drives authorization filtering on
    list endpoints (rules, dry-run runs/results), so a stale entry could leak
    rows for a catalog whose grant was just revoked — that is why the TTL is
    deliberately short. The explicit trade-off: a revoked catalog grant can
    remain visible in list filtering for up to 30 seconds; in exchange, list
    endpoints no longer pay a per-request UC ``catalogs.list`` round trip
    (live-measured as the dominant list-page latency). The underlying OBO
    ``WorkspaceClient`` is cached separately (45 min), so a cache miss only
    re-issues the ``catalogs.list`` request, not the full auth handshake.

    Local-dev fallback: when the OBO header is absent (``get_obo_ws`` fell
    back to the local default-auth client) there is no per-user token to key
    on, so the listing stays uncached — exactly the previous behaviour.
    """
    if not token:
        return await _fetch_catalog_names(obo_ws)
    token_hash = hashlib.sha256(token.encode()).hexdigest()
    return await _list_user_catalog_names(token_hash, obo_ws)


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
    "get_database_reset_service",
    "get_ai_gateway",
    "get_ai_rules_service",
    "get_contract_rules_service",
    "get_rules_catalog_service",
    "get_registry_service",
    "get_monitored_table_service",
    "get_apply_rules_service",
    "get_materializer",
    "get_monitored_table_version_service",
    "get_run_set_service",
    "get_binding_run_service",
    "get_data_product_service",
    "get_discovery_service",
    "get_view_service",
    "get_job_service",
    "get_sql_connector",
    "get_user_role",
    "get_comments_service",
    "get_compute_service",
    "get_preview_sql_executor",
    "get_table_data_service",
    "get_rule_test_service",
    "get_review_status_service",
    "get_schedule_config_service",
    "get_demo_status_store",
    "get_demo_seed_service",
    "require_role",
    "require_runner",
    "CurrentUserRole",
    "CurrentUserRunner",
    "get_user_runner_flag",
    "get_user_catalog_names",
    "rt",
]
