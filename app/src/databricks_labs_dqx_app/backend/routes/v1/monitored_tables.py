"""Monitored Tables routes — Phase 3B/3C (register/list/get/delete/apply + submit-for-review + profiling read).

Layer 2 of the Rules Registry
(``docs/superpowers/specs/2026-07-02-rules-registry-design.md`` §7): a thin
binding recording that a table is under active Rules Registry governance,
plus the live link of applied registry rules and the materializer that
renders them into ``dq_quality_rules`` (Phase 3C).
"""

from typing import Annotated

from databricks.sdk import WorkspaceClient
from fastapi import APIRouter, Depends, HTTPException, Query

from databricks_labs_dqx_app.backend.common.approvals import ApprovalMode, mark_auto_approver, should_auto_approve
from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.common.permissions import ObjectType, Privilege
from databricks_labs_dqx_app.backend.dependencies import (
    CurrentPrincipalIds,
    CurrentUserRole,
    get_app_settings_service,
    get_apply_rules_service,
    get_binding_run_service,
    get_discovery_service,
    get_draft_run_gate_service,
    get_materializer,
    get_monitored_table_service,
    get_monitored_table_version_service,
    get_obo_ws,
    get_pending_application_service,
    get_permissions_service,
    get_profiling_suggestion_service,
    get_registry_service,
    get_rule_suggester,
    get_rules_catalog_service,
    get_tag_suggestion_service,
    require_role,
    require_runner,
)
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.draft_run_gate_service import (
    DraftRunGateService,
    DraftRunRequiredError,
)
from databricks_labs_dqx_app.backend.services.permissions_service import PermissionsService
from databricks_labs_dqx_app.backend.logger import logger
from databricks_labs_dqx_app.backend.models import (
    AppliedRuleOut,
    ApplyRuleIn,
    BatchRecordPendingApplicationsFailure,
    BatchRecordPendingApplicationsIn,
    BatchRecordPendingApplicationsOut,
    BulkRegisterMonitoredTablesIn,
    BulkRegisterMonitoredTablesOut,
    MonitoredTableDetailOut,
    MonitoredTableOut,
    MonitoredTableProfileOut,
    MonitoredTableReviewOut,
    MonitoredTableSummaryOut,
    MonitoredTableVersionChecksOut,
    MonitoredTableVersionOut,
    ApplyProfilingSuggestionsIn,
    ApplyProfilingSuggestionsOut,
    PendingApplicationOut,
    ProfilingSuggestionOut,
    RegisterMonitoredTableIn,
    RunMonitoredTableIn,
    RunMonitoredTableOut,
    UpdateMonitoredTableScheduleIn,
    SaveAppliedRulesIn,
    SetAppliedRulePinIn,
    SetAppliedRuleSeverityOverrideIn,
    SuggestRulesOut,
    TagSuggestionsOut,
)
from databricks_labs_dqx_app.backend.registry_models import MonitoredTable, get_rule_name
from databricks_labs_dqx_app.backend.services.apply_rules_service import (
    ApplyRulesService,
    DesiredAppliedRule,
    MappingIncompleteError,
    RuleNotPublishedError,
    UnsafeRowFilterError,
)
from databricks_labs_dqx_app.backend.services.binding_run_service import (
    BindingNotFoundError,
    BindingRunError,
    BindingRunService,
    MissingSnapshotError,
    NeverApprovedError,
)
from databricks_labs_dqx_app.backend.run_config_store import RunConfigTooLargeError
from databricks_labs_dqx_app.backend.services.discovery import DiscoveryService
from databricks_labs_dqx_app.backend.services.materializer import MaterializationError, Materializer
from databricks_labs_dqx_app.backend.services.monitored_table_service import (
    AppliedRuleSummary,
    DuplicateMonitoredTableError,
    MonitoredTableService,
    MonitoredTableSummary,
)
from databricks_labs_dqx_app.backend.services.monitored_table_versions import MonitoredTableVersionService
from databricks_labs_dqx_app.backend.services.pending_application_service import PendingApplicationService
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService
from databricks_labs_dqx_app.backend.services.profiling_suggestion_service import (
    BindingNotFoundError as ProfilingBindingNotFoundError,
    ProfilingSuggestionService,
)
from databricks_labs_dqx_app.backend.services.rule_suggester import RuleSuggester
from databricks_labs_dqx_app.backend.services.tag_suggestion_service import TagSuggestionService
from databricks_labs_dqx_app.backend.services.rules_catalog_service import RulesCatalogService

router = APIRouter()

_ALL_ROLES = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR, UserRole.VIEWER]
_AUTHORS_AND_ABOVE = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR]
_APPROVERS_ONLY = [UserRole.ADMIN, UserRole.RULE_APPROVER]


def _current_user_email(obo_ws: WorkspaceClient) -> str:
    user = obo_ws.current_user.me()
    return user.user_name or "unknown"


# ------------------------------------------------------------------
# List / Get
# ------------------------------------------------------------------


@router.get(
    "",
    response_model=list[MonitoredTableSummaryOut],
    operation_id="listMonitoredTables",
    dependencies=[require_role(*_ALL_ROLES)],
)
def list_monitored_tables(
    svc: Annotated[MonitoredTableService, Depends(get_monitored_table_service)],
    version_svc: Annotated[MonitoredTableVersionService, Depends(get_monitored_table_version_service)],
    materializer: Annotated[Materializer, Depends(get_materializer)],
    status: Annotated[str | None, Query(description="Filter by status")] = None,
    steward: Annotated[str | None, Query(description="Filter by steward")] = None,
    catalog: Annotated[str | None, Query(description="Filter by catalog part of table_fqn")] = None,
    schema: Annotated[str | None, Query(description="Filter by schema part of table_fqn")] = None,
    name: Annotated[str | None, Query(description="Substring search over table_fqn")] = None,
) -> list[MonitoredTableSummaryOut]:
    """List monitored tables, optionally filtered, with per-table applied-rule counts."""
    try:
        summaries = svc.list_monitored_tables(status=status, steward=steward, catalog=catalog, schema=schema, name=name)
        _apply_snapshot_check_counts(summaries, version_svc, materializer)
        return [MonitoredTableSummaryOut.from_domain(s) for s in summaries]
    except Exception as e:
        logger.error(f"Failed to list monitored tables: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to list monitored tables: {e}")


def _apply_snapshot_check_counts(
    summaries: list[MonitoredTableSummary],
    version_svc: MonitoredTableVersionService,
    materializer: Materializer,
) -> None:
    """Overwrite each summary's ``check_count`` with a count from the SAME source as its DQ score.

    B2-25: the overview "# Checks" must agree with the DQ score and the detail
    page. The score is derived from the FROZEN per-version snapshot (via
    ``dq_metrics``), whereas ``MonitoredTableService.list_monitored_tables``
    counts live ``dq_quality_rules`` rows — a transient set that a
    re-materialization can (wrongly, pre-Fix-B) drop to zero, so a scored table
    could show 0 checks. Count from the snapshot instead:

    * approved binding (``version > 0``) -> the cached ``check_count`` of its
      current frozen snapshot, resolved for ALL such bindings in one batched
      :meth:`MonitoredTableVersionService.snapshot_counts_many` call;
    * never-approved binding (``version == 0``, no snapshot) -> the live render
      count (exactly what a draft run would execute), resolved for ALL such
      bindings in one batched
      :meth:`Materializer.render_binding_checks_counts_many` call.

    Both branches are query-bounded regardless of how many bindings the list
    holds (B2-141): the never-approved branch previously called
    ``render_binding_checks`` once per binding, fanning out to ~``3N + 2·ΣR``
    sequential OLTP round-trips on a fresh (all-draft) install; it now costs a
    constant handful of grouped queries. Mirrors
    :class:`DataProductService`'s pinned-vs-live member-count split.
    """
    pins = [(s.table.binding_id, s.table.version) for s in summaries if s.table.version > 0]
    snapshot_counts = version_svc.snapshot_counts_many(pins) if pins else {}

    # Bindings that must fall back to the live draft-render count: every
    # never-approved binding, plus any approved one whose frozen snapshot row
    # is missing (so the overview still reflects what a draft run would run).
    live_bindings: list[tuple[str, str]] = []
    for summary in summaries:
        version = summary.table.version
        if version > 0 and snapshot_counts.get((summary.table.binding_id, version)) is not None:
            continue
        live_bindings.append((summary.table.binding_id, summary.table.table_fqn))
    try:
        live_counts = materializer.render_binding_checks_counts_many(live_bindings)
    except MaterializationError:
        live_counts = {}

    for summary in summaries:
        version = summary.table.version
        if version > 0:
            snapshot = snapshot_counts.get((summary.table.binding_id, version))
            if snapshot is not None:
                summary.check_count = snapshot[1]
                continue
        summary.check_count = live_counts.get(summary.table.binding_id, 0)


@router.get(
    "/{binding_id}",
    response_model=MonitoredTableDetailOut,
    operation_id="getMonitoredTable",
    dependencies=[require_role(*_ALL_ROLES)],
)
def get_monitored_table(
    binding_id: str,
    svc: Annotated[MonitoredTableService, Depends(get_monitored_table_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    tag_suggestions: Annotated[TagSuggestionService, Depends(get_tag_suggestion_service)],
) -> MonitoredTableDetailOut:
    """Get a monitored table binding plus its applied rules (joined to rule name/dimension/severity tags).

    When tag-auto-apply is on, first runs a selective apply-on-tag rescan for
    this table (OBO, so it sees the caller's tags) so any newly-matching rules
    are attached and appear in the response immediately — rather than waiting for
    the periodic background sweep. Best-effort: a rescan failure never blocks the
    read, and it is a no-op when the toggle is off.
    """
    try:
        detail = svc.get(binding_id)
        if detail is None:
            raise HTTPException(status_code=404, detail=f"Monitored table not found: {binding_id}")
        try:
            attached = tag_suggestions.apply_matches(binding_id, _current_user_email(obo_ws))
        except Exception:
            logger.warning("Tag auto-apply rescan on open failed (non-fatal)", exc_info=True)
            attached = 0
        # Re-read only when the rescan actually attached rows, so the response
        # includes them; otherwise reuse the detail we already loaded.
        if attached:
            detail = svc.get(binding_id) or detail
        return MonitoredTableDetailOut.from_domain(detail)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get monitored table {binding_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get monitored table: {e}")


# ------------------------------------------------------------------
# Register / Delete
# ------------------------------------------------------------------


@router.post(
    "",
    response_model=MonitoredTableSummaryOut,
    operation_id="registerMonitoredTable",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def register_monitored_table(
    body: RegisterMonitoredTableIn,
    svc: Annotated[MonitoredTableService, Depends(get_monitored_table_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    discovery: Annotated[DiscoveryService, Depends(get_discovery_service)],
    tag_suggestions: Annotated[TagSuggestionService, Depends(get_tag_suggestion_service)],
) -> MonitoredTableSummaryOut:
    """Register a table under Rules Registry governance (status ``draft``).

    When the caller does not pin a steward, default it to the table's Unity
    Catalog owner (resolved on-behalf-of the caller, so UC permissions are
    honoured), falling back to the creator when the owner can't be read. The
    owner may be a user, group, or service principal — it is stored verbatim.
    """
    try:
        user_email = _current_user_email(obo_ws)
        steward = body.steward or discovery.get_table_owner(body.table_fqn)
        table = svc.register(body.table_fqn, user_email, steward=steward)
        # Apply-on-tag: after a successful register, auto-attach every published
        # tag-mapped rule this table now matches — via TagSuggestionService, which
        # reads the table's tags OBO (as the caller) so it sees tags the app
        # service principal cannot. ``apply_matches`` is a no-op when the
        # tag_auto_apply toggle is off, and is best-effort here so it can never
        # turn a successful register into a 500.
        try:
            tag_suggestions.apply_matches(table.binding_id, user_email)
        except Exception:
            logger.warning("Tag auto-apply after register failed (non-fatal)", exc_info=True)
        return MonitoredTableSummaryOut.from_domain(MonitoredTableSummary(table=table, applied_rule_count=0))
    except DuplicateMonitoredTableError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to register monitored table: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to register monitored table: {e}")


@router.post(
    "/bulk",
    response_model=BulkRegisterMonitoredTablesOut,
    operation_id="bulkRegisterMonitoredTables",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def bulk_register_monitored_tables(
    body: BulkRegisterMonitoredTablesIn,
    svc: Annotated[MonitoredTableService, Depends(get_monitored_table_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    tag_suggestions: Annotated[TagSuggestionService, Depends(get_tag_suggestion_service)],
) -> BulkRegisterMonitoredTablesOut:
    """Register many tables under Rules Registry governance in one call.

    Already-monitored tables and syntactically invalid FQNs are reported
    back in the summary rather than failing the whole batch — see
    :meth:`MonitoredTableService.bulk_register`.

    Unlike single register, bulk register does **not** resolve each table's
    Unity Catalog owner: that would be one ``tables.get`` round-trip per table
    (N calls, plus rate-limit exposure) on a path meant for onboarding many
    tables quickly. When no steward is pinned, every binding defaults to the
    creator; a per-table owner can be assigned afterwards from the table's
    Permissions tab.
    """
    try:
        user_email = _current_user_email(obo_ws)
        result = svc.bulk_register(body.table_fqns, user_email, steward=body.steward)
        # Apply-on-tag: auto-attach matches for only the NEWLY-registered tables
        # (never skipped_existing/invalid). ``BulkRegisterResult.registered`` is a
        # list of table FQNs (no binding_id), so resolve each binding via
        # ``get_by_table_fqn``, then ``apply_matches`` (OBO, no-op when the toggle
        # is off — so a disabled feature still does N cheap lookups here; matches
        # the previous behaviour and the onboarding path is not latency-critical).
        # Best-effort per table; guarded so it can never turn a successful
        # bulk-register into a 500.
        for fqn in result.registered:
            try:
                detail = svc.get_by_table_fqn(fqn)
                if detail is not None:
                    tag_suggestions.apply_matches(detail.table.binding_id, user_email)
            except Exception:
                logger.warning("Tag auto-apply after bulk-register failed (non-fatal)", exc_info=True)
        return BulkRegisterMonitoredTablesOut.from_domain(result)
    except Exception as e:
        logger.error(f"Failed to bulk-register monitored tables: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to bulk-register monitored tables: {e}")


@router.delete(
    "/{binding_id}",
    operation_id="deleteMonitoredTable",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def delete_monitored_table(
    binding_id: str,
    svc: Annotated[MonitoredTableService, Depends(get_monitored_table_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    role: CurrentUserRole,
    principal_ids: CurrentPrincipalIds,
    perms: Annotated[PermissionsService, Depends(get_permissions_service)],
) -> dict[str, str]:
    """Delete a monitored table binding and its applied rules.

    Requires ``MODIFY`` on the monitored table (direct/inherited/owner) unless
    the caller is an admin/approver.

    TODO(Phase 3C): once the materializer exists, block/handle
    de-materialization of any ``dq_quality_rules`` rows tied to this
    binding's applications before allowing deletion.
    """
    user_email = _current_user_email(obo_ws)
    perms.require_object(
        ObjectType.MONITORED_TABLE.value,
        binding_id,
        Privilege.MODIFY,
        role=role,
        principal_ids=set(principal_ids),
        principal_email=user_email,
    )
    try:
        svc.delete(binding_id, user_email)
        return {"status": "deleted", "binding_id": binding_id}
    except RuntimeError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to delete monitored table {binding_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to delete monitored table: {e}")


@router.patch(
    "/{binding_id}/schedule",
    response_model=MonitoredTableOut,
    operation_id="updateMonitoredTableSchedule",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def update_monitored_table_schedule(
    binding_id: str,
    body: UpdateMonitoredTableScheduleIn,
    svc: Annotated[MonitoredTableService, Depends(get_monitored_table_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    role: CurrentUserRole,
    principal_ids: CurrentPrincipalIds,
    perms: Annotated[PermissionsService, Depends(get_permissions_service)],
) -> MonitoredTableOut:
    """Set or clear a monitored table's run schedule (P21 item 14).

    Requires ``MODIFY`` on the monitored table unless the caller is an
    admin/approver. Orthogonal to the review lifecycle — does NOT flip the
    binding's status. An approved table with a cron fires on the in-app scheduler.
    """
    user_email = _current_user_email(obo_ws)
    perms.require_object(
        ObjectType.MONITORED_TABLE.value,
        binding_id,
        Privilege.MODIFY,
        role=role,
        principal_ids=set(principal_ids),
        principal_email=user_email,
    )
    try:
        table = svc.update_schedule(
            binding_id, body.schedule_cron, body.schedule_tz, user_email, schedule_kind=body.schedule_kind
        )
        return MonitoredTableOut.from_domain(table)
    except RuntimeError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to update monitored table schedule {binding_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to update monitored table schedule: {e}")


# ------------------------------------------------------------------
# Profiling (READ-ONLY — reuses dq_profiling_results, never writes here)
# ------------------------------------------------------------------


@router.get(
    "/{binding_id}/profile",
    response_model=MonitoredTableProfileOut,
    operation_id="getMonitoredTableProfile",
    dependencies=[require_role(*_ALL_ROLES)],
)
def get_monitored_table_profile(
    binding_id: str,
    svc: Annotated[MonitoredTableService, Depends(get_monitored_table_service)],
) -> MonitoredTableProfileOut:
    """Return the most recent profiling result for this monitored table's underlying table."""
    try:
        detail = svc.get(binding_id)
        if detail is None:
            raise HTTPException(status_code=404, detail=f"Monitored table not found: {binding_id}")
        profile = svc.get_latest_profile(detail.table.table_fqn)
        if profile is None:
            raise HTTPException(
                status_code=404,
                detail=f"No profiling results found for table: {detail.table.table_fqn}",
            )
        return MonitoredTableProfileOut.from_domain(profile)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get profile for monitored table {binding_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get profile: {e}")


# ------------------------------------------------------------------
# Frozen version snapshots (Data Products Task 2)
# ------------------------------------------------------------------


@router.get(
    "/{binding_id}/versions",
    response_model=list[MonitoredTableVersionOut],
    operation_id="listMonitoredTableVersions",
    dependencies=[require_role(*_ALL_ROLES)],
)
def list_monitored_table_versions(
    binding_id: str,
    version_svc: Annotated[MonitoredTableVersionService, Depends(get_monitored_table_version_service)],
) -> list[MonitoredTableVersionOut]:
    """List a monitored table's frozen approved-rule-set version snapshots (newest first).

    Metadata only — ``checks_json`` is omitted; the frozen checks for a
    specific version are resolved separately at run time. Backs the
    version-pin dropdown on the monitored-table Run action and the product
    member pin picker.
    """
    try:
        versions = version_svc.list_versions(binding_id)
        return [MonitoredTableVersionOut.from_domain(v) for v in versions]
    except Exception as e:
        logger.error(f"Failed to list versions for monitored table {binding_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to list monitored table versions: {e}")


@router.get(
    "/{binding_id}/versions/{version}/checks",
    response_model=MonitoredTableVersionChecksOut,
    operation_id="getMonitoredTableVersionChecks",
    dependencies=[require_role(*_ALL_ROLES)],
)
def get_monitored_table_version_checks(
    binding_id: str,
    version: int,
    version_svc: Annotated[MonitoredTableVersionService, Depends(get_monitored_table_version_service)],
) -> MonitoredTableVersionChecksOut:
    """Return the frozen ``checks_json`` for a specific monitored-table version.

    Complements ``listMonitoredTableVersions`` (metadata only): this is the
    heavy per-version check payload that backs the Drafts & Review change-diff
    popout, letting the UI diff a binding's previously frozen checks (vN-1)
    against the proposed (current) rule set. Returns an empty ``checks`` list
    when no snapshot exists for the requested version.
    """
    try:
        checks = version_svc.get_checks(binding_id, version)
    except LookupError:
        checks = []
    except Exception as e:
        logger.error(f"Failed to get frozen checks for monitored table {binding_id} v{version}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get monitored table version checks: {e}")
    return MonitoredTableVersionChecksOut(binding_id=binding_id, version=version, checks=checks)


@router.post(
    "/{binding_id}/run",
    response_model=RunMonitoredTableOut,
    operation_id="runMonitoredTable",
    # Same orthogonal RUNNER gate as the existing Run Rules batch endpoint
    # (``routes/v1/dryrun.py:batch_run_from_catalog``) — a runner mapping
    # (or admin) is required regardless of the caller's primary role.
    dependencies=[require_runner()],
)
def run_monitored_table(
    binding_id: str,
    body: RunMonitoredTableIn,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    run_svc: Annotated[BindingRunService, Depends(get_binding_run_service)],
) -> RunMonitoredTableOut:
    """Run a monitored table's approved (latest or pinned) or draft checks.

    Resolves checks per design spec §4.1: ``source='draft'`` renders the
    binding's current persisted applied-rules state; ``source='approved'``
    with *version* pins a frozen snapshot, and with no *version* uses the
    binding's latest approved snapshot (409 if the table has never been
    approved). Submits through the same job path as the existing Run
    Rules batch endpoint and mints a run set of one.
    """
    try:
        user_email = _current_user_email(obo_ws)
        result = run_svc.run_binding(
            binding_id,
            source=body.source,
            version=body.version,
            user_email=user_email,
            trigger="manual",
            rule_ids=body.rule_ids,
        )
        return RunMonitoredTableOut(
            run_set_id=result.run_set_id,
            run_id=result.run_id,
            job_run_id=result.job_run_id,
            view_fqn=result.view_fqn,
        )
    except BindingNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except NeverApprovedError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except MissingSnapshotError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except BindingRunError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RunConfigTooLargeError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to run monitored table {binding_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to run monitored table: {e}")


# ------------------------------------------------------------------
# Apply / unapply / pin / severity-override (Phase 3C)
# ------------------------------------------------------------------


@router.post(
    "/{binding_id}/applied-rules",
    response_model=AppliedRuleOut,
    operation_id="applyRuleToTable",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def apply_rule_to_table(
    binding_id: str,
    body: ApplyRuleIn,
    svc: Annotated[ApplyRulesService, Depends(get_apply_rules_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    role: CurrentUserRole,
    principal_ids: CurrentPrincipalIds,
    perms: Annotated[PermissionsService, Depends(get_permissions_service)],
) -> AppliedRuleOut:
    """Apply a published registry rule to a monitored table's column mapping.

    Applying a rule mutates the monitored table's rule set, so it requires
    ``APPLY`` on the monitored table (in the day-one baseline) unless the
    caller is an admin/approver.
    """
    user_email = _current_user_email(obo_ws)
    perms.require_object(
        ObjectType.MONITORED_TABLE.value,
        binding_id,
        Privilege.APPLY,
        role=role,
        principal_ids=set(principal_ids),
        principal_email=user_email,
    )
    try:
        applied = svc.apply_rule(
            binding_id,
            body.rule_id,
            body.column_mapping,
            user_email,
            pinned_version=body.pinned_version,
            severity_override=body.severity_override,
            row_filter=body.row_filter,
            pass_threshold=body.pass_threshold,
            tags=body.tags,
        )
        return AppliedRuleOut.from_domain(applied)
    except UnsafeRowFilterError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except MappingIncompleteError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except RuleNotPublishedError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to apply rule to monitored table {binding_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to apply rule: {e}")


@router.post(
    "/pending-applications/batch",
    response_model=BatchRecordPendingApplicationsOut,
    operation_id="batchRecordPendingApplications",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def batch_record_pending_applications(
    body: BatchRecordPendingApplicationsIn,
    pending: Annotated[PendingApplicationService, Depends(get_pending_application_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> BatchRecordPendingApplicationsOut:
    """Stage applications for rules that landed ``pending_approval`` (Bulk Contract Import Phase 2).

    Records each ``(binding_id, rule_id, column_mapping)`` in
    ``dq_pending_applications``; on the rule's later approval,
    ``_publish_registry_rule`` drains them into real applied-rule links. This
    is a lightweight staging write (no rule/binding validation or
    materialization here) — the activation path re-validates via
    ``ApplyRulesService.apply_rule``, so an entry whose binding/rule vanishes
    before approval is silently skipped there rather than failing the import.

    Partial success is allowed; per-entry errors are returned in ``failed[]``
    with a generic message (details are logged server-side).
    """
    recorded = 0
    failed: list[BatchRecordPendingApplicationsFailure] = []
    user_email = _current_user_email(obo_ws)
    for index, entry in enumerate(body.applications):
        try:
            pending.record(entry.binding_id, entry.rule_id, entry.column_mapping, user_email)
            recorded += 1
        except Exception as e:
            logger.error(
                "Failed to record pending application (binding %s, rule %s): %s",
                entry.binding_id,
                entry.rule_id,
                e,
                exc_info=True,
            )
            failed.append(
                BatchRecordPendingApplicationsFailure(
                    index=index,
                    error="Failed to record pending application.",
                )
            )
    return BatchRecordPendingApplicationsOut(recorded=recorded, failed=failed)


@router.get(
    "/{binding_id}/pending-applications",
    response_model=list[PendingApplicationOut],
    operation_id="listPendingApplications",
    dependencies=[require_role(*_ALL_ROLES)],
)
def list_pending_applications(
    binding_id: str,
    pending: Annotated[PendingApplicationService, Depends(get_pending_application_service)],
    registry: Annotated[RegistryService, Depends(get_registry_service)],
) -> list[PendingApplicationOut]:
    """List applications staged against this binding that are waiting on rule approval.

    Recorded by Bulk Contract Import when a freshly-created rule lands
    ``pending_approval`` (approval-enabled orgs): the intended
    ``(binding, rule, column_mapping)`` is parked in ``dq_pending_applications``
    and drained into a real ``dq_applied_rules`` link by
    ``_publish_registry_rule`` when the rule is approved. These are NOT applied
    rules yet (no materialized checks) — the Apply Rules tab surfaces them
    read-only so the staged intent is visible instead of the table looking like
    it has no rules. Enriched with the referenced rule's name/status in one
    batched lookup; ``None`` when the rule has since been deleted.
    """
    try:
        rows = pending.list_for_binding(binding_id)
        rules = registry.get_rules_many([r.rule_id for r in rows])
        out: list[PendingApplicationOut] = []
        for row in rows:
            rule = rules.get(row.rule_id)
            out.append(
                PendingApplicationOut(
                    id=row.id or "",
                    binding_id=row.binding_id,
                    rule_id=row.rule_id,
                    rule_name=get_rule_name(rule.user_metadata) if rule else None,
                    rule_status=rule.status if rule else None,
                    column_mapping=row.column_mapping,
                    created_by=row.created_by,
                    created_at=row.created_at.isoformat() if row.created_at else None,
                )
            )
        return out
    except Exception as e:
        logger.error("Failed to list pending applications for binding %s: %s", binding_id, e, exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to list pending applications.")


@router.put(
    "/{binding_id}/applied-rules",
    response_model=list[AppliedRuleOut],
    operation_id="saveAppliedRules",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def save_applied_rules(
    binding_id: str,
    body: SaveAppliedRulesIn,
    svc: Annotated[ApplyRulesService, Depends(get_apply_rules_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    role: CurrentUserRole,
    principal_ids: CurrentPrincipalIds,
    perms: Annotated[PermissionsService, Depends(get_permissions_service)],
) -> list[AppliedRuleOut]:
    """Reconcile the FULL desired set of applied rules for a monitored table in one batch.

    Requires ``APPLY`` on the monitored table unless the caller is an
    admin/approver. Backs the staged Apply Rules editor: the frontend stages
    every add / mapping-edit / severity-override / pin / removal locally and
    calls this once on Save-as-draft or Publish instead of firing an immediate
    write per edit. Does NOT materialize — materialization stays gated behind
    the existing publish route.
    """
    user_email = _current_user_email(obo_ws)
    perms.require_object(
        ObjectType.MONITORED_TABLE.value,
        binding_id,
        Privilege.APPLY,
        role=role,
        principal_ids=set(principal_ids),
        principal_email=user_email,
    )
    try:
        desired = [
            DesiredAppliedRule(
                rule_id=entry.rule_id,
                column_mapping=entry.column_mapping,
                pinned_version=entry.pinned_version,
                severity_override=entry.severity_override,
                row_filter=entry.row_filter,
                pass_threshold=entry.pass_threshold,
                tags=entry.tags,
            )
            for entry in body.applications
        ]
        applied = svc.save_applied_rules(binding_id, desired, user_email)
        # Return the ENRICHED shape (rule_name/dimension/severity populated),
        # matching how the GET builds ``applied_rules`` via
        # ``AppliedRuleOut.from_summary`` (B2-26). The lean ``from_domain``
        # shape would seed the frontend list with raw GUIDs and a blank
        # severity until the next background refetch.
        out: list[AppliedRuleOut] = []
        for a in applied:
            name, dimension, severity, source = svc.rule_display_tags(a.rule_id)
            out.append(
                AppliedRuleOut.from_summary(
                    AppliedRuleSummary(
                        applied_rule=a,
                        rule_name=name,
                        rule_dimension=dimension,
                        rule_severity=severity,
                        rule_source=source,
                    )
                )
            )
        return out
    except UnsafeRowFilterError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except MappingIncompleteError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except RuleNotPublishedError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to save applied rules for monitored table {binding_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to save applied rules: {e}")


@router.delete(
    "/{binding_id}/applied-rules/{applied_rule_id}",
    operation_id="removeAppliedRule",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def remove_applied_rule(
    binding_id: str,
    applied_rule_id: str,
    svc: Annotated[ApplyRulesService, Depends(get_apply_rules_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    role: CurrentUserRole,
    principal_ids: CurrentPrincipalIds,
    perms: Annotated[PermissionsService, Depends(get_permissions_service)],
) -> dict[str, str]:
    """Remove an applied rule and every ``dq_quality_rules`` row it materialized.

    Requires ``APPLY`` on the monitored table unless the caller is an admin/approver.
    """
    perms.require_object(
        ObjectType.MONITORED_TABLE.value,
        binding_id,
        Privilege.APPLY,
        role=role,
        principal_ids=set(principal_ids),
        principal_email=_current_user_email(obo_ws),
    )
    try:
        svc.remove_applied(applied_rule_id, _current_user_email(obo_ws))
        return {"status": "removed", "binding_id": binding_id, "applied_rule_id": applied_rule_id}
    except RuntimeError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to remove applied rule {applied_rule_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to remove applied rule: {e}")


@router.patch(
    "/{binding_id}/applied-rules/{applied_rule_id}/pin",
    response_model=AppliedRuleOut,
    operation_id="setAppliedRulePin",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def set_applied_rule_pin(
    binding_id: str,
    applied_rule_id: str,
    body: SetAppliedRulePinIn,
    svc: Annotated[ApplyRulesService, Depends(get_apply_rules_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    role: CurrentUserRole,
    principal_ids: CurrentPrincipalIds,
    perms: Annotated[PermissionsService, Depends(get_permissions_service)],
) -> AppliedRuleOut:
    """Pin (or, with ``pinned_version=None``, unpin) an applied rule's version.

    Requires ``APPLY`` on the monitored table unless the caller is an admin/approver.
    """
    perms.require_object(
        ObjectType.MONITORED_TABLE.value,
        binding_id,
        Privilege.APPLY,
        role=role,
        principal_ids=set(principal_ids),
        principal_email=_current_user_email(obo_ws),
    )
    try:
        applied = svc.set_pin(applied_rule_id, body.pinned_version)
        return AppliedRuleOut.from_domain(applied)
    except RuntimeError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to set pin for applied rule {applied_rule_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to set pin: {e}")


@router.patch(
    "/{binding_id}/applied-rules/{applied_rule_id}/severity-override",
    response_model=AppliedRuleOut,
    operation_id="setAppliedRuleSeverityOverride",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def set_applied_rule_severity_override(
    binding_id: str,
    applied_rule_id: str,
    body: SetAppliedRuleSeverityOverrideIn,
    svc: Annotated[ApplyRulesService, Depends(get_apply_rules_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    role: CurrentUserRole,
    principal_ids: CurrentPrincipalIds,
    perms: Annotated[PermissionsService, Depends(get_permissions_service)],
) -> AppliedRuleOut:
    """Set (or, with ``severity=None``, clear) an applied rule's severity override.

    Requires ``APPLY`` on the monitored table unless the caller is an admin/approver.
    """
    perms.require_object(
        ObjectType.MONITORED_TABLE.value,
        binding_id,
        Privilege.APPLY,
        role=role,
        principal_ids=set(principal_ids),
        principal_email=_current_user_email(obo_ws),
    )
    try:
        applied = svc.set_severity_override(applied_rule_id, body.severity)
        return AppliedRuleOut.from_domain(applied)
    except RuntimeError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to set severity override for applied rule {applied_rule_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to set severity override: {e}")


# ------------------------------------------------------------------
# Submit-for-review lifecycle (submit / approve / reject) — Phase 3C / P16-H
#
# Monitored tables carry the SAME review lifecycle as registry-authored
# rules (draft -> pending_approval -> approved/rejected). Rather than a
# parallel status-mutation implementation, these routes REUSE the per-rule
# transition path (``RulesCatalogService.set_status`` — the exact call
# ``routes/v1/rules.py`` submit/approve/reject make) to move each of the
# binding's materialized ``dq_quality_rules`` rows, so audit/history/version
# semantics are identical for a table's checks whether they were submitted
# one at a time from Drafts & Review or in bulk from here. The binding's own
# status is then rolled up from its checks. The scheduler is untouched: it
# still runs only ``dq_quality_rules`` rows at ``status='approved'``.
# ------------------------------------------------------------------


def _transition_binding_checks(
    monitored_tables_svc: MonitoredTableService,
    rules_catalog: RulesCatalogService,
    binding_id: str,
    *,
    from_status: str,
    to_status: str,
    user_email: str,
) -> int:
    """Move every materialized check of *binding_id* from *from_status* to *to_status*.

    Reuses ``RulesCatalogService.set_status`` (the per-rule transition path)
    so nothing about a check's audit trail differs from a hand-submitted one.
    Per-row failures (e.g. a duplicate-pending guard) are logged and skipped
    rather than aborting the whole binding — mirroring
    ``RulesCatalogService.set_status_by_table``'s resilience. Returns the
    count of checks actually transitioned.
    """
    count = 0
    for rule_id, status in monitored_tables_svc.list_materialized_rule_statuses(binding_id):
        if status != from_status:
            continue
        try:
            rules_catalog.set_status(rule_id, to_status, user_email)
            count += 1
        except Exception:  # one bad row must not abort the whole binding
            logger.warning(
                "Failed to transition materialized check %s (%s -> %s) for binding %s",
                rule_id,
                from_status,
                to_status,
                binding_id,
                exc_info=True,
            )
    return count


def _rollup_binding_status(monitored_tables_svc: MonitoredTableService, binding_id: str) -> str:
    """Roll a binding's status up from its materialized checks' statuses.

    Any check still ``pending_approval`` keeps the binding ``pending_approval``
    (something is still awaiting review); otherwise if any check is
    ``approved`` the binding is ``approved`` (its live checks can run); with
    neither, the binding falls back to ``draft``. This makes an unchanged
    re-submit idempotent (all-approved stays ``approved``) while a re-submit
    after edits — where changed rows go back to ``pending_approval`` via the
    materializer's Behaviour A/B — returns the binding to ``pending_approval``.
    """
    statuses = {status for _, status in monitored_tables_svc.list_materialized_rule_statuses(binding_id)}
    if "pending_approval" in statuses:
        return "pending_approval"
    if "approved" in statuses:
        return "approved"
    return "draft"


def _approve_binding_checks(
    monitored_tables_svc: MonitoredTableService,
    rules_catalog: RulesCatalogService,
    version_svc: MonitoredTableVersionService,
    binding_id: str,
    approver: str,
) -> tuple[MonitoredTable, int, int | None]:
    """Approve a binding's ``pending_approval`` checks, roll up, and freeze a version.

    The approval half shared by :func:`approve_monitored_table` (explicit
    approve) and :func:`submit_monitored_table` (auto-approve under the
    ``auto_bypass`` / ``disabled`` approvals modes). Assumes the binding's
    checks are already at ``pending_approval`` (the submit half puts them there).
    Returns ``(table, approved_count, new_version)``.
    """
    approved = _transition_binding_checks(
        monitored_tables_svc,
        rules_catalog,
        binding_id,
        from_status="pending_approval",
        to_status="approved",
        user_email=approver,
    )
    table = monitored_tables_svc.set_status(
        binding_id, _rollup_binding_status(monitored_tables_svc, binding_id), approver
    )
    new_version = version_svc.freeze_new_version(binding_id, approver)
    return table, approved, new_version


@router.post(
    "/{binding_id}/submit",
    response_model=MonitoredTableReviewOut,
    operation_id="submitMonitoredTable",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def submit_monitored_table(
    binding_id: str,
    monitored_tables_svc: Annotated[MonitoredTableService, Depends(get_monitored_table_service)],
    materializer: Annotated[Materializer, Depends(get_materializer)],
    rules_catalog: Annotated[RulesCatalogService, Depends(get_rules_catalog_service)],
    version_svc: Annotated[MonitoredTableVersionService, Depends(get_monitored_table_version_service)],
    app_settings: Annotated[AppSettingsService, Depends(get_app_settings_service)],
    draft_run_gate: Annotated[DraftRunGateService, Depends(get_draft_run_gate_service)],
    perms: Annotated[PermissionsService, Depends(get_permissions_service)],
    role: CurrentUserRole,
    principal_ids: CurrentPrincipalIds,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> MonitoredTableReviewOut:
    """Submit a monitored table for review.

    Materializes the binding's applied rules into ``dq_quality_rules`` (the
    UI has already persisted any staged edits via ``saveAppliedRules``), then
    submits every freshly-materialized ``draft`` check for approval — reusing
    the same per-rule transition the Drafts & Review queue uses — and rolls
    the binding up to ``pending_approval``. Idempotent: re-submitting an
    unchanged, already-approved table leaves its approved checks untouched.

    Rejected-binding recovery: after a reject the binding and its checks sit
    at ``rejected``. An unchanged re-submit does not change any check content,
    so the materializer leaves those rows at ``rejected`` (it only resets a
    row to ``draft`` when its rendered content actually changed). Left alone
    they would be stuck — ``draft -> pending_approval`` never picks them up
    and the binding would roll back down to ``draft`` with a success toast but
    ``affected_check_count=0``. So first walk any ``rejected`` rows back to
    ``draft`` (a legal per-rule transition), which the ``draft ->
    pending_approval`` step below then re-enters into review and counts.
    """
    try:
        user_email = _current_user_email(obo_ws)
        # Require-draft-run gate (issue B2-12): when the admin setting is on, the
        # binding cannot enter review (nor take the auto-approve shortcut) until
        # a draft run has been recorded for its table. Checked BEFORE any state
        # transition so it blocks both paths uniformly. 409 when unsatisfied.
        gate_detail = monitored_tables_svc.get(binding_id)
        if gate_detail is None:
            raise HTTPException(status_code=404, detail=f"Monitored table not found: {binding_id}")
        draft_run_gate.enforce(
            enabled=app_settings.get_require_draft_run_before_submit(),
            table_fqns=[gate_detail.table.table_fqn],
            # B2-118: the binding's ``updated_at`` is bumped on every applied-
            # rules save (see ApplyRulesService.save_applied_rules), so a run
            # must be newer than the last edit to count as a fresh test.
            last_change_time=gate_detail.table.updated_at,
        )
        materializer.materialize_binding(binding_id)
        # Recover rejected checks so an unchanged re-submit re-enters review
        # (rejected -> draft -> pending_approval, both legal transitions).
        _transition_binding_checks(
            monitored_tables_svc,
            rules_catalog,
            binding_id,
            from_status="rejected",
            to_status="draft",
            user_email=user_email,
        )
        submitted = _transition_binding_checks(
            monitored_tables_svc,
            rules_catalog,
            binding_id,
            from_status="draft",
            to_status="pending_approval",
            user_email=user_email,
        )
        table = monitored_tables_svc.set_status(
            binding_id, _rollup_binding_status(monitored_tables_svc, binding_id), user_email
        )
        # Approvals mode (issue #94): in ``disabled`` mode, or ``auto_bypass``
        # when the caller can edit AND approve this binding, publish in the same
        # call — the caller is recorded as the approver with an ``(auto)`` marker.
        # ``enabled`` never auto-approves, so skip the predicate's permission +
        # owner lookups entirely in that (default) mode.
        mode = app_settings.get_approvals_mode()
        can_edit_and_approve = mode != ApprovalMode.ENABLED and perms.can_edit_and_approve(
            ObjectType.MONITORED_TABLE.value,
            binding_id,
            role=role,
            principal_ids=set(principal_ids),
            owner_email=perms.get_object_owner(ObjectType.MONITORED_TABLE.value, binding_id),
            principal_email=user_email,
        )
        if should_auto_approve(mode, can_edit_and_approve=can_edit_and_approve):
            table, approved, new_version = _approve_binding_checks(
                monitored_tables_svc, rules_catalog, version_svc, binding_id, mark_auto_approver(user_email)
            )
            return MonitoredTableReviewOut(
                table=MonitoredTableOut.from_domain(table),
                affected_check_count=approved,
                new_version=new_version,
            )
        return MonitoredTableReviewOut(table=MonitoredTableOut.from_domain(table), affected_check_count=submitted)
    except HTTPException:
        raise
    except DraftRunRequiredError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except MaterializationError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to submit monitored table {binding_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to submit monitored table: {e}")


@router.post(
    "/{binding_id}/approve",
    response_model=MonitoredTableReviewOut,
    operation_id="approveMonitoredTable",
    dependencies=[require_role(*_APPROVERS_ONLY)],
)
def approve_monitored_table(
    binding_id: str,
    monitored_tables_svc: Annotated[MonitoredTableService, Depends(get_monitored_table_service)],
    rules_catalog: Annotated[RulesCatalogService, Depends(get_rules_catalog_service)],
    version_svc: Annotated[MonitoredTableVersionService, Depends(get_monitored_table_version_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> MonitoredTableReviewOut:
    """Approve a monitored table — approving every ``pending_approval`` check mapped to it.

    Reuses the per-rule approve transition so each check's audit trail is
    identical to a hand-approval, then rolls the binding up to ``approved``.
    From here the scheduler picks the checks up (it runs only ``approved``
    ``dq_quality_rules`` rows).

    Table approval is the ONLY event that bumps the monitored-table version:
    after the binding rolls up to ``approved`` the newly-approved rule set is
    frozen as the next version (design spec §3.2) via
    :meth:`MonitoredTableVersionService.freeze_new_version`, and the new
    version is returned in the response.

    Only a binding currently ``pending_approval`` can be approved — mirrors
    the per-rule transition guard (``RulesCatalogService.VALID_TRANSITIONS``)
    so an already-``draft``/``approved``/``rejected`` binding can't be
    re-approved out of band.
    """
    try:
        detail = monitored_tables_svc.get(binding_id)
        if detail is None:
            raise HTTPException(status_code=404, detail=f"Monitored table not found: {binding_id}")
        if detail.table.status != "pending_approval":
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Cannot approve monitored table {binding_id}: status is "
                    f"'{detail.table.status}', expected 'pending_approval'"
                ),
            )
        user_email = _current_user_email(obo_ws)
        table, approved, new_version = _approve_binding_checks(
            monitored_tables_svc, rules_catalog, version_svc, binding_id, user_email
        )
        return MonitoredTableReviewOut(
            table=MonitoredTableOut.from_domain(table),
            affected_check_count=approved,
            new_version=new_version,
        )
    except HTTPException:
        raise
    except RuntimeError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to approve monitored table {binding_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to approve monitored table: {e}")


@router.post(
    "/{binding_id}/reject",
    response_model=MonitoredTableReviewOut,
    operation_id="rejectMonitoredTable",
    dependencies=[require_role(*_APPROVERS_ONLY)],
)
def reject_monitored_table(
    binding_id: str,
    monitored_tables_svc: Annotated[MonitoredTableService, Depends(get_monitored_table_service)],
    rules_catalog: Annotated[RulesCatalogService, Depends(get_rules_catalog_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> MonitoredTableReviewOut:
    """Reject a monitored table — rejecting every ``pending_approval`` check mapped to it.

    Matches the per-rule reject semantics exactly (``routes/v1/rules.py``
    reject sets a check to ``rejected``), so no check is left dangling in
    ``pending_approval`` under a rejected table, and flips the binding itself
    to ``rejected``.

    Only a binding currently ``pending_approval`` can be rejected. Without
    this guard, rejecting an already-``approved`` binding would flip the
    binding's own status to ``rejected`` while its materialized checks stay
    ``approved`` and keep executing in the scheduler — the checks' per-rule
    transitions only move ``pending_approval`` rows
    (``RulesCatalogService.VALID_TRANSITIONS["approved"] = {"draft"}``), so
    the binding and its checks would silently disagree.
    """
    try:
        detail = monitored_tables_svc.get(binding_id)
        if detail is None:
            raise HTTPException(status_code=404, detail=f"Monitored table not found: {binding_id}")
        if detail.table.status != "pending_approval":
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Cannot reject monitored table {binding_id}: status is "
                    f"'{detail.table.status}', expected 'pending_approval'"
                ),
            )
        user_email = _current_user_email(obo_ws)
        rejected = _transition_binding_checks(
            monitored_tables_svc,
            rules_catalog,
            binding_id,
            from_status="pending_approval",
            to_status="rejected",
            user_email=user_email,
        )
        table = monitored_tables_svc.set_status(binding_id, "rejected", user_email)
        return MonitoredTableReviewOut(table=MonitoredTableOut.from_domain(table), affected_check_count=rejected)
    except HTTPException:
        raise
    except RuntimeError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to reject monitored table {binding_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to reject monitored table: {e}")


@router.post(
    "/{binding_id}/revert",
    response_model=MonitoredTableReviewOut,
    operation_id="revertMonitoredTable",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def revert_monitored_table(
    binding_id: str,
    monitored_tables_svc: Annotated[MonitoredTableService, Depends(get_monitored_table_service)],
    rules_catalog: Annotated[RulesCatalogService, Depends(get_rules_catalog_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> MonitoredTableReviewOut:
    """Withdraw a pending submission — walk the binding back to ``draft``.

    The counterpart to submit: an author who submitted a binding for review can
    pull it back to keep editing before an approver acts, without a reject
    (which is the approver's decision and leaves a ``rejected`` audit trail).
    Every ``pending_approval`` check mapped to the binding is walked back to
    ``draft`` (a legal per-rule transition), then the binding itself flips to
    ``draft``.

    Only a binding currently ``pending_approval`` can be reverted — 409
    otherwise. Gated to authors-and-above; the front end only surfaces it to
    the submission's owner (or an approver), matching the per-rule revoke.
    """
    try:
        detail = monitored_tables_svc.get(binding_id)
        if detail is None:
            raise HTTPException(status_code=404, detail=f"Monitored table not found: {binding_id}")
        if detail.table.status != "pending_approval":
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Cannot revert monitored table {binding_id}: status is "
                    f"'{detail.table.status}', expected 'pending_approval'"
                ),
            )
        user_email = _current_user_email(obo_ws)
        reverted = _transition_binding_checks(
            monitored_tables_svc,
            rules_catalog,
            binding_id,
            from_status="pending_approval",
            to_status="draft",
            user_email=user_email,
        )
        table = monitored_tables_svc.set_status(binding_id, "draft", user_email)
        return MonitoredTableReviewOut(table=MonitoredTableOut.from_domain(table), affected_check_count=reverted)
    except HTTPException:
        raise
    except RuntimeError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to revert monitored table {binding_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to revert monitored table: {e}")


# ------------------------------------------------------------------
# AI mapping suggester (Phase 4C — design spec §8)
# ------------------------------------------------------------------


@router.post(
    "/{binding_id}/suggest-rules",
    response_model=SuggestRulesOut,
    operation_id="suggestRulesForTable",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
async def suggest_rules_for_table(
    binding_id: str,
    svc: Annotated[RuleSuggester, Depends(get_rule_suggester)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> SuggestRulesOut:
    """Suggest published registry rules (with a complete column mapping) for a monitored table.

    Always returns HTTP 200 with ``available=False`` + a ``reason`` for every
    degraded path — Vector Search/embedding/AI not configured, retrieval or
    judge failure — so a deployment with no AI/Vector Search infra behaves
    exactly like today. Never raises for a missing-infra deployment.
    """
    user_email = _current_user_email(obo_ws)
    result = await svc.suggest(binding_id, user_email)
    return SuggestRulesOut.from_domain(result)


# ------------------------------------------------------------------
# Tag-based rule suggestions (apply-on-tag — OFF path, Task 10b)
# ------------------------------------------------------------------


@router.get(
    "/{binding_id}/tag-suggestions",
    response_model=TagSuggestionsOut,
    operation_id="listTagSuggestions",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def list_tag_suggestions(
    binding_id: str,
    svc: Annotated[TagSuggestionService, Depends(get_tag_suggestion_service)],
) -> TagSuggestionsOut:
    """List tag-matched published rules (with a representative column mapping) for a monitored table.

    The OFF-path counterpart to auto-apply: when ``tag_auto_apply`` is off,
    tag-matched rules surface here as accept-to-attach suggestions instead of
    auto-attaching. Best-effort — any read/service failure degrades to an empty
    list with HTTP 200; this route never raises for a missing match or an
    unreadable table (mirroring the suggest-rules contract).
    """
    try:
        suggestions = svc.suggest(binding_id)
    except Exception:
        logger.warning(f"Failed to list tag suggestions for monitored table {binding_id}", exc_info=True)
        return TagSuggestionsOut()
    return TagSuggestionsOut.from_domain(suggestions)


# ------------------------------------------------------------------
# Profile-page profiler suggestions (B2-82 — dqlake-style placement)
# ------------------------------------------------------------------


@router.get(
    "/{binding_id}/profile/suggestions",
    response_model=list[ProfilingSuggestionOut],
    operation_id="listProfilingSuggestions",
    dependencies=[require_role(*_ALL_ROLES)],
)
def list_profiling_suggestions(
    binding_id: str,
    svc: Annotated[ProfilingSuggestionService, Depends(get_profiling_suggestion_service)],
) -> list[ProfilingSuggestionOut]:
    """List the DQX profiler's applicable rule suggestions for the Profile page.

    Read-only and side-effect-free: it introspects the latest profile's
    generated checks against the check-function registry to build applicable
    suggestions. NO registry rule is created or approved here — that happens
    only when a user explicitly applies one via ``applyProfilingSuggestion``.
    Returns an empty list when the table has no profile yet.
    """
    try:
        suggestions = svc.list_suggestions(binding_id)
    except ProfilingBindingNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to list profiling suggestions for monitored table {binding_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to list profiling suggestions: {e}")
    return [ProfilingSuggestionOut.from_domain(s) for s in suggestions]


@router.post(
    "/{binding_id}/profile/suggestions/apply",
    response_model=ApplyProfilingSuggestionsOut,
    operation_id="applyProfilingSuggestions",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def apply_profiling_suggestions(
    binding_id: str,
    body: ApplyProfilingSuggestionsIn,
    svc: Annotated[ProfilingSuggestionService, Depends(get_profiling_suggestion_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    role: CurrentUserRole,
    principal_ids: CurrentPrincipalIds,
    perms: Annotated[PermissionsService, Depends(get_permissions_service)],
) -> ApplyProfilingSuggestionsOut:
    """Apply the selected profiler suggestions to the monitored table in one action.

    This is the ONLY path that resolves-or-creates + approves the underlying
    registry rules (via ``RegistryService.match_or_create_approved_rule`` —
    idempotent, validated, audited) before binding them to the table. Selecting
    or listing suggestions creates nothing. Requires ``APPLY`` on the monitored
    table (mirroring the ``applyRuleToTable`` gate) unless the caller is an
    admin/approver. Partial failures are reported in the response body
    (``failed``) rather than aborting the whole request.
    """
    user_email = _current_user_email(obo_ws)
    perms.require_object(
        ObjectType.MONITORED_TABLE.value,
        binding_id,
        Privilege.APPLY,
        role=role,
        principal_ids=set(principal_ids),
        principal_email=user_email,
    )
    try:
        result = svc.apply_suggestions(binding_id, body.indices, user_email)
        return ApplyProfilingSuggestionsOut.from_domain(result)
    except ProfilingBindingNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to apply profiling suggestions to monitored table {binding_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to apply profiling suggestions: {e}")
