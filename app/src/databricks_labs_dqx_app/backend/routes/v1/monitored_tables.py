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

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.dependencies import (
    get_apply_rules_service,
    get_binding_run_service,
    get_materializer,
    get_monitored_table_service,
    get_monitored_table_version_service,
    get_obo_ws,
    get_rule_suggester,
    get_rules_catalog_service,
    require_role,
    require_runner,
)
from databricks_labs_dqx_app.backend.logger import logger
from databricks_labs_dqx_app.backend.models import (
    AppliedRuleOut,
    ApplyRuleIn,
    BulkRegisterMonitoredTablesIn,
    BulkRegisterMonitoredTablesOut,
    MonitoredTableDetailOut,
    MonitoredTableOut,
    MonitoredTableProfileOut,
    MonitoredTableReviewOut,
    MonitoredTableSummaryOut,
    MonitoredTableVersionOut,
    RegisterMonitoredTableIn,
    RunMonitoredTableIn,
    RunMonitoredTableOut,
    UpdateMonitoredTableScheduleIn,
    SaveAppliedRulesIn,
    SetAppliedRulePinIn,
    SetAppliedRuleSeverityOverrideIn,
    SuggestRulesOut,
)
from databricks_labs_dqx_app.backend.services.apply_rules_service import (
    ApplyRulesService,
    DesiredAppliedRule,
    MappingIncompleteError,
    RuleNotPublishedError,
)
from databricks_labs_dqx_app.backend.services.binding_run_service import (
    BindingNotFoundError,
    BindingRunError,
    BindingRunService,
    MissingSnapshotError,
    NeverApprovedError,
)
from databricks_labs_dqx_app.backend.services.materializer import MaterializationError, Materializer
from databricks_labs_dqx_app.backend.services.monitored_table_service import (
    DuplicateMonitoredTableError,
    MonitoredTableService,
    MonitoredTableSummary,
)
from databricks_labs_dqx_app.backend.services.monitored_table_versions import MonitoredTableVersionService
from databricks_labs_dqx_app.backend.services.rule_suggester import RuleSuggester
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
    status: Annotated[str | None, Query(description="Filter by status")] = None,
    steward: Annotated[str | None, Query(description="Filter by steward")] = None,
    catalog: Annotated[str | None, Query(description="Filter by catalog part of table_fqn")] = None,
    schema: Annotated[str | None, Query(description="Filter by schema part of table_fqn")] = None,
    name: Annotated[str | None, Query(description="Substring search over table_fqn")] = None,
) -> list[MonitoredTableSummaryOut]:
    """List monitored tables, optionally filtered, with per-table applied-rule counts."""
    try:
        summaries = svc.list_monitored_tables(
            status=status, steward=steward, catalog=catalog, schema=schema, name=name
        )
        return [MonitoredTableSummaryOut.from_domain(s) for s in summaries]
    except Exception as e:
        logger.error(f"Failed to list monitored tables: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to list monitored tables: {e}")


@router.get(
    "/{binding_id}",
    response_model=MonitoredTableDetailOut,
    operation_id="getMonitoredTable",
    dependencies=[require_role(*_ALL_ROLES)],
)
def get_monitored_table(
    binding_id: str,
    svc: Annotated[MonitoredTableService, Depends(get_monitored_table_service)],
) -> MonitoredTableDetailOut:
    """Get a monitored table binding plus its applied rules (joined to rule name/dimension/severity tags)."""
    try:
        detail = svc.get(binding_id)
        if detail is None:
            raise HTTPException(status_code=404, detail=f"Monitored table not found: {binding_id}")
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
) -> MonitoredTableSummaryOut:
    """Register a table under Rules Registry governance (status ``draft``)."""
    try:
        user_email = _current_user_email(obo_ws)
        table = svc.register(body.table_fqn, user_email, steward=body.steward)
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
) -> BulkRegisterMonitoredTablesOut:
    """Register many tables under Rules Registry governance in one call.

    Already-monitored tables and syntactically invalid FQNs are reported
    back in the summary rather than failing the whole batch — see
    :meth:`MonitoredTableService.bulk_register`.
    """
    try:
        user_email = _current_user_email(obo_ws)
        result = svc.bulk_register(body.table_fqns, user_email, steward=body.steward)
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
) -> dict[str, str]:
    """Delete a monitored table binding and its applied rules.

    TODO(Phase 3C): once the materializer exists, block/handle
    de-materialization of any ``dq_quality_rules`` rows tied to this
    binding's applications before allowing deletion.
    """
    try:
        user_email = _current_user_email(obo_ws)
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
) -> MonitoredTableOut:
    """Set or clear a monitored table's run schedule (P21 item 14).

    Orthogonal to the review lifecycle — does NOT flip the binding's status.
    An approved table with a cron fires on the in-app scheduler.
    """
    try:
        user_email = _current_user_email(obo_ws)
        table = svc.update_schedule(binding_id, body.schedule_cron, body.schedule_tz, user_email)
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
            sample_size=body.sample_size,
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
) -> AppliedRuleOut:
    """Apply a published registry rule to a monitored table's column mapping."""
    try:
        user_email = _current_user_email(obo_ws)
        applied = svc.apply_rule(
            binding_id,
            body.rule_id,
            body.column_mapping,
            user_email,
            pinned_version=body.pinned_version,
            severity_override=body.severity_override,
            tags=body.tags,
        )
        return AppliedRuleOut.from_domain(applied)
    except MappingIncompleteError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except RuleNotPublishedError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to apply rule to monitored table {binding_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to apply rule: {e}")


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
) -> list[AppliedRuleOut]:
    """Reconcile the FULL desired set of applied rules for a monitored table in one batch.

    Backs the staged Apply Rules editor: the frontend stages every add /
    mapping-edit / severity-override / pin / removal locally and calls this
    once on Save-as-draft or Publish instead of firing an immediate write per
    edit. Does NOT materialize — materialization stays gated behind the
    existing publish route.
    """
    try:
        user_email = _current_user_email(obo_ws)
        desired = [
            DesiredAppliedRule(
                rule_id=entry.rule_id,
                column_mapping=entry.column_mapping,
                pinned_version=entry.pinned_version,
                severity_override=entry.severity_override,
                tags=entry.tags,
            )
            for entry in body.applications
        ]
        applied = svc.save_applied_rules(binding_id, desired, user_email)
        return [AppliedRuleOut.from_domain(a) for a in applied]
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
) -> dict[str, str]:
    """Remove an applied rule and every ``dq_quality_rules`` row it materialized."""
    try:
        svc.remove_applied(applied_rule_id)
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
) -> AppliedRuleOut:
    """Pin (or, with ``pinned_version=None``, unpin) an applied rule's version."""
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
) -> AppliedRuleOut:
    """Set (or, with ``severity=None``, clear) an applied rule's severity override."""
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
        return MonitoredTableReviewOut(table=MonitoredTableOut.from_domain(table), affected_check_count=submitted)
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
        approved = _transition_binding_checks(
            monitored_tables_svc,
            rules_catalog,
            binding_id,
            from_status="pending_approval",
            to_status="approved",
            user_email=user_email,
        )
        table = monitored_tables_svc.set_status(
            binding_id, _rollup_binding_status(monitored_tables_svc, binding_id), user_email
        )
        new_version = version_svc.freeze_new_version(binding_id, user_email)
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
