"""Monitored Tables routes — Phase 3B/3C (register/list/get/delete/apply/publish + profiling read).

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
    get_materializer,
    get_monitored_table_service,
    get_obo_ws,
    get_rule_suggester,
    require_role,
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
    MonitoredTableSummaryOut,
    PublishMonitoredTableOut,
    RegisterMonitoredTableIn,
    SetAppliedRulePinIn,
    SetAppliedRuleSeverityOverrideIn,
    SuggestRulesOut,
)
from databricks_labs_dqx_app.backend.services.apply_rules_service import (
    ApplyRulesService,
    MappingIncompleteError,
    RuleNotPublishedError,
)
from databricks_labs_dqx_app.backend.services.materializer import MaterializationError, Materializer
from databricks_labs_dqx_app.backend.services.monitored_table_service import (
    DuplicateMonitoredTableError,
    MonitoredTableService,
    MonitoredTableSummary,
)
from databricks_labs_dqx_app.backend.services.rule_suggester import RuleSuggester

router = APIRouter()

_ALL_ROLES = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR, UserRole.VIEWER]
_AUTHORS_AND_ABOVE = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR]


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
# Publish (materialize) — Phase 3C
# ------------------------------------------------------------------


@router.post(
    "/{binding_id}/publish",
    response_model=PublishMonitoredTableOut,
    operation_id="publishMonitoredTable",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def publish_monitored_table(
    binding_id: str,
    monitored_tables_svc: Annotated[MonitoredTableService, Depends(get_monitored_table_service)],
    materializer: Annotated[Materializer, Depends(get_materializer)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> PublishMonitoredTableOut:
    """Publish a monitored table and materialize its applied rules into ``dq_quality_rules``.

    Materialized rows always enter the existing per-table ``draft`` review
    flow (never auto-approved) — see ``backend/services/materializer.py``
    for the full approval/auto-upgrade semantics.
    """
    try:
        user_email = _current_user_email(obo_ws)
        table = monitored_tables_svc.publish(binding_id, user_email)
        materialized_ids = materializer.materialize_binding(binding_id)
        return PublishMonitoredTableOut(
            table=MonitoredTableOut.from_domain(table), materialized_rule_ids=materialized_ids
        )
    except MaterializationError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to publish monitored table {binding_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to publish monitored table: {e}")


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
