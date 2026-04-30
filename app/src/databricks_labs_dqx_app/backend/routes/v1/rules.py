from typing import Annotated

from databricks.sdk import WorkspaceClient
from fastapi import APIRouter, Depends, HTTPException, Query

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.dependencies import (
    get_obo_ws,
    get_rules_catalog_service,
    get_user_catalog_names,
    require_role,
)
from databricks_labs_dqx_app.backend.logger import logger
from databricks_labs_dqx_app.backend.models import (
    BatchSaveRulesIn,
    BatchSaveRulesOut,
    CheckDuplicatesIn,
    CheckDuplicatesOut,
    RuleCatalogEntryOut,
    SaveRulesIn,
    SetStatusIn,
)
from databricks_labs_dqx_app.backend.services.rules_catalog_service import RulesCatalogService

router = APIRouter()

_ALL_ROLES = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR, UserRole.VIEWER]
_AUTHORS_AND_ABOVE = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR]
_APPROVERS_ONLY = [UserRole.ADMIN, UserRole.RULE_APPROVER]

_SQL_CHECK_PREFIX = "__sql_check__/"


def _catalog_of(fqn: str) -> str:
    """Extract the catalog part from a fully qualified table name."""
    if fqn.startswith(_SQL_CHECK_PREFIX):
        fqn = fqn[len(_SQL_CHECK_PREFIX) :]
    parts = fqn.split(".", 1)
    return parts[0] if parts else ""


def _display_name(fqn: str) -> str:
    return fqn[len(_SQL_CHECK_PREFIX) :] if fqn.startswith(_SQL_CHECK_PREFIX) else fqn


def _entry_to_out(entry) -> RuleCatalogEntryOut:
    return RuleCatalogEntryOut(
        table_fqn=entry.table_fqn,
        display_name=_display_name(entry.table_fqn),
        checks=entry.checks,
        version=entry.version,
        status=entry.status,
        source=entry.source,
        rule_id=entry.rule_id,
        created_by=entry.created_by,
        created_at=entry.created_at,
        updated_by=entry.updated_by,
        updated_at=entry.updated_at,
    )


# ------------------------------------------------------------------
# List / Get
# ------------------------------------------------------------------


@router.get(
    "",
    response_model=list[RuleCatalogEntryOut],
    operation_id="listRules",
    dependencies=[require_role(*_ALL_ROLES)],
)
async def list_rules(
    svc: Annotated[RulesCatalogService, Depends(get_rules_catalog_service)],
    user_catalogs: Annotated[frozenset[str], Depends(get_user_catalog_names)],
    status: Annotated[str | None, Query(description="Filter by status")] = None,
) -> list[RuleCatalogEntryOut]:
    """List rules filtered to catalogs the current user can access."""
    try:
        entries = svc.list_rules(status=status)
        return [
            _entry_to_out(e)
            for e in entries
            if e.table_fqn.startswith(_SQL_CHECK_PREFIX) or _catalog_of(e.table_fqn) in user_catalogs
        ]
    except Exception as e:
        logger.error(f"Failed to list rules: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to list rules: {e}")


@router.get(
    "/{table_fqn:path}",
    response_model=list[RuleCatalogEntryOut],
    operation_id="getRules",
    dependencies=[require_role(*_ALL_ROLES)],
)
def get_rules(
    table_fqn: str,
    svc: Annotated[RulesCatalogService, Depends(get_rules_catalog_service)],
    user_catalogs: Annotated[frozenset[str], Depends(get_user_catalog_names)],
) -> list[RuleCatalogEntryOut]:
    """Get all individual rules for a specific table."""
    try:
        if not table_fqn.startswith(_SQL_CHECK_PREFIX) and _catalog_of(table_fqn) not in user_catalogs:
            raise HTTPException(status_code=403, detail="You do not have access to this table's catalog")
        entries = svc.list_rules_for_table(table_fqn)
        if not entries:
            raise HTTPException(status_code=404, detail=f"No rules found for table: {table_fqn}")
        return [_entry_to_out(e) for e in entries]
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get rules for {table_fqn}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get rules: {e}")


# ------------------------------------------------------------------
# Save / Update
# ------------------------------------------------------------------


@router.post(
    "",
    response_model=list[RuleCatalogEntryOut],
    operation_id="saveRules",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def save_rules(
    body: SaveRulesIn,
    svc: Annotated[RulesCatalogService, Depends(get_rules_catalog_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> list[RuleCatalogEntryOut]:
    """Save rules. Each check becomes an individual rule row.

    If ``rule_id`` is set, updates an existing rule instead.
    """
    try:
        user = obo_ws.current_user.me()
        user_email = user.user_name or "unknown"

        if body.rule_id:
            entry = svc.update_rule(body.rule_id, body.checks, user_email)
            return [_entry_to_out(entry)]

        entries = svc.save(body.table_fqn, body.checks, user_email, source=body.source)
        return [_entry_to_out(e) for e in entries]
    except Exception as e:
        logger.error(f"Failed to save rules for {body.table_fqn}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to save rules: {e}")


@router.post(
    "/batch",
    response_model=BatchSaveRulesOut,
    operation_id="batchSaveRules",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def batch_save_rules(
    body: BatchSaveRulesIn,
    svc: Annotated[RulesCatalogService, Depends(get_rules_catalog_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> BatchSaveRulesOut:
    """Save the same set of checks to multiple tables (reusable rules)."""
    if not body.table_fqns:
        raise HTTPException(status_code=400, detail="table_fqns must not be empty")
    user = obo_ws.current_user.me()
    user_email = user.user_name or "unknown"
    saved: list[RuleCatalogEntryOut] = []
    failed: list[dict[str, str]] = []
    for fqn in body.table_fqns:
        try:
            entries = svc.save(fqn, body.checks, user_email, source=body.source)
            saved.extend([_entry_to_out(e) for e in entries])
        except Exception as e:
            logger.error(f"Failed to save rules for {fqn}: {e}", exc_info=True)
            failed.append({"table_fqn": fqn, "error": str(e)})
    return BatchSaveRulesOut(saved=saved, failed=failed)


# ------------------------------------------------------------------
# Duplicate detection
# ------------------------------------------------------------------


@router.post(
    "/check-duplicates",
    response_model=CheckDuplicatesOut,
    operation_id="checkDuplicates",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def check_duplicates(
    body: CheckDuplicatesIn,
    svc: Annotated[RulesCatalogService, Depends(get_rules_catalog_service)],
) -> CheckDuplicatesOut:
    """Check if any of the provided checks already exist for the given table."""
    try:
        dupes = svc.find_duplicates(
            body.table_fqn,
            body.checks,
            exclude_rule_id=body.exclude_rule_id,
            exclude_rule_ids=body.exclude_rule_ids or None,
        )
        return CheckDuplicatesOut(duplicates=dupes)
    except Exception as e:
        logger.error(f"Failed to check duplicates: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to check duplicates: {e}")


# ------------------------------------------------------------------
# Delete
# ------------------------------------------------------------------


@router.delete(
    "/{rule_id}",
    operation_id="deleteRule",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def delete_rule(
    rule_id: str,
    svc: Annotated[RulesCatalogService, Depends(get_rules_catalog_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> dict[str, str]:
    """Delete a single rule by rule_id."""
    try:
        user = obo_ws.current_user.me()
        user_email = user.user_name or "unknown"
        svc.delete(rule_id, user_email)
        return {"status": "deleted", "rule_id": rule_id}
    except Exception as e:
        logger.error(f"Failed to delete rule {rule_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to delete rule: {e}")


# ------------------------------------------------------------------
# Status transitions (per rule_id)
# ------------------------------------------------------------------


@router.post(
    "/{rule_id}/submit",
    response_model=RuleCatalogEntryOut,
    operation_id="submitRuleForApproval",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def submit_for_approval(
    rule_id: str,
    svc: Annotated[RulesCatalogService, Depends(get_rules_catalog_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    body: SetStatusIn | None = None,
) -> RuleCatalogEntryOut:
    """Submit an individual rule for approval."""
    try:
        user = obo_ws.current_user.me()
        user_email = user.user_name or "unknown"
        expected_version = body.expected_version if body else None
        entry = svc.set_status(rule_id, "pending_approval", user_email, expected_version)
        return _entry_to_out(entry)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to submit rule for approval: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to submit for approval: {e}")


@router.post(
    "/{rule_id}/revoke",
    response_model=RuleCatalogEntryOut,
    operation_id="revokeSubmission",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def revoke_submission(
    rule_id: str,
    svc: Annotated[RulesCatalogService, Depends(get_rules_catalog_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    body: SetStatusIn | None = None,
) -> RuleCatalogEntryOut:
    """Revoke a pending submission back to draft."""
    try:
        user = obo_ws.current_user.me()
        user_email = user.user_name or "unknown"
        expected_version = body.expected_version if body else None
        entry = svc.set_status(rule_id, "draft", user_email, expected_version)
        return _entry_to_out(entry)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to revoke submission: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to revoke submission: {e}")


@router.post(
    "/{rule_id}/approve",
    response_model=RuleCatalogEntryOut,
    operation_id="approveRule",
    dependencies=[require_role(*_APPROVERS_ONLY)],
)
def approve_rules(
    rule_id: str,
    svc: Annotated[RulesCatalogService, Depends(get_rules_catalog_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    body: SetStatusIn | None = None,
) -> RuleCatalogEntryOut:
    """Approve an individual rule."""
    try:
        user = obo_ws.current_user.me()
        user_email = user.user_name or "unknown"
        expected_version = body.expected_version if body else None
        entry = svc.set_status(rule_id, "approved", user_email, expected_version)
        return _entry_to_out(entry)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to approve rule: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to approve rule: {e}")


@router.post(
    "/backfill-ids",
    operation_id="backfillRuleIds",
    dependencies=[require_role(*_APPROVERS_ONLY)],
)
def backfill_rule_ids(
    svc: Annotated[RulesCatalogService, Depends(get_rules_catalog_service)],
) -> dict[str, int]:
    """Assign UUIDs to rules that are missing a rule_id (legacy rows)."""
    try:
        count = svc.backfill_rule_ids()
        return {"repaired": count}
    except Exception as e:
        logger.error(f"Failed to backfill rule IDs: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to backfill rule IDs: {e}")


@router.post(
    "/{rule_id}/reject",
    response_model=RuleCatalogEntryOut,
    operation_id="rejectRule",
    dependencies=[require_role(*_APPROVERS_ONLY)],
)
def reject_rules(
    rule_id: str,
    svc: Annotated[RulesCatalogService, Depends(get_rules_catalog_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    body: SetStatusIn | None = None,
) -> RuleCatalogEntryOut:
    """Reject an individual rule."""
    try:
        user = obo_ws.current_user.me()
        user_email = user.user_name or "unknown"
        expected_version = body.expected_version if body else None
        entry = svc.set_status(rule_id, "rejected", user_email, expected_version)
        return _entry_to_out(entry)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to reject rule: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to reject rule: {e}")
