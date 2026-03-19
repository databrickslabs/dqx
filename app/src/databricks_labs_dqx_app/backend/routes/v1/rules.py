from typing import Annotated

from databricks.sdk import WorkspaceClient
from fastapi import APIRouter, Depends, HTTPException, Query

from databricks_labs_dqx_app.backend.dependencies import get_obo_ws, get_rules_catalog_service
from databricks_labs_dqx_app.backend.logger import logger
from databricks_labs_dqx_app.backend.models import RuleCatalogEntryOut, SaveRulesIn, SetStatusIn
from databricks_labs_dqx_app.backend.services.rules_catalog_service import RulesCatalogService

router = APIRouter()


def _entry_to_out(entry) -> RuleCatalogEntryOut:
    return RuleCatalogEntryOut(
        table_fqn=entry.table_fqn,
        checks=entry.checks,
        version=entry.version,
        status=entry.status,
        created_by=entry.created_by,
        created_at=entry.created_at,
        updated_by=entry.updated_by,
        updated_at=entry.updated_at,
    )


@router.get("", response_model=list[RuleCatalogEntryOut], operation_id="listRules")
def list_rules(
    svc: Annotated[RulesCatalogService, Depends(get_rules_catalog_service)],
    status: Annotated[str | None, Query(description="Filter by status")] = None,
) -> list[RuleCatalogEntryOut]:
    """List all rule sets in the catalog, optionally filtered by status."""
    try:
        entries = svc.list_rules(status=status)
        return [_entry_to_out(e) for e in entries]
    except Exception as e:
        logger.error(f"Failed to list rules: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to list rules: {e}")


@router.get("/{table_fqn:path}", response_model=RuleCatalogEntryOut, operation_id="getRules")
def get_rules(
    table_fqn: str,
    svc: Annotated[RulesCatalogService, Depends(get_rules_catalog_service)],
) -> RuleCatalogEntryOut:
    """Get the rule set for a specific table."""
    try:
        entry = svc.get(table_fqn)
        if entry is None:
            raise HTTPException(status_code=404, detail=f"No rules found for table: {table_fqn}")
        return _entry_to_out(entry)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get rules for {table_fqn}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get rules: {e}")


@router.post("", response_model=RuleCatalogEntryOut, operation_id="saveRules")
def save_rules(
    body: SaveRulesIn,
    svc: Annotated[RulesCatalogService, Depends(get_rules_catalog_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> RuleCatalogEntryOut:
    """Save (upsert) a rule set for a table."""
    try:
        user = obo_ws.current_user.me()
        user_email = user.user_name or "unknown"
        entry = svc.save(body.table_fqn, body.checks, user_email)
        return _entry_to_out(entry)
    except Exception as e:
        logger.error(f"Failed to save rules for {body.table_fqn}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to save rules: {e}")


@router.delete("/{table_fqn:path}", operation_id="deleteRules")
def delete_rules(
    table_fqn: str,
    svc: Annotated[RulesCatalogService, Depends(get_rules_catalog_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> dict[str, str]:
    """Delete the rule set for a table."""
    try:
        user = obo_ws.current_user.me()
        user_email = user.user_name or "unknown"
        svc.delete(table_fqn, user_email)
        return {"status": "deleted", "table_fqn": table_fqn}
    except Exception as e:
        logger.error(f"Failed to delete rules for {table_fqn}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to delete rules: {e}")


@router.post("/{table_fqn:path}/submit", response_model=RuleCatalogEntryOut, operation_id="submitRulesForApproval")
def submit_for_approval(
    table_fqn: str,
    svc: Annotated[RulesCatalogService, Depends(get_rules_catalog_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    body: SetStatusIn | None = None,
) -> RuleCatalogEntryOut:
    """Submit a rule set for approval."""
    try:
        user = obo_ws.current_user.me()
        user_email = user.user_name or "unknown"
        expected_version = body.expected_version if body else None
        entry = svc.set_status(table_fqn, "pending_approval", user_email, expected_version)
        return _entry_to_out(entry)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to submit rules for approval: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to submit for approval: {e}")


@router.post("/{table_fqn:path}/approve", response_model=RuleCatalogEntryOut, operation_id="approveRules")
def approve_rules(
    table_fqn: str,
    svc: Annotated[RulesCatalogService, Depends(get_rules_catalog_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    body: SetStatusIn | None = None,
) -> RuleCatalogEntryOut:
    """Approve a rule set (admin only)."""
    try:
        user = obo_ws.current_user.me()
        user_email = user.user_name or "unknown"
        expected_version = body.expected_version if body else None
        entry = svc.set_status(table_fqn, "approved", user_email, expected_version)
        return _entry_to_out(entry)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to approve rules: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to approve rules: {e}")


@router.post("/{table_fqn:path}/reject", response_model=RuleCatalogEntryOut, operation_id="rejectRules")
def reject_rules(
    table_fqn: str,
    svc: Annotated[RulesCatalogService, Depends(get_rules_catalog_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    body: SetStatusIn | None = None,
) -> RuleCatalogEntryOut:
    """Reject a rule set (admin only)."""
    try:
        user = obo_ws.current_user.me()
        user_email = user.user_name or "unknown"
        expected_version = body.expected_version if body else None
        entry = svc.set_status(table_fqn, "rejected", user_email, expected_version)
        return _entry_to_out(entry)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to reject rules: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to reject rules: {e}")
