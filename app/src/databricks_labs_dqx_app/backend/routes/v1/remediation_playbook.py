"""Thin passthrough endpoints onto the remediation-playbook Volume."""

from __future__ import annotations

from typing import Annotated

from databricks.sdk import WorkspaceClient
from fastapi import APIRouter, Depends, HTTPException

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.dependencies import (
    get_obo_ws,
    get_remediation_playbook_service,
    require_role,
)
from databricks_labs_dqx_app.backend.logger import logger
from databricks_labs_dqx_app.backend.models import (
    RemediationPlaybookContentIn,
    RemediationPlaybookEntryIn,
    RemediationPlaybookEntryOut,
)
from databricks_labs_dqx_app.backend.services.remediation_playbook_service import RemediationPlaybookService

router = APIRouter()

# Same RBAC shape as the Rules pages: everyone can view, authors and up
# can create/edit — these runbooks directly drive automated remediation
# of production data, so viewers stay read-only.
_ALL_ROLES = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR, UserRole.VIEWER]
_AUTHORS_AND_ABOVE = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR]


@router.get(
    "",
    response_model=list[RemediationPlaybookEntryOut],
    operation_id="listRemediationPlaybookEntries",
    dependencies=[require_role(*_ALL_ROLES)],
)
def list_entries(
    svc: Annotated[RemediationPlaybookService, Depends(get_remediation_playbook_service)],
) -> list[RemediationPlaybookEntryOut]:
    """List every dataset's remediation runbook on the Volume."""
    try:
        return [RemediationPlaybookEntryOut(**e) for e in svc.list_entries()]
    except Exception as e:
        logger.error("Failed to list remediation runbooks: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to list remediation runbooks: {e}")


@router.get(
    "/{filename}",
    response_model=RemediationPlaybookEntryOut,
    operation_id="getRemediationPlaybookEntry",
    dependencies=[require_role(*_ALL_ROLES)],
)
def get_entry(
    filename: str,
    svc: Annotated[RemediationPlaybookService, Depends(get_remediation_playbook_service)],
) -> RemediationPlaybookEntryOut:
    try:
        entry = svc.get(filename)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if entry is None:
        raise HTTPException(status_code=404, detail=f"Remediation runbook '{filename}' not found")
    return RemediationPlaybookEntryOut(**entry)


@router.post(
    "",
    response_model=RemediationPlaybookEntryOut,
    operation_id="saveRemediationPlaybookEntry",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def save_entry(
    body: RemediationPlaybookEntryIn,
    svc: Annotated[RemediationPlaybookService, Depends(get_remediation_playbook_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> RemediationPlaybookEntryOut:
    """Write (create or overwrite) the runbook file for ``body.table_fqn``."""
    try:
        user = obo_ws.current_user.me()
        user_email = user.user_name or "unknown"
        entry = svc.save(body.table_fqn, body.runbook_yaml, user_email)
        return RemediationPlaybookEntryOut(**entry)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to save remediation runbook: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to save remediation runbook: {e}")


@router.put(
    "/{filename}",
    response_model=RemediationPlaybookEntryOut,
    operation_id="updateRemediationPlaybookContent",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def update_content(
    filename: str,
    body: RemediationPlaybookContentIn,
    svc: Annotated[RemediationPlaybookService, Depends(get_remediation_playbook_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> RemediationPlaybookEntryOut:
    """Overwrite an existing runbook's content in place, by filename.

    Use this (not ``saveRemediationPlaybookEntry``) when editing a
    runbook that was loaded from the list — the filename's table_fqn
    can't be reliably reconstructed for re-derivation, and doesn't need
    to be: the file's location isn't changing, only its content.
    """
    try:
        user = obo_ws.current_user.me()
        user_email = user.user_name or "unknown"
        entry = svc.update_content(filename, body.runbook_yaml, user_email)
        return RemediationPlaybookEntryOut(**entry)
    except LookupError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to update remediation runbook: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to update remediation runbook: {e}")


@router.delete(
    "/{filename}",
    status_code=204,
    operation_id="deleteRemediationPlaybookEntry",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def delete_entry(
    filename: str,
    svc: Annotated[RemediationPlaybookService, Depends(get_remediation_playbook_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> None:
    """Remove a dataset's runbook file from the Volume."""
    try:
        user = obo_ws.current_user.me()
        user_email = user.user_name or "unknown"
        svc.delete(filename, user_email)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error("Failed to delete remediation runbook: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to delete remediation runbook: {e}")
