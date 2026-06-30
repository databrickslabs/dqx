"""Per-run review status endpoints.

Surface for the dropdown that lives next to the comments thread on the
Runs detail page, plus the audit-history list shown below it. Any
authenticated user can change a run's status (matches the comments
behaviour — business reviewers don't have a special role); the
catalogue of allowed values is admin-managed in
``/api/v1/config/run-review-statuses`` (see ``config.py``).

Route shape
-----------
``GET /api/v1/runs/{run_id}/review-status`` — current effective value
(persisted row or virtual default) plus ``updated_by`` / ``updated_at``
metadata.

``PUT /api/v1/runs/{run_id}/review-status`` — set the status. Validated
against the live catalogue server-side.

``DELETE /api/v1/runs/{run_id}/review-status`` — revert to the
catalogue default (drops the explicit row, appends a history entry).

``GET /api/v1/runs/{run_id}/review-status/history`` — last ≤200 audit
rows, newest first. Surfaced as a collapsible list on the run detail
page.
"""

from __future__ import annotations

from typing import Annotated

from databricks.sdk import WorkspaceClient
from fastapi import APIRouter, Depends, HTTPException

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.dependencies import (
    get_obo_ws,
    get_review_status_service,
    require_role,
)
from databricks_labs_dqx_app.backend.logger import logger
from databricks_labs_dqx_app.backend.services.review_status_service import (
    ReviewStatusHistoryEntry,
    ReviewStatusRecord,
    ReviewStatusService,
)
from pydantic import BaseModel

router = APIRouter()

# Visible to every authenticated principal. Matches the comments
# routes — review actions are deliberately broad-reach so business
# users can self-serve without being assigned a role.
_ALL_ROLES = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR, UserRole.VIEWER]


class ReviewStatusOut(BaseModel):
    """Current effective review status for a run."""

    run_id: str
    status: str
    updated_by: str | None = None
    updated_at: str | None = None
    is_default: bool = False


class SetReviewStatusIn(BaseModel):
    status: str


class ReviewStatusHistoryEntryOut(BaseModel):
    run_id: str
    status: str
    previous_status: str | None = None
    changed_by: str
    changed_at: str | None = None


class ReviewStatusHistoryOut(BaseModel):
    history: list[ReviewStatusHistoryEntryOut]


def _record_to_out(record: ReviewStatusRecord) -> ReviewStatusOut:
    return ReviewStatusOut(
        run_id=record.run_id,
        status=record.status,
        updated_by=record.updated_by,
        updated_at=record.updated_at,
        is_default=record.is_default,
    )


def _history_entry_to_out(entry: ReviewStatusHistoryEntry) -> ReviewStatusHistoryEntryOut:
    return ReviewStatusHistoryEntryOut(
        run_id=entry.run_id,
        status=entry.status,
        previous_status=entry.previous_status,
        changed_by=entry.changed_by,
        changed_at=entry.changed_at,
    )


@router.get(
    "/{run_id}/review-status",
    response_model=ReviewStatusOut,
    operation_id="getRunReviewStatus",
    dependencies=[require_role(*_ALL_ROLES)],
)
def get_run_review_status(
    run_id: str,
    svc: Annotated[ReviewStatusService, Depends(get_review_status_service)],
) -> ReviewStatusOut:
    """Return the effective status for the run.

    Falls back to the catalogue default when no explicit row exists —
    the UI uses ``is_default`` to render an "(auto)" hint and skip the
    ``updated_by`` / ``updated_at`` metadata that would otherwise be
    misleading for an unreviewed run.
    """
    try:
        return _record_to_out(svc.get_effective(run_id))
    except Exception as e:
        logger.error("Failed to get review status for run %s: %s", run_id, e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get review status: {e}")


@router.put(
    "/{run_id}/review-status",
    response_model=ReviewStatusOut,
    operation_id="setRunReviewStatus",
    dependencies=[require_role(*_ALL_ROLES)],
)
def set_run_review_status(
    run_id: str,
    body: SetReviewStatusIn,
    svc: Annotated[ReviewStatusService, Depends(get_review_status_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> ReviewStatusOut:
    """Set the review status for a run.

    Records the change in the audit history with the previous effective
    value (virtual default included) so the run detail page can render
    "Pending review → Acknowledged" naturally.
    """
    try:
        user = obo_ws.current_user.me()
        user_email = user.user_name or "unknown"
        record = svc.set_status(run_id, body.status, user_email=user_email)
        return _record_to_out(record)
    except ValueError as e:
        # Service raises ValueError for unknown status / empty inputs.
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to set review status for run %s: %s", run_id, e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to set review status: {e}")


@router.delete(
    "/{run_id}/review-status",
    response_model=ReviewStatusOut,
    operation_id="clearRunReviewStatus",
    dependencies=[require_role(*_ALL_ROLES)],
)
def clear_run_review_status(
    run_id: str,
    svc: Annotated[ReviewStatusService, Depends(get_review_status_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> ReviewStatusOut:
    """Revert the run to the catalogue default.

    Drops the explicit row and appends a history entry. Useful when a
    reviewer wants to "unacknowledge" something without picking another
    explicit value (e.g. the run was re-classified and should go back
    into the unreviewed queue).
    """
    try:
        user = obo_ws.current_user.me()
        user_email = user.user_name or "unknown"
        record = svc.clear_status(run_id, user_email=user_email)
        return _record_to_out(record)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to clear review status for run %s: %s", run_id, e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to clear review status: {e}")


@router.get(
    "/{run_id}/review-status/history",
    response_model=ReviewStatusHistoryOut,
    operation_id="getRunReviewStatusHistory",
    dependencies=[require_role(*_ALL_ROLES)],
)
def get_run_review_status_history(
    run_id: str,
    svc: Annotated[ReviewStatusService, Depends(get_review_status_service)],
) -> ReviewStatusHistoryOut:
    """Return up to 200 most-recent audit rows, newest first."""
    try:
        history = svc.get_history(run_id)
        return ReviewStatusHistoryOut(history=[_history_entry_to_out(h) for h in history])
    except Exception as e:
        logger.error(
            "Failed to load review status history for run %s: %s",
            run_id,
            e,
            exc_info=True,
        )
        raise HTTPException(status_code=500, detail=f"Failed to load review status history: {e}")
