"""Schedule-grant preflight route (Task 12).

The schedule editor calls this when it opens to learn, per table, whether the
caller can grant SELECT to the scheduler service principals. When they cannot,
the UI hard-blocks the save and shows the users/groups that hold MANAGE. The
save-time endpoints enforce the same gate server-side (backend is the source of
truth) — this endpoint is purely the read-side check so the UI can pre-block.
"""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.dependencies import (
    get_schedule_grant_service,
    require_role,
)
from databricks_labs_dqx_app.backend.logger import logger
from databricks_labs_dqx_app.backend.models import (
    ManageHolderOut,
    SchedulePreflightIn,
    SchedulePreflightOut,
    SchedulePreflightTableOut,
)
from databricks_labs_dqx_app.backend.services.schedule_grant_service import ScheduleGrantService

router = APIRouter()

# Setting up a schedule is an authoring action, so the preflight matches the
# roles that may edit a schedule (authors, approvers, admins).
_AUTHORS_AND_ABOVE = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR]


@router.post(
    "/preflight",
    response_model=SchedulePreflightOut,
    operation_id="preflightScheduleGrants",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
async def preflight_schedule_grants(
    body: SchedulePreflightIn,
    grant_svc: Annotated[ScheduleGrantService, Depends(get_schedule_grant_service)],
) -> SchedulePreflightOut:
    """Return per-table grantability for the tables a schedule will run against."""
    try:
        results = await grant_svc.preflight_async(body.table_fqns)
    except Exception as e:
        logger.error("Schedule-grant preflight failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to check schedule permissions.")
    return SchedulePreflightOut(
        tables=[
            SchedulePreflightTableOut(
                fqn=r.fqn,
                can_manage=r.can_manage,
                manage_holders=[ManageHolderOut(principal=h["principal"], type=h["type"]) for h in r.manage_holders],
            )
            for r in results
        ]
    )
