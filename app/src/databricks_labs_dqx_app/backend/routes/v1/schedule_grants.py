"""Schedule-grant preflight route (Task 12).

The schedule editor calls this when it opens to learn, per table, whether the
caller can grant SELECT to the scheduler service principals. When they cannot,
the UI hard-blocks the save and shows the users/groups that hold MANAGE. The
save-time endpoints enforce the same gate server-side (backend is the source of
truth) — this endpoint is purely the read-side check so the UI can pre-block.

``POST /apply`` runs the catalog and schema grants the preflight lists, as the
signed-in user, when they explicitly ask for it. Saving a schedule never does.
"""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.dependencies import (
    get_obo_sql_executor,
    get_schedule_grant_service,
    require_role,
)
from databricks_labs_dqx_app.backend.logger import logger
from databricks_labs_dqx_app.backend.models import (
    ManageHolderOut,
    ScheduleGrantApplyIn,
    ScheduleGrantApplyOut,
    ScheduleGrantFailureOut,
    ScheduleMissingGrantOut,
    SchedulePreflightIn,
    SchedulePreflightOut,
    SchedulePreflightTableOut,
)
from databricks_labs_dqx_app.backend.services.schedule_grant_service import (
    ScheduleGrantService,
    TablePreflight,
    WarehouseUnavailableError,
    inspection_failure_detail,
)
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor

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
    except WarehouseUnavailableError as e:
        logger.warning("Schedule-grant preflight inconclusive: %s", e)
        raise HTTPException(status_code=503, detail=inspection_failure_detail(e))
    except Exception as e:
        logger.error("Schedule-grant preflight failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to check schedule permissions.")
    return _preflight_out(results)


@router.post(
    "/apply",
    response_model=ScheduleGrantApplyOut,
    operation_id="applyScheduleGrants",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
async def apply_schedule_grants(
    body: ScheduleGrantApplyIn,
    grant_svc: Annotated[ScheduleGrantService, Depends(get_schedule_grant_service)],
    obo_sql: Annotated[SqlExecutor, Depends(get_obo_sql_executor)],
) -> ScheduleGrantApplyOut:
    """Run the missing scheduler grants for these tables as the signed-in user, then re-check.

    The request carries only table names; the statements come from the server's own
    preflight and run under the caller's credentials, never the app service principal.
    """
    try:
        result = await grant_svc.apply_missing_grants_async(body.table_fqns, obo_sql)
    except WarehouseUnavailableError as e:
        logger.warning(f"Schedule grants not applied: access check inconclusive ({type(e).__name__})")
        raise HTTPException(status_code=503, detail=inspection_failure_detail(e))
    except Exception as e:
        logger.error(f"Applying schedule grants failed ({type(e).__name__})")
        raise HTTPException(status_code=500, detail="Failed to give the scheduler access.")
    return ScheduleGrantApplyOut(
        preflight=_preflight_out(result.preflight),
        applied=result.applied,
        total=result.total,
        failed=[
            ScheduleGrantFailureOut(fqn=f.fqn, statement=f.statement, error_code=f.error_code) for f in result.failed
        ],
        recheck_failed=result.recheck_failed,
    )


def _preflight_out(results: list[TablePreflight]) -> SchedulePreflightOut:
    """Convert per-table preflight results to the API response."""
    return SchedulePreflightOut(
        tables=[
            SchedulePreflightTableOut(
                fqn=r.fqn,
                can_manage=r.can_manage,
                manage_holders=[
                    ManageHolderOut(principal=h["principal"], type=h["type"], display_name=h.get("display_name"))
                    for h in r.manage_holders
                ],
                missing_grants=[
                    ScheduleMissingGrantOut(
                        principal=g["principal"],
                        display_name=g["display_name"],
                        privilege=g["privilege"],
                        securable_type=g["securable_type"],
                        securable=g["securable"],
                        statement=g["remediation"],
                    )
                    for g in r.missing_grants
                ],
                access_unverified=r.access_unverified,
                unverified_reason=r.unverified_reason or None,
            )
            for r in results
        ]
    )
