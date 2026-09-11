import asyncio
from typing import Annotated

from databricks.sdk import WorkspaceClient
from fastapi import APIRouter, Depends, HTTPException

from databricks_labs_dqx_app.backend.common.authorization import CAN_RUN_ROLES, UserRole
from databricks_labs_dqx_app.backend.dependencies import (
    get_obo_ws,
    get_schedule_config_service,
    get_schedule_grant_service,
    require_role,
)
from databricks_labs_dqx_app.backend.logger import logger
from databricks_labs_dqx_app.backend.models import (
    ScheduleConfigHistoryOut,
    ScheduleConfigIn,
    ScheduleConfigOut,
)
from databricks_labs_dqx_app.backend.services.schedule_config_service import (
    ScheduleConfigEntry,
    ScheduleConfigService,
)
from databricks_labs_dqx_app.backend.services.schedule_grant_service import (
    CannotManageError,
    ScheduleGrantService,
    manage_block_detail,
)

router = APIRouter()

_ADMINS = [UserRole.ADMIN]

# Bound fan-out for the per-table grant enforcement. ``scope_mode='all'`` can
# resolve to hundreds of approved-rule tables; each table costs a handful of
# blocking SDK round-trips. Running them under a bounded semaphore keeps the
# save well within the client timeout without flooding the workspace API.
_GRANT_CONCURRENCY = 8

# Schedule listing/reading is gated on CAN_RUN_ROLES: the schedules tab
# lives inside the Run Rules page, which only ADMIN and RULE_AUTHOR may
# see. Mutation endpoints stay admin-only.


def _schedule_is_enabled(config: dict) -> bool:
    """Whether a scope-config schedule is active (would fire runs).

    Saving a schedule enables it by default; only an explicit ``enabled: false``
    or ``paused: true`` in the config marks it dormant, in which case no
    source-table access is needed yet.
    """
    return bool(config.get("enabled", True)) and not bool(config.get("paused", False))


async def _enforce_scheduler_grants(
    config: dict,
    svc: ScheduleConfigService,
    grant_svc: ScheduleGrantService,
) -> None:
    """Gate + grant scheduler SELECT on a scope-config schedule's target tables.

    Hard-blocks (403) when the caller lacks MANAGE on any resolved table; grants
    (idempotently) to the scheduler SPs otherwise. A no-op for dormant schedules
    or scopes that resolve to no tables.

    Every in-scope table is MANAGE-gated **and** (re-)granted on every save —
    re-saving is how a revoked or previously-failed grant is re-detected and
    re-applied, so coverage is never trimmed to a delta. The work is made fast,
    not smaller: caller/SP identities are resolved once per request (memoized on
    *grant_svc*) and the per-table gate + grant round-trips run concurrently
    under a bounded semaphore. MANAGE is checked exactly once per table — the
    grant path trusts the gate's decision.
    """
    if not _schedule_is_enabled(config):
        return
    target_fqns = await asyncio.to_thread(svc.resolve_scope_table_fqns, config)
    if not target_fqns:
        return

    sem = asyncio.Semaphore(_GRANT_CONCURRENCY)

    # Resolve the caller identity once, up front, so the concurrent gate checks
    # below read it from cache instead of each re-issuing current_user.me().
    await asyncio.to_thread(grant_svc.prime_caller_identity)

    async def _gate(fqn: str) -> tuple[str, bool]:
        async with sem:
            return fqn, await grant_svc.user_can_manage_async(fqn)

    gate_results = await asyncio.gather(*(_gate(fqn) for fqn in target_fqns))
    manageable = [fqn for fqn, can_manage in gate_results if can_manage]
    blocked_fqns = [fqn for fqn, can_manage in gate_results if not can_manage]

    if blocked_fqns:
        # Enumerate MANAGE holders for every blocked table (concurrently) so the
        # 403 lists all of them, not just the first.
        async def _holders(fqn: str) -> tuple[str, list[dict[str, str]]]:
            async with sem:
                return fqn, await grant_svc.manage_holders_async(fqn)

        blocked = list(await asyncio.gather(*(_holders(fqn) for fqn in blocked_fqns)))
        raise HTTPException(status_code=403, detail=manage_block_detail(blocked))

    # All tables are MANAGE-gated; grant SELECT to the scheduler SPs concurrently,
    # skipping the (now-redundant) per-table MANAGE re-check.
    await asyncio.to_thread(grant_svc.prime_scheduler_sp_identities)

    async def _grant(fqn: str) -> None:
        async with sem:
            await grant_svc.grant_select_precleared_async(fqn)

    try:
        await asyncio.gather(*(_grant(fqn) for fqn in manageable))
    except CannotManageError as e:
        raise HTTPException(status_code=403, detail=manage_block_detail([(e.fqn, e.manage_holders)]))
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to grant scheduler access for schedule scope: %s", e, exc_info=True)
        raise HTTPException(
            status_code=502,
            detail="Could not grant the scheduler read access to the scheduled tables. Please try again.",
        )


def _save_schedule_entry(
    obo_ws: WorkspaceClient,
    svc: ScheduleConfigService,
    body: ScheduleConfigIn,
) -> ScheduleConfigEntry:
    """Persist the schedule row (blocking SDK + OLTP calls); run off the event loop."""
    user = obo_ws.current_user.me()
    user_email = user.user_name or "unknown"
    return svc.save(body.schedule_name, body.config, user_email)


def _notify_scheduler() -> None:
    try:
        from databricks_labs_dqx_app.backend._scheduler_registry import notify_scheduler

        notify_scheduler()
    except Exception:
        pass


@router.get(
    "",
    response_model=list[ScheduleConfigOut],
    operation_id="listSchedules",
    dependencies=[require_role(*CAN_RUN_ROLES)],
)
def list_schedules(
    svc: Annotated[ScheduleConfigService, Depends(get_schedule_config_service)],
) -> list[ScheduleConfigOut]:
    """List all schedule configurations."""
    try:
        entries = svc.list_schedules()
        return [
            ScheduleConfigOut(
                schedule_name=e.schedule_name,
                config=e.config,
                version=e.version,
                created_by=e.created_by,
                created_at=e.created_at,
                updated_by=e.updated_by,
                updated_at=e.updated_at,
            )
            for e in entries
        ]
    except Exception as e:
        logger.error("Failed to list schedules: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to list schedules: {e}")


@router.get(
    "/{name}",
    response_model=ScheduleConfigOut,
    operation_id="getSchedule",
    dependencies=[require_role(*CAN_RUN_ROLES)],
)
def get_schedule(
    name: str,
    svc: Annotated[ScheduleConfigService, Depends(get_schedule_config_service)],
) -> ScheduleConfigOut:
    """Get a single schedule configuration by name."""
    entry = svc.get(name)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"Schedule '{name}' not found")
    return ScheduleConfigOut(
        schedule_name=entry.schedule_name,
        config=entry.config,
        version=entry.version,
        created_by=entry.created_by,
        created_at=entry.created_at,
        updated_by=entry.updated_by,
        updated_at=entry.updated_at,
    )


@router.post(
    "",
    response_model=ScheduleConfigOut,
    operation_id="saveSchedule",
    dependencies=[require_role(*_ADMINS)],
)
async def save_schedule(
    body: ScheduleConfigIn,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    svc: Annotated[ScheduleConfigService, Depends(get_schedule_config_service)],
    grant_svc: Annotated[ScheduleGrantService, Depends(get_schedule_grant_service)],
) -> ScheduleConfigOut:
    """Create or update a schedule configuration.

    When the schedule is enabled, the caller must be able to grant the scheduler
    service principals SELECT on every table the schedule's scope resolves to —
    scheduled runs read those tables as the SPs, without an OBO token. Tables the
    caller can grant on are granted (idempotently) before saving; if they lack
    MANAGE on any, the save is hard-blocked (403) naming the blocked tables and,
    for each, the users/groups that hold MANAGE (Task 12).
    """
    await _enforce_scheduler_grants(body.config, svc, grant_svc)
    try:
        entry = await asyncio.to_thread(_save_schedule_entry, obo_ws, svc, body)
        _notify_scheduler()
        return ScheduleConfigOut(
            schedule_name=entry.schedule_name,
            config=entry.config,
            version=entry.version,
            created_by=entry.created_by,
            created_at=entry.created_at,
            updated_by=entry.updated_by,
            updated_at=entry.updated_at,
        )
    except Exception as e:
        logger.error("Failed to save schedule %s: %s", body.schedule_name, e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to save schedule: {e}")


@router.delete(
    "/{name}",
    response_model=dict[str, str],
    operation_id="deleteSchedule",
    dependencies=[require_role(*_ADMINS)],
)
def delete_schedule(
    name: str,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    svc: Annotated[ScheduleConfigService, Depends(get_schedule_config_service)],
) -> dict[str, str]:
    """Delete a schedule configuration by name."""
    try:
        user = obo_ws.current_user.me()
        user_email = user.user_name or "unknown"
        svc.delete(name, user_email)
        _notify_scheduler()
        return {"deleted": name}
    except Exception as e:
        logger.error("Failed to delete schedule %s: %s", name, e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to delete schedule: {e}")


@router.get(
    "/{name}/history",
    response_model=list[ScheduleConfigHistoryOut],
    operation_id="getScheduleHistory",
    dependencies=[require_role(*CAN_RUN_ROLES)],
)
def get_schedule_history(
    name: str,
    svc: Annotated[ScheduleConfigService, Depends(get_schedule_config_service)],
) -> list[ScheduleConfigHistoryOut]:
    """Get the change history for a schedule configuration."""
    try:
        history = svc.get_history(name)
        return [
            ScheduleConfigHistoryOut(
                schedule_name=h["schedule_name"],
                config=h["config"],
                version=h["version"],
                action=h["action"],
                changed_by=h["changed_by"],
                changed_at=h["changed_at"],
            )
            for h in history
        ]
    except Exception as e:
        logger.error("Failed to get history for schedule %s: %s", name, e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get schedule history: {e}")
