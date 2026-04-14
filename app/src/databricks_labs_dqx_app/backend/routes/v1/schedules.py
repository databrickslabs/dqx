from __future__ import annotations

from typing import Annotated

from databricks.sdk import WorkspaceClient
from fastapi import APIRouter, Depends, HTTPException

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.dependencies import (
    get_obo_ws,
    get_schedule_config_service,
    require_role,
)
from databricks_labs_dqx_app.backend.logger import logger
from databricks_labs_dqx_app.backend.models import (
    ScheduleConfigHistoryOut,
    ScheduleConfigIn,
    ScheduleConfigOut,
)
from databricks_labs_dqx_app.backend.services.schedule_config_service import ScheduleConfigService

router = APIRouter()

_ADMINS = [UserRole.ADMIN]


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
    dependencies=[require_role(*_ADMINS)],
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
    dependencies=[require_role(*_ADMINS)],
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
def save_schedule(
    body: ScheduleConfigIn,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    svc: Annotated[ScheduleConfigService, Depends(get_schedule_config_service)],
) -> ScheduleConfigOut:
    """Create or update a schedule configuration."""
    try:
        user = obo_ws.current_user.me()
        user_email = user.user_name or "unknown"
        entry = svc.save(body.schedule_name, body.config, user_email)
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
    existing = svc.get(name)
    if existing is None:
        raise HTTPException(status_code=404, detail=f"Schedule '{name}' not found")
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
    dependencies=[require_role(*_ADMINS)],
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
