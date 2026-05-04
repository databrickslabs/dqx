from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.dependencies import get_app_settings_service, require_role
from databricks_labs_dqx_app.backend.logger import logger
from pydantic import BaseModel

from databricks_labs_dqx_app.backend.models import (
    ConfigIn,
    ConfigOut,
    RunConfigIn,
    RunConfigOut,
)
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService

_TZ_SETTING_KEY = "display_timezone"
_TZ_DEFAULT = "UTC"


class TimezoneOut(BaseModel):
    timezone: str


class TimezoneIn(BaseModel):
    timezone: str


def _notify_scheduler() -> None:
    """Best-effort reload of the background scheduler after config changes."""
    try:
        from databricks_labs_dqx_app.backend._scheduler_registry import notify_scheduler

        notify_scheduler()
    except Exception:
        pass


router = APIRouter()


@router.get(
    "",
    response_model=ConfigOut,
    operation_id="config",
    dependencies=[require_role(UserRole.ADMIN)],
)
def get_config(
    svc: Annotated[AppSettingsService, Depends(get_app_settings_service)],
) -> ConfigOut:
    """Load workspace config from application state (admin only)."""
    try:
        config = svc.get_config()
        logger.info(f"Loaded config with {len(config.run_configs)} run configs")
        return ConfigOut(config=config)
    except Exception as e:
        logger.error(f"Failed to load config: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to load configuration: {e}")


@router.post(
    "",
    response_model=ConfigOut,
    operation_id="saveConfig",
    dependencies=[require_role(UserRole.ADMIN)],
)
def save_config(
    body: ConfigIn,
    svc: Annotated[AppSettingsService, Depends(get_app_settings_service)],
) -> ConfigOut:
    """Save workspace config to application state (admin only)."""
    try:
        svc.save_config(body.config)
        _notify_scheduler()
        config = svc.get_config()
        return ConfigOut(config=config)
    except Exception as e:
        logger.error(f"Failed to save config: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to save configuration: {e}")


@router.get(
    "/run/{name}",
    response_model=RunConfigOut,
    operation_id="getRunConfig",
    dependencies=[require_role(UserRole.ADMIN)],
)
def get_run_config(
    name: str,
    svc: Annotated[AppSettingsService, Depends(get_app_settings_service)],
) -> RunConfigOut:
    """Get a single run config by name (admin only)."""
    config = svc.get_config()
    for rc in config.run_configs:
        if rc.name == name:
            return RunConfigOut(config=rc)
    raise HTTPException(status_code=404, detail=f"Run config '{name}' not found")


@router.post(
    "/run",
    response_model=RunConfigOut,
    operation_id="saveRunConfig",
    dependencies=[require_role(UserRole.ADMIN)],
)
def save_run_config(
    body: RunConfigIn,
    svc: Annotated[AppSettingsService, Depends(get_app_settings_service)],
) -> RunConfigOut:
    """Save a run config — creates or updates by name (admin only)."""
    config = svc.get_config()
    # Replace existing or append
    updated = False
    for i, rc in enumerate(config.run_configs):
        if rc.name == body.config.name:
            config.run_configs[i] = body.config
            updated = True
            break
    if not updated:
        config.run_configs.append(body.config)

    svc.save_config(config)
    _notify_scheduler()
    return RunConfigOut(config=body.config)


@router.delete(
    "/run/{name}",
    response_model=ConfigOut,
    operation_id="deleteRunConfig",
    dependencies=[require_role(UserRole.ADMIN)],
)
def delete_run_config(
    name: str,
    svc: Annotated[AppSettingsService, Depends(get_app_settings_service)],
) -> ConfigOut:
    """Delete a run config by name (admin only)."""
    config = svc.get_config()
    original_count = len(config.run_configs)
    config.run_configs = [rc for rc in config.run_configs if rc.name != name]

    if len(config.run_configs) == original_count:
        raise HTTPException(status_code=404, detail=f"Run config '{name}' not found")

    svc.save_config(config)
    _notify_scheduler()
    return ConfigOut(config=config)


@router.get(
    "/timezone",
    response_model=TimezoneOut,
    operation_id="getTimezone",
)
def get_timezone(
    svc: Annotated[AppSettingsService, Depends(get_app_settings_service)],
) -> TimezoneOut:
    """Get the display timezone (accessible by all authenticated users)."""
    tz = svc.get_setting(_TZ_SETTING_KEY) or _TZ_DEFAULT
    return TimezoneOut(timezone=tz)


@router.put(
    "/timezone",
    response_model=TimezoneOut,
    operation_id="saveTimezone",
    dependencies=[require_role(UserRole.ADMIN)],
)
def save_timezone(
    body: TimezoneIn,
    svc: Annotated[AppSettingsService, Depends(get_app_settings_service)],
) -> TimezoneOut:
    """Set the display timezone (admin only)."""
    svc.save_setting(_TZ_SETTING_KEY, body.timezone)
    return TimezoneOut(timezone=body.timezone)
