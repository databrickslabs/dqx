from typing import Annotated

from databricks.sdk import WorkspaceClient
from fastapi import APIRouter, Depends, HTTPException

from databricks_labs_dqx_app.backend.dependencies import get_obo_ws
from databricks_labs_dqx_app.backend.models import InstallationSettings
from databricks_labs_dqx_app.backend.settings import SettingsManager

# No role guard: SettingsManager operates only on the caller's own
# /Users/{user}/.dqx/app.yml, and OBO already enforces user-level isolation.
router = APIRouter()


@router.get("", response_model=InstallationSettings, operation_id="get_settings")
def get_settings(obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)]):
    return SettingsManager(obo_ws).get_settings()


@router.post("", response_model=InstallationSettings, operation_id="save_settings")
def save_settings(settings: InstallationSettings, obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)]):
    try:
        return SettingsManager(obo_ws).save_settings(settings)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
