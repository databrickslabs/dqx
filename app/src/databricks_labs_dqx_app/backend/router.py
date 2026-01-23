from typing import Annotated

from databricks.labs.dqx.config_serializer import ConfigSerializer
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceDoesNotExist
from databricks.sdk.service.iam import User as UserOut
from fastapi import APIRouter, Depends, HTTPException, Query

from .config import conf
from .dependencies import get_dqx_engine, get_obo_ws
from .models import (
    ChecksIn,
    ChecksOut,
    ConfigIn,
    ConfigOut,
    InstallationSettings,
    RunConfigIn,
    RunConfigOut,
    VersionOut,
)
from .settings import SettingsManager

api = APIRouter(prefix=conf.api_prefix)


def get_install_folder(ws: WorkspaceClient, path: str | None) -> str:
    folder = path
    if not folder:
        folder = SettingsManager(ws).get_settings().install_folder

    # ConfigSerializer expects a folder path and appends config.yml internally (via underlying blueprint/installation)
    # If the user provided a full file path ending in .yml, we strip it.
    folder = folder.strip()
    if folder.endswith(".yml") or folder.endswith(".yaml"):
         if "/" in folder:
             folder = folder.rsplit("/", 1)[0]

    return folder


@api.get("/version", response_model=VersionOut, operation_id="version")
async def version():
    return VersionOut.from_metadata()


@api.get("/current-user", response_model=UserOut, operation_id="currentUser")
def me(obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)]):
    return obo_ws.current_user.me()


@api.get("/settings", response_model=InstallationSettings, operation_id="get_settings")
def get_settings(obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)]):
    return SettingsManager(obo_ws).get_settings()


@api.post("/settings", response_model=InstallationSettings, operation_id="save_settings")
def save_settings(
    settings: InstallationSettings,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)]
):
    try:
        return SettingsManager(obo_ws).save_settings(settings)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@api.get("/config", response_model=ConfigOut, operation_id="config")
def get_config(
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    path: str | None = Query(None, description="Path to the configuration folder")
) -> ConfigOut:
    # ask user which configuration should be used in the UI
    # there is always just one single configuration config.yml per the whole workspace.
    # and one single config.yml can include multiple run configurations.
    install_folder = get_install_folder(obo_ws, path)
    serializer = ConfigSerializer(obo_ws)
    try:
        return ConfigOut(config=serializer.load_config(install_folder=install_folder))
    except ResourceDoesNotExist:
        raise HTTPException(status_code=404, detail=f"Configuration not found at {install_folder}")


@api.post("/config", response_model=ConfigOut, operation_id="save_config")
def save_config(
    body: ConfigIn,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    path: str | None = Query(None, description="Path to the configuration folder")
) -> ConfigOut:
    install_folder = get_install_folder(obo_ws, path)
    serializer = ConfigSerializer(obo_ws)
    serializer.save_config(body.config, install_folder=install_folder)
    return ConfigOut(config=serializer.load_config(install_folder=install_folder))


@api.get("/config/run/{name}", response_model=RunConfigOut, operation_id="get_run_config")
def get_run_config(
    name: str,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    path: str | None = Query(None, description="Path to the configuration folder")
) -> RunConfigOut:
    # per each run config there is a separate, single unique checks_location.
    # checks_location can be a workspace file path or a table name.
    # it also can be a volume file path.
    # it also can be a delta table name.
    # it also can be a lakebase table name.
    install_folder = get_install_folder(obo_ws, path)
    serializer = ConfigSerializer(obo_ws)
    try:
        return RunConfigOut(config=serializer.load_run_config(run_config_name=name, install_folder=install_folder))
    except ResourceDoesNotExist:
        raise HTTPException(status_code=404, detail=f"Run config '{name}' not found")


@api.post("/config/run", response_model=RunConfigOut, operation_id="save_run_config")
def save_run_config(
    body: RunConfigIn,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    path: str | None = Query(None, description="Path to the configuration folder")
) -> RunConfigOut:
    install_folder = get_install_folder(obo_ws, path)
    serializer = ConfigSerializer(obo_ws)
    serializer.save_run_config(body.config, install_folder=install_folder)
    return RunConfigOut(config=serializer.load_run_config(run_config_name=body.config.name, install_folder=install_folder))


@api.delete("/config/run/{name}", response_model=ConfigOut, operation_id="delete_run_config")
def delete_run_config(
    name: str,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    path: str | None = Query(None, description="Path to the configuration folder")
) -> ConfigOut:
    install_folder = get_install_folder(obo_ws, path)
    serializer = ConfigSerializer(obo_ws)

    try:
        config = serializer.load_config(install_folder=install_folder)
    except ResourceDoesNotExist:
        raise HTTPException(status_code=404, detail=f"Configuration not found at {install_folder}")

    # Filter out the run config with the given name
    original_count = len(config.run_configs)
    config.run_configs = [rc for rc in config.run_configs if rc.name != name]

    if len(config.run_configs) == original_count:
        raise HTTPException(status_code=404, detail=f"Run config '{name}' not found")

    serializer.save_config(config, install_folder=install_folder)
    return ConfigOut(config=config)


@api.get("/config/run/{name}/checks", response_model=ChecksOut, operation_id="get_run_checks")
def get_run_checks(
    name: str,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    engine: Annotated[DQEngine, Depends(get_dqx_engine)],
    path: str | None = Query(None, description="Path to the configuration folder")
) -> ChecksOut:
    install_folder = get_install_folder(obo_ws, path)
    serializer = ConfigSerializer(obo_ws)
    try:
        run_config = serializer.load_run_config(run_config_name=name, install_folder=install_folder)
    except ResourceDoesNotExist:
        raise HTTPException(status_code=404, detail=f"Run config '{name}' not found")

    # Use the factory to get the handler and storage config
    # Accessing protected member _checks_handler_factory as it seems to be the way to get the storage config
    # properly constructed from the RunConfig
    try:
        _, storage_config = engine._checks_handler_factory.create_for_run_config(run_config)
        checks = engine.load_checks(storage_config)
        return ChecksOut(checks=checks)
    except Exception as e:
        # Checks might not exist yet, or other issues
        # If it's just missing, maybe return empty list?
        # But for now let's propagate or return empty if we want to allow empty checks
        # If checks are missing, engine might raise.
        # But let's assume engine handles missing checks by returning empty list or raising.
        # If it raises ResourceDoesNotExist, we might want 404.
        raise HTTPException(status_code=500, detail=str(e))


@api.post("/config/run/{name}/checks", response_model=ChecksOut, operation_id="save_run_checks")
def save_run_checks(
    name: str,
    body: ChecksIn,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    engine: Annotated[DQEngine, Depends(get_dqx_engine)],
    path: str | None = Query(None, description="Path to the configuration folder")
) -> ChecksOut:
    install_folder = get_install_folder(obo_ws, path)
    serializer = ConfigSerializer(obo_ws)
    try:
        run_config = serializer.load_run_config(run_config_name=name, install_folder=install_folder)
    except ResourceDoesNotExist:
        raise HTTPException(status_code=404, detail=f"Run config '{name}' not found")

    _, storage_config = engine._checks_handler_factory.create_for_run_config(run_config)

    engine.save_checks(body.checks, storage_config)
    return ChecksOut(checks=body.checks)
