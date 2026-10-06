"""Pre-migration setup readiness and bootstrap reconciliation APIs."""

import asyncio
import json
from typing import Annotated

from databricks.labs.dqx.errors import InvalidParameterError
from databricks.sdk import WorkspaceClient
from fastapi import APIRouter, Depends, HTTPException, Request, status

from databricks_labs_dqx_app.backend.config import AppConfig
from databricks_labs_dqx_app.backend.dependencies import (
    SetupAccess,
    get_conf,
    get_obo_ws,
    get_setup_access,
    get_setup_configuration_store,
    get_setup_orchestrator,
    get_sp_ws,
    get_optional_setup_sql_executor,
    require_setup_admin,
    sanitize_setup_display,
)
from databricks_labs_dqx_app.backend.setup.configuration import SetupChoices, SetupConfigurationStore, validate_choices
from databricks_labs_dqx_app.backend.setup.models import SetupConfigurationRequest, SetupReport, SetupStatusResponse
from databricks_labs_dqx_app.backend.setup.orchestrator import SetupOrchestrator
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor
from databricks_labs_dqx_app.backend.setup.runtime import setup_runtime

router = APIRouter()


@router.get("/status", response_model=SetupStatusResponse, operation_id="getSetupStatus")
async def get_setup_status(
    access: Annotated[SetupAccess, Depends(get_setup_access)],
    config: Annotated[AppConfig, Depends(get_conf)],
    request: Request,
) -> SetupStatusResponse:
    """Return readiness, the caller's bootstrap setup access and the resolved configuration."""
    orchestrator = getattr(request.app.state, "setup_orchestrator", None)
    return SetupStatusResponse(
        report=setup_runtime.report(),
        can_manage=access.can_manage,
        admin_group=sanitize_setup_display(config.admin_group) or "",
        configuration=orchestrator.configuration_view() if orchestrator is not None else None,
    )


def _error(status_code: int, code: str, detail: str | None = None) -> HTTPException:
    body = {"code": code} if detail is None else {"code": code, "detail": detail}
    return HTTPException(status_code=status_code, detail=body)


def _catalog_exists(reader_ws: WorkspaceClient, catalog: str) -> bool:
    try:
        reader_ws.catalogs.get(catalog)
    except Exception:
        return False
    return True


def _group_exists(sp_ws: WorkspaceClient, group: str) -> bool:
    try:
        matches = sp_ws.groups.list(filter=f"displayName eq {json.dumps(group)}", attributes="id,displayName")
        return any((match.display_name or "").casefold() == group.casefold() for match in matches)
    except Exception:
        return False


@router.post("/configuration", response_model=SetupReport, operation_id="configureSetup")
async def configure_setup(
    body: SetupConfigurationRequest,
    access: Annotated[SetupAccess, require_setup_admin()],
    orchestrator: Annotated[SetupOrchestrator, Depends(get_setup_orchestrator)],
    store: Annotated[SetupConfigurationStore, Depends(get_setup_configuration_store)],
    config: Annotated[AppConfig, Depends(get_conf)],
    reader_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    sp_ws: Annotated[WorkspaceClient, Depends(get_sp_ws)],
) -> SetupReport:
    """Validate and save catalog, prefix and audience group, then run the setup workflow."""
    if orchestrator.configuration_view().source == "deployment":
        raise _error(status.HTTP_409_CONFLICT, "configuration_managed_by_deployment")
    choices = SetupChoices(catalog=body.catalog, prefix=body.prefix, audience_group=body.audience_group)
    locked, saved = await asyncio.to_thread(lambda: (store.is_locked(), store.load()))
    if locked and saved != choices:
        raise _error(status.HTTP_409_CONFLICT, "configuration_locked")
    if not (locked and saved == choices):
        try:
            validate_choices(choices, config.admin_group)
        except InvalidParameterError as error:
            raise _error(status.HTTP_422_UNPROCESSABLE_CONTENT, "configuration_invalid", str(error)) from None
        if not await asyncio.to_thread(_catalog_exists, reader_ws, choices.catalog):
            raise _error(status.HTTP_422_UNPROCESSABLE_CONTENT, "catalog_not_found")
        if not await asyncio.to_thread(_group_exists, sp_ws, choices.audience_group):
            raise _error(status.HTTP_422_UNPROCESSABLE_CONTENT, "audience_group_not_found")
        await asyncio.to_thread(lambda: store.save(choices, user_email=access.user_name))
    return await orchestrator.reconcile(setup_user=access.user_name, reader_ws=reader_ws, reader_sql=None)


@router.post("/reconcile", response_model=SetupReport, operation_id="reconcileSetup")
async def reconcile_setup(
    access: Annotated[SetupAccess, require_setup_admin()],
    orchestrator: Annotated[SetupOrchestrator, Depends(get_setup_orchestrator)],
    reader_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    reader_sql: Annotated[SqlExecutor | None, Depends(get_optional_setup_sql_executor)],
) -> SetupReport:
    """Run the serialized setup workflow as a bootstrap administrator."""
    return await orchestrator.reconcile(setup_user=access.user_name, reader_ws=reader_ws, reader_sql=reader_sql)
