"""Pre-migration setup readiness and bootstrap reconciliation APIs."""

import asyncio
import json
import logging
from typing import Annotated

from databricks.labs.dqx.errors import InvalidParameterError
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, PermissionDenied
from databricks.sdk.errors.base import DatabricksError
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

logger = logging.getLogger(__name__)

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
    """Whether the caller can see *catalog*; unexpected SDK failures raise HTTP 502."""
    try:
        reader_ws.catalogs.get(catalog)
    except (NotFound, PermissionDenied):
        return False
    except DatabricksError as error:
        logger.warning(f"Catalog existence check failed: {type(error).__name__}")
        raise _error(status.HTTP_502_BAD_GATEWAY, "catalog_check_failed") from None
    return True


def _group_exists(sp_ws: WorkspaceClient, group: str) -> bool:
    """Whether a workspace group named *group* exists; unexpected SDK failures raise HTTP 502."""
    try:
        matches = list(
            sp_ws.groups.list(
                filter=f"displayName eq {json.dumps(group, ensure_ascii=False)}", attributes="id,displayName"
            )
        )
    except NotFound:
        return False
    except DatabricksError as error:
        logger.warning(f"Group existence check failed: {type(error).__name__}")
        raise _error(status.HTTP_502_BAD_GATEWAY, "group_check_failed") from None
    return any((match.display_name or "").casefold() == group.casefold() for match in matches)


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
    if config.has_deployment_storage:
        raise _error(status.HTTP_409_CONFLICT, "configuration_managed_by_deployment")
    try:
        storage, audience = validate_choices(
            SetupChoices(catalog=body.catalog, prefix=body.prefix, audience_group=body.audience_group),
            config.admin_group,
        )
    except InvalidParameterError as error:
        raise _error(status.HTTP_422_UNPROCESSABLE_CONTENT, "configuration_invalid", str(error)) from None
    choices = SetupChoices(catalog=storage.catalog, prefix=storage.schema, audience_group=audience.groups[0])
    if not await asyncio.to_thread(_catalog_exists, reader_ws, choices.catalog):
        raise _error(status.HTTP_422_UNPROCESSABLE_CONTENT, "catalog_not_found")
    if not await asyncio.to_thread(_group_exists, sp_ws, choices.audience_group):
        raise _error(status.HTTP_422_UNPROCESSABLE_CONTENT, "audience_group_not_found")
    outcome = await orchestrator.save_configuration(store, choices, user_email=access.user_name)
    if outcome == "locked":
        raise _error(status.HTTP_409_CONFLICT, "configuration_locked")
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
