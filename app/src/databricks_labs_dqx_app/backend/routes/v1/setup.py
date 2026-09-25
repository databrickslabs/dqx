"""Pre-migration setup readiness and bootstrap reconciliation APIs."""

from typing import Annotated

from fastapi import APIRouter, Depends

from databricks_labs_dqx_app.backend.config import AppConfig
from databricks_labs_dqx_app.backend.dependencies import (
    SetupAccess,
    get_conf,
    get_setup_access,
    get_setup_orchestrator,
    require_setup_admin,
    sanitize_setup_display,
)
from databricks_labs_dqx_app.backend.setup.models import SetupReport, SetupStatusResponse
from databricks_labs_dqx_app.backend.setup.orchestrator import SetupOrchestrator
from databricks_labs_dqx_app.backend.setup.runtime import setup_runtime

router = APIRouter()


@router.get("/status", response_model=SetupStatusResponse, operation_id="getSetupStatus")
async def get_setup_status(
    access: Annotated[SetupAccess, Depends(get_setup_access)],
    config: Annotated[AppConfig, Depends(get_conf)],
) -> SetupStatusResponse:
    """Return readiness and the caller's bootstrap setup-management access."""
    return SetupStatusResponse(
        report=setup_runtime.report(),
        can_manage=access.can_manage,
        admin_group=sanitize_setup_display(config.admin_group) or "",
    )


@router.post("/reconcile", response_model=SetupReport, operation_id="reconcileSetup")
async def reconcile_setup(
    access: Annotated[SetupAccess, require_setup_admin()],
    orchestrator: Annotated[SetupOrchestrator, Depends(get_setup_orchestrator)],
) -> SetupReport:
    """Run the serialized setup workflow as a bootstrap administrator."""
    return await orchestrator.reconcile(setup_user=access.user_name)
