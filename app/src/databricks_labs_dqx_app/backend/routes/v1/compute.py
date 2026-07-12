"""Compute settings routes (P22-B) — SQL warehouse + jobs-compute pickers and
the app-SP warehouse-access check + one-click grant.

Ports dqlake's Settings "jobs" section: an admin picks the SQL warehouse used
for app-side ad-hoc SQL (the View Data tab, discovery-style reads) and the jobs
compute used by the task-runner submission path.

**Listing** the available warehouses / clusters runs under the acting user's
On-Behalf-Of client (``get_obo_ws``), so the pickers reflect exactly what that
user can see rather than the app SP's broader visibility.

**SQL warehouse** is respected end-to-end. :func:`resolve_warehouse_id` reads
this setting (env fallback) wherever the app builds an ad-hoc SqlExecutor — the
View Data preview executor honours it (``dependencies.get_preview_sql_executor``)
— and it is now also threaded into the task-runner job: ``get_job_service``
resolves the configured warehouse and ``JobService.submit_run`` passes it as the
``warehouse_id`` job parameter, which ``databricks.yml`` declares and forwards to
the wheel task's ``--warehouse_id`` arg, where the runner uses it for its
temp-view cleanup path.

**Jobs compute** is persisted, surfaced, and covered by the app-SP access check,
but is NOT applied at submission time — mirroring dqlake, which likewise only
records the selection. The task runner runs on its bundle-defined (serverless)
environment: ``jobs.run_now`` against a pre-defined job takes only
``job_parameters`` and has no per-run compute-override seam, so honouring an
``existing_cluster`` selection would require redefining the job's compute rather
than a run-time override. Left as a documented follow-up.

**Access check + grant** (task 8): the check self-inspects with the app SP and
falls back to the admin's OBO client to read the warehouse ACL; the grant is
applied with the admin's OBO client (the app SP usually can't CAN_MANAGE a
warehouse it doesn't own). All routes are ADMIN-gated.
"""

from __future__ import annotations

from typing import Annotated, Literal

from databricks.sdk import WorkspaceClient
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from databricks_labs_dqx_app.backend.common.authorization import UserRole, get_user_email
from databricks_labs_dqx_app.backend.dependencies import (
    get_app_settings_service,
    get_compute_service,
    get_obo_ws,
    require_role,
)
from databricks_labs_dqx_app.backend.logger import logger
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.compute_service import ComputeService, resolve_warehouse_id

router = APIRouter()


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class WarehouseOut(BaseModel):
    id: str
    name: str
    serverless: bool
    running: bool


class ClusterOut(BaseModel):
    cluster_id: str
    cluster_name: str
    state: str


class JobsComputeModel(BaseModel):
    """The jobs-compute selection. ``existing_cluster`` carries a ``cluster_id``."""

    kind: Literal["serverless", "existing_cluster"] = "serverless"
    cluster_id: str | None = None


class ComputeSettingsOut(BaseModel):
    sql_warehouse_id: str = Field(default="", description="Configured warehouse id, '' when unset (env fallback).")
    effective_warehouse_id: str = Field(default="", description="Warehouse actually used after env fallback.")
    warehouse_is_override: bool = Field(default=False, description="True when an admin override is set.")
    jobs_compute: JobsComputeModel = Field(default_factory=JobsComputeModel)


class ComputeSettingsIn(BaseModel):
    """Update payload — omitted fields are left unchanged."""

    sql_warehouse_id: str | None = None
    jobs_compute: JobsComputeModel | None = None


class WarehouseAccessOut(BaseModel):
    status: Literal["granted", "missing", "unknown"]
    warehouse_id: str
    sp_application_id: str = ""
    can_grant: bool = True


class GrantWarehouseAccessIn(BaseModel):
    warehouse_id: str


# ---------------------------------------------------------------------------
# Listing
# ---------------------------------------------------------------------------


@router.get(
    "/warehouses",
    response_model=list[WarehouseOut],
    operation_id="listComputeWarehouses",
    dependencies=[require_role(UserRole.ADMIN)],
)
async def list_warehouses(
    svc: Annotated[ComputeService, Depends(get_compute_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> list[WarehouseOut]:
    """List the SQL warehouses the acting user can see (OBO), or ``[]`` on failure."""
    try:
        warehouses = await svc.list_warehouses_async(lister_ws=obo_ws)
    except Exception:
        logger.warning("Failed to list SQL warehouses", exc_info=True)
        return []
    return [WarehouseOut(id=w.id, name=w.name, serverless=w.serverless, running=w.running) for w in warehouses]


@router.get(
    "/clusters",
    response_model=list[ClusterOut],
    operation_id="listComputeClusters",
    dependencies=[require_role(UserRole.ADMIN)],
)
async def list_clusters(
    svc: Annotated[ComputeService, Depends(get_compute_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> list[ClusterOut]:
    """List the all-purpose clusters the acting user can see (OBO), or ``[]`` on failure."""
    try:
        clusters = await svc.list_clusters_async(lister_ws=obo_ws)
    except Exception:
        logger.warning("Failed to list clusters", exc_info=True)
        return []
    return [ClusterOut(cluster_id=c.cluster_id, cluster_name=c.cluster_name, state=c.state) for c in clusters]


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------


def _settings_out(app_settings: AppSettingsService) -> ComputeSettingsOut:
    configured = app_settings.get_sql_warehouse_id()
    jobs = app_settings.get_jobs_compute()
    return ComputeSettingsOut(
        sql_warehouse_id=configured or "",
        effective_warehouse_id=resolve_warehouse_id(app_settings),
        warehouse_is_override=bool(configured),
        jobs_compute=JobsComputeModel(**jobs),
    )


@router.get(
    "/settings",
    response_model=ComputeSettingsOut,
    operation_id="getComputeSettings",
    dependencies=[require_role(UserRole.ADMIN)],
)
def get_compute_settings(
    app_settings: Annotated[AppSettingsService, Depends(get_app_settings_service)],
) -> ComputeSettingsOut:
    """Return the current compute settings + the effective warehouse (admin only)."""
    return _settings_out(app_settings)


@router.put(
    "/settings",
    response_model=ComputeSettingsOut,
    operation_id="saveComputeSettings",
    dependencies=[require_role(UserRole.ADMIN)],
)
def save_compute_settings(
    body: ComputeSettingsIn,
    app_settings: Annotated[AppSettingsService, Depends(get_app_settings_service)],
    email: Annotated[str, Depends(get_user_email)],
) -> ComputeSettingsOut:
    """Update one or both compute settings (admin only)."""
    if body.sql_warehouse_id is None and body.jobs_compute is None:
        raise HTTPException(status_code=400, detail="At least one of sql_warehouse_id or jobs_compute must be provided.")
    if body.sql_warehouse_id is not None:
        app_settings.save_sql_warehouse_id(body.sql_warehouse_id, user_email=email)
    if body.jobs_compute is not None:
        app_settings.save_jobs_compute(body.jobs_compute.model_dump(), user_email=email)
    logger.info("Saved compute settings (by=%s)", email)
    return _settings_out(app_settings)


# ---------------------------------------------------------------------------
# App-SP warehouse access check + grant (task 8)
# ---------------------------------------------------------------------------


@router.get(
    "/warehouse-access",
    response_model=WarehouseAccessOut,
    operation_id="getWarehouseAccess",
    dependencies=[require_role(UserRole.ADMIN)],
)
async def get_warehouse_access(
    svc: Annotated[ComputeService, Depends(get_compute_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    warehouse_id: Annotated[str, Query(description="Warehouse id to check")],
) -> WarehouseAccessOut:
    """Check whether the app SP has CAN_USE on *warehouse_id*.

    Never raises for the access-read itself — returns ``"unknown"`` when the ACL
    can't be read so the UI shows no false warning.
    """
    wid = (warehouse_id or "").strip()
    if not wid:
        raise HTTPException(status_code=400, detail="warehouse_id is required.")
    status = await svc.warehouse_access_status_async(wid, reader_ws=obo_ws)
    return WarehouseAccessOut(status=status, warehouse_id=wid, sp_application_id=svc.sp_application_id())


@router.post(
    "/warehouse-access/grant",
    response_model=WarehouseAccessOut,
    operation_id="grantWarehouseAccess",
    dependencies=[require_role(UserRole.ADMIN)],
)
async def grant_warehouse_access(
    body: GrantWarehouseAccessIn,
    svc: Annotated[ComputeService, Depends(get_compute_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> WarehouseAccessOut:
    """Grant the app SP CAN_USE on the warehouse via the admin's OBO client."""
    wid = (body.warehouse_id or "").strip()
    if not wid:
        raise HTTPException(status_code=400, detail="warehouse_id is required.")
    try:
        await svc.grant_warehouse_can_use_async(wid, grantor_ws=obo_ws)
    except Exception as e:
        logger.warning("Failed to grant warehouse access on %s: %s", wid, e, exc_info=True)
        raise HTTPException(
            status_code=502,
            detail="Could not grant access. You need CAN MANAGE on this warehouse to grant it.",
        )
    status = await svc.warehouse_access_status_async(wid, reader_ws=obo_ws)
    return WarehouseAccessOut(status=status, warehouse_id=wid, sp_application_id=svc.sp_application_id())
