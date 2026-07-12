"""ComputeService — SQL warehouse + jobs-compute discovery and SP access checks (P22-B).

Backs the admin Configuration page's Compute section (ports dqlake's Settings
"jobs" section) and the App SP access check + one-click grant (task 8).

Split-auth, mirroring the rest of the app:

- **Listing** warehouses / clusters runs under the acting user's On-Behalf-Of
  client (passed per call as ``lister_ws``) so the picker reflects exactly what
  that user can see — never more than their own Unity Catalog / workspace
  visibility. The app SP is only a startup-context fallback when no OBO client
  is available.
- **Self-inspecting** the app service principal's warehouse permission runs as
  the app SP (``sp_ws``) — it must read the SP's own ACL entry.
- **Granting** ``CAN_USE`` to the app SP is applied with the *admin viewer's*
  On-Behalf-Of client (passed per call), because the app SP typically cannot
  ``CAN_MANAGE`` a warehouse it does not own, whereas the admin can. The route
  layer is ADMIN-gated and passes the OBO client as the grantor.
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from typing import Literal

from databricks.sdk import WorkspaceClient

from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService

logger = logging.getLogger(__name__)

# The app SP's warehouse permission is "sufficient" if it holds any of these
# levels — CAN_USE is the minimum to run queries; CAN_MANAGE / IS_OWNER imply it.
_SUFFICIENT_WAREHOUSE_LEVELS = {"CAN_USE", "CAN_MANAGE", "IS_OWNER"}

AccessStatus = Literal["granted", "missing", "unknown"]


@dataclass
class WarehouseInfo:
    id: str
    name: str
    serverless: bool
    running: bool


@dataclass
class ClusterInfo:
    cluster_id: str
    cluster_name: str
    state: str


class ComputeService:
    """Discovery + access checks for SQL warehouses and jobs compute."""

    def __init__(self, sp_ws: WorkspaceClient, app_settings: AppSettingsService) -> None:
        self._sp_ws = sp_ws
        self._app_settings = app_settings

    # ------------------------------------------------------------------
    # Listing (app SP)
    # ------------------------------------------------------------------

    def list_warehouses(self, lister_ws: WorkspaceClient | None = None) -> list[WarehouseInfo]:
        """Return the workspace's SQL warehouses.

        Listing runs under *lister_ws* — the acting user's On-Behalf-Of client —
        so the picker reflects the warehouses that user can actually see. Falls
        back to the app SP only when no OBO client is supplied (e.g. startup).
        """
        ws = lister_ws or self._sp_ws
        out: list[WarehouseInfo] = []
        for warehouse in ws.warehouses.list():
            wid = getattr(warehouse, "id", None)
            if not wid:
                continue
            state = getattr(warehouse, "state", None)
            state_str = getattr(state, "value", None) or (str(state) if state else "")
            out.append(
                WarehouseInfo(
                    id=str(wid),
                    name=getattr(warehouse, "name", None) or str(wid),
                    serverless=bool(getattr(warehouse, "enable_serverless_compute", False)),
                    running=state_str.upper() == "RUNNING",
                )
            )
        out.sort(key=lambda w: w.name.lower())
        return out

    def list_clusters(self, lister_ws: WorkspaceClient | None = None) -> list[ClusterInfo]:
        """Return the workspace's all-purpose clusters.

        Listing runs under *lister_ws* — the acting user's On-Behalf-Of client —
        so the picker reflects the clusters that user can actually see. Falls
        back to the app SP only when no OBO client is supplied (e.g. startup).

        Server-side filtered to UI/API cluster sources so the call does not
        paginate over every terminated job/pipeline cluster (dqlake's
        documented perf gotcha).
        """
        from databricks.sdk.service.compute import ClusterSource, ListClustersFilterBy

        ws = lister_ws or self._sp_ws
        filter_by = ListClustersFilterBy(cluster_sources=[ClusterSource.UI, ClusterSource.API])
        out: list[ClusterInfo] = []
        for cluster in ws.clusters.list(filter_by=filter_by):
            cid = getattr(cluster, "cluster_id", None)
            if not cid:
                continue
            state = getattr(cluster, "state", None)
            state_str = getattr(state, "value", None) or (str(state) if state else "")
            out.append(
                ClusterInfo(
                    cluster_id=str(cid),
                    cluster_name=getattr(cluster, "cluster_name", None) or str(cid),
                    state=state_str,
                )
            )
        out.sort(key=lambda c: c.cluster_name.lower())
        return out

    # ------------------------------------------------------------------
    # App SP identity + warehouse access check / grant (task 8)
    # ------------------------------------------------------------------

    def sp_application_id(self) -> str:
        """Return the app service principal's application (client) id.

        In a deployed Databricks App the SP client id is injected as
        ``DATABRICKS_CLIENT_ID``; we prefer that and fall back to the SP's
        own SCIM ``me()`` identity for local dev.
        """
        env_id = (os.environ.get("DATABRICKS_CLIENT_ID") or "").strip()
        if env_id:
            return env_id
        try:
            me = self._sp_ws.current_user.me()
            return (me.user_name or me.id or "").strip()
        except Exception:  # pragma: no cover - defensive
            logger.warning("Could not resolve app SP identity via current_user.me()", exc_info=True)
            return ""

    def warehouse_access_status(self, warehouse_id: str, reader_ws: WorkspaceClient) -> AccessStatus:
        """Return whether the app SP has a sufficient permission on *warehouse_id*.

        Tries to read the warehouse's permission ACL — first self-inspecting
        with the app SP, then falling back to *reader_ws* (the admin OBO
        client) when the SP cannot read the ACL (it usually lacks CAN_MANAGE).
        Returns ``"granted"``/``"missing"`` when the ACL could be read, and
        ``"unknown"`` when neither client could read it (so the UI shows no
        false alarm).
        """
        sp_id = self.sp_application_id()
        if not sp_id:
            return "unknown"

        acl = self._read_warehouse_acl(warehouse_id, self._sp_ws)
        if acl is None:
            acl = self._read_warehouse_acl(warehouse_id, reader_ws)
        if acl is None:
            return "unknown"

        for ace in acl:
            principal = getattr(ace, "service_principal_name", None)
            if principal != sp_id:
                continue
            for perm in getattr(ace, "all_permissions", None) or []:
                level = getattr(perm, "permission_level", None)
                level_str = getattr(level, "value", None) or str(level)
                if level_str in _SUFFICIENT_WAREHOUSE_LEVELS:
                    return "granted"
        return "missing"

    @staticmethod
    def _read_warehouse_acl(warehouse_id: str, ws: WorkspaceClient) -> list | None:
        try:
            perms = ws.warehouses.get_permissions(warehouse_id)
        except Exception:
            return None
        return list(getattr(perms, "access_control_list", None) or [])

    def grant_warehouse_can_use(self, warehouse_id: str, grantor_ws: WorkspaceClient) -> None:
        """Grant the app SP ``CAN_USE`` on *warehouse_id* via *grantor_ws* (admin OBO).

        Uses ``update_permissions`` (additive PATCH) so existing grants on the
        warehouse are preserved. Raises on failure so the route surfaces an
        honest error to the admin.
        """
        from databricks.sdk.service.sql import WarehouseAccessControlRequest, WarehousePermissionLevel

        sp_id = self.sp_application_id()
        if not sp_id:
            raise RuntimeError("Could not resolve the app service principal identity to grant access.")

        grantor_ws.warehouses.update_permissions(
            warehouse_id,
            access_control_list=[
                WarehouseAccessControlRequest(
                    service_principal_name=sp_id,
                    permission_level=WarehousePermissionLevel.CAN_USE,
                )
            ],
        )
        logger.info("Granted CAN_USE on warehouse %s to app SP", warehouse_id)

    # ------------------------------------------------------------------
    # Async wrappers (SDK calls are blocking)
    # ------------------------------------------------------------------

    async def list_warehouses_async(self, lister_ws: WorkspaceClient | None = None) -> list[WarehouseInfo]:
        return await asyncio.to_thread(self.list_warehouses, lister_ws)

    async def list_clusters_async(self, lister_ws: WorkspaceClient | None = None) -> list[ClusterInfo]:
        return await asyncio.to_thread(self.list_clusters, lister_ws)

    async def warehouse_access_status_async(self, warehouse_id: str, reader_ws: WorkspaceClient) -> AccessStatus:
        return await asyncio.to_thread(self.warehouse_access_status, warehouse_id, reader_ws)

    async def grant_warehouse_can_use_async(self, warehouse_id: str, grantor_ws: WorkspaceClient) -> None:
        return await asyncio.to_thread(self.grant_warehouse_can_use, warehouse_id, grantor_ws)


def resolve_warehouse_id(app_settings: AppSettingsService) -> str:
    """Return the effective SQL warehouse id for app-side ad-hoc SQL.

    The admin override in ``dq_app_settings`` wins; otherwise we fall back to
    the bundle-bound ``DATABRICKS_WAREHOUSE_ID`` / ``DATABRICKS_SQL_WAREHOUSE_ID``
    env var (today's behaviour). Returns ``""`` when nothing is configured so
    callers can raise a clear error.
    """
    configured = app_settings.get_sql_warehouse_id()
    if configured:
        return configured
    return os.environ.get("DATABRICKS_WAREHOUSE_ID") or os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID") or ""
