"""Monitored Tables routes — Phase 3B (register/list/get/delete + profiling read).

Layer 2 of the Rules Registry
(``docs/superpowers/specs/2026-07-02-rules-registry-design.md`` §7): a thin
binding recording that a table is under active Rules Registry governance,
plus the live link of applied registry rules. Apply/map/materialize (Phase
3C) is out of scope here — only CRUD on the binding + a read-only view of
the existing profiling results.
"""

from typing import Annotated

from databricks.sdk import WorkspaceClient
from fastapi import APIRouter, Depends, HTTPException, Query

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.dependencies import (
    get_monitored_table_service,
    get_obo_ws,
    require_role,
)
from databricks_labs_dqx_app.backend.logger import logger
from databricks_labs_dqx_app.backend.models import (
    MonitoredTableDetailOut,
    MonitoredTableProfileOut,
    MonitoredTableSummaryOut,
    RegisterMonitoredTableIn,
)
from databricks_labs_dqx_app.backend.services.monitored_table_service import (
    DuplicateMonitoredTableError,
    MonitoredTableService,
    MonitoredTableSummary,
)

router = APIRouter()

_ALL_ROLES = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR, UserRole.VIEWER]
_AUTHORS_AND_ABOVE = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR]


def _current_user_email(obo_ws: WorkspaceClient) -> str:
    user = obo_ws.current_user.me()
    return user.user_name or "unknown"


# ------------------------------------------------------------------
# List / Get
# ------------------------------------------------------------------


@router.get(
    "",
    response_model=list[MonitoredTableSummaryOut],
    operation_id="listMonitoredTables",
    dependencies=[require_role(*_ALL_ROLES)],
)
def list_monitored_tables(
    svc: Annotated[MonitoredTableService, Depends(get_monitored_table_service)],
    status: Annotated[str | None, Query(description="Filter by status")] = None,
    steward: Annotated[str | None, Query(description="Filter by steward")] = None,
    catalog: Annotated[str | None, Query(description="Filter by catalog part of table_fqn")] = None,
    schema: Annotated[str | None, Query(description="Filter by schema part of table_fqn")] = None,
    name: Annotated[str | None, Query(description="Substring search over table_fqn")] = None,
) -> list[MonitoredTableSummaryOut]:
    """List monitored tables, optionally filtered, with per-table applied-rule counts."""
    try:
        summaries = svc.list_monitored_tables(
            status=status, steward=steward, catalog=catalog, schema=schema, name=name
        )
        return [MonitoredTableSummaryOut.from_domain(s) for s in summaries]
    except Exception as e:
        logger.error(f"Failed to list monitored tables: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to list monitored tables: {e}")


@router.get(
    "/{binding_id}",
    response_model=MonitoredTableDetailOut,
    operation_id="getMonitoredTable",
    dependencies=[require_role(*_ALL_ROLES)],
)
def get_monitored_table(
    binding_id: str,
    svc: Annotated[MonitoredTableService, Depends(get_monitored_table_service)],
) -> MonitoredTableDetailOut:
    """Get a monitored table binding plus its applied rules (joined to rule name/dimension/severity tags)."""
    try:
        detail = svc.get(binding_id)
        if detail is None:
            raise HTTPException(status_code=404, detail=f"Monitored table not found: {binding_id}")
        return MonitoredTableDetailOut.from_domain(detail)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get monitored table {binding_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get monitored table: {e}")


# ------------------------------------------------------------------
# Register / Delete
# ------------------------------------------------------------------


@router.post(
    "",
    response_model=MonitoredTableSummaryOut,
    operation_id="registerMonitoredTable",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def register_monitored_table(
    body: RegisterMonitoredTableIn,
    svc: Annotated[MonitoredTableService, Depends(get_monitored_table_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> MonitoredTableSummaryOut:
    """Register a table under Rules Registry governance (status ``draft``)."""
    try:
        user_email = _current_user_email(obo_ws)
        table = svc.register(body.table_fqn, user_email, steward=body.steward)
        return MonitoredTableSummaryOut.from_domain(MonitoredTableSummary(table=table, applied_rule_count=0))
    except DuplicateMonitoredTableError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to register monitored table: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to register monitored table: {e}")


@router.delete(
    "/{binding_id}",
    operation_id="deleteMonitoredTable",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def delete_monitored_table(
    binding_id: str,
    svc: Annotated[MonitoredTableService, Depends(get_monitored_table_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> dict[str, str]:
    """Delete a monitored table binding and its applied rules.

    TODO(Phase 3C): once the materializer exists, block/handle
    de-materialization of any ``dq_quality_rules`` rows tied to this
    binding's applications before allowing deletion.
    """
    try:
        user_email = _current_user_email(obo_ws)
        svc.delete(binding_id, user_email)
        return {"status": "deleted", "binding_id": binding_id}
    except RuntimeError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to delete monitored table {binding_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to delete monitored table: {e}")


# ------------------------------------------------------------------
# Profiling (READ-ONLY — reuses dq_profiling_results, never writes here)
# ------------------------------------------------------------------


@router.get(
    "/{binding_id}/profile",
    response_model=MonitoredTableProfileOut,
    operation_id="getMonitoredTableProfile",
    dependencies=[require_role(*_ALL_ROLES)],
)
def get_monitored_table_profile(
    binding_id: str,
    svc: Annotated[MonitoredTableService, Depends(get_monitored_table_service)],
) -> MonitoredTableProfileOut:
    """Return the most recent profiling result for this monitored table's underlying table."""
    try:
        detail = svc.get(binding_id)
        if detail is None:
            raise HTTPException(status_code=404, detail=f"Monitored table not found: {binding_id}")
        profile = svc.get_latest_profile(detail.table.table_fqn)
        if profile is None:
            raise HTTPException(
                status_code=404,
                detail=f"No profiling results found for table: {detail.table.table_fqn}",
            )
        return MonitoredTableProfileOut.from_domain(profile)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get profile for monitored table {binding_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get profile: {e}")
