"""Read-only validation-status endpoints for external uptime monitors.

Exposes the pass/fail status of the most recent (or a specific) validation
run so an external monitor such as Site24x7 — or any other uptime checker
that can authenticate with a Databricks OAuth token — can poll DQX Studio
as a health check: 200 on a clean run, 503 on a failed or dirty one.
"""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.dependencies import (
    get_job_service,
    get_sp_sql_executor,
    require_role,
)
from databricks_labs_dqx_app.backend.models import ValidationStatusOut
from databricks_labs_dqx_app.backend.services.job_service import JobService
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor

router = APIRouter()

_RUNS_TABLE = "dq_validation_runs"
_ALL_ROLES = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR, UserRole.VIEWER]


def _status_out(row: dict) -> ValidationStatusOut:
    return ValidationStatusOut(
        status=row.get("status") or "UNKNOWN",
        source_table_fqn=row.get("source_table_fqn") or "",
        run_id=row.get("run_id") or "",
        error_rows=row.get("error_rows"),
        warning_rows=row.get("warning_rows"),
        updated_at=row.get("updated_at"),
    )


def _raise_for_status(row: dict) -> ValidationStatusOut:
    """Return the status body on pass, or raise 503 on fail.

    Pass = a completed run with no error rows. Anything else (FAILED,
    CANCELED, or SUCCESS with error_rows > 0) is reported as a failure so
    an uptime monitor sees it as "down".
    """
    out = _status_out(row)
    # Compare the Pydantic-coerced int: the raw row comes from the Statement
    # Execution API, where every value is a string ("0" != 0).
    if out.status == "SUCCESS" and (out.error_rows or 0) == 0:
        return out
    raise HTTPException(status_code=503, detail=out.model_dump())


@router.get(
    "/status/table/{table_fqn:path}",
    response_model=ValidationStatusOut,
    operation_id="getValidationStatusByTable",
    dependencies=[require_role(*_ALL_ROLES)],
)
def get_validation_status_by_table(
    table_fqn: str,
    job_svc: Annotated[JobService, Depends(get_job_service)],
    sql: Annotated[SqlExecutor, Depends(get_sp_sql_executor)],
) -> ValidationStatusOut:
    """Return the latest run's pass/fail status for a table.

    Meant for external polling (e.g. a Site24x7 REST monitor authenticating
    with a Databricks OAuth token) — 200 on a clean run, 503 on a failed one.
    """
    row = job_svc.get_latest_run_result_row(sql.fqn(_RUNS_TABLE), table_fqn)
    if row is None:
        raise HTTPException(status_code=404, detail=f"No validation run recorded for '{table_fqn}'")
    return _raise_for_status(row)


@router.get(
    "/status/run/{run_id}",
    response_model=ValidationStatusOut,
    operation_id="getValidationStatusByRun",
    dependencies=[require_role(*_ALL_ROLES)],
)
def get_validation_status_by_run(
    run_id: str,
    job_svc: Annotated[JobService, Depends(get_job_service)],
    sql: Annotated[SqlExecutor, Depends(get_sp_sql_executor)],
) -> ValidationStatusOut:
    """Return a specific run's pass/fail status.

    Same 200/503 contract as ``getValidationStatusByTable`` but keyed by
    ``run_id`` instead of table name.
    """
    row = job_svc.get_run_result_row(sql.fqn(_RUNS_TABLE), run_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found")
    return _raise_for_status(row)
