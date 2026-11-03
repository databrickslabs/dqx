"""Read-only validation-status endpoints for external uptime monitors.

Exposes the pass/fail status of the most recent (or a specific) validation
run so an external monitor such as Site24x7 — or any other uptime checker
that can authenticate with a Databricks OAuth token — can poll DQX Studio
as a health check: 200 on a clean run, 503 on a failed or dirty one.
"""

from datetime import datetime, timedelta, timezone
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.common.validation import require_valid
from databricks_labs_dqx_app.backend.dependencies import (
    get_job_service,
    get_sp_sql_executor,
    require_role,
)
from databricks_labs_dqx_app.backend.models import ValidationStatusOut
from databricks_labs_dqx_app.backend.services.job_service import JobService
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor
from databricks_labs_dqx_app.backend.sql_utils import validate_fqn, validate_run_id

router = APIRouter()

_RUNS_TABLE = "dq_validation_runs"
_ALL_ROLES = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR, UserRole.VIEWER]
_MAX_AGE_LIMIT_MINUTES = 60 * 24 * 90  # matches the default run-history retention


def _status_out(row: dict) -> ValidationStatusOut:
    return ValidationStatusOut(
        status=row.get("status") or "UNKNOWN",
        source_table_fqn=row.get("source_table_fqn") or "",
        run_id=row.get("run_id") or "",
        error_rows=row.get("error_rows"),
        warning_rows=row.get("warning_rows"),
        updated_at=row.get("updated_at"),
    )


def _completed_before(updated_at_epoch: str | None, cutoff: datetime) -> bool:
    """True when the run's completion instant is older than *cutoff*.

    *updated_at_epoch* is ``unix_timestamp(updated_at)`` from the query — the
    completion instant as epoch seconds, which is time-zone independent. Reading
    the epoch avoids parsing the session-local timestamp string (the Statement
    Execution API does not pin the session zone to UTC) and comparing it as if
    it were UTC. A missing or unreadable value counts as stale, so a monitor
    that asked for a freshness bound is never told a run of unknown age is fresh.
    """
    if not updated_at_epoch:
        return True
    try:
        completed_epoch = float(updated_at_epoch)
    except (TypeError, ValueError):
        return True
    return completed_epoch < cutoff.timestamp()


def _raise_for_status(row: dict, stale: bool = False) -> ValidationStatusOut:
    """Return the status body on pass, or raise 503 on fail.

    Pass = a completed run with no error rows that is not stale. Anything
    else (FAILED, CANCELED, SUCCESS with error_rows > 0, or a run older than
    the caller's freshness bound) is reported as a failure so an uptime
    monitor sees it as "down".
    """
    out = _status_out(row).model_copy(update={"stale": stale})
    # Compare the Pydantic-coerced int: the raw row comes from the Statement
    # Execution API, where every value is a string ("0" != 0).
    if out.status == "SUCCESS" and (out.error_rows or 0) == 0 and not stale:
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
    max_age_minutes: Annotated[
        int | None,
        Query(
            ge=1,
            le=_MAX_AGE_LIMIT_MINUTES,
            description="Report 503 when the latest completed run finished longer ago than this",
        ),
    ] = None,
) -> ValidationStatusOut:
    """Return the latest completed run's pass/fail status for a table.

    Meant for external polling (e.g. a Site24x7 REST monitor authenticating
    with a Databricks OAuth token) — 200 on a clean run, 503 on a failed one.
    In-progress and canceled runs are skipped: they say nothing about the data.

    Set *max_age_minutes* to a little more than the table's schedule interval
    so a validation job that stops running is reported as down (503 with
    ``stale: true``) instead of returning its last result indefinitely.
    400 for a malformed table name.
    """
    require_valid(validate_fqn, table_fqn)
    row = job_svc.get_latest_completed_run_result_row(sql.fqn(_RUNS_TABLE), table_fqn)
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"No completed validation run for '{table_fqn}' (in-progress and canceled runs are not reported)",
        )
    stale = max_age_minutes is not None and _completed_before(
        row.get("updated_at_epoch"), datetime.now(timezone.utc) - timedelta(minutes=max_age_minutes)
    )
    return _raise_for_status(row, stale)


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
    ``run_id`` instead of table name; a canceled run is reported as such (503).
    400 for a malformed run id.
    """
    require_valid(validate_run_id, run_id)
    row = job_svc.get_run_status_row(sql.fqn(_RUNS_TABLE), run_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found")
    return _raise_for_status(row)
