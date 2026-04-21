"""Shared run-status helpers used by both dryrun and profiler routes.

Eliminates near-identical _update_*_status / _get_*_run_owner functions.
"""

from __future__ import annotations

import logging

from databricks.sdk.service.sql import Disposition, Format, StatementState

from databricks_labs_dqx_app.backend.config import AppConfig
from databricks_labs_dqx_app.backend.services.job_service import JobService
from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

logger = logging.getLogger(__name__)


def update_run_status(
    job_svc: JobService,
    app_conf: AppConfig,
    table_name: str,
    run_id: str,
    status: str,
    error_message: str | None,
    canceled_by: str | None = None,
) -> None:
    """Update a RUNNING placeholder row with a terminal status.

    Works for any run table (dq_validation_runs, dq_profiling_results).
    """
    table = f"{app_conf.catalog}.{app_conf.schema_name}.{table_name}"
    er = escape_sql_string(run_id)
    es = escape_sql_string(status)
    em = escape_sql_string(error_message or "")

    set_clause = f"status = '{es}', error_message = '{em}', updated_at = CAST(current_timestamp() AS STRING)"
    if canceled_by:
        ec = escape_sql_string(canceled_by)
        set_clause += f", canceled_by = '{ec}'"

    sql = f"UPDATE {table} SET {set_clause} WHERE run_id = '{er}' AND status = 'RUNNING'"
    try:
        resp = job_svc._ws.statement_execution.execute_statement(
            warehouse_id=job_svc._warehouse_id,
            statement=sql,
            catalog=job_svc._catalog,
            schema=job_svc._schema,
            disposition=Disposition.INLINE,
            format=Format.JSON_ARRAY,
        )
        if resp.status and resp.status.state == StatementState.FAILED:
            msg = resp.status.error.message if resp.status.error else "Unknown error"
            logger.warning("Failed to update %s status for %s: %s", table_name, run_id, msg)
        else:
            logger.info("Updated %s run %s status to %s", table_name, run_id, status)
    except Exception as exc:
        logger.warning("Failed to update %s status for %s: %s", table_name, run_id, exc)


def get_run_owner(
    job_svc: JobService,
    app_conf: AppConfig,
    table_name: str,
    run_id: str,
) -> str | None:
    """Look up the requesting_user for a given run_id in any run table."""
    row = _get_run_fields(job_svc, app_conf, table_name, run_id, "requesting_user")
    return row[0] if row else None


def get_run_view_fqn(
    job_svc: JobService,
    app_conf: AppConfig,
    table_name: str,
    run_id: str,
) -> tuple[str | None, str | None]:
    """Look up (view_fqn, requesting_user) for a run_id from the runs table.

    Returns (None, None) when the run is not found.
    """
    row = _get_run_fields(job_svc, app_conf, table_name, run_id, "view_fqn, requesting_user")
    if row and len(row) >= 2:
        return row[0], row[1]
    return None, None


class RunMetadata:
    """Server-side metadata for a run, looked up by run_id."""

    __slots__ = ("view_fqn", "requesting_user", "job_run_id")

    def __init__(self, view_fqn: str | None, requesting_user: str | None, job_run_id: int | None) -> None:
        self.view_fqn = view_fqn
        self.requesting_user = requesting_user
        self.job_run_id = job_run_id


def get_run_metadata(
    job_svc: JobService,
    app_conf: AppConfig,
    table_name: str,
    run_id: str,
) -> RunMetadata:
    """Look up (view_fqn, requesting_user, job_run_id) for a run_id.

    Returns a RunMetadata with None fields when the run is not found.
    """
    row = _get_run_fields(
        job_svc,
        app_conf,
        table_name,
        run_id,
        "view_fqn, requesting_user, CAST(job_run_id AS STRING)",
    )
    if row and len(row) >= 3:
        jri = int(row[2]) if row[2] else None
        return RunMetadata(view_fqn=row[0], requesting_user=row[1], job_run_id=jri)
    return RunMetadata(view_fqn=None, requesting_user=None, job_run_id=None)


def _get_run_fields(
    job_svc: JobService,
    app_conf: AppConfig,
    table_name: str,
    run_id: str,
    columns: str,
) -> list[str] | None:
    """Fetch specific columns for a run_id from any run table."""
    table = f"{app_conf.catalog}.{app_conf.schema_name}.{table_name}"
    er = escape_sql_string(run_id)
    sql = f"SELECT {columns} FROM {table} WHERE run_id = '{er}' LIMIT 1"  # noqa: S608
    try:
        resp = job_svc._ws.statement_execution.execute_statement(
            warehouse_id=job_svc._warehouse_id,
            statement=sql,
            catalog=job_svc._catalog,
            schema=job_svc._schema,
            disposition=Disposition.INLINE,
            format=Format.JSON_ARRAY,
        )
        if resp.status and resp.status.state == StatementState.FAILED:
            return None
        if resp.result and resp.result.data_array and resp.result.data_array[0]:
            return resp.result.data_array[0]
    except Exception:
        pass
    return None
