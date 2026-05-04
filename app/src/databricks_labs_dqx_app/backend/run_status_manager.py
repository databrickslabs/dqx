"""Shared run-status helpers used by both dryrun and profiler routes.

Eliminates near-identical _update_*_status / _get_*_run_owner functions.
All SQL is executed via :class:`SqlExecutor` — no private-attribute access
to any other service.
"""

from __future__ import annotations

import logging

from databricks_labs_dqx_app.backend.config import AppConfig
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor
from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

logger = logging.getLogger(__name__)


def update_run_status(
    sql: SqlExecutor,
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

    stmt = f"UPDATE {table} SET {set_clause} WHERE run_id = '{er}' AND status = 'RUNNING'"
    try:
        sql.execute(stmt)
        logger.info("Updated %s run %s status to %s", table_name, run_id, status)
    except Exception as exc:
        logger.warning("Failed to update %s status for %s: %s", table_name, run_id, exc)


def get_run_owner(
    sql: SqlExecutor,
    app_conf: AppConfig,
    table_name: str,
    run_id: str,
) -> str | None:
    """Look up the requesting_user for a given run_id in any run table."""
    row = _get_run_fields(sql, app_conf, table_name, run_id, "requesting_user")
    return row[0] if row else None


def get_run_view_fqn(
    sql: SqlExecutor,
    app_conf: AppConfig,
    table_name: str,
    run_id: str,
) -> tuple[str | None, str | None]:
    """Look up (view_fqn, requesting_user) for a run_id from the runs table.

    Returns (None, None) when the run is not found.
    """
    row = _get_run_fields(sql, app_conf, table_name, run_id, "view_fqn, requesting_user")
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


def has_terminal_result(
    sql: SqlExecutor,
    app_conf: AppConfig,
    table_name: str,
    run_id: str,
) -> str | None:
    """Check if the task runner already wrote a terminal result for this run_id.

    Returns the status string (e.g. ``"SUCCESS"`` or ``"FAILED"``) when a
    non-RUNNING row exists, or ``None`` when only a RUNNING placeholder (or
    no row at all) is present.
    """
    table = f"{app_conf.catalog}.{app_conf.schema_name}.{table_name}"
    er = escape_sql_string(run_id)
    stmt = f"SELECT status FROM {table} " f"WHERE run_id = '{er}' AND status != 'RUNNING' " f"LIMIT 1"  # noqa: S608
    try:
        rows = sql.query(stmt)
        if rows and rows[0]:
            return rows[0][0]
    except Exception:
        pass
    return None


def get_run_metadata(
    sql: SqlExecutor,
    app_conf: AppConfig,
    table_name: str,
    run_id: str,
) -> RunMetadata:
    """Look up (view_fqn, requesting_user, job_run_id) for a run_id.

    Returns a RunMetadata with None fields when the run is not found.
    """
    row = _get_run_fields(
        sql,
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
    sql: SqlExecutor,
    app_conf: AppConfig,
    table_name: str,
    run_id: str,
    columns: str,
) -> list[str] | None:
    """Fetch specific columns for a run_id from any run table.

    When multiple rows exist for the same run_id (e.g. a RUNNING placeholder
    and a terminal result written by the task runner), prefer the row that
    has a non-null job_run_id so that the status endpoint can always look up
    the Databricks job run.
    """
    table = f"{app_conf.catalog}.{app_conf.schema_name}.{table_name}"
    er = escape_sql_string(run_id)
    stmt = (  # noqa: S608
        f"SELECT {columns} FROM {table} "
        f"WHERE run_id = '{er}' "
        f"ORDER BY job_run_id IS NOT NULL DESC, created_at DESC "
        f"LIMIT 1"
    )
    try:
        rows = sql.query(stmt)
        if rows and rows[0]:
            return rows[0]
    except Exception:
        pass
    return None
