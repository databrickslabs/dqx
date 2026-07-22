"""Shared run-status helpers used by both dryrun and profiler routes.

Eliminates near-identical _update_*_status / _get_*_run_owner functions.
All SQL is executed via :class:`SqlExecutor` — no private-attribute access
to any other service.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Protocol

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

    # updated_at is TIMESTAMP in the baseline; pass current_timestamp()
    # directly rather than casting to STRING.
    set_clause = f"status = '{es}', error_message = '{em}', updated_at = current_timestamp()"
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

    __slots__ = ("view_fqn", "requesting_user", "job_run_id", "source_table_fqn")

    def __init__(
        self,
        view_fqn: str | None,
        requesting_user: str | None,
        job_run_id: int | None,
        source_table_fqn: str | None = None,
    ) -> None:
        self.view_fqn = view_fqn
        self.requesting_user = requesting_user
        self.job_run_id = job_run_id
        self.source_table_fqn = source_table_fqn


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
    stmt = f"SELECT status FROM {table} WHERE run_id = '{er}' AND status != 'RUNNING' LIMIT 1"  # noqa: S608
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
    """Look up (view_fqn, requesting_user, job_run_id, source_table_fqn) for a run_id.

    Returns a RunMetadata with None fields when the run is not found.
    """
    row = _get_run_fields(
        sql,
        app_conf,
        table_name,
        run_id,
        "view_fqn, requesting_user, CAST(job_run_id AS STRING), source_table_fqn",
    )
    if row and len(row) >= 4:
        jri = int(row[2]) if row[2] else None
        return RunMetadata(view_fqn=row[0], requesting_user=row[1], job_run_id=jri, source_table_fqn=row[3])
    return RunMetadata(view_fqn=None, requesting_user=None, job_run_id=None, source_table_fqn=None)


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


# ---------------------------------------------------------------------------
# Reconciliation of stale RUNNING placeholder rows
# ---------------------------------------------------------------------------
#
# A run's lifecycle is: the app inserts a RUNNING placeholder (as the SP, which
# always has write access) and the task runner later overwrites it with a
# terminal row. When the task dies *before* writing its terminal result — an
# import error, an OOM, an externally-killed run, or a PERMISSION_DENIED that
# also blocks the runner's own error-result write — the placeholder is never
# flipped and Runs History shows the run stuck on RUNNING forever, so failed
# runs never surface as FAILED.
#
# The per-run status poll (``get_dry_run_status``) already reconciles, but only
# for runs a client is actively polling. The listing endpoint reconciles here
# so the correction is authoritative and independent of any open browser tab.


class _JobStatusLike(Protocol):
    """Structural type for the ``RunStatus`` returned by ``JobService.get_run_status``."""

    state: str
    result_state: str | None
    message: str | None


# Lifecycle states that mean the Databricks job run has stopped for good.
_TERMINAL_LIFECYCLE_STATES = frozenset({"TERMINATED", "INTERNAL_ERROR", "SKIPPED"})

# Bound the Jobs-API fan-out per list request; rows arrive newest-first so the
# most relevant RUNNING rows are reconciled first.
_MAX_RECONCILE_PER_CALL = 25

# Short-lived cache of job-run status keyed by job_run_id, so repeated Runs
# History polls (the page refetches every few seconds) don't hammer the Jobs
# API for the same genuinely-running run. {job_run_id: (status, expires_at)}.
_STATUS_CACHE_TTL_SECONDS = 30.0
_status_cache: dict[int, tuple[_JobStatusLike, float]] = {}

# A RUNNING row older than this threshold that cannot be reconciled via the
# Jobs API (no job_run_id, or the job status is unavailable) is treated as an
# unrecoverable stale placeholder and flipped to FAILED. Real runs complete
# well within a few hours on these dev/UAT targets; 12 h is a safe floor.
_STALE_RUNNING_MAX_AGE_HOURS = 12


def _cached_job_status(job_run_id: int, status_fn: Callable[[int], _JobStatusLike]) -> _JobStatusLike:
    """Return the job-run status, memoised for ``_STATUS_CACHE_TTL_SECONDS``."""
    now = time.monotonic()
    entry = _status_cache.get(job_run_id)
    if entry is not None and entry[1] > now:
        return entry[0]
    status = status_fn(job_run_id)
    _status_cache[job_run_id] = (status, now + _STATUS_CACHE_TTL_SECONDS)
    # Opportunistically evict expired entries so the cache can't grow unbounded
    # in a long-lived worker.
    if len(_status_cache) > 512:
        for key in [k for k, (_, exp) in _status_cache.items() if exp <= now]:
            _status_cache.pop(key, None)
    return status


def _get_running_job_run_ids(
    sql: SqlExecutor,
    app_conf: AppConfig,
    table_name: str,
    run_ids: list[str],
) -> dict[str, int]:
    """Return ``{run_id: job_run_id}`` for still-RUNNING rows among *run_ids*.

    One batched query (rather than one per run) keeps reconciliation cheap.
    Rows without a job_run_id are omitted — they cannot be reconciled against
    the Jobs API.
    """
    if not run_ids:
        return {}
    table = f"{app_conf.catalog}.{app_conf.schema_name}.{table_name}"
    in_list = ", ".join(f"'{escape_sql_string(r)}'" for r in run_ids)
    stmt = (  # noqa: S608
        f"SELECT run_id, CAST(job_run_id AS STRING) FROM {table} "
        f"WHERE status = 'RUNNING' AND job_run_id IS NOT NULL "
        f"AND run_id IN ({in_list})"
    )
    mapping: dict[str, int] = {}
    try:
        for row in sql.query(stmt) or []:
            if row and len(row) >= 2 and row[0] and row[1]:
                try:
                    mapping[row[0]] = int(row[1])
                except (TypeError, ValueError):
                    continue
    except Exception as exc:
        logger.warning("Failed to look up job_run_ids for RUNNING rows: %s", exc)
    return mapping


def reconcile_running_rows(
    sql: SqlExecutor,
    app_conf: AppConfig,
    table_name: str,
    rows: list[dict[str, str | None]],
    status_fn: Callable[[int], _JobStatusLike],
) -> None:
    """Flip stale RUNNING placeholder rows to their terminal status in place.

    For each RUNNING row that carries a *job_run_id*, query the Databricks Jobs
    API (bounded to :data:`_MAX_RECONCILE_PER_CALL` rows, memoised for
    :data:`_STATUS_CACHE_TTL_SECONDS`). When the job run has terminated,
    persist the corrected status to the Delta row and mutate the supplied dict
    so the caller's response reflects the true outcome immediately.

    A SUCCESS terminal state is intentionally *not* written back: on success
    the task runner owns the terminal row (with metrics), and forcing a bare
    SUCCESS here would surface a metric-less run. This mirrors
    ``get_dry_run_status``. Everything is best-effort — any failure is logged
    and the row is left untouched so listing never breaks.

    **Stale-placeholder fallback:** when a RUNNING row cannot be resolved via
    the Jobs API (no *job_run_id*, or the Jobs API lookup raised an exception)
    AND its *created_at* is older than :data:`_STALE_RUNNING_MAX_AGE_HOURS`,
    it is flipped to FAILED with a clear error message. This prevents ancient
    placeholder rows from keeping the frontend in an infinite polling loop.
    Only genuinely old, unresolvable rows are affected; recent rows and rows
    whose Jobs-API state is still active are left untouched.
    """
    running_ids = [rid for r in rows if r.get("status") == "RUNNING" and (rid := r.get("run_id"))]
    if not running_ids:
        return
    job_run_ids = _get_running_job_run_ids(sql, app_conf, table_name, running_ids[:_MAX_RECONCILE_PER_CALL])

    now_utc = datetime.now(timezone.utc)

    for row in rows:
        run_id = row.get("run_id")
        if row.get("status") != "RUNNING" or not run_id:
            continue
        job_run_id = job_run_ids.get(run_id)

        if job_run_id is not None:
            # --- Jobs-API path ---
            try:
                status = _cached_job_status(job_run_id, status_fn)
            except Exception as exc:
                logger.warning("reconcile: failed to fetch job status for run %s: %s", run_id, exc)
                # Fall through to stale-age check below; if the row is old
                # enough it will still be flipped to FAILED.
                status = None

            if status is not None:
                if status.state not in _TERMINAL_LIFECYCLE_STATES:
                    # Job still running — do not touch the row.
                    continue
                if status.result_state == "SUCCESS":
                    # Runner owns the terminal SUCCESS row; don't clobber it.
                    continue
                new_status = "CANCELED" if status.result_state == "CANCELED" else "FAILED"
                error_message = status.message or f"Run finished with state: {status.state}"
                update_run_status(sql, app_conf, table_name, run_id, status=new_status, error_message=error_message)
                row["status"] = new_status
                if not row.get("error_message"):
                    row["error_message"] = error_message
                continue
            # status is None (Jobs-API raised) — fall through to stale-age check.

        # --- Stale-age fallback ---
        # Reached when: (a) no job_run_id, or (b) Jobs API raised an exception.
        # Flip to FAILED only if the row is old enough to be unrecoverable.
        created_at_str = row.get("created_at")
        if not created_at_str:
            continue
        try:
            # created_at arrives as a CAST(TIMESTAMP AS STRING), which Spark
            # formats as "YYYY-MM-DD HH:MM:SS[.fraction]" (no timezone suffix).
            # Treat it as UTC and replace the space separator for fromisoformat.
            created_at = datetime.fromisoformat(str(created_at_str).replace(" ", "T"))
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        age_hours = (now_utc - created_at).total_seconds() / 3600.0
        if age_hours < _STALE_RUNNING_MAX_AGE_HOURS:
            continue
        stale_message = (
            f"Run status unrecoverable (stale RUNNING placeholder); "
            f"marked failed after {_STALE_RUNNING_MAX_AGE_HOURS}h."
        )
        update_run_status(sql, app_conf, table_name, run_id, status="FAILED", error_message=stale_message)
        row["status"] = "FAILED"
        if not row.get("error_message"):
            row["error_message"] = stale_message
        logger.info(f"reconcile: flipped stale RUNNING row {run_id} to FAILED (age {age_hours:.1f}h)")
