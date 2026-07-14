"""Pure SQL builders that re-date (or delete) genuine engine-run rows.

These helpers fabricate a multi-week quality trend from real DQ runs by
shifting each run's timestamps in Delta (*dq_metrics* / *dq_validation_runs*)
and by adjusting back-dated rows in the OLTP *dq_score_history* table. Every
value is engine-computed; only the timestamps are moved. A pair of delete
builders drops the rows of throwaway runs (e.g. the validation gate's baseline
runs) so they cannot win a "latest run" selection against the re-dated trend.

All functions are pure: they take fully-qualified table names (already
qualified by the caller) plus scalar values and return a SQL string. User- or
run-derived string values (*run_id*, *scope_type*, *scope_key* and the target
timestamp) are escaped via *escape_sql_string* (ANSI doubled-quote escaping).
Fully-qualified table names are app-internal constants and are interpolated
verbatim.
"""

from __future__ import annotations

from datetime import datetime

from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string


def iso(dt: datetime) -> str:
    """Format a datetime as a SQL timestamp literal body.

    Args:
        dt: The datetime to format (expected UTC).

    Returns:
        A string of the form *YYYY-MM-DD HH:MM:SS*.
    """
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _ts(target_iso: str) -> str:
    """Build a ``CAST('<iso>' AS TIMESTAMP)`` expression from an escaped literal."""
    return f"CAST('{escape_sql_string(target_iso)}' AS TIMESTAMP)"


def build_redate_metrics_sql(metrics_fqn: str, run_id: str, target_iso: str) -> str:
    """Build SQL to re-date a *dq_metrics* run's *run_time*.

    Args:
        metrics_fqn: Fully-qualified *dq_metrics* table name.
        run_id: The run identifier to match.
        target_iso: Target timestamp literal body (*YYYY-MM-DD HH:MM:SS*).

    Returns:
        An ``UPDATE`` statement targeting the matched run.
    """
    return f"UPDATE {metrics_fqn} SET run_time = {_ts(target_iso)} WHERE run_id = '{escape_sql_string(run_id)}'"


def build_redate_runs_sql(runs_fqn: str, run_id: str, target_iso: str, duration_seconds: int = 45) -> str:
    """Build SQL to re-date a *dq_validation_runs* run's *created_at* / *updated_at*.

    The run's *created_at* (start) is set to *target_iso* and its *updated_at*
    (end) is set to *target_iso* plus *duration_seconds*, preserving a realistic
    positive span. The Runs History "Time" column reads
    ``timestampdiff(SECOND, MIN(created_at), MAX(COALESCE(updated_at, created_at)))``
    (see ``job_service.list_dryrun_rows``) and only emits a value when
    ``run_ended_at > run_started_at``. Collapsing both timestamps to a single
    instant would make that span zero, so the column shows a blank "–";
    offsetting the end by a small realistic duration keeps it a believable value.

    Args:
        runs_fqn: Fully-qualified *dq_validation_runs* table name.
        run_id: The run identifier to match.
        target_iso: Target timestamp literal body (*YYYY-MM-DD HH:MM:SS*); the
            run's start instant.
        duration_seconds: The run's fabricated wall-clock duration in seconds
            (a positive offset applied to *updated_at*). Must be positive so the
            derived span is positive.

    Returns:
        An ``UPDATE`` statement targeting the matched run.
    """
    start = _ts(target_iso)
    end = f"{start} + INTERVAL {int(duration_seconds)} SECONDS"
    return (
        f"UPDATE {runs_fqn} SET created_at = {start}, updated_at = {end} "
        f"WHERE run_id = '{escape_sql_string(run_id)}'"
    )


def build_delete_metrics_sql(metrics_fqn: str, run_id: str) -> str:
    """Build SQL to delete a run's *dq_metrics* rows.

    Used to discard a throwaway run (e.g. a validation-gate baseline run) so it
    cannot win a "latest published run" selection against the re-dated trend.

    Args:
        metrics_fqn: Fully-qualified *dq_metrics* table name.
        run_id: The run identifier whose rows to delete.

    Returns:
        A ``DELETE`` statement targeting the matched run.
    """
    return f"DELETE FROM {metrics_fqn} WHERE run_id = '{escape_sql_string(run_id)}'"


def build_delete_runs_sql(runs_fqn: str, run_id: str) -> str:
    """Build SQL to delete a run's *dq_validation_runs* row.

    Used to discard a throwaway run (e.g. a validation-gate baseline run) so it
    cannot win a "latest published run" selection against the re-dated trend.

    Args:
        runs_fqn: Fully-qualified *dq_validation_runs* table name.
        run_id: The run identifier whose row to delete.

    Returns:
        A ``DELETE`` statement targeting the matched run.
    """
    return f"DELETE FROM {runs_fqn} WHERE run_id = '{escape_sql_string(run_id)}'"


def build_redate_latest_history_sql(history_fqn: str, scope_type: str, scope_key: str, target_iso: str) -> str:
    """Build SQL to re-date the most recently appended *dq_score_history* row of a scope.

    Used when re-dating a point that *ScoreCacheService* just appended
    (``computed_at = now()``) rather than inserting a fresh back-dated row.

    Args:
        history_fqn: Fully-qualified *dq_score_history* table name.
        scope_type: Scope type, one of ``"table"``, ``"product"`` or ``"global"``.
        scope_key: Scope key identifying the trend series.
        target_iso: Target timestamp literal body (*YYYY-MM-DD HH:MM:SS*).

    Returns:
        An ``UPDATE`` statement moving the latest row's *computed_at* and
        *run_time* to *target_iso*.
    """
    e_type, e_key = escape_sql_string(scope_type), escape_sql_string(scope_key)
    return (
        f"UPDATE {history_fqn} SET computed_at = {_ts(target_iso)}, run_time = {_ts(target_iso)} "
        f"WHERE scope_type = '{e_type}' AND scope_key = '{e_key}' AND computed_at = ("
        f"SELECT MAX(computed_at) FROM {history_fqn} "
        f"WHERE scope_type = '{e_type}' AND scope_key = '{e_key}')"
    )


def build_delete_history_after_sql(history_fqn: str, cutoff_iso: str) -> str:
    """Build SQL to delete every *dq_score_history* row appended after *cutoff_iso*.

    Used after the final "truthful now" cache refresh: that refresh appends one
    real-wall-clock (``computed_at = now()``) trend point per scope which is
    never re-dated and would pollute the back-dated weekly trend. Every genuine
    weekly point was already re-dated to at-or-before the cutoff, so a plain
    ``computed_at > cutoff`` delete strips exactly the polluting appends across
    all scopes in one statement — no run_id or scope filter needed.

    Args:
        history_fqn: Fully-qualified *dq_score_history* table name.
        cutoff_iso: Cutoff timestamp literal body (*YYYY-MM-DD HH:MM:SS*); rows
            with *computed_at* strictly greater than this are deleted.

    Returns:
        A ``DELETE`` statement removing rows newer than the cutoff.
    """
    return f"DELETE FROM {history_fqn} WHERE computed_at > {_ts(cutoff_iso)}"
