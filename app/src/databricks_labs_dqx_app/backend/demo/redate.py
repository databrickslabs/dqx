"""Pure SQL builders that re-date (or delete) genuine engine-run rows.

These helpers fabricate a multi-week quality trend from real DQ runs by
shifting each run's timestamps in Delta (*dq_metrics* / *dq_validation_runs*)
and by adjusting back-dated rows in the OLTP *dq_score_history* table. Every
value is engine-computed; only the timestamps are moved. A pair of delete
builders drops the rows of throwaway runs (e.g. the validation gate's baseline
runs) so they cannot win a "latest run" selection against the re-dated trend.

All functions are pure. Statements with runtime values return SQL text and
parameters separately; callers supply their executor's parameter marker.
Fully-qualified table names are app-internal constants and are interpolated
verbatim.
"""

from datetime import datetime
from collections.abc import Callable

SqlParameters = dict[str, str | int]
SqlStatement = tuple[str, SqlParameters]


def iso(dt: datetime) -> str:
    """Format a datetime as a SQL timestamp literal body.

    Args:
        dt: The datetime to format (expected UTC).

    Returns:
        A string of the form *YYYY-MM-DD HH:MM:SS*.
    """
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _ts(marker: Callable[[str], str]) -> str:
    """Build a timestamp cast around an executor-specific parameter marker."""
    return f"CAST({marker('target_iso')} AS TIMESTAMP)"


def build_redate_metrics_sql(
    metrics_fqn: str, run_id: str, target_iso: str, *, marker: Callable[[str], str]
) -> SqlStatement:
    """Build SQL to re-date a *dq_metrics* run's *run_time*.

    Args:
        metrics_fqn: Fully-qualified *dq_metrics* table name.
        run_id: The run identifier to match.
        target_iso: Target timestamp literal body (*YYYY-MM-DD HH:MM:SS*).
        marker: The executor's named parameter marker function.

    Returns:
        An ``UPDATE`` template and its bound values.
    """
    return (
        f"UPDATE {metrics_fqn} SET run_time = {_ts(marker)} WHERE run_id = {marker('run_id')}",
        {"run_id": run_id, "target_iso": target_iso},
    )


def build_redate_runs_sql(
    runs_fqn: str, run_id: str, target_iso: str, duration_seconds: int = 45, *, marker: Callable[[str], str]
) -> SqlStatement:
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
        marker: The executor's named parameter marker function.

    Returns:
        An ``UPDATE`` template and its bound values.
    """
    start = _ts(marker)
    end = f"{start} + INTERVAL {int(duration_seconds)} SECONDS"
    return (
        f"UPDATE {runs_fqn} SET created_at = {start}, updated_at = {end} WHERE run_id = {marker('run_id')}",
        {"run_id": run_id, "target_iso": target_iso},
    )


def build_redate_versions_sql(
    versions_fqn: str, binding_id: str, version: int, target_iso: str, *, marker: Callable[[str], str]
) -> SqlStatement:
    """Build SQL to re-date a ``dq_monitored_table_versions`` freeze's *created_at*.

    A binding's version freezes are written at seed-time "now" (see
    ``MonitoredTableVersionService.freeze_new_version``), but every run in the
    demo trend is back-dated into the past. Left unmoved, every freeze would sit
    *after* every re-dated run, so ``annotate_trend_versions`` (which stamps each
    trend point with the highest version whose freeze is at/-before the run
    instant) would resolve every point to version 0 and the results-over-time
    chart would show no version markers. Re-dating each freeze's *created_at*
    into the historical window places the version bumps mid-timeline so the
    trend resolves increasing versions and the markers appear.

    Args:
        versions_fqn: Fully-qualified *dq_monitored_table_versions* table name.
        binding_id: The monitored-table binding whose freeze to re-date.
        version: The version integer identifying the freeze row.
        target_iso: Target timestamp literal body (*YYYY-MM-DD HH:MM:SS*).
        marker: The executor's named parameter marker function.

    Returns:
        An ``UPDATE`` template and its bound values.
    """
    return (
        f"UPDATE {versions_fqn} SET created_at = {_ts(marker)} "
        + f"WHERE binding_id = {marker('binding_id')} AND version = {marker('version')}",
        {"binding_id": binding_id, "version": int(version), "target_iso": target_iso},
    )


def build_delete_metrics_sql(metrics_fqn: str, run_id: str, *, marker: Callable[[str], str]) -> SqlStatement:
    """Build SQL to delete a run's *dq_metrics* rows.

    Used to discard a throwaway run (e.g. a validation-gate baseline run) so it
    cannot win a "latest published run" selection against the re-dated trend.

    Args:
        metrics_fqn: Fully-qualified *dq_metrics* table name.
        run_id: The run identifier whose rows to delete.
        marker: The executor's named parameter marker function.

    Returns:
        A ``DELETE`` template and its bound values.
    """
    return f"DELETE FROM {metrics_fqn} WHERE run_id = {marker('run_id')}", {"run_id": run_id}


def build_delete_runs_sql(runs_fqn: str, run_id: str, *, marker: Callable[[str], str]) -> SqlStatement:
    """Build SQL to delete a run's *dq_validation_runs* row.

    Used to discard a throwaway run (e.g. a validation-gate baseline run) so it
    cannot win a "latest published run" selection against the re-dated trend.

    Args:
        runs_fqn: Fully-qualified *dq_validation_runs* table name.
        run_id: The run identifier whose row to delete.
        marker: The executor's named parameter marker function.

    Returns:
        A ``DELETE`` template and its bound values.
    """
    return f"DELETE FROM {runs_fqn} WHERE run_id = {marker('run_id')}", {"run_id": run_id}


def build_delete_orphan_metrics_sql(metrics_fqn: str, runs_fqn: str) -> str:
    """Build SQL to delete *dq_metrics* rows whose run has no *dq_validation_runs* row.

    A run's *dq_metrics* rows and its *dq_validation_runs* row are written by
    the same serverless job, but the metrics can trickle in over several
    seconds. The validation gate deletes each throwaway run from BOTH tables
    once its misfire assertions pass (see ``_delete_run``); if a late batch of
    that job's metric rows lands *after* the delete, it survives as a
    "``run_id`` with metrics but no validation-run row" — a stray real-wall-clock
    trend point on the dimension/severity charts. Every legitimate weekly run
    keeps a (re-dated) *dq_validation_runs* row, so a run_id present in
    *dq_metrics* but absent from *dq_validation_runs* is definitionally such a
    deleted-gate-run leftover. This anti-join delete strips exactly those
    orphans in one statement, run once after the trend is built and all gate
    jobs have quiesced.

    Args:
        metrics_fqn: Fully-qualified *dq_metrics* table name.
        runs_fqn: Fully-qualified *dq_validation_runs* table name.

    Returns:
        A ``DELETE`` statement removing metric rows with no matching run row.
    """
    return f"DELETE FROM {metrics_fqn} WHERE run_id NOT IN (SELECT run_id FROM {runs_fqn} WHERE run_id IS NOT NULL)"


def build_redate_latest_history_sql(
    history_fqn: str, scope_type: str, scope_key: str, target_iso: str, *, marker: Callable[[str], str]
) -> SqlStatement:
    """Build SQL to re-date the most recently appended *dq_score_history* row of a scope.

    Used when re-dating a point that *ScoreCacheService* just appended
    (``computed_at = now()``) rather than inserting a fresh back-dated row.

    Args:
        history_fqn: Fully-qualified *dq_score_history* table name.
        scope_type: Scope type, one of ``"table"``, ``"product"`` or ``"global"``.
        scope_key: Scope key identifying the trend series.
        target_iso: Target timestamp literal body (*YYYY-MM-DD HH:MM:SS*).
        marker: The executor's named parameter marker function.

    Returns:
        An ``UPDATE`` template and its bound values.
    """
    return (
        f"UPDATE {history_fqn} SET computed_at = {_ts(marker)}, run_time = {_ts(marker)} "
        + f"WHERE scope_type = {marker('scope_type')} AND scope_key = {marker('scope_key')} AND computed_at = ("
        + f"SELECT MAX(computed_at) FROM {history_fqn} "
        + f"WHERE scope_type = {marker('scope_type')} AND scope_key = {marker('scope_key')})",
        {"scope_type": scope_type, "scope_key": scope_key, "target_iso": target_iso},
    )


def build_delete_history_after_sql(history_fqn: str, cutoff_iso: str, *, marker: Callable[[str], str]) -> SqlStatement:
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
        marker: The executor's named parameter marker function.

    Returns:
        A ``DELETE`` template and its bound values.
    """
    return f"DELETE FROM {history_fqn} WHERE computed_at > {_ts(marker)}", {"target_iso": cutoff_iso}
