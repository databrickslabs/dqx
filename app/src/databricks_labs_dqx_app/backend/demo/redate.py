"""Pure SQL builders that re-date genuine engine-run rows.

These helpers fabricate a multi-week quality trend from real DQ runs by
shifting each run's timestamps in Delta (*dq_metrics* / *dq_validation_runs*)
and by inserting or adjusting back-dated rows in the OLTP *dq_score_history*
table. Every value is engine-computed; only the timestamps are moved.

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


def build_redate_runs_sql(runs_fqn: str, run_id: str, target_iso: str) -> str:
    """Build SQL to re-date a *dq_validation_runs* run's *created_at* / *updated_at*.

    Args:
        runs_fqn: Fully-qualified *dq_validation_runs* table name.
        run_id: The run identifier to match.
        target_iso: Target timestamp literal body (*YYYY-MM-DD HH:MM:SS*).

    Returns:
        An ``UPDATE`` statement targeting the matched run.
    """
    return (
        f"UPDATE {runs_fqn} SET created_at = {_ts(target_iso)}, updated_at = {_ts(target_iso)} "
        f"WHERE run_id = '{escape_sql_string(run_id)}'"
    )


def build_insert_history_sql(
    history_fqn: str,
    scope_type: str,
    scope_key: str,
    *,
    score: float,
    failed_tests: int | None,
    total_tests: int | None,
    target_iso: str,
) -> str:
    """Build SQL to insert a back-dated *dq_score_history* row.

    Args:
        history_fqn: Fully-qualified *dq_score_history* table name.
        scope_type: Scope type, one of ``"table"``, ``"product"`` or ``"global"``.
        scope_key: Scope key identifying the trend series.
        score: The engine-computed quality score.
        failed_tests: Number of failed tests, or None to render ``NULL``.
        total_tests: Number of total tests, or None to render ``NULL``.
        target_iso: Target timestamp literal body (*YYYY-MM-DD HH:MM:SS*).

    Returns:
        An ``INSERT INTO`` statement with both *run_time* and *computed_at*
        back-dated to *target_iso*.
    """
    failed = str(int(failed_tests)) if failed_tests is not None else "NULL"
    total = str(int(total_tests)) if total_tests is not None else "NULL"
    return (
        f"INSERT INTO {history_fqn} "
        f"(scope_type, scope_key, score, failed_tests, total_tests, run_time, computed_at) VALUES "
        f"('{escape_sql_string(scope_type)}', '{escape_sql_string(scope_key)}', {float(score)}, "
        f"{failed}, {total}, {_ts(target_iso)}, {_ts(target_iso)})"
    )


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
