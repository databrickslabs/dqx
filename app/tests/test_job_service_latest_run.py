"""``JobService.get_latest_completed_run_result_row`` backs the external status endpoints.

A throwaway preview run must never stand in for a table's real health, so the
lookup has to exclude ``run_type = 'preview'`` like every other run reader; and
in-progress or canceled runs carry no results, so they are skipped too. The
latest run is the one that finished last, and table names match regardless of
case, as Unity Catalog identifiers do.
"""

from unittest.mock import MagicMock

from databricks_labs_dqx_app.backend.services.job_service import JobService


def test_latest_run_lookup_excludes_preview_running_and_canceled_runs(sql_executor_mock: MagicMock) -> None:
    sql_executor_mock.query_dicts.return_value = [{"run_id": "r1", "status": "SUCCESS"}]
    svc = JobService(ws=MagicMock(), job_id="1", sql=sql_executor_mock, oltp_sql=MagicMock(name="oltp_sql"))

    row = svc.get_latest_completed_run_result_row("c.s.dq_validation_runs", "main.sales.orders")

    assert row == {"run_id": "r1", "status": "SUCCESS"}
    sql = sql_executor_mock.query_dicts.call_args.args[0]
    assert "COALESCE(run_type, 'dryrun') != 'preview'" in sql
    assert "status NOT IN ('RUNNING', 'CANCELED')" in sql
    assert "lower(source_table_fqn) = lower('main.sales.orders')" in sql
    assert "ORDER BY updated_at DESC" in sql


def test_latest_run_lookup_returns_none_when_no_runs(sql_executor_mock: MagicMock) -> None:
    sql_executor_mock.query_dicts.return_value = []
    svc = JobService(ws=MagicMock(), job_id="1", sql=sql_executor_mock, oltp_sql=MagicMock(name="oltp_sql"))

    assert svc.get_latest_completed_run_result_row("c.s.dq_validation_runs", "main.sales.orders") is None


def test_by_run_lookup_excludes_preview_and_is_deterministic(sql_executor_mock: MagicMock) -> None:
    # The by-run reader backs GET /monitoring/status/run/{run_id}. A throwaway
    # preview run must not stand in for a run's health (matching the by-table
    # reader), and the lookup must be deterministic if a run_id ever has more
    # than one terminal row. Canceled runs are kept — the by-run endpoint
    # reports a canceled run as 503 rather than hiding it.
    sql_executor_mock.query_dicts.return_value = [{"run_id": "r1", "status": "SUCCESS"}]
    svc = JobService(ws=MagicMock(), job_id="1", sql=sql_executor_mock, oltp_sql=MagicMock(name="oltp_sql"))

    row = svc.get_run_result_row("c.s.dq_validation_runs", "r1")

    assert row == {"run_id": "r1", "status": "SUCCESS"}
    sql = sql_executor_mock.query_dicts.call_args.args[0]
    assert "status != 'RUNNING'" in sql
    assert "COALESCE(run_type, 'dryrun') != 'preview'" in sql
    assert "ORDER BY updated_at DESC" in sql
    assert "CANCELED" not in sql
