"""``JobService.get_latest_run_result_row`` backs the external status endpoints.

A throwaway preview run must never stand in for a table's real health, so the
lookup has to exclude ``run_type = 'preview'`` like every other run reader.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from databricks_labs_dqx_app.backend.services.job_service import JobService


def test_latest_run_lookup_excludes_preview_runs(sql_executor_mock: MagicMock) -> None:
    sql_executor_mock.query_dicts.return_value = [{"run_id": "r1", "status": "SUCCESS"}]
    svc = JobService(ws=MagicMock(), job_id="1", sql=sql_executor_mock)

    row = svc.get_latest_run_result_row("c.s.dq_validation_runs", "main.sales.orders")

    assert row == {"run_id": "r1", "status": "SUCCESS"}
    sql = sql_executor_mock.query_dicts.call_args.args[0]
    assert "COALESCE(run_type, 'dryrun') != 'preview'" in sql
    assert "status != 'RUNNING'" in sql


def test_latest_run_lookup_returns_none_when_no_runs(sql_executor_mock: MagicMock) -> None:
    sql_executor_mock.query_dicts.return_value = []
    svc = JobService(ws=MagicMock(), job_id="1", sql=sql_executor_mock)

    assert svc.get_latest_run_result_row("c.s.dq_validation_runs", "main.sales.orders") is None
