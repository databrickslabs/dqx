"""Unit tests for ``JobService.list_dryrun_rows`` duration derivation (B2-126).

``dq_validation_runs`` has no duration column, so the run's real wall-clock
runtime is computed in SQL from the RUNNING-placeholder → terminal-row span.
These tests pin the generated SQL (the value warehouse-side, so we assert the
formula rather than execute it) to catch regressions in the derivation — in
particular that the completion instant comes from ``MAX(COALESCE(updated_at,
created_at))`` and NOT ``MAX(created_at)`` (which the runner back-dates to
exclude cluster startup, understating the runtime — the original bug).
"""

from __future__ import annotations

from unittest.mock import MagicMock, create_autospec

import pytest

from databricks_labs_dqx_app.backend.services.job_service import JobService


@pytest.fixture
def job_service(sql_executor_mock: MagicMock) -> JobService:
    from databricks.sdk import WorkspaceClient

    ws = create_autospec(WorkspaceClient, instance=True)
    return JobService(ws=ws, job_id="123", sql=sql_executor_mock)


def _dryrun_sql(job_service: JobService, sql_executor_mock: MagicMock) -> str:
    sql_executor_mock.query_dicts.return_value = []
    job_service.list_dryrun_rows("cat.sch.dq_validation_runs")
    sql_executor_mock.query_dicts.assert_called_once()
    return sql_executor_mock.query_dicts.call_args.args[0]


class TestListDryrunRowsDuration:
    def test_projects_duration_seconds_column(self, job_service: JobService, sql_executor_mock: MagicMock) -> None:
        sql = _dryrun_sql(job_service, sql_executor_mock)
        assert "AS duration_seconds" in sql

    def test_span_uses_placeholder_start_and_coalesced_end(
        self, job_service: JobService, sql_executor_mock: MagicMock
    ) -> None:
        sql = _dryrun_sql(job_service, sql_executor_mock)
        # Start = earliest created_at (the app-written RUNNING placeholder, stamped
        # at job submission so it spans cluster startup).
        assert "MIN(created_at) OVER (PARTITION BY run_id) AS run_started_at" in sql
        # End = latest completion instant. Must coalesce so the placeholder's NULL
        # updated_at doesn't win the MAX, and must NOT use MAX(created_at) (the
        # runner back-dates the terminal created_at, excluding startup).
        assert "MAX(COALESCE(updated_at, created_at)) OVER (PARTITION BY run_id) AS run_ended_at" in sql
        assert "timestampdiff(SECOND, run_started_at, run_ended_at)" in sql

    def test_duration_gated_on_placeholder_and_positive_span(
        self, job_service: JobService, sql_executor_mock: MagicMock
    ) -> None:
        sql = _dryrun_sql(job_service, sql_executor_mock)
        # No placeholder (old runs) or a non-positive span (still RUNNING) => NULL,
        # so the UI shows an em dash / live tick rather than a misleading value.
        assert "WHEN has_placeholder > 0 AND run_ended_at > run_started_at" in sql
        assert "ELSE NULL END AS duration_seconds" in sql
