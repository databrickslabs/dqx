"""Unit tests for which runs ``JobService.list_dryrun_rows`` keeps visible.

Runs History was losing failed and canceled runs. The rows were never deleted —
the visibility gate tested ``status = 'RUNNING'`` to decide whether the app had
written a submission row for a run, and every non-successful outcome rewrites
that row's status (``reconcile_running_rows``, the per-run status poll, the
hourly stale tmp-view sweep, Cancel). Successful runs were unaffected, because
the runner appends its own terminal SUCCESS row and nothing rewrites their
placeholder — so history looked like it dropped runs at random.

The gate now keys on ``job_run_id``, which is stamped at insert and never
rewritten. As in ``test_job_service_duration.py`` the SQL runs warehouse-side,
so these tests pin the generated predicate rather than execute it.
"""

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


class TestListDryrunRowsVisibility:
    def test_submission_marker_survives_status_rewrites(
        self, job_service: JobService, sql_executor_mock: MagicMock
    ) -> None:
        """``has_submission`` must accept a reconciled placeholder, not just RUNNING."""
        sql = _dryrun_sql(job_service, sql_executor_mock)
        assert "status = 'RUNNING' OR job_run_id IS NOT NULL" in sql
        assert "AS has_submission" in sql

    def test_visibility_gate_uses_the_submission_marker(
        self, job_service: JobService, sql_executor_mock: MagicMock
    ) -> None:
        """The WHERE clause must gate on ``has_submission``, never ``has_placeholder``.

        Gating on ``has_placeholder`` is the original bug: the first reconcile of
        a failed run drops it out of history.
        """
        sql = _dryrun_sql(job_service, sql_executor_mock)
        assert "OR has_submission > 0)" in sql
        assert "OR has_placeholder > 0)" not in sql

    def test_scheduled_runs_stay_visible_without_a_submission_row(
        self, job_service: JobService, sql_executor_mock: MagicMock
    ) -> None:
        """Scheduled runs are visible on their run_type alone (no placeholder needed)."""
        sql = _dryrun_sql(job_service, sql_executor_mock)
        assert "COALESCE(run_type, 'dryrun') IN ('scheduled')" in sql

    def test_preview_runs_remain_excluded(self, job_service: JobService, sql_executor_mock: MagicMock) -> None:
        """Widening the gate must not leak throw-away preview runs into history."""
        sql = _dryrun_sql(job_service, sql_executor_mock)
        assert "COALESCE(run_type, 'dryrun') != 'preview'" in sql

    def test_duration_still_requires_a_live_placeholder(
        self, job_service: JobService, sql_executor_mock: MagicMock
    ) -> None:
        """Duration must NOT follow visibility onto ``has_submission``.

        A reconciled placeholder's ``updated_at`` is when we noticed the failure
        (the sweep is hourly), so reusing the wider marker here would report that
        gap as the runtime. The run stays visible with an em dash instead.
        """
        sql = _dryrun_sql(job_service, sql_executor_mock)
        assert "WHEN has_placeholder > 0 AND run_ended_at > run_started_at" in sql
        assert (
            "SUM(CASE WHEN status = 'RUNNING' THEN 1 ELSE 0 END) OVER (PARTITION BY run_id) AS has_placeholder" in sql
        )
