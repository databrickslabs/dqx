"""Unit tests for the recent-failures endpoints.

Both ``GET /dryrun/runs/recent-failures`` and ``GET /profiler/runs/recent-failures``
feed the app-wide toast watcher, which every open tab polls. They read failed
task-runner job runs from the Jobs API (via :class:`JobService`) so the poll
never keeps the SQL warehouse awake. These tests verify:

1. Only the endpoint's own task types are returned (validation vs. profile).
2. The validation feed honours the caller's catalog access.
3. The result is bounded at _RECENT_FAILURES_LIMIT with the minimal RunFailureOut shape.
4. Neither feed ever queries the SQL warehouse.
"""

from unittest.mock import MagicMock, create_autospec

import pytest

from databricks_labs_dqx_app.backend.models import RunFailureOut
from databricks_labs_dqx_app.backend.routes.v1.dryrun import (
    list_recent_validation_failures,
    _RECENT_FAILURES_LIMIT as DRYRUN_LIMIT,
)
from databricks_labs_dqx_app.backend.routes.v1.profiler import (
    list_recent_profile_failures,
    _RECENT_FAILURES_LIMIT as PROFILER_LIMIT,
)
from databricks_labs_dqx_app.backend.services.job_service import JobService
from databricks_labs_dqx_app.backend.services.task_runner_runs import TaskRunnerRun


def _failed_run(
    app_run_id: str,
    *,
    task_type: str = "dryrun",
    fqn: str | None = "main.public.orders",
) -> TaskRunnerRun:
    return TaskRunnerRun(
        job_run_id=hash(app_run_id) & 0xFFFF,
        app_run_id=app_run_id,
        task_type=task_type,
        source_table_fqn=fqn,
        is_preview=False,
        life_cycle_state="TERMINATED",
        result_state="FAILED",
        start_time_ms=1_790_000_000_000,
    )


@pytest.fixture
def job_service_mock() -> MagicMock:
    return create_autospec(JobService, instance=True)


async def _validation(job_svc: MagicMock, catalogs: frozenset[str] = frozenset({"main"})):
    return await list_recent_validation_failures(job_svc=job_svc, user_catalogs=catalogs)


class TestListRecentValidationFailures:
    async def test_returns_only_validation_task_types(self, job_service_mock):
        job_service_mock.list_recent_failed_runs.return_value = [
            _failed_run("run-dryrun"),
            _failed_run("run-scheduled", task_type="scheduled"),
            _failed_run("run-profile", task_type="profile"),
        ]

        result = await _validation(job_service_mock)

        assert [r.run_id for r in result] == ["run-dryrun", "run-scheduled"]
        assert all(r.status == "FAILED" for r in result)

    async def test_excludes_runs_from_inaccessible_catalogs(self, job_service_mock):
        job_service_mock.list_recent_failed_runs.return_value = [
            _failed_run("run-visible", fqn="main.public.orders"),
            _failed_run("run-hidden", fqn="restricted.public.orders"),
        ]

        result = await _validation(job_service_mock)

        assert [r.run_id for r in result] == ["run-visible"]

    async def test_includes_sql_check_prefix_runs(self, job_service_mock):
        job_service_mock.list_recent_failed_runs.return_value = [
            _failed_run("run-sql", fqn="__sql_check__/my_check"),
        ]

        result = await _validation(job_service_mock, catalogs=frozenset())

        assert [r.source_table_fqn for r in result] == ["__sql_check__/my_check"]

    async def test_result_bounded_at_limit(self, job_service_mock):
        job_service_mock.list_recent_failed_runs.return_value = [
            _failed_run(f"f-{i}") for i in range(DRYRUN_LIMIT + 10)
        ]

        result = await _validation(job_service_mock)

        assert len(result) == DRYRUN_LIMIT

    async def test_returns_minimal_fields_only(self, job_service_mock):
        job_service_mock.list_recent_failed_runs.return_value = [_failed_run("run-failed")]

        result = await _validation(job_service_mock)

        assert result == [
            RunFailureOut(
                run_id="run-failed",
                source_table_fqn="main.public.orders",
                status="FAILED",
                created_at="2026-09-21T14:13:20+00:00",
            )
        ]

    async def test_runs_without_a_source_table_are_skipped(self, job_service_mock):
        # The catalog filter cannot be applied to a run with no table, so it
        # is dropped rather than shown to everyone.
        job_service_mock.list_recent_failed_runs.return_value = [
            _failed_run("run-known"),
            _failed_run("run-unknown", fqn=None),
        ]

        result = await _validation(job_service_mock)

        assert [r.run_id for r in result] == ["run-known"]

    async def test_empty_list_when_no_failures(self, job_service_mock):
        job_service_mock.list_recent_failed_runs.return_value = []

        assert await _validation(job_service_mock) == []


class TestListRecentProfileFailures:
    async def test_returns_only_profile_task_types(self, job_service_mock):
        job_service_mock.list_recent_failed_runs.return_value = [
            _failed_run("run-profile", task_type="profile"),
            _failed_run("run-dryrun"),
        ]

        result = await list_recent_profile_failures(job_svc=job_service_mock)

        assert [r.run_id for r in result] == ["run-profile"]

    async def test_result_bounded_at_limit(self, job_service_mock):
        job_service_mock.list_recent_failed_runs.return_value = [
            _failed_run(f"f-{i}", task_type="profile") for i in range(PROFILER_LIMIT + 10)
        ]

        result = await list_recent_profile_failures(job_svc=job_service_mock)

        assert len(result) == PROFILER_LIMIT

    async def test_returns_minimal_fields_only(self, job_service_mock):
        job_service_mock.list_recent_failed_runs.return_value = [_failed_run("run-p", task_type="profile")]

        result = await list_recent_profile_failures(job_svc=job_service_mock)

        assert result == [
            RunFailureOut(
                run_id="run-p",
                source_table_fqn="main.public.orders",
                status="FAILED",
                created_at="2026-09-21T14:13:20+00:00",
            )
        ]

    async def test_empty_list_when_no_failures(self, job_service_mock):
        job_service_mock.list_recent_failed_runs.return_value = []

        assert await list_recent_profile_failures(job_svc=job_service_mock) == []
