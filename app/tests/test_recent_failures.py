"""Unit tests for the recent-failures endpoints.

Both ``GET /dryrun/runs/recent-failures`` and ``GET /profiler/runs/recent-failures``
feed the app-wide toast watcher, which every open tab polls. They read failed
task-runner job runs from the Jobs API (via :class:`JobService`) so the poll
never keeps the SQL warehouse awake. These tests verify:

1. Only the endpoint's own task types are returned (validation vs. profile).
2. The validation feed honours the caller's catalog access.
3. The result is bounded at _RECENT_FAILURES_LIMIT with the minimal RunFailureOut shape.
4. The run table is queried only for runs whose config was staged out of the
   job parameters, and never otherwise.
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
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor


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
    svc = create_autospec(JobService, instance=True)
    svc.lookup_source_tables.return_value = {}
    return svc


@pytest.fixture
def sql_executor() -> MagicMock:
    sql = create_autospec(SqlExecutor, instance=True)
    sql.fqn.side_effect = lambda name: f"dqx.dqx_studio.{name}"
    return sql


async def _validation(job_svc: MagicMock, sql: MagicMock, catalogs: frozenset[str] = frozenset({"main"})):
    return await list_recent_validation_failures(job_svc=job_svc, user_catalogs=catalogs, sql=sql)


class TestListRecentValidationFailures:
    async def test_returns_only_validation_task_types(self, job_service_mock, sql_executor):
        job_service_mock.list_recent_failed_runs.return_value = [
            _failed_run("run-dryrun"),
            _failed_run("run-scheduled", task_type="scheduled"),
            _failed_run("run-profile", task_type="profile"),
        ]

        result = await _validation(job_service_mock, sql_executor)

        assert [r.run_id for r in result] == ["run-dryrun", "run-scheduled"]
        assert all(r.status == "FAILED" for r in result)

    async def test_excludes_runs_from_inaccessible_catalogs(self, job_service_mock, sql_executor):
        job_service_mock.list_recent_failed_runs.return_value = [
            _failed_run("run-visible", fqn="main.public.orders"),
            _failed_run("run-hidden", fqn="restricted.public.orders"),
        ]

        result = await _validation(job_service_mock, sql_executor)

        assert [r.run_id for r in result] == ["run-visible"]

    async def test_includes_sql_check_prefix_runs(self, job_service_mock, sql_executor):
        job_service_mock.list_recent_failed_runs.return_value = [
            _failed_run("run-sql", fqn="__sql_check__/my_check"),
        ]

        result = await _validation(job_service_mock, sql_executor, catalogs=frozenset())

        assert [r.source_table_fqn for r in result] == ["__sql_check__/my_check"]

    async def test_result_bounded_at_limit(self, job_service_mock, sql_executor):
        job_service_mock.list_recent_failed_runs.return_value = [
            _failed_run(f"f-{i}") for i in range(DRYRUN_LIMIT + 10)
        ]

        result = await _validation(job_service_mock, sql_executor)

        assert len(result) == DRYRUN_LIMIT

    async def test_returns_minimal_fields_only(self, job_service_mock, sql_executor):
        job_service_mock.list_recent_failed_runs.return_value = [_failed_run("run-failed")]

        result = await _validation(job_service_mock, sql_executor)

        assert result == [
            RunFailureOut(
                run_id="run-failed",
                source_table_fqn="main.public.orders",
                status="FAILED",
                created_at="2026-09-21T14:13:20+00:00",
            )
        ]

    async def test_never_queries_the_warehouse_when_configs_are_inline(self, job_service_mock, sql_executor):
        job_service_mock.list_recent_failed_runs.return_value = [_failed_run("run-failed")]

        await _validation(job_service_mock, sql_executor)

        job_service_mock.lookup_source_tables.assert_not_called()
        sql_executor.query.assert_not_called()
        sql_executor.query_dicts.assert_not_called()

    async def test_staged_config_runs_resolve_their_table_from_the_run_table(self, job_service_mock, sql_executor):
        job_service_mock.list_recent_failed_runs.return_value = [
            _failed_run("run-inline"),
            _failed_run("run-staged", fqn=None),
            _failed_run("run-unknown", fqn=None),
        ]
        job_service_mock.lookup_source_tables.return_value = {"run-staged": "main.public.big"}

        result = await _validation(job_service_mock, sql_executor)

        job_service_mock.lookup_source_tables.assert_called_once_with(
            "dqx.dqx_studio.dq_validation_runs", ["run-staged", "run-unknown"]
        )
        # A staged run whose table cannot be resolved is dropped (the catalog
        # filter cannot be applied to it).
        assert [(r.run_id, r.source_table_fqn) for r in result] == [
            ("run-inline", "main.public.orders"),
            ("run-staged", "main.public.big"),
        ]

    async def test_empty_list_when_no_failures(self, job_service_mock, sql_executor):
        job_service_mock.list_recent_failed_runs.return_value = []

        assert await _validation(job_service_mock, sql_executor) == []


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
        job_service_mock.lookup_source_tables.assert_not_called()

    async def test_empty_list_when_no_failures(self, job_service_mock):
        job_service_mock.list_recent_failed_runs.return_value = []

        assert await list_recent_profile_failures(job_svc=job_service_mock) == []
