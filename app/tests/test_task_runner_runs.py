"""Tests for reading task-runner job runs from the Jobs API (no SQL warehouse)."""

import json
from unittest.mock import create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import BaseRun, JobParameter, RunLifeCycleState, RunResultState, RunState

from databricks_labs_dqx_app.backend.services.task_runner_runs import (
    RECENT_RUNS_LIMIT,
    cached_recent_completed_runs,
    list_active_app_run_ids,
    list_recent_completed_runs,
    parse_task_runner_run,
)


def make_run(
    *,
    job_run_id: int = 1,
    app_run_id: str | None = "run-1",
    task_type: str = "dryrun",
    config: dict | str | None = None,
    life_cycle: RunLifeCycleState | None = RunLifeCycleState.TERMINATED,
    result: RunResultState | None = RunResultState.SUCCESS,
    start_time: int | None = 1_790_000_000_000,
) -> BaseRun:
    if config is None:
        config = {"source_table_fqn": "main.sales.orders", "checks": []}
    params = [JobParameter(name="task_type", value=task_type)]
    if app_run_id is not None:
        params.append(JobParameter(name="run_id", value=app_run_id))
    params.append(JobParameter(name="config_json", value=config if isinstance(config, str) else json.dumps(config)))
    return BaseRun(
        run_id=job_run_id,
        job_parameters=params,
        state=RunState(life_cycle_state=life_cycle, result_state=result),
        start_time=start_time,
    )


@pytest.fixture
def ws() -> WorkspaceClient:
    return create_autospec(WorkspaceClient, instance=True)


class TestParseTaskRunnerRun:
    def test_maps_job_parameters_and_state(self):
        run = parse_task_runner_run(make_run(job_run_id=42, app_run_id="abc", task_type="profile"))

        assert run is not None
        assert (run.job_run_id, run.app_run_id, run.task_type) == (42, "abc", "profile")
        assert run.source_table_fqn == "main.sales.orders"
        assert run.is_terminal and not run.is_failed and not run.is_preview
        assert run.created_at == "2026-09-21T14:13:20+00:00"

    def test_run_without_app_run_id_is_ignored(self):
        assert parse_task_runner_run(make_run(app_run_id=None)) is None

    def test_staged_config_has_no_source_table(self):
        run = parse_task_runner_run(make_run(config={"__manifest__": True}))

        assert run is not None and run.source_table_fqn is None

    def test_unparseable_config_has_no_source_table(self):
        run = parse_task_runner_run(make_run(config="not json"))

        assert run is not None and run.source_table_fqn is None

    def test_parameter_default_is_used_when_no_value_is_set(self):
        base = make_run()
        base.job_parameters = [
            JobParameter(name="run_id", default="from-default"),
            JobParameter(name="task_type", value="dryrun"),
        ]

        run = parse_task_runner_run(base)

        assert run is not None and run.app_run_id == "from-default"

    @pytest.mark.parametrize(
        "config",
        [
            {"source_table_fqn": "main.s.t", "skip_history": True},
            {"source_table_fqn": "main.s.t", "run_type": "preview"},
        ],
    )
    def test_previews_are_flagged(self, config):
        run = parse_task_runner_run(make_run(config=config))

        assert run is not None and run.is_preview

    @pytest.mark.parametrize(
        ("life_cycle", "result", "failed"),
        [
            (RunLifeCycleState.TERMINATED, RunResultState.FAILED, True),
            (RunLifeCycleState.TERMINATED, RunResultState.TIMEDOUT, True),
            (RunLifeCycleState.INTERNAL_ERROR, RunResultState.FAILED, True),
            (RunLifeCycleState.TERMINATED, RunResultState.SUCCESS, False),
            (RunLifeCycleState.TERMINATED, RunResultState.CANCELED, False),
            (RunLifeCycleState.RUNNING, None, False),
        ],
    )
    def test_failure_classification_matches_reconcile(self, life_cycle, result, failed):
        run = parse_task_runner_run(make_run(life_cycle=life_cycle, result=result))

        assert run is not None and run.is_failed is failed


class TestListRuns:
    def test_recent_completed_runs_are_count_bounded(self, ws):
        ws.jobs.list_runs.return_value = iter(make_run(job_run_id=i, app_run_id=f"r{i}") for i in range(500))

        runs = list_recent_completed_runs(ws, 7, max_runs=30)

        assert [run.app_run_id for run in runs] == [f"r{i}" for i in range(30)]
        ws.jobs.list_runs.assert_called_once_with(job_id=7, completed_only=True, limit=25)

    def test_active_app_run_ids(self, ws):
        ws.jobs.list_runs.return_value = [
            make_run(app_run_id="a", life_cycle=RunLifeCycleState.RUNNING, result=None),
            make_run(app_run_id=None, life_cycle=RunLifeCycleState.PENDING, result=None),
        ]

        assert list_active_app_run_ids(ws, 7) == {"a"}
        ws.jobs.list_runs.assert_called_once_with(job_id=7, active_only=True, limit=25)

    async def test_cached_runs_share_one_jobs_api_call(self, ws):
        ws.jobs.list_runs.side_effect = lambda **_: iter([make_run()])

        first = await cached_recent_completed_runs(ws, 7)
        second = await cached_recent_completed_runs(ws, 7)

        assert first == second
        ws.jobs.list_runs.assert_called_once_with(job_id=7, completed_only=True, limit=25)

    def test_recent_runs_limit_covers_the_toast_window(self):
        # The toast shows up to 50 failures; the feed must look at least that far back.
        assert RECENT_RUNS_LIMIT >= 50
