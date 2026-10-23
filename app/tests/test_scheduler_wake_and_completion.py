"""Scheduler wake signal and prompt completion of runs started without a watching page.

With no schedules configured the scheduler sleeps an hour between ticks, so a
run started from the UI (with no page left open to refresh scores) used to
show no score or last-run for up to an hour — and a run whose job ended before
the runner wrote its result stayed RUNNING until the daily sweep. These tests
cover the wake marker that cuts the idle sleep short, run-set tracking, and
the immediate FAILED / CANCELED marking of runs whose job ended without a row.
"""

import asyncio
import json
from collections.abc import Callable, Iterable
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, create_autospec

import pytest
from databricks.sdk.service.jobs import (
    BaseRun,
    JobParameter,
    RunLifeCycleState,
    RunResultState,
    RunState,
)

from databricks_labs_dqx_app.backend.services.monitored_table_service import MonitoredTableService
from databricks_labs_dqx_app.backend.services.scheduler_wake import SchedulerWakeMarker
from databricks_labs_dqx_app.backend.services.score_cache_service import ScoreCacheService


async def _wait_until(predicate: Callable[[], bool], timeout: float = 3.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.01)


class TestSchedulerWakeMarker:
    def test_every_signal_changes_the_version(self, tmp_path):
        marker = SchedulerWakeMarker(tmp_path / "wake")
        assert marker.version() is None
        marker.signal()
        first = marker.version()
        marker.signal()
        assert first is not None
        assert marker.version() != first

    def test_signal_never_raises_when_the_marker_cannot_be_written(self, tmp_path):
        SchedulerWakeMarker(tmp_path / "missing-dir" / "wake").signal()

    def test_compact_keeps_the_marker_small(self, tmp_path):
        marker = SchedulerWakeMarker(tmp_path / "wake")
        for _ in range(1100):
            marker.signal()
        version = marker.compact()
        assert version is not None
        assert version[1] == 0
        assert marker.version() == version


class TestWaitForNextTick:
    """Idle sleeps are long, but a wake from any worker cuts them short."""

    @staticmethod
    def _scheduler(make_scheduler, tmp_path, *, gap: float = 0.0) -> tuple[Any, SchedulerWakeMarker]:
        marker = SchedulerWakeMarker(tmp_path / "wake")
        svc, _mocks = make_scheduler(wake_marker=marker, wake_check_seconds=0.01, wake_min_tick_gap_seconds=gap)
        return svc, marker

    async def test_idle_sleep_lasts_without_a_wake(self, make_scheduler, tmp_path):
        svc, _marker = self._scheduler(make_scheduler, tmp_path)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(svc.wait_for_next_tick(False), timeout=0.2)

    async def test_wake_ends_an_idle_sleep(self, make_scheduler, tmp_path):
        svc, marker = self._scheduler(make_scheduler, tmp_path)
        marker.signal()
        await asyncio.wait_for(svc.wait_for_next_tick(False), timeout=2)

    async def test_wake_does_not_shorten_the_active_cadence(self, make_scheduler, tmp_path):
        svc, marker = self._scheduler(make_scheduler, tmp_path)
        marker.signal()
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(svc.wait_for_next_tick(True), timeout=0.2)

    async def test_reload_still_ends_an_idle_sleep(self, make_scheduler, tmp_path):
        svc, _marker = self._scheduler(make_scheduler, tmp_path)
        svc.reload()
        await asyncio.wait_for(svc.wait_for_next_tick(False), timeout=2)

    async def test_wake_waits_out_the_minimum_tick_gap(self, make_scheduler, tmp_path):
        svc, marker = self._scheduler(make_scheduler, tmp_path, gap=0.3)
        marker.signal()
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(svc.wait_for_next_tick(False), timeout=0.1)


class TestWakeTriggersTick:
    async def test_wake_runs_a_tick_while_idle(self, make_scheduler, tmp_path):
        marker = SchedulerWakeMarker(tmp_path / "wake")
        svc, mocks = make_scheduler(wake_marker=marker, wake_check_seconds=0.01, distinct_sql=True)

        def config_loads() -> int:
            return sum(1 for c in mocks.oltp.query.call_args_list if c.args and "dq_schedule_configs" in c.args[0])

        svc.start()
        try:
            await _wait_until(lambda: config_loads() == 1)
            await asyncio.sleep(0.1)
            assert config_loads() == 1  # no schedules: idle, so no second tick on its own
            marker.signal()
            await _wait_until(lambda: config_loads() == 2)
        finally:
            await svc.stop()


def _ended_run(app_run_id: str, life_cycle: str, result: str | None, message: str = "") -> BaseRun:
    return BaseRun(
        run_id=99,
        job_parameters=[
            JobParameter(name="run_id", value=app_run_id),
            JobParameter(name="task_type", value="dryrun"),
            JobParameter(name="config_json", value=json.dumps({"source_table_fqn": "main.sales.orders"})),
        ],
        state=RunState(
            life_cycle_state=RunLifeCycleState(life_cycle),
            result_state=RunResultState(result) if result else None,
            state_message=message,
        ),
    )


def _completion_scheduler(
    make_scheduler,
    *,
    completed_runs: Iterable[BaseRun] = (),
    rows: Iterable[tuple[str, str, str, str]] = (),
) -> tuple[Any, SimpleNamespace, MagicMock, MagicMock]:
    """A scheduler with a manual-run run set holding run ``r1`` and a fake Jobs API + run table."""
    score_cache = create_autospec(ScoreCacheService, instance=True)
    score_cache.refresh_all_for_tables.return_value = (1, 0)
    monitored = create_autospec(MonitoredTableService, instance=True)
    svc, mocks = make_scheduler(
        catalog="main",
        schema="dqx",
        tmp_schema="dqx_tmp",
        distinct_sql=True,
        job_id="7",
        score_cache_service=score_cache,
        monitored_table_service=monitored,
    )
    mocks.oltp.query.side_effect = lambda stmt, *a, **k: [("r1",)] if "dq_run_set_members" in stmt else []
    completed = list(completed_runs)
    mocks.ws.jobs.list_runs.side_effect = lambda **kwargs: list(completed) if kwargs.get("completed_only") else []
    table_rows = list(rows)

    def query(stmt: str, *a: object, **k: object) -> list[tuple[str, str, str]]:
        # Mirror the terminal-row filter: only non-RUNNING rows match.
        return [(rid, fqn, view) for rid, fqn, view, status in table_rows if status != "RUNNING"]

    mocks.sql.query.side_effect = query
    return svc, mocks, score_cache, monitored


def _updates(mocks: SimpleNamespace) -> list[str]:
    return [c.args[0] for c in mocks.sql.execute.call_args_list if c.args and "UPDATE" in c.args[0]]


class TestPromptCompletionForUnwatchedRuns:
    NOW = datetime(2026, 5, 1, 9, 0, 0, tzinfo=timezone.utc)
    LATER = NOW + timedelta(minutes=3)  # past the Jobs API grace for never-seen-active runs

    @pytest.mark.parametrize("status", ["SUCCESS", "FAILED", "CANCELED"])
    def test_terminal_row_refreshes_score_and_last_run(self, make_scheduler, status):
        svc, _mocks, score_cache, monitored = _completion_scheduler(
            make_scheduler, rows=[("r1", "main.sales.orders", "", status)]
        )

        svc.refresh_scores_for_completed_runs(self.NOW)  # tracks the new run set's run
        score_cache.refresh_all_for_tables.assert_not_called()

        svc.refresh_scores_for_completed_runs(self.LATER)
        score_cache.refresh_all_for_tables.assert_called_once_with(["main.sales.orders"])
        monitored.refresh_run_timestamps.assert_called_once_with(["main.sales.orders"])

    @pytest.mark.parametrize(
        ("life_cycle", "result", "expected"),
        [
            ("TERMINATED", "CANCELED", "CANCELED"),
            ("TERMINATED", "FAILED", "FAILED"),
            ("TERMINATED", "TIMEDOUT", "FAILED"),
            ("INTERNAL_ERROR", None, "FAILED"),
            ("SKIPPED", None, "FAILED"),
        ],
    )
    def test_job_ended_without_a_row_is_marked_at_once(self, make_scheduler, life_cycle, result, expected):
        svc, mocks, score_cache, monitored = _completion_scheduler(
            make_scheduler,
            completed_runs=[_ended_run("r1", life_cycle, result, message="Task failed:\nboom")],
            rows=[("r1", "main.sales.orders", "", "RUNNING")],
        )

        svc.refresh_scores_for_completed_runs(self.NOW)
        svc.refresh_scores_for_completed_runs(self.LATER)

        updates = _updates(mocks)
        assert len(updates) == 1
        assert f"status = '{expected}'" in updates[0]
        assert "WHERE run_id = 'r1' AND status = 'RUNNING'" in updates[0]
        assert "error_message = 'Task failed: boom'" in updates[0]  # collapsed to one line
        score_cache.refresh_all_for_tables.assert_called_once_with(["main.sales.orders"])
        monitored.refresh_run_timestamps.assert_called_once_with(["main.sales.orders"])

        # Completed: later ticks neither look it up nor update it again.
        svc.refresh_scores_for_completed_runs(self.LATER + timedelta(minutes=1))
        assert len(_updates(mocks)) == 1

    def test_ended_job_without_a_message_gets_a_generic_one(self, make_scheduler):
        svc, mocks, _score_cache, _monitored = _completion_scheduler(
            make_scheduler,
            completed_runs=[_ended_run("r1", "INTERNAL_ERROR", None)],
            rows=[("r1", "main.sales.orders", "", "RUNNING")],
        )

        svc.refresh_scores_for_completed_runs(self.NOW)
        svc.refresh_scores_for_completed_runs(self.LATER)

        assert "error_message = 'Run finished with state: INTERNAL_ERROR'" in _updates(mocks)[0]

    def test_succeeded_job_without_a_row_is_never_marked(self, make_scheduler):
        # The runner owns a SUCCESS row; a lagging read must not be overwritten.
        svc, mocks, score_cache, _monitored = _completion_scheduler(
            make_scheduler,
            completed_runs=[_ended_run("r1", "TERMINATED", "SUCCESS")],
            rows=[("r1", "main.sales.orders", "", "RUNNING")],
        )

        svc.refresh_scores_for_completed_runs(self.NOW)
        svc.refresh_scores_for_completed_runs(self.LATER)

        assert _updates(mocks) == []
        score_cache.refresh_all_for_tables.assert_not_called()

    def test_jobs_api_failure_for_ended_runs_falls_back_quietly(self, make_scheduler):
        svc, mocks, score_cache, _monitored = _completion_scheduler(
            make_scheduler, rows=[("r1", "main.sales.orders", "", "RUNNING")]
        )

        def list_runs(**kwargs: object) -> list[BaseRun]:
            if kwargs.get("completed_only"):
                raise RuntimeError("jobs api down")
            return []

        mocks.ws.jobs.list_runs.side_effect = list_runs

        svc.refresh_scores_for_completed_runs(self.NOW)
        svc.refresh_scores_for_completed_runs(self.LATER)  # must not raise

        assert _updates(mocks) == []
        score_cache.refresh_all_for_tables.assert_not_called()
