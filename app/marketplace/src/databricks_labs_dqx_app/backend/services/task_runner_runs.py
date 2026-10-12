"""Read task-runner job runs from the Jobs API instead of the Delta run tables.

Every validation and profiling run is a run of the task-runner job, and its
job parameters already carry what callers need: the app ``run_id``, the
``task_type`` and the ``config_json`` holding ``source_table_fqn``. Reading run
state here costs no SQL warehouse time, so pollers can learn that runs have
finished (or failed) without keeping the warehouse awake.
"""

import asyncio
import itertools
import json
from dataclasses import dataclass
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import BaseRun

from databricks_labs_dqx_app.backend.cache import app_cache

# Lifecycle states that mean the job run has stopped for good — the same set
# ``run_status_manager.reconcile_running_rows`` treats as terminal.
TERMINAL_LIFECYCLE_STATES = frozenset({"TERMINATED", "INTERNAL_ERROR", "SKIPPED"})

# Terminal result states that are NOT a failure. Anything else terminal reads
# as FAILED, matching how the reconcile path flips a stale placeholder.
_NON_FAILURE_RESULT_STATES = frozenset({"SUCCESS", "CANCELED"})

_PAGE_SIZE = 25

# How many of the most recent completed runs the failure feed looks at. Count-
# bounded rather than time-bounded so a tab that was hidden (polling paused)
# still catches up on failures from while it was away.
RECENT_RUNS_LIMIT = 100


@dataclass(frozen=True)
class TaskRunnerRun:
    """One task-runner job run, reduced to the fields the app reads from it."""

    job_run_id: int
    app_run_id: str
    task_type: str
    # None when the config was staged out of the job parameters (oversized run).
    source_table_fqn: str | None
    is_preview: bool
    life_cycle_state: str | None
    result_state: str | None
    start_time_ms: int | None

    @property
    def is_terminal(self) -> bool:
        return self.life_cycle_state in TERMINAL_LIFECYCLE_STATES

    @property
    def is_failed(self) -> bool:
        return self.is_terminal and self.result_state not in _NON_FAILURE_RESULT_STATES

    @property
    def created_at(self) -> str | None:
        if self.start_time_ms is None:
            return None
        return datetime.fromtimestamp(self.start_time_ms / 1000, tz=timezone.utc).isoformat()


def parse_task_runner_run(run: BaseRun) -> TaskRunnerRun | None:
    """Map a Jobs API run to :class:`TaskRunnerRun`; None when it carries no app run id."""
    params = {p.name: p.value if p.value is not None else p.default for p in run.job_parameters or [] if p.name}
    app_run_id = (params.get("run_id") or "").strip()
    if not app_run_id or run.run_id is None:
        return None
    try:
        config = json.loads(params.get("config_json") or "{}")
    except ValueError:
        config = {}
    if not isinstance(config, dict):
        config = {}
    source = config.get("source_table_fqn")
    state = run.state
    return TaskRunnerRun(
        job_run_id=run.run_id,
        app_run_id=app_run_id,
        task_type=params.get("task_type") or "",
        source_table_fqn=source if isinstance(source, str) and source else None,
        # Ad-hoc previews are submitted with ``skip_history`` (no run-history row);
        # newer runners tag them ``run_type='preview'``. Neither shows in history.
        is_preview=bool(config.get("skip_history")) or config.get("run_type") == "preview",
        life_cycle_state=state.life_cycle_state.value if state and state.life_cycle_state else None,
        result_state=state.result_state.value if state and state.result_state else None,
        start_time_ms=run.start_time,
    )


def list_recent_completed_runs(ws: WorkspaceClient, job_id: int, max_runs: int) -> list[TaskRunnerRun]:
    """Return the *max_runs* most recently started completed runs of *job_id*, newest first."""
    runs = ws.jobs.list_runs(job_id=job_id, completed_only=True, limit=_PAGE_SIZE)
    parsed = (parse_task_runner_run(run) for run in itertools.islice(runs, max_runs))
    return [run for run in parsed if run is not None]


def list_active_app_run_ids(ws: WorkspaceClient, job_id: int) -> set[str]:
    """Return the app run ids of every run of *job_id* that has not finished yet."""
    parsed = (
        parse_task_runner_run(run) for run in ws.jobs.list_runs(job_id=job_id, active_only=True, limit=_PAGE_SIZE)
    )
    return {run.app_run_id for run in parsed if run is not None}


@app_cache.cached("jobs:recent-completed:{job_id}", ttl=30, reliable=True)
async def cached_recent_completed_runs(ws: WorkspaceClient, job_id: int) -> list[TaskRunnerRun]:
    """:func:`list_recent_completed_runs` shared across requests for 30s.

    Every open tab polls the failure feed, so the cache keeps it at one Jobs
    API call per window regardless of how many users have the app open.
    """
    return await asyncio.to_thread(list_recent_completed_runs, ws, job_id, RECENT_RUNS_LIMIT)
