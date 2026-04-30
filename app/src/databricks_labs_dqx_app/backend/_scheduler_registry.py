from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from databricks_labs_dqx_app.backend.services.scheduler_service import SchedulerService

_scheduler: SchedulerService | None = None


def get_scheduler() -> SchedulerService | None:
    return _scheduler


def set_scheduler(sched: SchedulerService | None) -> None:
    global _scheduler  # noqa: PLW0603
    _scheduler = sched


def notify_scheduler() -> None:
    sched = _scheduler
    if sched is not None:
        sched.reload()
