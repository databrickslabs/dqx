import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from databricks_labs_dqx_app.backend.services.scheduler_service import SchedulerService

logger = logging.getLogger(__name__)

_scheduler: "SchedulerService | None" = None


def get_scheduler() -> "SchedulerService | None":
    return _scheduler


def set_scheduler(sched: "SchedulerService | None") -> None:
    global _scheduler  # noqa: PLW0603
    _scheduler = sched


def notify_scheduler() -> None:
    sched = _scheduler
    if sched is not None:
        try:
            sched.reload()
        except Exception:
            logger.exception("Failed to notify scheduler; it will retry on the next poll")
