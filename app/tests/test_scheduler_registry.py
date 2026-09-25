import logging
from unittest.mock import create_autospec

import pytest

from databricks_labs_dqx_app.backend import _scheduler_registry
from databricks_labs_dqx_app.backend.services.scheduler_service import SchedulerService


def test_notify_scheduler_logs_and_swallows_reload_failure(caplog: pytest.LogCaptureFixture) -> None:
    scheduler = create_autospec(SchedulerService, instance=True)
    scheduler.reload.side_effect = RuntimeError("wake failed")
    _scheduler_registry.set_scheduler(scheduler)

    try:
        with caplog.at_level(logging.WARNING, logger=_scheduler_registry.__name__):
            _scheduler_registry.notify_scheduler()
    finally:
        _scheduler_registry.set_scheduler(None)

    scheduler.reload.assert_called_once_with()
    assert "Failed to notify scheduler" in caplog.text
    assert any(record.exc_info for record in caplog.records)
