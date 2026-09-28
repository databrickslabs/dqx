"""Unit tests for the e2e demo-run retry helper (retries serverless INTERNAL_ERROR only)."""

from unittest.mock import Mock, create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import OperationFailed

from tests.e2e import conftest as e2e_conftest
from tests.e2e.conftest import run_job_and_validate

_INTERNAL_ERROR = "failed to reach TERMINATED or SKIPPED, got RunLifeCycleState.INTERNAL_ERROR"
_TASK_FAILED = "failed to reach TERMINATED or SKIPPED, got RunResultState.FAILED"


@pytest.fixture(autouse=True)
def _no_backoff_sleep(monkeypatch):
    """Skip the real backoff sleeps so the retry tests run instantly."""
    monkeypatch.setattr(e2e_conftest.time, "sleep", lambda *_: None)


def test_run_job_and_validate_retries_internal_error_then_succeeds():
    ws = create_autospec(WorkspaceClient)
    ws.jobs.run_now_and_wait.side_effect = [
        OperationFailed(_INTERNAL_ERROR),
        OperationFailed(_INTERNAL_ERROR),
        Mock(run_id=123),
    ]

    run_job_and_validate(ws, job_id=1, task_key="demo", max_attempts=3)

    assert ws.jobs.run_now_and_wait.call_count == 3
    ws.jobs.wait_get_run_job_terminated_or_skipped.assert_called_once()


def test_run_job_and_validate_gives_up_after_max_attempts():
    ws = create_autospec(WorkspaceClient)
    ws.jobs.run_now_and_wait.side_effect = OperationFailed(_INTERNAL_ERROR)

    with pytest.raises(OperationFailed, match="INTERNAL_ERROR"):
        run_job_and_validate(ws, job_id=1, task_key="demo", max_attempts=3)

    assert ws.jobs.run_now_and_wait.call_count == 3


def test_run_job_and_validate_does_not_retry_non_transient_failures():
    ws = create_autospec(WorkspaceClient)
    ws.jobs.run_now_and_wait.side_effect = OperationFailed(_TASK_FAILED)

    with pytest.raises(OperationFailed):
        run_job_and_validate(ws, job_id=1, task_key="demo", max_attempts=3)

    # A non-INTERNAL_ERROR failure is a real problem and is surfaced on the first attempt.
    assert ws.jobs.run_now_and_wait.call_count == 1
