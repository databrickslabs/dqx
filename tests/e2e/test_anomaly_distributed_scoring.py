"""Exercise the exact local wheel across independent training and scoring sessions."""

import json
import logging
from datetime import timedelta

import pytest

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError, OperationFailed
from databricks.sdk.service.jobs import PerformanceTarget, RunResultState, RunTask

from tests.anomaly_e2e import PackagedAnomalyCase, safe_packaged_result, safe_packaged_task_failure

logger = logging.getLogger(__name__)


@pytest.mark.timeout(3900)
def test_run_anomaly_distributed_scoring_notebook(
    ws: WorkspaceClient, packaged_anomaly_case: PackagedAnomalyCase
) -> None:
    """Reload a persisted model and score with contributions on the suite's compute."""
    waiter = ws.jobs.run_now(job_id=packaged_anomaly_case.job_id)
    assert waiter.run_id is not None
    try:
        run = waiter.result(timeout=timedelta(minutes=60))
    except OperationFailed:
        run = ws.jobs.get_run(run_id=waiter.run_id)
    except (TimeoutError, KeyboardInterrupt):
        _cancel_anomaly_run(ws, waiter.run_id)
        raise
    if packaged_anomaly_case.serverless:
        assert run.effective_performance_target == PerformanceTarget.STANDARD
    assert run.tasks and len(run.tasks) == 2, "Both packaged anomaly tasks must be present"
    failed_tasks = [task for task in run.tasks if not task.state or task.state.result_state != RunResultState.SUCCESS]
    for task in failed_tasks:
        _report_anomaly_task_failure(ws, task)
    if failed_tasks:
        raise AssertionError("Packaged anomaly task failed; sanitized worker frames are in the test log")
    completed = set()
    for task in run.tasks:
        assert task.run_id is not None
        output = ws.jobs.get_run_output(task.run_id)
        if not packaged_anomaly_case.serverless and task.cluster_instance and task.cluster_instance.cluster_id:
            assert task.cluster_instance.cluster_id == packaged_anomaly_case.cluster_id
        assert task.task_key in {"train", "score"}
        assert output.notebook_output and output.notebook_output.result and not output.notebook_output.truncated
        evidence = safe_packaged_result(output.notebook_output.result, packaged_anomaly_case, task.task_key)
        evidence["actual_compute"] = (
            "serverless"
            if (task.effective_performance_target or run.effective_performance_target) is not None
            else "classic" if task.cluster_instance and task.cluster_instance.cluster_id else "unknown"
        )
        logger.info(f"Packaged anomaly provenance: {json.dumps(evidence, sort_keys=True)}")
        completed.add(task.task_key)
    assert completed == {"train", "score"}


def _report_anomaly_task_failure(ws: WorkspaceClient, task: RunTask) -> None:
    """Retain safe evidence even when task output is unavailable."""
    output = None
    output_error = None
    if task.run_id is not None:
        try:
            output = ws.jobs.get_run_output(task.run_id)
        except DatabricksError as error:
            output_error = type(error).__name__
    failure = safe_packaged_task_failure(task, output)
    failure["output_error_type"] = output_error
    logger.error(f"Packaged anomaly task failed: {json.dumps(failure, sort_keys=True)}")


def _cancel_anomaly_run(ws: WorkspaceClient, run_id: int) -> None:
    """Cancel only this test's run and bound teardown after an interrupted wait."""
    try:
        ws.jobs.cancel_run(run_id=run_id)
        ws.jobs.wait_get_run_job_terminated_or_skipped(run_id=run_id, timeout=timedelta(minutes=5))
    except OperationFailed:
        # A cancelled terminal run is reported as unsuccessful by the SDK waiter.
        return
    except (DatabricksError, TimeoutError):
        logger.error("Packaged anomaly run cancellation did not confirm termination before cleanup")
