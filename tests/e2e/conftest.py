import logging
import os
from datetime import timedelta
from pathlib import Path
import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat
from databricks.sdk.service.jobs import NotebookTask, Task, TerminationTypeType, Run

logger = logging.getLogger(__name__)
logging.getLogger("tests").setLevel("DEBUG")
logging.getLogger("databricks.labs.dqx").setLevel("DEBUG")

RETRY_INTERVAL_SECONDS = 30


@pytest.fixture
def library_ref() -> str:
    test_library_ref = "git+https://github.com/databrickslabs/dqx"
    if os.getenv("REF_NAME"):
        test_library_ref = f"{test_library_ref}.git@refs/pull/{os.getenv('REF_NAME')}"
    return test_library_ref


def run_notebook_job(
    notebook_path: Path,
    make_notebook,
    make_job,
    library_reference=None,
    base_parameters=None,
    timeout_minutes=30,
    task_key="notebook_task",
    ws=None,
):
    if ws is None:
        ws = WorkspaceClient()
    with open(notebook_path, "rb") as f:
        notebook = make_notebook(content=f, format=ImportFormat.SOURCE)
    notebook_fs_path = notebook.as_fuse().as_posix()
    params = base_parameters or {}
    if library_reference:
        params.setdefault("test_library_ref", library_reference)
    notebook_task = NotebookTask(notebook_path=notebook_fs_path, base_parameters=params)
    job = make_job(tasks=[Task(task_key=task_key, notebook_task=notebook_task)])
    waiter = ws.jobs.run_now_and_wait(job.job_id)
    run = ws.jobs.wait_get_run_job_terminated_or_skipped(
        run_id=waiter.run_id,
        timeout=timedelta(minutes=timeout_minutes),
        callback=lambda r: validate_run_status(r, ws),
    )
    logger.info(f"Job run {run.run_id} completed successfully for {task_key}")


def validate_run_status(run: Run, client: WorkspaceClient):
    task = run.tasks[0]
    termination_details = run.status.termination_details
    assert (
        termination_details.type == TerminationTypeType.SUCCESS
    ), f"Run of '{task.task_key}' failed with message: {client.jobs.get_run_output(task.run_id).error}"
