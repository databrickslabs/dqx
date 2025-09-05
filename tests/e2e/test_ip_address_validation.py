import logging

from datetime import timedelta
from pathlib import Path
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat
from databricks.sdk.service.jobs import NotebookTask, Run, Task, TerminationTypeType


logger = logging.getLogger(__name__)
RETRY_INTERVAL_SECONDS = 30


def test_run_ip_address_validation_notebook(make_notebook, make_job, library_ref):
    ws = WorkspaceClient()
    path = Path(__file__).parent / "notebooks" / "ip_address_validation_notebook.py"
    with open(path, "rb") as f:
        notebook = make_notebook(content=f, format=ImportFormat.SOURCE)

    notebook_path = notebook.as_fuse().as_posix()
    notebook_task = NotebookTask(
        notebook_path=notebook_path,
        base_parameters={"test_library_ref": library_ref},
    )
    job = make_job(
        tasks=[Task(task_key="ip_address_validation_notebook", notebook_task=notebook_task)],
    )

    waiter = ws.jobs.run_now_and_wait(job.job_id)
    run = ws.jobs.wait_get_run_job_terminated_or_skipped(
        run_id=waiter.run_id,
        timeout=timedelta(minutes=30),
        callback=lambda r: validate_run_status(r, client=ws),
    )
    logging.info(f"Job run {run.run_id} completed successfully for ip_address_validation_notebook")


def validate_run_status(run: Run, client: WorkspaceClient) -> None:
    """
    Validates that a job task run completed successfully.

    Args:
      run: *Run* object returned from a *WorkspaceClient.jobs.submit(...)* command.
      client: *WorkspaceClient* object for getting task output.
    """
    task = run.tasks[0]
    termination_details = run.status.termination_details

    assert (
        termination_details.type == TerminationTypeType.SUCCESS
    ), f"Run of '{task.task_key}' failed with message: {client.jobs.get_run_output(task.run_id).error}"
