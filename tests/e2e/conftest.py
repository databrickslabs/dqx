from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import Run, TerminationTypeType


def validate_run_status(run: Run, client: WorkspaceClient) -> None:
    """
    Validates that a job task run completed successfully.
    :param run: `Run` object returned from a `WorkspaceClient.jobs.submit(...)` command
    :param client: `WorkspaceClient` object for getting task output
    """
    task = run.tasks[0]
    termination_details = run.status.termination_details

    assert (
        termination_details.type == TerminationTypeType.SUCCESS
    ), f"Run of '{task.task_key}' failed with message: {client.jobs.get_run_output(task.run_id).error}"
