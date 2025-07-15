import asyncio
import logging
import os

from pathlib import Path
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat
from databricks.sdk.service.jobs import NotebookTask, SubmitTask, RunLifecycleStateV2State, TerminationTypeType


logging.getLogger("tests").setLevel("DEBUG")
logging.getLogger("databricks.labs.dqx").setLevel("DEBUG")
logger = logging.getLogger(__name__)


def test_run_all_demo_notebooks_succeed(make_notebook):
    demo_folder = Path(__file__).parent.parent.parent / "demos"
    notebook_paths = [path for path in os.listdir(demo_folder) if path.endswith((".py", ".ipynb", ".dbc"))]
    ws = WorkspaceClient()

    run_ids = []
    for notebook_path in notebook_paths:
        logger.info(f"Running demo notebook '{notebook_path}'")
        path = demo_folder / notebook_path
        import_format = (
            ImportFormat.JUPYTER
            if notebook_path.endswith(".ipynb")
            else ImportFormat.DBC if notebook_path.endswith(".dbc") else ImportFormat.SOURCE
        )
        with open(path, "rb") as f:
            notebook = make_notebook(content=f, format=import_format)
        job_run = ws.jobs.submit(
            tasks=[
                SubmitTask(task_key="demo_run", notebook_task=NotebookTask(notebook_path=notebook.as_fuse().as_posix()))
            ]
        )
        run_ids.append([job_run.run_id])

    async def wait_for_completion(run_id, poll_interval=10):
        while True:
            run = ws.jobs.get_run(run_id)
            if run.status.state == RunLifecycleStateV2State.TERMINATED:
                run_details = run.status.termination_details
                assert (
                    run_details.type == TerminationTypeType.SUCCESS
                ), f"Run ended with status {run_details.type.value}: {run_details.message}"
                return
            await asyncio.sleep(poll_interval)

    async def validate_submit_job_runs(runs):
        for completion in asyncio.as_completed(runs, timeout=1800):
            await completion

    demo_runs = [wait_for_completion(run_id) for run_id in run_ids]
    asyncio.run(validate_submit_job_runs(demo_runs))
