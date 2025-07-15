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

    async def get_submit_job_result(notebook_path):
        path = demo_folder / notebook_path
        with open(path, "rb") as f:
            notebook = make_notebook(content=f, format=ImportFormat.AUTO)
        return await ws.jobs.submit(
            tasks=[
                SubmitTask(task_key="demo_run", notebook_task=NotebookTask(notebook_path=notebook.as_fuse().as_posix()))
            ]
        )

    async def validate_submit_job_runs(runs):
        for completion in asyncio.as_completed(runs):
            run = await completion
            if run.status.state == RunLifecycleStateV2State.TERMINATED:
                run_details = run.status.termination_details
                assert (
                    run_details.type == TerminationTypeType.SUCCESS
                ), f"Run ended with status {run_details.type.value}: {run_details.message}"

    runs = [get_submit_job_result(notebook_path) for notebook_path in notebook_paths]
    asyncio.run(validate_submit_job_runs(runs))
