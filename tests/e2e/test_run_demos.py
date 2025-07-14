import logging
import os

from pathlib import Path
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import NotebookTask, SubmitTask, RunLifecycleStateV2State, TerminationTypeType


logging.getLogger("tests").setLevel("DEBUG")
logging.getLogger("databricks.labs.dqx").setLevel("DEBUG")
logger = logging.getLogger(__name__)


def test_run_all_demo_notebooks_succeed(make_notebook):
    demo_folder = Path(__file__).parent.parent.parent / "demos"
    notebook_paths = [path for path in os.listdir(demo_folder) if path.endswith(".py")]
    ws = WorkspaceClient()

    run_ids = []
    for notebook_path in notebook_paths:
        path = demo_folder / notebook_path
        with open(path, "rb") as f:
            notebook = make_notebook(content=f)
            wait_for_run = ws.jobs.submit(
                tasks=[SubmitTask(task_key="demo_run", notebook_task=NotebookTask(notebook_path=notebook.as_fuse()))]
            )
            run_ids.append(wait_for_run.response.run_id)

    for run_id in run_ids:
        run = ws.jobs.get_run(run_id)
        if run.status.state == RunLifecycleStateV2State.TERMINATED:
            run_details = run.status.termination_details
            assert (
                run_details.type == TerminationTypeType.SUCCESS
            ), f"Run ended with status {run_details.type.value}: {run_details.message}"
