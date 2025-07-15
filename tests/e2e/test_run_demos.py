import logging

from pathlib import Path
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat
from databricks.sdk.service.jobs import NotebookTask, SubmitTask, TerminationTypeType


logging.getLogger("tests").setLevel("DEBUG")
logging.getLogger("databricks.labs.dqx").setLevel("DEBUG")
logger = logging.getLogger(__name__)


def test_run_dqx_demo_library(make_notebook):
    path = Path(__file__).parent.parent.parent / "demos" / "dqx_demo_library.py"
    ws = WorkspaceClient()
    with open(path, "rb") as f:
        notebook = make_notebook(content=f, format=ImportFormat.SOURCE)
    notebook_path = notebook.as_fuse().as_posix()
    run = ws.jobs.submit_and_wait(
        tasks=[SubmitTask(task_key="dqx_demo_library", notebook_task=NotebookTask(notebook_path=notebook_path))]
    )
    run_details = run.status.termination_details
    task = run.tasks[0]
    assert (
        run_details.type == TerminationTypeType.SUCCESS
    ), f"Run of '{task.task_key}' failed with output: {task.status.termination_details.message}"


def test_run_dqx_manufacturing_demo(make_notebook):
    path = Path(__file__).parent.parent.parent / "demos" / "dqx_manufacturing_demo.py"
    ws = WorkspaceClient()
    with open(path, "rb") as f:
        notebook = make_notebook(content=f, format=ImportFormat.SOURCE)
    notebook_path = notebook.as_fuse().as_posix()
    run = ws.jobs.submit_and_wait(
        tasks=[SubmitTask(task_key="dqx_manufacturing_demo", notebook_task=NotebookTask(notebook_path=notebook_path))]
    )
    run_details = run.status.termination_details
    task = run.tasks[0]
    assert (
        run_details.type == TerminationTypeType.SUCCESS
    ), f"Run of '{task.task_key}' failed with output: {task.status.termination_details.message}"


def test_run_dqx_quick_start_demo_library(make_notebook):
    path = Path(__file__).parent.parent.parent / "demos" / "dqx_quick_start_demo_library.py"
    ws = WorkspaceClient()
    with open(path, "rb") as f:
        notebook = make_notebook(content=f, format=ImportFormat.SOURCE)
    notebook_path = notebook.as_fuse().as_posix()
    run = ws.jobs.submit_and_wait(
        tasks=[
            SubmitTask(task_key="dqx_quick_start_demo_library", notebook_task=NotebookTask(notebook_path=notebook_path))
        ]
    )
    run_details = run.status.termination_details
    task = run.tasks[0]
    assert (
        run_details.type == TerminationTypeType.SUCCESS
    ), f"Run of '{task.task_key}' failed with output: {task.status.termination_details.message}"
