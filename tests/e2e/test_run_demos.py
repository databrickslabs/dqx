import logging
import time

from pathlib import Path
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat
from databricks.sdk.service.jobs import NotebookTask, Task, RunLifecycleStateV2State, TerminationTypeType


logging.getLogger("tests").setLevel("DEBUG")
logging.getLogger("databricks.labs.dqx").setLevel("DEBUG")
logger = logging.getLogger(__name__)


RETRY_INTERVAL_SECONDS = 30


def test_run_dqx_demo_library(make_notebook, make_catalog, make_schema, make_job):
    path = Path(__file__).parent.parent.parent / "demos" / "dqx_demo_library.py"
    ws = WorkspaceClient()
    with open(path, "rb") as f:
        notebook = make_notebook(content=f, format=ImportFormat.SOURCE)

    catalog = "main"
    schema = make_schema(catalog_name=catalog).name
    notebook_path = notebook.as_fuse().as_posix()
    notebook_task = NotebookTask(
        notebook_path=notebook_path, base_parameters={"demo_database": catalog, "demo_schema": schema}
    )
    job = make_job(tasks=[Task(task_key="dqx_demo_library", notebook_task=notebook_task)])
    run = ws.jobs.run_now(job.job_id)

    while True:
        run_details = ws.jobs.get_run(run.run_id)
        if run_details.status.state == RunLifecycleStateV2State.TERMINATED:
            break
        time.sleep(RETRY_INTERVAL_SECONDS)

    task = run_details.tasks[0]
    termination_details = run_details.status.termination_details
    assert (
        termination_details.type == TerminationTypeType.SUCCESS
    ), f"Run of '{task.task_key}' failed with message: {ws.jobs.get_run_output(task.run_id).error}"


def test_run_dqx_manufacturing_demo(make_notebook, make_directory, make_schema, make_job):
    path = Path(__file__).parent.parent.parent / "demos" / "dqx_manufacturing_demo.py"
    ws = WorkspaceClient()
    with open(path, "rb") as f:
        notebook = make_notebook(content=f, format=ImportFormat.SOURCE)
    make_directory(path="/quality_rules")

    catalog = "main"
    schema = make_schema(catalog_name=catalog).name
    notebook_path = notebook.as_fuse().as_posix()
    notebook_task = NotebookTask(
        notebook_path=notebook_path, base_parameters={"demo_database": catalog, "demo_schema": schema}
    )
    job = make_job(tasks=[Task(task_key="dqx_manufacturing_demo", notebook_task=notebook_task)])
    run = ws.jobs.run_now(job.job_id)

    while True:
        run_details = ws.jobs.get_run(run.run_id)
        if run_details.status.state == RunLifecycleStateV2State.TERMINATED:
            break
        time.sleep(RETRY_INTERVAL_SECONDS)

    task = run_details.tasks[0]
    termination_details = run_details.status.termination_details
    assert (
        termination_details.type == TerminationTypeType.SUCCESS
    ), f"Run of '{task.task_key}' failed with message: {ws.jobs.get_run_output(task.run_id).error}"


def test_run_dqx_quick_start_demo_library(make_notebook, make_job):
    path = Path(__file__).parent.parent.parent / "demos" / "dqx_quick_start_demo_library.py"
    ws = WorkspaceClient()
    with open(path, "rb") as f:
        notebook = make_notebook(content=f, format=ImportFormat.SOURCE)

    notebook_path = notebook.as_fuse().as_posix()
    notebook_task = NotebookTask(notebook_path=notebook_path)
    job = make_job(tasks=[Task(task_key="dqx_quick_start_demo_library", notebook_task=notebook_task)])
    run = ws.jobs.run_now(job.job_id)

    while True:
        run_details = ws.jobs.get_run(run.run_id)
        if run_details.status.state == RunLifecycleStateV2State.TERMINATED:
            break
        time.sleep(RETRY_INTERVAL_SECONDS)

    task = run_details.tasks[0]
    termination_details = run_details.status.termination_details
    assert (
        termination_details.type == TerminationTypeType.SUCCESS
    ), f"Run of '{task.task_key}' failed with message: {ws.jobs.get_run_output(task.run_id).error}"


def test_run_dqx_demo_pii_detection(make_notebook, make_cluster, make_job):
    path = Path(__file__).parent.parent.parent / "demos" / "dqx_demo_pii_detection.py"
    ws = WorkspaceClient()
    with open(path, "rb") as f:
        notebook = make_notebook(content=f, format=ImportFormat.SOURCE)

    notebook_path = notebook.as_fuse().as_posix()
    notebook_task = NotebookTask(notebook_path=notebook_path)
    cluster = make_cluster(single_node=True, spark_version="15.4.x-scala2.12")
    job = make_job(tasks=[Task(task_key="dqx_demo_pii_detection", notebook_task=notebook_task, existing_cluster_id=cluster.cluster_id)])
    run = ws.jobs.run_now(job.job_id)

    while True:
        run_details = ws.jobs.get_run(run.run_id)
        if run_details.status.state == RunLifecycleStateV2State.TERMINATED:
            break
        time.sleep(RETRY_INTERVAL_SECONDS)

    task = run_details.tasks[0]
    termination_details = run_details.status.termination_details
    assert (
        termination_details.type == TerminationTypeType.SUCCESS
    ), f"Run of '{task.task_key}' failed with message: {ws.jobs.get_run_output(task.run_id).error}"
