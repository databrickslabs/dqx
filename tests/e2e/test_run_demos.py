import logging
import os
import time

from datetime import timedelta
from pathlib import Path
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat
from databricks.sdk.service.pipelines import NotebookLibrary, PipelineLibrary, UpdateInfoState
from databricks.sdk.service.jobs import NotebookTask, Run, Task, TerminationTypeType

logging.getLogger("tests").setLevel("DEBUG")
logging.getLogger("databricks.labs.dqx").setLevel("DEBUG")
logger = logging.getLogger(__name__)


RETRY_INTERVAL_SECONDS = 30
TEST_LIBRARY_REF = f"git+https://github.com/databrickslabs/dqx.git@{os.getenv('REF_NAME')}"


def test_run_dqx_demo_library(make_notebook, make_schema, make_job):
    path = Path(__file__).parent.parent.parent / "demos" / "dqx_demo_library.py"
    ws = WorkspaceClient()
    with open(path, "rb") as f:
        notebook = make_notebook(content=f, format=ImportFormat.SOURCE)
        directory = notebook.as_fuse().parent.as_posix()

    catalog = "main"
    schema = make_schema(catalog_name=catalog).name
    notebook_path = notebook.as_fuse().as_posix()
    notebook_task = NotebookTask(
        notebook_path=notebook_path,
        base_parameters={
            "demo_database_name": catalog,
            "demo_schema_name": schema,
            "demo_file_directory": directory,
            "test_library_ref": TEST_LIBRARY_REF,
        },
    )
    job = make_job(tasks=[Task(task_key="dqx_demo_library", notebook_task=notebook_task)])

    waiter = ws.jobs.run_now(job.job_id)
    run = waiter.result(timeout=timedelta(minutes=30), callback=lambda r: validate_demo_run_status(r, ws))
    logging.info(f"Job run {run.run_id} completed successfully for dqx_manufacturing_demo")


def test_run_dqx_manufacturing_demo(make_notebook, make_directory, make_schema, make_job):
    path = Path(__file__).parent.parent.parent / "demos" / "dqx_manufacturing_demo.py"
    ws = WorkspaceClient()
    with open(path, "rb") as f:
        notebook = make_notebook(content=f, format=ImportFormat.SOURCE)
        folder = notebook.as_fuse().parent / "quality_rules"
        make_directory(path=folder)

    catalog = "main"
    schema = make_schema(catalog_name=catalog).name
    notebook_path = notebook.as_fuse().as_posix()
    notebook_task = NotebookTask(
        notebook_path=notebook_path,
        base_parameters={"demo_database": catalog, "demo_schema": schema, "test_library_ref": TEST_LIBRARY_REF},
    )
    job = make_job(tasks=[Task(task_key="dqx_manufacturing_demo", notebook_task=notebook_task)])

    waiter = ws.jobs.run_now(job.job_id)
    run = waiter.result(timeout=timedelta(minutes=30), callback=lambda r: validate_demo_run_status(r, ws))
    logging.info(f"Job run {run.run_id} completed successfully for dqx_manufacturing_demo")


def test_run_dqx_quick_start_demo_library(make_notebook, make_job):
    path = Path(__file__).parent.parent.parent / "demos" / "dqx_quick_start_demo_library.py"
    ws = WorkspaceClient()
    with open(path, "rb") as f:
        notebook = make_notebook(content=f, format=ImportFormat.SOURCE)

    notebook_path = notebook.as_fuse().as_posix()
    notebook_task = NotebookTask(notebook_path=notebook_path, base_parameters={"test_library_ref": TEST_LIBRARY_REF})
    job = make_job(tasks=[Task(task_key="dqx_quick_start_demo_library", notebook_task=notebook_task)])

    waiter = ws.jobs.run_now(job.job_id)
    run = waiter.result(timeout=timedelta(minutes=30), callback=lambda r: validate_demo_run_status(r, ws))
    logging.info(f"Job run {run.run_id} completed successfully for dqx_quick_start_demo_library")


def test_run_dqx_demo_pii_detection(make_notebook, make_cluster, make_job):
    path = Path(__file__).parent.parent.parent / "demos" / "dqx_demo_pii_detection.py"
    ws = WorkspaceClient()
    with open(path, "rb") as f:
        notebook = make_notebook(content=f, format=ImportFormat.SOURCE)

    notebook_path = notebook.as_fuse().as_posix()
    notebook_task = NotebookTask(notebook_path=notebook_path, base_parameters={"test_library_ref": TEST_LIBRARY_REF})
    cluster = make_cluster(single_node=True, spark_version="15.4.x-scala2.12")
    job = make_job(
        tasks=[
            Task(task_key="dqx_demo_pii_detection", notebook_task=notebook_task, existing_cluster_id=cluster.cluster_id)
        ]
    )

    waiter = ws.jobs.run_now(job.job_id)
    run = waiter.result(timeout=timedelta(minutes=30), callback=lambda r: validate_demo_run_status(r, ws))
    logging.info(f"Job run {run.run_id} completed successfully for dqx_demo_pii_detection")


def test_run_dqx_dlt_demo(make_notebook, make_pipeline):
    path = Path(__file__).parent.parent.parent / "demos" / "dqx_dlt_demo.py"
    ws = WorkspaceClient()
    with open(path, "rb") as f:
        notebook = make_notebook(content=f, format=ImportFormat.SOURCE)

    notebook_path = notebook.as_fuse().as_posix()
    pipeline = make_pipeline(libraries=[PipelineLibrary(notebook=NotebookLibrary(notebook_path))])
    update = ws.pipelines.start_update(pipeline.pipeline_id)

    while True:
        update_details = ws.pipelines.get_update(pipeline_id=pipeline.pipeline_id, update_id=update.update_id)
        if update_details.update.state in [UpdateInfoState.CANCELED, UpdateInfoState.COMPLETED, UpdateInfoState.FAILED]:
            break
        time.sleep(RETRY_INTERVAL_SECONDS)

    assert update_details.update.state == UpdateInfoState.COMPLETED, f"Run of pipeline '{pipeline.pipeline_id}' failed"


def test_run_dqx_demo_tool(installation_ctx, make_schema, make_notebook, make_job):
    catalog = "main"
    schema = make_schema(catalog_name=catalog).name
    installation_ctx.replace(
        extend_prompts={
            r'Provide location for the input data *': '/databricks-datasets/delta-sharing/samples/nyctaxi_2019',
            r'Provide format for the input data (e.g. delta, parquet, csv, json) *': 'delta',
            r'Provide output table in the format `catalog.schema.table` or `schema.table`': f'{catalog}.{schema}.output_table',
            r'Provide quarantined table in the format `catalog.schema.table` or `schema.table`': f'{catalog}.{schema}.quarantine_table',
        },
    )
    installation_ctx.workspace_installer.run(installation_ctx.config)
    product_name = installation_ctx.product_info.product_name()
    install_path = installation_ctx.installation.install_folder()

    path = Path(__file__).parent.parent.parent / "demos" / "dqx_demo_tool.py"
    ws = WorkspaceClient()
    with open(path, "rb") as f:
        notebook = make_notebook(content=f, format=ImportFormat.SOURCE)

    notebook_path = notebook.as_fuse().as_posix()
    notebook_task = NotebookTask(
        notebook_path=notebook_path,
        base_parameters={
            "dqx_installation_path": f"/Workspace{install_path}",
            "dqx_product_name": product_name,
        },
    )
    job = make_job(tasks=[Task(task_key="dqx_demo_tool", notebook_task=notebook_task)])

    waiter = ws.jobs.run_now(job.job_id)
    run = waiter.result(timeout=timedelta(minutes=30), callback=validate_demo_run_status)
    logging.info(f"Job run {run.run_id} completed successfully for dqx_demo_tool")


def validate_demo_run_status(run: Run) -> None:
    """
    Validates that a demo run completed successfully.
    :param run: `Run` object returned from a `WorkspaceClient.jobs.submit(...)` command
    """
    client = WorkspaceClient()
    task = run.tasks[0]
    termination_details = run.status.termination_details

    assert (
        termination_details.type == TerminationTypeType.SUCCESS
    ), f"Run of '{task.task_key}' failed with message: {client.jobs.get_run_output(task.run_id).error}"
