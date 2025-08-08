import logging
import os

from datetime import timedelta
from pathlib import Path
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat
from databricks.sdk.service.pipelines import NotebookLibrary, PipelinesEnvironment, PipelineLibrary
from databricks.sdk.service.jobs import NotebookTask, PipelineTask, Task

from tests.e2e.conftest import validate_run_status

logging.getLogger("tests").setLevel("DEBUG")
logging.getLogger("databricks.labs.dqx").setLevel("DEBUG")
logger = logging.getLogger(__name__)

RETRY_INTERVAL_SECONDS = 30
TEST_LIBRARY_REF = "git+https://github.com/databrickslabs/dqx"
if os.getenv("REF_NAME"):
    TEST_LIBRARY_REF = f"{TEST_LIBRARY_REF}.git@refs/pull/{os.getenv('REF_NAME')}"
logger.info(f"Running demo tests from {TEST_LIBRARY_REF}")


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

    waiter = ws.jobs.run_now_and_wait(job.job_id)
    run = ws.jobs.wait_get_run_job_terminated_or_skipped(
        run_id=waiter.run_id,
        timeout=timedelta(minutes=30),
        callback=lambda r: validate_run_status(r, client=ws),
    )
    logging.info(f"Job run {run.run_id} completed successfully for dqx_demo_library")


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

    waiter = ws.jobs.run_now_and_wait(job.job_id)
    run = ws.jobs.wait_get_run_job_terminated_or_skipped(
        run_id=waiter.run_id,
        timeout=timedelta(minutes=30),
        callback=lambda r: validate_run_status(r, client=ws),
    )
    logging.info(f"Job run {run.run_id} completed successfully for dqx_manufacturing_demo")


def test_run_dqx_quick_start_demo_library(make_notebook, make_job):
    path = Path(__file__).parent.parent.parent / "demos" / "dqx_quick_start_demo_library.py"
    ws = WorkspaceClient()
    with open(path, "rb") as f:
        notebook = make_notebook(content=f, format=ImportFormat.SOURCE)

    notebook_path = notebook.as_fuse().as_posix()
    notebook_task = NotebookTask(notebook_path=notebook_path, base_parameters={"test_library_ref": TEST_LIBRARY_REF})
    job = make_job(tasks=[Task(task_key="dqx_quick_start_demo_library", notebook_task=notebook_task)])

    waiter = ws.jobs.run_now_and_wait(job.job_id)
    run = ws.jobs.wait_get_run_job_terminated_or_skipped(
        run_id=waiter.run_id,
        timeout=timedelta(minutes=30),
        callback=lambda r: validate_run_status(r, client=ws),
    )
    logging.info(f"Job run {run.run_id} completed successfully for dqx_quick_start_demo_library")


def test_run_dqx_demo_pii_detection(make_notebook, make_job):
    path = Path(__file__).parent.parent.parent / "demos" / "dqx_demo_pii_detection.py"
    ws = WorkspaceClient()
    with open(path, "rb") as f:
        notebook = make_notebook(content=f, format=ImportFormat.SOURCE)

    notebook_path = notebook.as_fuse().as_posix()
    notebook_task = NotebookTask(
        notebook_path=notebook_path,
        base_parameters={"test_library_ref": f"databricks-labs-dqx[pii] @ {TEST_LIBRARY_REF}"},
    )
    job = make_job(tasks=[Task(task_key="dqx_demo_pii_detection", notebook_task=notebook_task)])

    waiter = ws.jobs.run_now_and_wait(job.job_id)
    run = ws.jobs.wait_get_run_job_terminated_or_skipped(
        run_id=waiter.run_id,
        timeout=timedelta(minutes=30),
        callback=lambda r: validate_run_status(r, client=ws),
    )
    logging.info(f"Job run {run.run_id} completed successfully for dqx_demo_pii_detection")


def test_run_dqx_dlt_demo(make_notebook, make_pipeline, make_job):
    path = Path(__file__).parent.parent.parent / "demos" / "dqx_dlt_demo.py"
    ws = WorkspaceClient()
    with open(path, "rb") as f:
        notebook = make_notebook(content=f, format=ImportFormat.SOURCE)

    notebook_path = notebook.as_fuse().as_posix()
    pipeline = make_pipeline(
        libraries=[PipelineLibrary(notebook=NotebookLibrary(notebook_path))],
        environment=PipelinesEnvironment(dependencies=[TEST_LIBRARY_REF]),
    )
    pipeline_task = PipelineTask(pipeline_id=pipeline.pipeline_id)
    job = make_job(tasks=[Task(task_key="dqx_dlt_demo", pipeline_task=pipeline_task)])

    waiter = ws.jobs.run_now_and_wait(job.job_id)
    run = ws.jobs.wait_get_run_job_terminated_or_skipped(
        run_id=waiter.run_id,
        timeout=timedelta(minutes=30),
        callback=lambda r: validate_run_status(r, client=ws),
    )
    logging.info(f"Job run {run.run_id} completed successfully for dqx_dlt_demo")


def test_run_dqx_demo_tool(installation_ctx, make_schema, make_notebook, make_job):
    catalog = "main"
    schema = make_schema(catalog_name=catalog).name
    installation_ctx.replace(
        extend_prompts={
            r'Provide location for the input data .*': '/databricks-datasets/delta-sharing/samples/nyctaxi_2019',
            r'Provide output table .*': f'{catalog}.{schema}.output_table',
            r'Provide quarantined table .*': f'{catalog}.{schema}.quarantine_table',
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

    waiter = ws.jobs.run_now_and_wait(job.job_id)
    run = ws.jobs.wait_get_run_job_terminated_or_skipped(
        run_id=waiter.run_id,
        timeout=timedelta(minutes=30),
        callback=lambda r: validate_run_status(r, client=ws),
    )
    logging.info(f"Job run {run.run_id} completed successfully for dqx_demo_tool")
