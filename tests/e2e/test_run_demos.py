import logging
import shutil
import subprocess

from datetime import timedelta
from pathlib import Path
from uuid import uuid4
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat
from databricks.sdk.service.pipelines import NotebookLibrary, PipelinesEnvironment, PipelineLibrary
from databricks.sdk.service.jobs import NotebookTask, PipelineTask, Run, Task, TerminationTypeType
from tests.e2e.conftest import new_classic_job_cluster


logger = logging.getLogger(__name__)


def test_run_dqx_demo_library(make_notebook, make_schema, make_job, library_ref):
    ws = WorkspaceClient()
    path = Path(__file__).parent.parent.parent / "demos" / "dqx_demo_library.py"
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
            "test_library_ref": library_ref,
        },
    )
    job = make_job(tasks=[Task(task_key="dqx_demo_library", notebook_task=notebook_task)])

    waiter = ws.jobs.run_now_and_wait(job.job_id)
    run = ws.jobs.wait_get_run_job_terminated_or_skipped(
        run_id=waiter.run_id,
        timeout=timedelta(minutes=30),
        callback=lambda r: validate_run_status(r, ws),
    )
    logging.info(f"Job run {run.run_id} completed successfully for dqx_demo_library")


def test_run_dqx_manufacturing_demo(make_notebook, make_directory, make_schema, make_job, library_ref):
    ws = WorkspaceClient()
    path = Path(__file__).parent.parent.parent / "demos" / "dqx_manufacturing_demo.py"
    with open(path, "rb") as f:
        notebook = make_notebook(content=f, format=ImportFormat.SOURCE)
        folder = notebook.as_fuse().parent / "quality_rules"
        make_directory(path=folder)

    catalog = "main"
    schema = make_schema(catalog_name=catalog).name
    notebook_path = notebook.as_fuse().as_posix()
    notebook_task = NotebookTask(
        notebook_path=notebook_path,
        base_parameters={"demo_database": catalog, "demo_schema": schema, "test_library_ref": library_ref},
    )
    job = make_job(tasks=[Task(task_key="dqx_manufacturing_demo", notebook_task=notebook_task)])

    waiter = ws.jobs.run_now_and_wait(job.job_id)
    run = ws.jobs.wait_get_run_job_terminated_or_skipped(
        run_id=waiter.run_id,
        timeout=timedelta(minutes=30),
        callback=lambda r: validate_run_status(r, ws),
    )
    logging.info(f"Job run {run.run_id} completed successfully for dqx_manufacturing_demo")


def test_run_dqx_quick_start_demo_library(make_notebook, make_job, library_ref):
    ws = WorkspaceClient()
    path = Path(__file__).parent.parent.parent / "demos" / "dqx_quick_start_demo_library.py"
    with open(path, "rb") as f:
        notebook = make_notebook(content=f, format=ImportFormat.SOURCE)

    notebook_path = notebook.as_fuse().as_posix()
    notebook_task = NotebookTask(notebook_path=notebook_path, base_parameters={"test_library_ref": library_ref})
    job = make_job(tasks=[Task(task_key="dqx_quick_start_demo_library", notebook_task=notebook_task)])

    waiter = ws.jobs.run_now_and_wait(job.job_id)
    run = ws.jobs.wait_get_run_job_terminated_or_skipped(
        run_id=waiter.run_id,
        timeout=timedelta(minutes=30),
        callback=lambda r: validate_run_status(r, ws),
    )
    logging.info(f"Job run {run.run_id} completed successfully for dqx_quick_start_demo_library")


def test_run_dqx_demo_pii_detection(make_notebook, make_job, library_ref):
    ws = WorkspaceClient()
    path = Path(__file__).parent.parent.parent / "demos" / "dqx_demo_pii_detection.py"
    with open(path, "rb") as f:
        notebook = make_notebook(content=f, format=ImportFormat.SOURCE)

    notebook_path = notebook.as_fuse().as_posix()
    notebook_task = NotebookTask(
        notebook_path=notebook_path,
        base_parameters={"test_library_ref": library_ref},
    )
    job = make_job(tasks=[Task(task_key="dqx_demo_pii_detection", notebook_task=notebook_task)])

    waiter = ws.jobs.run_now_and_wait(job.job_id)
    run = ws.jobs.wait_get_run_job_terminated_or_skipped(
        run_id=waiter.run_id,
        timeout=timedelta(minutes=30),
        callback=lambda r: validate_run_status(r, ws),
    )
    logging.info(f"Job run {run.run_id} completed successfully for dqx_demo_pii_detection")


def test_run_dqx_dlt_demo(make_notebook, make_pipeline, make_job, library_ref):
    ws = WorkspaceClient()
    path = Path(__file__).parent.parent.parent / "demos" / "dqx_dlt_demo.py"
    with open(path, "rb") as f:
        notebook = make_notebook(content=f, format=ImportFormat.SOURCE)

    notebook_path = notebook.as_fuse().as_posix()
    pipeline = make_pipeline(
        libraries=[PipelineLibrary(notebook=NotebookLibrary(notebook_path))],
        environment=PipelinesEnvironment(dependencies=[library_ref]),
    )
    pipeline_task = PipelineTask(pipeline_id=pipeline.pipeline_id)
    job = make_job(tasks=[Task(task_key="dqx_dlt_demo", pipeline_task=pipeline_task)])

    waiter = ws.jobs.run_now_and_wait(job.job_id)
    run = ws.jobs.wait_get_run_job_terminated_or_skipped(
        run_id=waiter.run_id,
        timeout=timedelta(minutes=30),
        callback=lambda r: validate_run_status(r, ws),
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
        callback=lambda r: validate_run_status(r, ws),
    )
    logging.info(f"Job run {run.run_id} completed successfully for dqx_demo_tool")


def test_run_dqx_streaming_demo_native(make_notebook, make_schema, make_job, tmp_path, library_ref):
    ws = WorkspaceClient()
    path = Path(__file__).parent.parent.parent / "demos" / "dqx_streaming_demo_native.py"
    with open(path, "rb") as f:
        notebook = make_notebook(content=f, format=ImportFormat.SOURCE)
    catalog = "main"
    schema = make_schema(catalog_name=catalog).name
    notebook_path = notebook.as_fuse().as_posix()

    # Use the temporary directory for outputs
    run_id = str(uuid4())
    base_output_path = tmp_path / run_id
    base_parameters = {
        "demo_catalog_name": catalog,
        "demo_schema_name": schema,
        "silver_checkpoint": f"{base_output_path}/silver_checkpoint",
        "quarantine_checkpoint": f"{base_output_path}/quarantine_checkpoint",
        "test_library_ref": library_ref,
    }
    notebook_task = NotebookTask(notebook_path=notebook_path, base_parameters=base_parameters)
    job = make_job(tasks=[Task(task_key="dqx_streaming_demo", notebook_task=notebook_task)])
    waiter = ws.jobs.run_now_and_wait(job.job_id)
    run = ws.jobs.wait_get_run_job_terminated_or_skipped(
        run_id=waiter.run_id,
        timeout=timedelta(minutes=30),
        callback=lambda r: validate_run_status(r, client=ws),
    )
    logging.info(f"Job run {run.run_id} completed successfully for dqx_streaming_demo")


def test_run_dqx_streaming_demo_diy(make_notebook, make_job, tmp_path, library_ref):
    ws = WorkspaceClient()
    path = Path(__file__).parent.parent.parent / "demos" / "dqx_streaming_demo_diy.py"
    with open(path, "rb") as f:
        notebook = make_notebook(content=f, format=ImportFormat.SOURCE)
    notebook_path = notebook.as_fuse().as_posix()

    # Use the temporary directory for outputs
    run_id = str(uuid4())
    base_output_path = tmp_path / run_id
    base_parameters = {
        "silver_checkpoint": f"{base_output_path}/silver_checkpoint",
        "silver_table": f"{base_output_path}/silver_table",
        "quarantine_checkpoint": f"{base_output_path}/quarantine_checkpoint",
        "quarantine_table": f"{base_output_path}/quarantine_table",
        "test_library_ref": library_ref,
    }
    notebook_task = NotebookTask(notebook_path=notebook_path, base_parameters=base_parameters)
    job = make_job(tasks=[Task(task_key="dqx_streaming_demo", notebook_task=notebook_task)])
    waiter = ws.jobs.run_now_and_wait(job.job_id)
    run = ws.jobs.wait_get_run_job_terminated_or_skipped(
        run_id=waiter.run_id,
        timeout=timedelta(minutes=30),
        callback=lambda r: validate_run_status(r, client=ws),
    )
    logging.info(f"Job run {run.run_id} completed successfully for dqx_streaming_demo")


def test_run_dqx_demo_asset_bundle(make_schema, make_random, library_ref):
    cli_path = shutil.which("databricks")
    path = Path(__file__).parent.parent.parent / "demos" / "dqx_demo_asset_bundle"
    catalog = "main"
    schema = make_schema(catalog_name=catalog).name
    run_id = make_random(10).lower()

    try:
        subprocess.run([cli_path, "bundle", "validate"], check=True, capture_output=True, cwd=path)
        subprocess.run(
            [
                cli_path,
                "bundle",
                "deploy",
                f'--var="library_ref={library_ref}"',
                f'--var="demo_catalog={catalog}"',
                f'--var="demo_schema={schema}"',
                f'--var="run_id={run_id}"',
                '--force-lock',
                "--auto-approve",
            ],
            check=True,
            capture_output=True,
            cwd=path,
        )
        subprocess.run([cli_path, "bundle", "run", "dqx_demo_job"], check=True, capture_output=True, cwd=path)
    finally:
        subprocess.run([cli_path, "bundle", "destroy", "--auto-approve"], check=True, capture_output=True, cwd=path)


def test_run_dqx_multi_table_demo(make_notebook, make_schema, make_job, library_ref):

    ws = WorkspaceClient()
    path = Path(__file__).parent.parent.parent / "demos" / "dqx_multi_table_demo.py"
    with open(path, "rb") as f:
        notebook = make_notebook(content=f, format=ImportFormat.SOURCE)

    catalog = "main"
    schema = make_schema(catalog_name=catalog).name
    notebook_path = notebook.as_fuse().as_posix()
    notebook_task = NotebookTask(
        notebook_path=notebook_path,
        base_parameters={"demo_catalog_name": catalog, "demo_schema_name": schema, "test_library_ref": library_ref},
    )
    job = make_job(tasks=[Task(task_key="dqx_multi_table_demo", notebook_task=notebook_task)])

    waiter = ws.jobs.run_now_and_wait(job.job_id)
    run = ws.jobs.wait_get_run_job_terminated_or_skipped(
        run_id=waiter.run_id,
        timeout=timedelta(minutes=30),
        callback=lambda r: validate_run_status(r, ws),
    )
    logging.info(f"Job run {run.run_id} completed successfully for dqx_multi_table_demo")


def test_run_dqx_demo_summary_metrics(make_notebook, make_schema, make_job, library_ref):
    ws = WorkspaceClient()
    path = Path(__file__).parent.parent.parent / "demos" / "dqx_demo_summary_metrics.py"
    with open(path, "rb") as f:
        notebook = make_notebook(content=f, format=ImportFormat.SOURCE)

    catalog = "main"
    schema = make_schema(catalog_name=catalog).name
    notebook_path = notebook.as_fuse().as_posix()
    notebook_task = NotebookTask(
        notebook_path=notebook_path,
        base_parameters={
            "demo_catalog_name": catalog,
            "demo_schema_name": schema,
            "test_library_ref": library_ref,
        },
    )
    job = make_job(
        tasks=[Task(task_key="dqx_demo_library", notebook_task=notebook_task, new_cluster=new_classic_job_cluster())]
    )

    waiter = ws.jobs.run_now_and_wait(job.job_id)
    run = ws.jobs.wait_get_run_job_terminated_or_skipped(
        run_id=waiter.run_id,
        timeout=timedelta(minutes=30),
        callback=lambda r: validate_run_status(r, ws),
    )
    logging.info(f"Job run {run.run_id} completed successfully for dqx_demo_summary_metrics")


def validate_run_status(run: Run, client: WorkspaceClient) -> None:
    """
    Validates that a job task run completed successfully.

    Args:
        run: `Run` object returned from a `WorkspaceClient.jobs.submit(...)` command
        client: `WorkspaceClient` object for getting task output
    """
    task = run.tasks[0]
    termination_details = run.status.termination_details

    assert (
        termination_details.type == TerminationTypeType.SUCCESS
    ), f"Run of '{task.task_key}' failed with message: {client.jobs.get_run_output(task.run_id).error}"
