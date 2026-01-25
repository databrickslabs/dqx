import logging
import shutil
import subprocess

from datetime import timedelta
from pathlib import Path
from uuid import uuid4
from tempfile import TemporaryDirectory
from databricks.sdk.service.workspace import ImportFormat
from databricks.sdk.service.pipelines import NotebookLibrary, PipelinesEnvironment, PipelineLibrary
from databricks.sdk.service.jobs import NotebookTask, PipelineTask, Task

from tests.conftest import TEST_CATALOG
from tests.e2e.conftest import new_classic_job_cluster, validate_run_status

logger = logging.getLogger(__name__)


def test_run_dqx_demo_library(ws, make_notebook, make_schema, make_job, library_ref):
    path = Path(__file__).parent.parent.parent / "demos" / "dqx_demo_library.py"
    with open(path, "rb") as f:
        notebook = make_notebook(content=f, format=ImportFormat.SOURCE)
        directory = notebook.as_fuse().parent.as_posix()

    catalog = TEST_CATALOG
    schema = make_schema(catalog_name=catalog).name
    notebook_path = notebook.as_fuse().as_posix()
    notebook_task = NotebookTask(
        notebook_path=notebook_path,
        base_parameters={
            "demo_catalog": catalog,
            "demo_schema": schema,
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


def test_run_dqx_manufacturing_demo(ws, make_notebook, make_directory, make_schema, make_job, library_ref):
    path = Path(__file__).parent.parent.parent / "demos" / "dqx_manufacturing_demo.py"
    with open(path, "rb") as f:
        notebook = make_notebook(content=f, format=ImportFormat.SOURCE)
        folder = notebook.as_fuse().parent / "quality_rules"
        make_directory(path=folder)

    catalog = TEST_CATALOG
    schema = make_schema(catalog_name=catalog).name
    notebook_path = notebook.as_fuse().as_posix()
    notebook_task = NotebookTask(
        notebook_path=notebook_path,
        base_parameters={"demo_catalog": catalog, "demo_schema": schema, "test_library_ref": library_ref},
    )
    job = make_job(tasks=[Task(task_key="dqx_manufacturing_demo", notebook_task=notebook_task)])

    waiter = ws.jobs.run_now_and_wait(job.job_id)
    run = ws.jobs.wait_get_run_job_terminated_or_skipped(
        run_id=waiter.run_id,
        timeout=timedelta(minutes=30),
        callback=lambda r: validate_run_status(r, ws),
    )
    logging.info(f"Job run {run.run_id} completed successfully for dqx_manufacturing_demo")


def test_run_dqx_quick_start_demo_library(ws, make_notebook, make_job, library_ref):
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


def test_run_dqx_demo_pii_detection(ws, make_notebook, make_job, library_ref):
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


def test_run_dqx_dlt_demo(
    skip_if_classic_compute, ws, make_notebook, make_schema, make_pipeline, make_job, library_ref
):
    """
    Test running the DLT demo notebook in a serverless pipeline.
    No need to trigger from non-serverless runtime, since the dlt pipeline use own cluster anyway.
    """
    catalog = TEST_CATALOG
    schema = make_schema(catalog_name=catalog).name

    path = Path(__file__).parent.parent.parent / "demos" / "dqx_dlt_demo.py"
    with open(path, "rb") as f:
        notebook = make_notebook(content=f, format=ImportFormat.SOURCE)

    notebook_path = notebook.as_fuse().as_posix()
    pipeline = make_pipeline(
        # DLT / Lakeflow support 3 modes of execution:
        # * Full Unity Catalog (UC) mode, the so called DPM (Direct Publishing Mode).
        #   This mode supports write to arbitrary catalogs and schemas, and performs additional checks, e.g.
        #   prevent usage of collect() or schema inspection
        # * non-UC mode (legacy) where neither catalog & schema nor target are provided
        # * UC legacy mode which specifies the target schema where all datasets defined in the pipeline are published
        # As part of this test we use the latest full UC mode.
        catalog=catalog,
        schema=schema,
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


def test_run_dqx_demo_tool(ws, installation_ctx, make_schema, make_notebook, make_job):
    catalog = TEST_CATALOG
    schema = make_schema(catalog_name=catalog).name
    installation_ctx.replace(
        extend_prompts={
            r"Provide location for the input data .*": "/databricks-datasets/delta-sharing/samples/nyctaxi_2019",
            r"Provide output table .*": f"{catalog}.{schema}.output_table",
            r"Provide quarantined table .*": f"{catalog}.{schema}.quarantine_table",
        },
    )
    installation_ctx.workspace_installer.run(installation_ctx.config)
    product_name = installation_ctx.product_info.product_name()
    install_path = installation_ctx.installation.install_folder()

    path = Path(__file__).parent.parent.parent / "demos" / "dqx_demo_tool.py"
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


def test_run_dqx_streaming_demo_native(ws, make_notebook, make_schema, make_job, tmp_path, library_ref):
    path = Path(__file__).parent.parent.parent / "demos" / "dqx_streaming_demo_native.py"
    with open(path, "rb") as f:
        notebook = make_notebook(content=f, format=ImportFormat.SOURCE)
    catalog = TEST_CATALOG
    schema = make_schema(catalog_name=catalog).name
    notebook_path = notebook.as_fuse().as_posix()

    # Use the temporary directory for outputs
    run_id = str(uuid4())
    base_output_path = tmp_path / run_id
    base_parameters = {
        "demo_catalog": catalog,
        "demo_schema": schema,
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


def test_run_dqx_streaming_demo_diy(ws, make_notebook, make_job, tmp_path, library_ref):
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
    catalog = TEST_CATALOG
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


def test_run_dqx_multi_table_demo(ws, make_notebook, make_schema, make_job, library_ref):
    path = Path(__file__).parent.parent.parent / "demos" / "dqx_multi_table_demo.py"
    with open(path, "rb") as f:
        notebook = make_notebook(content=f, format=ImportFormat.SOURCE)

    catalog = TEST_CATALOG
    schema = make_schema(catalog_name=catalog).name
    notebook_path = notebook.as_fuse().as_posix()
    notebook_task = NotebookTask(
        notebook_path=notebook_path,
        base_parameters={"demo_catalog": catalog, "demo_schema": schema, "test_library_ref": library_ref},
    )
    job = make_job(tasks=[Task(task_key="dqx_multi_table_demo", notebook_task=notebook_task)])

    waiter = ws.jobs.run_now_and_wait(job.job_id)
    run = ws.jobs.wait_get_run_job_terminated_or_skipped(
        run_id=waiter.run_id,
        timeout=timedelta(minutes=30),
        callback=lambda r: validate_run_status(r, ws),
    )
    logging.info(f"Job run {run.run_id} completed successfully for dqx_multi_table_demo")


def test_run_dqx_demo_summary_metrics(ws, make_notebook, make_schema, make_job, library_ref):
    path = Path(__file__).parent.parent.parent / "demos" / "dqx_demo_summary_metrics.py"
    with open(path, "rb") as f:
        notebook = make_notebook(content=f, format=ImportFormat.SOURCE)

    catalog = TEST_CATALOG
    schema = make_schema(catalog_name=catalog).name
    notebook_path = notebook.as_fuse().as_posix()
    notebook_task = NotebookTask(
        notebook_path=notebook_path,
        base_parameters={
            "demo_catalog": catalog,
            "demo_schema": schema,
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


def test_run_dqx_ai_assisted_quality_checks_generation(ws, make_notebook, make_job, library_ref):
    path = Path(__file__).parent.parent.parent / "demos" / "dqx_demo_ai_assisted_checks_generation.py"
    with open(path, "rb") as f:
        notebook = make_notebook(content=f, format=ImportFormat.SOURCE)

    notebook_path = notebook.as_fuse().as_posix()
    notebook_task = NotebookTask(notebook_path=notebook_path, base_parameters={"test_library_ref": library_ref})
    job = make_job(tasks=[Task(task_key="dqx_demo_ai_assisted_checks_generation", notebook_task=notebook_task)])

    waiter = ws.jobs.run_now_and_wait(job.job_id)
    run = ws.jobs.wait_get_run_job_terminated_or_skipped(
        run_id=waiter.run_id,
        timeout=timedelta(minutes=30),
        callback=lambda r: validate_run_status(r, ws),
    )
    logging.info(f"Job run {run.run_id} completed successfully for dqx_demo_ai_assisted_checks_generation")


def test_run_dqx_demo_datacontract_odcs(ws, make_notebook, make_job, library_ref):
    """Test the ODCS v3.x data contract demo notebook."""
    path = Path(__file__).parent.parent.parent / "demos" / "dqx_demo_datacontract_odcs.py"
    with open(path, "rb") as f:
        notebook = make_notebook(content=f, format=ImportFormat.SOURCE)

    notebook_path = notebook.as_fuse().as_posix()
    notebook_task = NotebookTask(notebook_path=notebook_path, base_parameters={"test_library_ref": library_ref})
    job = make_job(tasks=[Task(task_key="dqx_demo_datacontract_odcs", notebook_task=notebook_task)])

    waiter = ws.jobs.run_now_and_wait(job.job_id)
    run = ws.jobs.wait_get_run_job_terminated_or_skipped(
        run_id=waiter.run_id,
        timeout=timedelta(minutes=30),
        callback=lambda r: validate_run_status(r, ws),
    )
    logging.info(f"Job run {run.run_id} completed successfully for dqx_demo_datacontract_odcs")


def test_run_dqx_demo_llm_pk_detection(ws, make_notebook, make_job, library_ref):
    path = Path(__file__).parent.parent.parent / "demos" / "dqx_demo_llm_pk_detection.py"
    with open(path, "rb") as f:
        notebook = make_notebook(content=f, format=ImportFormat.SOURCE)

    notebook_path = notebook.as_fuse().as_posix()
    notebook_task = NotebookTask(notebook_path=notebook_path, base_parameters={"test_library_ref": library_ref})
    job = make_job(tasks=[Task(task_key="dqx_demo_llm_pk_detection", notebook_task=notebook_task)])

    waiter = ws.jobs.run_now_and_wait(job.job_id)
    run = ws.jobs.wait_get_run_job_terminated_or_skipped(
        run_id=waiter.run_id,
        timeout=timedelta(minutes=30),
        callback=lambda r: validate_run_status(r, ws),
    )
    logging.info(f"Job run {run.run_id} completed successfully for dqx_demo_llm_pk_detection")


def test_run_dqx_anomaly_detection_demo(ws, make_notebook, make_job, library_ref):
    """Test the anomaly detection demo notebook."""
    path = Path(__file__).parent.parent.parent / "demos" / "dqx_anomaly_detection_demo.py"
    with open(path, "rb") as f:
        notebook = make_notebook(content=f, format=ImportFormat.SOURCE)

    notebook_path = notebook.as_fuse().as_posix()
    notebook_task = NotebookTask(notebook_path=notebook_path, base_parameters={"test_library_ref": library_ref})
    job = make_job(tasks=[Task(task_key="dqx_anomaly_detection_demo", notebook_task=notebook_task)])

    waiter = ws.jobs.run_now_and_wait(job.job_id)
    run = ws.jobs.wait_get_run_job_terminated_or_skipped(
        run_id=waiter.run_id,
        timeout=timedelta(minutes=30),
        callback=lambda r: validate_run_status(r, ws),
    )
    logging.info(f"Job run {run.run_id} completed successfully for dqx_anomaly_detection_demo")


def test_dbt_demo(make_schema, make_random, library_ref, debug_env):
    catalog = "main"
    schema = make_schema(catalog_name=catalog).name
    project_dir = Path(__file__).parent.parent.parent / "demos" / "dqx_demo_dbt"

    # Create a temporary directory for DBT profiles
    with TemporaryDirectory() as temp_dir:
        dbt_profiles_dir = Path(temp_dir) / "dbt"
        dbt_profiles_dir.mkdir(parents=True, exist_ok=True)

        client_id = debug_env.get("TOOLS_CLIENT_ID")
        client_secret = debug_env.get("TOOLS_DATABRICKS_SECRET")
        token = debug_env.get("DATABRICKS_TOKEN")  # for local execution
        auth_type = "token"  # for local execution

        if client_id and client_secret:  # for CI execution
            auth_type = "oauth"

        # Create the profiles.yml file
        profiles_yml_content = f"""
        dbt_demo:
          target: ci
          outputs:
            ci:
              type: databricks
              host: "{debug_env.get("DATABRICKS_HOST")}"
              http_path: "{debug_env.get("TEST_DEFAULT_WAREHOUSE_HTTP_PATH")}"
              catalog: "{catalog}"
              schema: "{schema}"
              auth_type: {auth_type}
              client_id: {client_id}
              client_secret: {client_secret}
              token: {token}
              threads: 1
              connect_timeout: 30
        """
        profiles_yml_path = dbt_profiles_dir / "profiles.yml"
        profiles_yml_path.write_text(profiles_yml_content.strip())

        # Run dbt run
        subprocess.run(
            ["dbt", "run", "--debug", "--project-dir", str(project_dir), "--profiles-dir", str(dbt_profiles_dir)],
            check=True,
        )
