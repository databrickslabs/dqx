import logging
import os

from datetime import timedelta
from pathlib import Path
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat
from databricks.sdk.service.jobs import NotebookTask, Task

from tests.e2e.conftest import validate_run_status

logging.getLogger("tests").setLevel("DEBUG")
logging.getLogger("databricks.labs.dqx").setLevel("DEBUG")
logger = logging.getLogger(__name__)

RETRY_INTERVAL_SECONDS = 30
TEST_LIBRARY_REF = "git+https://github.com/databrickslabs/dqx"
if os.getenv("REF_NAME"):
    TEST_LIBRARY_REF = f"{TEST_LIBRARY_REF}.git@refs/pull/{os.getenv('REF_NAME')}"
logger.info(f"Running PII detection tests from {TEST_LIBRARY_REF}")


def test_run_pii_detection_notebook(make_notebook, make_job):
    path = Path(__file__).parent / "notebooks" / "pii_detection_notebook.py"
    ws = WorkspaceClient()
    with open(path, "rb") as f:
        notebook = make_notebook(content=f, format=ImportFormat.SOURCE)

    notebook_path = notebook.as_fuse().as_posix()
    notebook_task = NotebookTask(
        notebook_path=notebook_path,
        base_parameters={"test_library_ref": TEST_LIBRARY_REF},
    )
    job = make_job(tasks=[Task(task_key="pii_detection_notebook", notebook_task=notebook_task)],)

    waiter = ws.jobs.run_now_and_wait(job.job_id)
    run = ws.jobs.wait_get_run_job_terminated_or_skipped(
        run_id=waiter.run_id,
        timeout=timedelta(minutes=30),
        callback=lambda r: validate_run_status(r, client=ws),
    )
    logging.info(f"Job run {run.run_id} completed successfully for pii_detection_notebook")
