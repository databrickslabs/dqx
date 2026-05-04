import logging
from pathlib import Path

from tests.constants import TEST_CATALOG
from tests.e2e.conftest import new_classic_job_cluster, run_notebook_job

logger = logging.getLogger(__name__)


def test_run_anomaly_distributed_scoring_notebook(make_schema, make_notebook, make_job, library_ref):
    """Run anomaly scoring in distributed (non-driver-only) mode on a cluster via notebook."""
    schema = make_schema(catalog_name=TEST_CATALOG).name
    notebook_path = Path(__file__).parent / "notebooks" / "anomaly_distributed_scoring_notebook.py"
    base_parameters = {
        "test_catalog": TEST_CATALOG,
        "test_schema": schema,
        "test_library_ref": library_ref,
    }
    run_notebook_job(
        notebook_path=notebook_path,
        make_notebook=make_notebook,
        make_job=make_job,
        library_reference=library_ref,
        task_key="anomaly_distributed_scoring_notebook",
        base_parameters=base_parameters,
        new_cluster=new_classic_job_cluster(),
    )
