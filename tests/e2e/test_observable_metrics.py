import logging
from pathlib import Path

from tests.conftest import TEST_CATALOG
from tests.e2e.conftest import new_classic_job_cluster, run_notebook_job


logger = logging.getLogger(__name__)


def test_run_observable_metrics_notebook(make_notebook, make_schema, make_volume, make_job, library_ref):
    # TODO these test should be moved integration tests once Spark Connect issue for observation is fixed
    notebook_path = Path(__file__).parent / "notebooks" / "observable_metrics_notebook.py"
    catalog = TEST_CATALOG
    schema = make_schema(catalog_name=catalog).name
    volume = make_volume(catalog_name=catalog, schema_name=schema).name
    run_notebook_job(
        notebook_path=notebook_path,
        make_notebook=make_notebook,
        make_job=make_job,
        library_reference=library_ref,
        task_key="observable_metrics_notebook",
        base_parameters={"catalog_name": catalog, "schema_name": schema, "volume_name": volume},
        new_cluster=new_classic_job_cluster(),
    )
