import logging
from pathlib import Path

from tests.conftest import TEST_CATALOG
from tests.e2e.conftest import new_classic_job_cluster, run_notebook_job


logger = logging.getLogger(__name__)


def test_run_apply_checks_foreachbatch_notebook(make_schema, make_volume, make_notebook, make_job, library_ref):
    schema = make_schema(catalog=TEST_CATALOG).name
    volume = make_volume(catalog=TEST_CATALOG, schema=schema).full_name
    notebook_path = Path(__file__).parent / "notebooks" / "save_dataframe_as_table_notebook.py"
    base_parameters = {
        "test_catalog": TEST_CATALOG,
        "test_schema": schema,
        "test_volume": volume,
        "test_library_ref": library_ref,
    }
    run_notebook_job(
        notebook_path=notebook_path,
        make_notebook=make_notebook,
        make_job=make_job,
        library_reference=library_ref,
        task_key="save_dataframe_as_table_notebook",
        base_parameters=base_parameters,
        new_cluster=new_classic_job_cluster(),
    )
