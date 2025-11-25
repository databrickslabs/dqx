import logging
from pathlib import Path
from tests.e2e.conftest import new_classic_job_cluster, run_notebook_job


logger = logging.getLogger(__name__)


def test_run_foreachbatch_notebook(make_notebook, make_job, library_ref):
    notebook_path = Path(__file__).parent / "notebooks" / "foreachbatch_notebook.py"
    run_notebook_job(
        notebook_path=notebook_path,
        make_notebook=make_notebook,
        make_job=make_job,
        library_reference=library_ref,
        task_key="foreachbatch_notebook",
        new_cluster=new_classic_job_cluster(),
    )
