import logging
from pathlib import Path

from tests.e2e.conftest import run_notebook_job


logger = logging.getLogger(__name__)


def test_run_apply_checks_foreachbatch_notebook(make_notebook, make_job, library_ref):
    notebook_path = Path(__file__).parent / "notebooks" / "apply_checks_foreachbatch_notebook.py"
    run_notebook_job(
        notebook_path=notebook_path,
        make_notebook=make_notebook,
        make_job=make_job,
        library_reference=library_ref,
        task_key="observable_metrics_notebook",
    )
