"""Production anomaly imports must work without pytest modifying third-party modules."""

import subprocess
import sys


def test_anomaly_scorers_import_in_fresh_interpreter() -> None:
    """Catch driver-only compatibility patches that leave Python workers broken."""
    completed = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            "import databricks.labs.dqx.anomaly.single_model_scorer; "
            "import databricks.labs.dqx.anomaly.ensemble_scorer; "
            "import shap; import numba; import coverage",
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )
    assert completed.returncode == 0, completed.stderr
