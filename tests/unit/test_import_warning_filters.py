"""What importing DQX does to warnings and to mlflow's logger.

DQX calls ``logging.captureWarnings(True)``, so a warning raised anywhere in the process is routed
through DQX's ``py.warnings`` handler and reads as if DQX emitted it. That makes the filter list a
user-facing contract rather than an implementation detail: too narrow and users see third-party noise
attributed to us, too broad and a real warning disappears.

Both assertions run in a **subprocess**. Under pytest they would be meaningless: pytest installs its own
warnings filters around every test, discarding what a library configured at import time, and other test
modules import mlflow and mutate its logger. A fresh interpreter is the only faithful instrument here.
"""

import subprocess
import sys
import textwrap


def _in_fresh_interpreter(body: str) -> str:
    """Run *body* in a new interpreter that imports DQX, and return its stdout."""
    result = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(body)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, f"subprocess failed:\n{result.stdout}\n{result.stderr}"
    return result.stdout


def test_application_field_naming_and_unrelated_warnings_remain_visible():
    output = _in_fresh_interpreter(
        """
        import warnings
        import databricks.labs.dqx
        from pydantic import BaseModel, ConfigDict

        survivors = []
        warnings.showwarning = lambda message, category, *a, **k: survivors.append(str(message))

        class ApplicationConfig(BaseModel):
            model_config = ConfigDict(protected_namespaces=("model_",))
            model_name: str = ""

        warnings.warn("an unrelated user warning", UserWarning)
        print("PROTECTED:", any("protected namespace" in s for s in survivors))
        print("UNRELATED:", any("an unrelated user warning" in s for s in survivors))
        """
    )
    assert "PROTECTED: True" in output, output
    assert "UNRELATED: True" in output, output


def test_import_preserves_the_applications_mlflow_log_level():
    output = _in_fresh_interpreter(
        """
        import logging
        logger = logging.getLogger("mlflow")
        logger.setLevel(logging.WARNING)
        import databricks.labs.dqx
        import mlflow.sklearn
        print("LEVEL:", logging.getLevelName(logger.level))
        print("WARNING_ENABLED:", logger.isEnabledFor(logging.WARNING))
        """
    )
    assert "LEVEL: WARNING" in output, output
    assert "WARNING_ENABLED: True" in output, output


def test_dqx_leaves_its_own_logger_at_info():
    """The counterpart: quieting mlflow must not quiet DQX's own progress messages."""
    output = _in_fresh_interpreter(
        """
        import logging
        import databricks.labs.dqx
        print("DATABRICKS:", logging.getLevelName(logging.getLogger("databricks").level))
        print("DQX_INFO_ENABLED:", logging.getLogger("databricks.labs.dqx.anomaly").isEnabledFor(logging.INFO))
        """
    )

    assert "DATABRICKS: INFO" in output, output
    assert "DQX_INFO_ENABLED: True" in output, output
