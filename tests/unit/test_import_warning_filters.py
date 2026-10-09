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


def test_the_mlflow_field_naming_warning_is_filtered_and_others_are_not():
    """mlflow declares a pydantic field named *model_name*, which trips pydantic's ``model_`` namespace.

    Traced to ``mlflow.entities.model_registry.prompt_version.PromptModelConfig``; it fires on
    ``import mlflow`` itself, before any DQX module loads, so DQX can neither avoid triggering it nor fix
    it upstream. The shape is reproduced here by setting *protected_namespaces* explicitly rather than by
    importing mlflow, because pydantic narrowed its own default in 2.9 and reworded the message: under the
    version pinned here a bare ``model_name`` field warns about nothing at all, so a test that declared
    one would pass whether or not the filter existed.
    """
    output = _in_fresh_interpreter(
        """
        import warnings
        import databricks.labs.dqx  # installs the filters
        from pydantic import BaseModel, ConfigDict

        survivors = []
        warnings.showwarning = lambda message, category, *a, **k: survivors.append(str(message))

        class LikeMlflowsPromptModelConfig(BaseModel):
            model_config = ConfigDict(protected_namespaces=("model_",))
            model_name: str = ""

        warnings.warn("an unrelated user warning", UserWarning)
        print("PROTECTED:", any("protected namespace" in s for s in survivors))
        print("UNRELATED:", any("an unrelated user warning" in s for s in survivors))
        """
    )

    assert "PROTECTED: False" in output, f"the mlflow naming warning should be filtered, got:\n{output}"
    assert "UNRELATED: True" in output, f"an unrelated UserWarning must still reach the user, got:\n{output}"


def test_mlflows_logger_stays_quiet_after_mlflow_is_imported():
    """The regression guard for an ordering bug: mlflow re-configures its own logger while importing.

    DQX set the level with the other logger levels, *before* importing mlflow a few lines later, so
    mlflow's import overwrote it and every mlflow INFO line reached the user anyway -- model-registration
    lines and tracing notices filling the anomaly demos' cell output. Measured sequence before the fix:
    NOTSET, then ERROR once DQX set it, then back to INFO once mlflow imported.
    """
    output = _in_fresh_interpreter(
        """
        import logging
        import databricks.labs.dqx  # sets the level, after importing mlflow itself
        print("AFTER_DQX:", logging.getLevelName(logging.getLogger("mlflow").level))
        import mlflow.sklearn  # a later submodule import must not undo it
        print("AFTER_SUBMODULE:", logging.getLevelName(logging.getLogger("mlflow").level))
        print("INFO_ENABLED:", logging.getLogger("mlflow.tracking").isEnabledFor(logging.INFO))
        """
    )

    assert "AFTER_DQX: ERROR" in output, f"expected mlflow quiet right after importing dqx, got:\n{output}"
    assert "AFTER_SUBMODULE: ERROR" in output, f"a later mlflow submodule import undid it:\n{output}"
    assert "INFO_ENABLED: False" in output, f"mlflow INFO records are still enabled:\n{output}"


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
