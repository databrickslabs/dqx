import logging
import re
import warnings

import databricks.sdk.useragent as ua
from databricks.labs.blueprint.logger import install_logger
from databricks.labs.dqx.__version__ import __version__

# Suppress Databricks notebook LSP warning
warnings.filterwarnings(
    "ignore",
    message=r".*make_tokens_by_line.*lineending",
    category=UserWarning,
)

# MLflow declares a pydantic field named *model_name*, which collides with pydantic's own ``model_``
# protected namespace and warns at class-definition time. Traced to
# ``mlflow.entities.model_registry.prompt_version.PromptModelConfig`` on a Databricks runtime
# (mlflow 3.16.1, pydantic 2.8.2) by walking ``BaseModel.__subclasses__()``; it fires on ``import
# mlflow`` itself, before any DQX module is imported, so nothing here causes it and nothing here can
# fix it. It is suppressed rather than left alone because DQX imports mlflow on the user's behalf from
# the anomaly modules, and DQX also calls ``logging.captureWarnings(True)`` below -- so without this
# filter every anomaly user sees an MLflow internal naming detail, attributed to DQX's logging, that
# they can do nothing about. Matched on the message so an unrelated pydantic warning still surfaces.
# The wording differs across pydantic releases -- 2.8 says `has conflict with protected namespace`,
# 2.9 onward `in '<Class>' conflicts with protected namespace` and quotes the field differently -- so the
# pattern keys on the parts both spell the same way rather than on one release's sentence.
warnings.filterwarnings(
    "ignore",
    message=r"""Field ["']model_name["'].*(conflict|conflicts) with protected namespace""",
    category=UserWarning,
)

# Do not reconfigure the root logger on import (issue #1136); a library should leave logging
# configuration to the application. We stay scoped to our OWN logger ("databricks.labs.dqx") and
# never touch root. A bare NullHandler alone means that on runtimes where NO logger from ours up to
# root has a handler (e.g. Databricks serverless), DQX's own WARNING/INFO records — including the
# plaintext-secret warning and the log alert destination — are silently dropped. So we attach a
# dedicated stderr StreamHandler as a baseline sink, but ONLY when no real handler exists anywhere in
# the hierarchy from our logger up to root. Checking the whole chain (not just our own logger) avoids
# double-logging when the application/runtime has already configured a root handler that receives our
# records via propagation.


def _has_effective_handler(logger_obj: logging.Logger) -> bool:
    """Whether any real (non-NullHandler) handler exists from *logger_obj* up to root.

    Mirrors logging's own propagation-based dispatch: a record emitted on *logger_obj* is delivered
    to the handlers of every ancestor while *propagate* is True. If any such handler exists the record
    is already sinked somewhere, so DQX must not add its own baseline handler (which would duplicate).
    """
    current: logging.Logger | None = logger_obj
    while current:
        if any(not isinstance(handler, logging.NullHandler) for handler in current.handlers):
            return True
        if not current.propagate:
            break
        current = current.parent
    return False


_dqx_logger = logging.getLogger("databricks.labs.dqx")
if not _has_effective_handler(_dqx_logger):
    # Reuse blueprint's install_logger (same NiceFormatter/stream the install logger uses) but point
    # it at OUR logger via root=_dqx_logger instead of the real root — so we never reconfigure root
    # (#1136). Only invoked when nothing upstream already sinks our records, so no double-logging.
    install_logger(root=_dqx_logger)
_dqx_logger.addHandler(logging.NullHandler())

# Route Python warnings through logging for consistent formatting
# (Some modules like check_funcs still use warnings.warn for backward compatibility)
logging.captureWarnings(True)
warnings_logger = logging.getLogger("py.warnings")
warnings_logger.setLevel(logging.INFO)
# Ensure captured warnings display the message correctly (avoids "%s" placeholder in some envs)
if not warnings_logger.handlers:
    _wh = logging.StreamHandler()
    _wh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s"))
    warnings_logger.addHandler(_wh)
    warnings_logger.propagate = False

# Configure logger levels
logging.getLogger("databricks").setLevel(logging.INFO)
logging.getLogger("pyspark.sql.connect.logging").setLevel(logging.CRITICAL)
logging.getLogger("pyspark.sql.connect.client.logging").setLevel(logging.CRITICAL)
# pyspark.pandas attaches a JVM-backed usage logger on import; under Spark Connect there is
# no local JVM, so the attach fails and emits a harmless WARNING on every import. Suppress it.
logging.getLogger("pyspark.pandas.usage_logger").setLevel(logging.ERROR)


# Disable MLflow Trace UI in notebooks
# databricks-langchain automatically enables MLflow tracing when it's imported
try:
    import mlflow

    # Disable the mlflow tracing and notebook display widget
    mlflow.tracing.disable_notebook_display()
    # Disable automatic tracing for LangChain (source of the trace data)
    mlflow.langchain.autolog(disable=True)
    # Quiet mlflow's own INFO chatter -- model-registration lines, tracing notices, autolog messages.
    # This MUST come after `import mlflow`, not with the other logger levels above: mlflow configures
    # its own logger while importing, so a level set beforehand is overwritten and every INFO line
    # reaches the user anyway. Measured before this moved: NOTSET -> DQX sets ERROR -> mlflow's import
    # puts it back to INFO. Setting it here sticks, and survives later `mlflow.*` submodule imports.
    logging.getLogger("mlflow").setLevel(logging.ERROR)
except Exception:
    # MLflow not installed, tracing not available, or configuration failed
    # (e.g., Databricks auth not available in CI). Nothing to quiet in that case: setting the level
    # here would not survive a later `import mlflow` anyway, for the reason given above.
    pass

ua.semver_pattern = re.compile(
    r"^"
    r"(?P<major>0|[1-9]\d*)\.(?P<minor>x|0|[1-9]\d*)(\.(?P<patch>x|0|[1-9x]\d*))?"
    r"(?:-(?P<pre_release>(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)"
    r"(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?"
    r"(?:\+(?P<build>[0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$"
)

# Add dqx/<version> for projects depending on dqx as a library
ua.with_extra("dqx", __version__)

# Add dqx/<version> for re-packaging of dqx, where product name is omitted
ua.with_product("dqx", __version__)
