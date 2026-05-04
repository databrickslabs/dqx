import logging
import re
import warnings

import databricks.sdk.useragent as ua
from databricks.labs.blueprint.logger import install_logger
from databricks.labs.dqx.__about__ import __version__

# Suppress Databricks notebook LSP warning
warnings.filterwarnings(
    "ignore",
    message=r".*make_tokens_by_line.*lineending",
    category=UserWarning,
)

install_logger()

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
logging.getLogger("mlflow").setLevel(logging.ERROR)
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
except Exception:
    # MLflow not installed, tracing not available, or configuration failed
    # (e.g., Databricks auth not available in CI)
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
