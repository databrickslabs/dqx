import logging
import re

import databricks.sdk.useragent as ua
from databricks.labs.blueprint.logger import install_logger
from databricks.labs.dqx.__about__ import __version__

install_logger()

logging.getLogger("databricks").setLevel(logging.INFO)
logging.getLogger("pyspark.sql.connect.logging").setLevel(logging.CRITICAL)
logging.getLogger("mlflow").setLevel(logging.ERROR)

# Disable MLflow Trace UI in notebooks
# databricks-langchain automatically enables MLflow tracing when it's imported
try:
    import mlflow

    # Disable the mlflow tracing and notebook display widget
    mlflow.tracing.disable_notebook_display()
    # Disable automatic tracing for LangChain (source of the trace data)
    mlflow.langchain.autolog(disable=True)
except (ImportError, AttributeError):
    # MLflow not installed or tracing not available
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
