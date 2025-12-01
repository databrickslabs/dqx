import re
import sys
import functools
import logging
from io import StringIO
from collections.abc import Callable
from pyspark.sql import DataFrame, SparkSession
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError


logger = logging.getLogger(__name__)


def log_telemetry(ws: WorkspaceClient, key: str, value: str) -> None:
    """
    Trace specific telemetry information in the Databricks workspace by setting user agent extra info.

    Args:
        ws: WorkspaceClient
        key: telemetry key to log
        value: telemetry value to log
    """
    new_config = ws.config.copy().with_user_agent_extra(key, value)
    logger.debug(f"Added User-Agent extra {key}={value}")

    # Recreate the WorkspaceClient from the same type to preserve type information
    ws = type(ws)(config=new_config)

    try:
        # use api that works on all workspaces and clusters including group assigned clusters
        ws.clusters.select_spark_version()
    except DatabricksError as e:
        # support local execution
        logger.debug(f"Databricks workspace is not available: {e}")


def telemetry_logger(key: str, value: str, workspace_client_attr: str = "ws") -> Callable:
    """
    Decorator to log telemetry for method calls.
    By default, it expects the decorated method to have "ws" attribute for workspace client.

    Usage:
        @telemetry_logger("telemetry_key", "telemetry_value")  # Uses "ws" attribute for workspace client by default
        @telemetry_logger("telemetry_key", "telemetry_value", "my_ws_client")  # Custom attribute

    Args:
        key: Telemetry key to log
        value: Telemetry value to log
        workspace_client_attr: Name of the workspace client attribute on the class (defaults to "ws")
    """

    def decorator(func: Callable) -> Callable:

        @functools.wraps(func)  # preserve function metadata
        def wrapper(self, *args, **kwargs):
            if hasattr(self, workspace_client_attr):
                workspace_client = getattr(self, workspace_client_attr)
                log_telemetry(workspace_client, key, value)
            else:
                raise AttributeError(
                    f"Workspace client attribute '{workspace_client_attr}' not found on {self.__class__.__name__}. "
                    f"Make sure your class has the specified workspace client attribute."
                )
            return func(self, *args, **kwargs)

        return wrapper

    return decorator


def count_tables_in_spark_plan(df: DataFrame) -> int:
    """
    Count the number of tables referenced in a DataFrame's Spark execution plan.

    This function analyzes the Analyzed Logical Plan section of the Spark execution plan
    to identify table references (via SubqueryAlias nodes). File-based DataFrames and
    in-memory DataFrames will return 0.

    Args:
        df: The Spark DataFrame to analyze

    Returns:
        The number of distinct tables found in the execution plan. Returns 0 if the plan
        cannot be retrieved or contains no table references.
    """
    try:
        plan_str = _get_spark_plan_as_string(df)
        if not plan_str:
            return 0
        tables = _extract_tables_from_spark_plan(plan_str)
        return len(tables)
    except Exception as e:
        logger.debug(f"Failed to count tables in Spark plan: {e}")
        return 0


def is_dlt_pipeline(spark: SparkSession) -> bool:
    try:
        # Attempt to retrieve the DLT pipeline ID from the Spark configuration
        dlt_pipeline_id = spark.conf.get('pipelines.id', None)
        return bool(dlt_pipeline_id)  # Return True if the ID exists, otherwise False
    except Exception:
        # Return False if an exception occurs (e.g. in non-DLT serverless clusters)
        return False


def _get_spark_plan_as_string(df: DataFrame) -> str:
    """
    Retrieve the Spark execution plan as a string by capturing df.explain() output.

    This function temporarily redirects stdout to capture the output of df.explain(True),
    which prints the detailed execution plan including the Analyzed Logical Plan.

    Args:
        df: The Spark DataFrame to get the execution plan from

    Returns:
        The complete execution plan as a string, or empty string if explain() fails
    """
    buf = StringIO()
    old_stdout = sys.stdout
    sys.stdout = buf
    try:
        df.explain(True)
    except Exception as e:
        logger.debug(f"Failed to get Spark execution plan: {e}")
        return ""
    finally:
        sys.stdout = old_stdout
    return buf.getvalue()


def _extract_tables_from_spark_plan(plan_str: str) -> set[str]:
    """
    Extract table names from the Analyzed Logical Plan section of a Spark execution plan.

    This function parses the Analyzed Logical Plan section and identifies table references
    by finding SubqueryAlias nodes, which Spark uses to represent table references in the
    logical plan. File-based sources (e.g., Delta files from volumes) and in-memory DataFrames
    do not create SubqueryAlias nodes and therefore won't be counted as tables.

    Args:
        plan_str: The complete Spark execution plan string (from df.explain(True))

    Returns:
        A set of distinct table names found in the plan. Returns empty set if no
        Analyzed Logical Plan section is found or no tables are referenced.
    """
    tables: set[str] = set()

    # Extract Analyzed Logical Plan section (stop at next "==")
    match = re.search(r"== Analyzed Logical Plan ==\s*(.*?)\n==", plan_str, re.DOTALL)  # non-greedy until next section
    if not match:
        return tables

    analyzed_text = match.group(1)

    # Extract SubqueryAlias names (only present if table is used)
    subquery_aliases = re.findall(r"SubqueryAlias\s+([^\s]+)", analyzed_text)
    tables.update(subquery_aliases)

    return tables
