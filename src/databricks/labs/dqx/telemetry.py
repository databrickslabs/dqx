import re
import sys
import functools
import logging
import hashlib
from io import StringIO
from collections import OrderedDict
from collections.abc import Callable
from pyspark.sql import DataFrame, SparkSession
from databricks.sdk import WorkspaceClient


logger = logging.getLogger(__name__)

# Bound the control-plane ping so a workspace brownout cannot stall the caller. The SDK's default
# retry window is 300s; on repeated 503s it blocks for that long and then raises TimeoutError. A few
# seconds is ample for a healthy workspace, and telemetry is best-effort so giving up early is fine.
_TELEMETRY_TIMEOUT_SECONDS = 5

# Telemetry is a user-agent adoption signal (which features/checks/streaming/DLT are used), not an
# invocation counter, so each (key, value) only needs to be sent once per process. Caching the pairs
# already sent means the ping fires at most once per signal instead of once per check per micro-batch
# — bounding both the load on the control plane and the number of times a brownout can stall a caller.
#
# Most signals are naturally bounded (a fixed set of check names, streaming/dlt/workflow flags), but
# input_table / input_path add one hashed entry per *distinct* input, so a very long-lived process that
# reads an ever-growing set of tables could accumulate unboundedly. Cap the cache and evict oldest-first
# (FIFO via OrderedDict) so memory is hard-bounded; the realistic ceiling is far below the cap, so
# eviction only ever kicks in for pathological workloads and merely allows an evicted signal to be
# re-sent once later — harmless for best-effort telemetry.
#
# No lock: dict membership/set/popitem are atomic under the GIL. A concurrent check-then-add race can at
# most fire one duplicate ping, and since eviction only runs when the cache is over capacity (never near
# empty) a concurrent eviction can at most drop one extra entry — both harmless for a best-effort call,
# so a lock isn't worth the machinery.
_TELEMETRY_CACHE_MAX_SIZE = 10_000
_sent_telemetry: "OrderedDict[tuple[str, str], None]" = OrderedDict()


def reset_telemetry_cache() -> None:
    """Clear the per-process telemetry dedup cache so previously sent signals can be sent again.

    Telemetry is deduplicated per process (each *(key, value)* is sent at most once). This resets that
    state; mainly useful for tests that assert on send behavior in isolation.
    """
    _sent_telemetry.clear()


def log_telemetry(ws: WorkspaceClient, key: str, value: str) -> None:
    """
    Trace specific telemetry information in the Databricks workspace by setting user agent extra info.

    Best-effort: telemetry must never affect the data path. Each *(key, value)* is sent at most once
    per process, the control-plane call is bounded by a short timeout, and any failure is swallowed
    (logged at debug) so telemetry can never block for long or propagate an exception into user code.

    Args:
        ws: WorkspaceClient
        key: telemetry key to log
        value: telemetry value to log
    """
    # Deduplicate per process: skip if this signal was already sent (or attempted). Marking on attempt
    # (not only on success) means at most one control-plane call per signal per process, so a brownout
    # cannot re-stall the caller on every subsequent micro-batch.
    if (key, value) in _sent_telemetry:
        return
    _sent_telemetry[(key, value)] = None
    if len(_sent_telemetry) > _TELEMETRY_CACHE_MAX_SIZE:
        # Evict the oldest signal to keep memory hard-bounded (last=False -> FIFO).
        _sent_telemetry.popitem(last=False)

    try:
        new_config = ws.config.copy().with_user_agent_extra(key, value)
        # Cap the retry window (default 300s) and per-request time so a control-plane brownout gives
        # up in seconds instead of stalling the caller for minutes.
        new_config.retry_timeout_seconds = _TELEMETRY_TIMEOUT_SECONDS
        new_config.http_timeout_seconds = _TELEMETRY_TIMEOUT_SECONDS
        logger.debug(f"Added User-Agent extra {key}={value}")

        # Recreate the WorkspaceClient from the same type to preserve type information
        ws = type(ws)(config=new_config)

        # use api that works on all workspaces and clusters including group assigned clusters
        ws.clusters.select_spark_version()
    except Exception as e:
        # Broad on purpose: telemetry is best-effort and must never propagate into the data path.
        # Covers workspace unavailability (local execution), SDK retry-exhaustion TimeoutError, and any
        # other failure.
        logger.debug(f"Telemetry not sent for {key}={value}: {e}")


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


def log_dataframe_telemetry(ws: WorkspaceClient, spark: SparkSession, df: DataFrame):
    """
    Log telemetry information about a Spark DataFrame to the Databricks workspace including:
    - List of tables used as inputs (hashed)
    - List of file paths used as inputs (hashed, excluding paths from tables)
    - Whether the DataFrame is streaming
    - Whether running in a Delta Live Tables (DLT) pipeline

    This function is designed to never throw exceptions - it will log errors but continue execution
    to ensure telemetry failures don't break the main application flow.

    Args:
        ws: WorkspaceClient
        spark: SparkSession
        df: DataFrame to analyze

    Returns:
        None
    """
    # Wrap the whole body: telemetry must never propagate into user code. log_telemetry and the plan
    # helpers are individually guarded, but df.isStreaming can itself raise (see issue #1001), so the
    # outer guard honors the "never throw" contract regardless of which step fails.
    try:
        _log_dataframe_telemetry(ws, spark, df)
    except Exception as e:
        # Best-effort: log and continue so a telemetry failure never breaks the data path.
        logger.debug(f"DataFrame telemetry not logged: {e}")


def _log_dataframe_telemetry(ws: WorkspaceClient, spark: SparkSession, df: DataFrame) -> None:
    """Unguarded body of *log_dataframe_telemetry*; the caller wraps it so it can never throw."""
    log_telemetry(ws, "streaming", str(df.isStreaming).lower())
    log_telemetry(ws, "dlt", str(is_dlt_pipeline(spark)).lower())

    plan_str = get_spark_plan_as_string(df)
    if plan_str:
        input_tables = get_tables_from_spark_plan(plan_str)
        for table in input_tables:
            log_telemetry(ws, "input_table", hashlib.sha256(("id:" + table).encode("utf-8")).hexdigest())

        input_paths = get_paths_from_spark_plan(plan_str, input_tables)
        for path in input_paths:
            log_telemetry(ws, "input_path", hashlib.sha256(("id:" + path).encode("utf-8")).hexdigest())


def get_tables_from_spark_plan(plan_str: str) -> set[str]:
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
    try:
        return _extract_tables_from_analyzed_plan(plan_str)
    except Exception as e:
        logger.debug(f"Failed to extract tables from Spark plan: {e}")
        return set()


def get_paths_from_spark_plan(plan_str: str, table_names: set[str] | None = None) -> set[str]:
    """
    Extract file paths from the Physical Plan section of a Spark execution plan.

    This function parses the Physical Plan section and identifies file path references
    by finding any *FileIndex patterns in the Location field (e.g., PreparedDeltaFileIndex,
    ParquetFileIndex, etc.). These paths represent direct file-based data sources
    (e.g., files from volumes, DBFS, S3, etc.) that are not registered as tables.

    Args:
        plan_str: The complete Spark execution plan string (from df.explain(True))
        table_names: Optional set of table names to exclude (paths associated with tables are skipped)

    Returns:
        A set of distinct file paths found in the plan. Returns empty set if no
        Physical Plan section is found or no paths are referenced.
    """
    try:
        return _extract_paths_from_physical_plan(plan_str, table_names)
    except Exception as e:
        logger.debug(f"Failed to extract paths from Spark plan: {e}")
        return set()


def _extract_tables_from_analyzed_plan(plan_str: str) -> set[str]:
    """Helper function to extract tables from the Analyzed Logical Plan section."""
    tables: set[str] = set()

    # Extract Analyzed Logical Plan section (stop at next "==")
    match = re.search(r"== Analyzed Logical Plan ==\s*(.*?)\n==", plan_str, re.DOTALL)
    if not match:
        return tables

    analyzed_text = match.group(1)

    # Extract SubqueryAlias names (only present if table is used)
    subquery_aliases = re.findall(r"SubqueryAlias\s+([^\s]+)", analyzed_text)
    tables.update(alias.replace("`", "") for alias in subquery_aliases)

    return tables


def _extract_paths_from_physical_plan(plan_str: str, table_names: set[str] | None = None) -> set[str]:
    """Helper function to extract paths from the Physical Plan section."""
    paths: set[str] = set()

    # Extract Physical Plan section (stop at next "==")
    match = re.search(r"== Physical Plan ==\s*(.*?)(?:\n==|$)", plan_str, re.DOTALL)
    if not match:
        return paths

    physical_text = match.group(1)
    if table_names is None:
        table_names = set()

    # Process line by line to check for table associations
    for line in physical_text.split('\n'):
        location_match = re.search(r"Location:\s+\w+FileIndex\([^)]+\)\[([^\]]+)\]", line)
        if not location_match:
            continue

        # Skip if this line contains any table name (indicating it's a table-based path)
        if any(table_name in line for table_name in table_names):
            continue

        # Extract and add non-empty paths
        paths_str = location_match.group(1)
        path_list = [p.strip() for p in paths_str.split(',') if p.strip()]
        paths.update(path_list)

    return paths


def is_dlt_pipeline(spark: SparkSession) -> bool:
    """
    Determine if the current Spark session is running within a Databricks Delta Live Tables (DLT) pipeline.

    Args:
        spark: The SparkSession to check

    Returns:
        True if running in a DLT pipeline, False otherwise
    """
    try:
        # Attempt to retrieve the DLT pipeline ID from the Spark configuration
        dlt_pipeline_id = spark.conf.get('pipelines.id', None)
        return bool(dlt_pipeline_id)  # Return True if the ID exists, otherwise False
    except Exception:
        # Return False if an exception occurs (e.g. in non-DLT serverless clusters)
        return False


def get_spark_plan_as_string(df: DataFrame) -> str:
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
