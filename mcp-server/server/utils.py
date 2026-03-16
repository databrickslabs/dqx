import contextvars
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

header_store = contextvars.ContextVar("header_store")

_ws = None
_spark = None
_profiler = None
_generator = None
_engine = None


def get_workspace_client():
    from databricks.sdk import WorkspaceClient

    is_databricks_app = "DATABRICKS_APP_NAME" in os.environ
    if not is_databricks_app:
        return WorkspaceClient()

    headers = header_store.get({})
    token = headers.get("x-forwarded-access-token")
    if not token:
        return WorkspaceClient()

    return WorkspaceClient(token=token, auth_type="pat")


def _get_ws():
    global _ws
    if _ws is None:
        from databricks.sdk import WorkspaceClient

        _ws = WorkspaceClient()
    return _ws


def _get_spark():
    global _spark, _profiler, _generator, _engine
    if _spark is not None:
        try:
            _spark.sql("SELECT 1")
        except Exception:
            logger.warning("Spark session expired, creating a new one")
            _spark = None
            _profiler = None
            _generator = None
            _engine = None
    if _spark is None:
        from databricks.connect import DatabricksSession

        _spark = DatabricksSession.builder.serverless(True).getOrCreate()
    return _spark


def _get_profiler():
    global _profiler
    if _profiler is None:
        from databricks.labs.dqx.profiler.profiler import DQProfiler

        _profiler = DQProfiler(workspace_client=_get_ws(), spark=_get_spark())
    return _profiler


def _get_generator():
    global _generator
    if _generator is None:
        from databricks.labs.dqx.profiler.generator import DQGenerator

        _generator = DQGenerator(workspace_client=_get_ws(), spark=_get_spark())
    return _generator


def _get_engine():
    global _engine
    if _engine is None:
        from databricks.labs.dqx.engine import DQEngine

        _engine = DQEngine(workspace_client=_get_ws(), spark=_get_spark())
    return _engine


def make_json_safe(value: Any) -> Any:
    """Recursively convert values that are not JSON-serializable (e.g. Decimal, datetime)."""
    import datetime
    from decimal import Decimal

    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: make_json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [make_json_safe(v) for v in value]
    return value


def compute_rule_summary(invalid_df) -> list[dict]:
    """Aggregate per-rule error/warning counts from the invalid DataFrame."""
    import pyspark.sql.functions as F

    summary: dict[str, dict[str, int]] = {}

    for col_name in ("_errors", "_warnings"):
        if col_name not in invalid_df.columns:
            continue
        exploded = invalid_df.select(F.explode(F.col(col_name)).alias("item"))
        rows = exploded.groupBy("item.name").count().collect()
        for row in rows:
            rule_name = row["name"] or "unknown"
            if rule_name not in summary:
                summary[rule_name] = {"error_count": 0, "warning_count": 0}
            if col_name == "_errors":
                summary[rule_name]["error_count"] = row["count"]
            else:
                summary[rule_name]["warning_count"] = row["count"]

    return [{"rule_name": name, **counts} for name, counts in summary.items()]
