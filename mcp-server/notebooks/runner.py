# Databricks notebook source

# COMMAND ----------
# %pip install databricks-labs-dqx

# COMMAND ----------

import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dqx-mcp-runner")

# COMMAND ----------

# Read parameters from notebook widgets
dbutils.widgets.text("operation", "")
dbutils.widgets.text("params", "{}")

operation = dbutils.widgets.get("operation")
params = json.loads(dbutils.widgets.get("params"))

logger.info(f"Running operation: {operation}, params keys: {list(params.keys())}")

# COMMAND ----------

def get_table_schema(params: dict) -> dict:
    """Retrieve schema and row count for a table."""
    table_name = params["table_name"]
    df = spark.table(table_name)
    columns = [
        {"name": f.name, "type": str(f.dataType), "nullable": f.nullable}
        for f in df.schema.fields
    ]
    row_count = df.count()
    return {
        "table_name": table_name,
        "columns": columns,
        "row_count": row_count,
    }

# COMMAND ----------

def profile_table(params: dict) -> dict:
    """Profile a table and return summary stats + profiles."""
    from databricks.sdk import WorkspaceClient
    from databricks.labs.dqx.profiler.profiler import DQProfiler
    from databricks.labs.dqx.config import InputConfig
    from dataclasses import asdict

    table_name = params["table_name"]
    columns = params.get("columns")
    options = params.get("options")

    ws = WorkspaceClient()
    profiler = DQProfiler(workspace_client=ws, spark=spark)
    input_config = InputConfig(location=table_name)
    summary_stats, profiles = profiler.profile_table(input_config, columns=columns, options=options)

    profiles_dicts = [asdict(p) for p in profiles]
    return {
        "table_name": table_name,
        "summary_stats": _make_json_safe(summary_stats),
        "profiles": _make_json_safe(profiles_dicts),
    }

# COMMAND ----------

def generate_rules(params: dict) -> dict:
    """Generate DQX rules from profiling output."""
    from databricks.sdk import WorkspaceClient
    from databricks.labs.dqx.profiler.profiler import DQProfile
    from databricks.labs.dqx.profiler.generator import DQGenerator

    profiles = params["profiles"]
    criticality = params.get("criticality", "error")

    dq_profiles = [
        DQProfile(
            name=p["name"],
            column=p["column"],
            description=p.get("description"),
            parameters=p.get("parameters"),
            filter=p.get("filter"),
        )
        for p in profiles
    ]

    ws = WorkspaceClient()
    generator = DQGenerator(workspace_client=ws, spark=spark)
    rules = generator.generate_dq_rules(dq_profiles, criticality=criticality)

    return {
        "rules": _make_json_safe(rules),
        "count": len(rules),
    }

# COMMAND ----------

def run_checks(params: dict) -> dict:
    """Run DQX checks against a table."""
    from databricks.sdk import WorkspaceClient
    from databricks.labs.dqx.engine import DQEngine
    import pyspark.sql.functions as F

    table_name = params["table_name"]
    checks = params["checks"]
    sample_size = params.get("sample_size", 50)

    ws = WorkspaceClient()
    engine = DQEngine(workspace_client=ws, spark=spark)

    df = spark.table(table_name)
    valid_df, invalid_df = engine.apply_checks_by_metadata_and_split(df, checks)

    total_rows = df.count()
    valid_rows = valid_df.count()
    invalid_rows = invalid_df.count()

    error_sample_rows = invalid_df.limit(sample_size).collect()
    error_sample = [_make_json_safe(row.asDict(recursive=True)) for row in error_sample_rows]

    # Compute per-rule summary
    rule_summary = _compute_rule_summary(invalid_df)

    return {
        "table_name": table_name,
        "total_rows": total_rows,
        "valid_rows": valid_rows,
        "invalid_rows": invalid_rows,
        "error_sample": error_sample,
        "rule_summary": rule_summary,
    }

# COMMAND ----------

def _make_json_safe(value):
    """Recursively convert non-JSON-serializable values."""
    import datetime
    from decimal import Decimal

    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _make_json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_make_json_safe(v) for v in value]
    return value

# COMMAND ----------

def _compute_rule_summary(invalid_df) -> list:
    """Aggregate per-rule error/warning counts."""
    import pyspark.sql.functions as F

    summary = {}
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

# COMMAND ----------

# Operation dispatch
OPERATIONS = {
    "get_table_schema": get_table_schema,
    "profile_table": profile_table,
    "generate_rules": generate_rules,
    "run_checks": run_checks,
}

# COMMAND ----------

try:
    if operation not in OPERATIONS:
        result = {"error": f"Unknown operation: {operation}. Valid: {list(OPERATIONS.keys())}"}
    else:
        result = OPERATIONS[operation](params)
except Exception as e:
    logger.error(f"Operation '{operation}' failed: {e}", exc_info=True)
    result = {"error": f"{type(e).__name__}: {str(e)}"}

# Check output size before exit (5MB limit)
output_json = json.dumps(result)
if len(output_json) > 4_500_000:  # 4.5MB safety margin
    result = {
        "error": "Output too large for notebook.exit() (>4.5MB). Try reducing sample_size.",
        "truncated": True,
    }
    output_json = json.dumps(result)

logger.info(f"Operation '{operation}' complete. Output size: {len(output_json)} bytes")
dbutils.notebook.exit(output_json)
