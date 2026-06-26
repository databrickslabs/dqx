# Databricks notebook source

# COMMAND ----------
# %pip install databricks-labs-dqx  # installed via environment spec in databricks.yml

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


def profile_table(params: dict) -> dict:
    """Profile a view and return summary stats + profiles."""
    from databricks.sdk import WorkspaceClient
    from databricks.labs.dqx.profiler.profiler import DQProfiler
    from databricks.labs.dqx.config import InputConfig
    from dataclasses import asdict

    view_name = params["view_name"]
    columns = params.get("columns")
    options = params.get("options")

    ws = WorkspaceClient()
    profiler = DQProfiler(workspace_client=ws, spark=spark)
    input_config = InputConfig(location=view_name)
    summary_stats, profiles = profiler.profile_table(input_config, columns=columns, options=options)

    profiles_dicts = [asdict(p) for p in profiles]
    return {
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


def load_checks(params: dict) -> dict:
    """Load DQX checks from a storage backend (table, UC volume, or workspace file).

    The backend is inferred from the location string by DQX's storage factory:
    a 'catalog.schema.table' name -> Delta table, a '/Volumes/...' path -> UC volume
    file, any other '/...' path -> workspace file.
    """
    from databricks.sdk import WorkspaceClient
    from databricks.labs.dqx.engine import DQEngine
    from databricks.labs.dqx.checks_storage import ChecksStorageHandlerFactory

    location = params["location"]
    run_config_name = params.get("run_config_name", "default")

    ws = WorkspaceClient()
    engine = DQEngine(workspace_client=ws, spark=spark)
    _handler, config = ChecksStorageHandlerFactory(ws, spark).create_for_location(location, run_config_name)
    checks = engine.load_checks(config)

    return {"checks": _make_json_safe(checks), "count": len(checks), "location": location}


# COMMAND ----------


def save_checks(params: dict) -> dict:
    """Save DQX checks to a storage backend (table, UC volume, or workspace file).

    The backend is inferred from the location string. For table backends, *mode*
    ('append' or 'overwrite') controls write semantics; it is ignored for file backends.
    """
    from databricks.sdk import WorkspaceClient
    from databricks.labs.dqx.engine import DQEngine
    from databricks.labs.dqx.checks_storage import ChecksStorageHandlerFactory

    checks = params["checks"]
    location = params["location"]
    run_config_name = params.get("run_config_name", "default")
    mode = params.get("mode", "append")

    ws = WorkspaceClient()
    engine = DQEngine(workspace_client=ws, spark=spark)
    _handler, config = ChecksStorageHandlerFactory(ws, spark).create_for_location(location, run_config_name)
    # Table/Lakebase configs expose a write mode; file backends do not.
    if hasattr(config, "mode"):
        config.mode = mode
    engine.save_checks(checks, config)

    # Grant the calling user access to the created table (table backends only — a 3-part name,
    # not a /Volumes or /Workspace path) so they can use it outside the MCP.
    grant_to = params.get("grant_to")
    granted_to = None
    if grant_to and "/" not in location and location.count(".") == 2:
        if _grant_table_access(location, grant_to):
            granted_to = grant_to

    return {"saved": True, "count": len(checks), "location": location, "access_granted_to": granted_to}


# COMMAND ----------


def apply_checks_and_save_to_table(params: dict) -> dict:
    """Apply checks to a view and persist results to output (and optional quarantine) tables."""
    from databricks.sdk import WorkspaceClient
    from databricks.labs.dqx.engine import DQEngine
    from databricks.labs.dqx.config import InputConfig, OutputConfig

    view_name = params["view_name"]
    checks = params["checks"]
    output_table = params["output_table"]
    quarantine_table = params.get("quarantine_table")
    mode = params.get("mode", "append")

    ws = WorkspaceClient()
    engine = DQEngine(workspace_client=ws, spark=spark)

    input_config = InputConfig(location=view_name)
    output_config = OutputConfig(location=output_table, mode=mode)
    quarantine_config = OutputConfig(location=quarantine_table, mode=mode) if quarantine_table else None

    engine.apply_checks_by_metadata_and_save_in_table(
        input_config=input_config,
        output_config=output_config,
        checks=checks,
        quarantine_config=quarantine_config,
    )

    # Grant the calling user access to the output (and quarantine) tables so they can use them
    # outside the MCP. Best-effort as the app SP (the owner of tables it just created).
    grant_to = params.get("grant_to")
    granted_tables = []
    if grant_to:
        for tbl in (output_table, quarantine_table):
            if tbl and _grant_table_access(tbl, grant_to):
                granted_tables.append(tbl)

    result = {"output_table": output_table, "output_rows": spark.table(output_table).count()}
    if quarantine_table:
        result["quarantine_table"] = quarantine_table
        result["quarantine_rows"] = spark.table(quarantine_table).count()
    result["access_granted_to"] = grant_to if granted_tables else None
    result["granted_tables"] = granted_tables
    return result


# COMMAND ----------


def generate_rules_from_contract(params: dict) -> dict:
    """Generate DQX rules from an ODCS data contract file.

    Deterministic by default: schema/quality rules are derived from the contract.
    Set process_text_rules=True to also process free-text expectations via the LLM
    (requires the [llm] extra).
    """
    from databricks.sdk import WorkspaceClient
    from databricks.labs.dqx.profiler.generator import DQGenerator

    contract_file = params["contract_file"]
    contract_format = params.get("contract_format", "odcs")
    process_text_rules = params.get("process_text_rules", False)
    default_criticality = params.get("default_criticality", "error")

    ws = WorkspaceClient()
    generator = DQGenerator(workspace_client=ws, spark=spark)
    rules = generator.generate_rules_from_contract(
        contract_file=contract_file,
        contract_format=contract_format,
        process_text_rules=process_text_rules,
        default_criticality=default_criticality,
    )

    return {"rules": _make_json_safe(rules), "count": len(rules)}


# COMMAND ----------


def run_checks(params: dict) -> dict:
    """Run DQX checks against a view."""
    from databricks.sdk import WorkspaceClient
    from databricks.labs.dqx.engine import DQEngine

    view_name = params["view_name"]
    checks = params["checks"]
    sample_size = params.get("sample_size", 50)

    ws = WorkspaceClient()
    engine = DQEngine(workspace_client=ws, spark=spark)

    df = spark.table(view_name)
    valid_df, invalid_df = engine.apply_checks_by_metadata_and_split(df, checks)

    total_rows = df.count()
    valid_rows = valid_df.count()
    invalid_rows = invalid_df.count()

    error_sample_rows = invalid_df.limit(sample_size).collect()
    error_sample = [_make_json_safe(row.asDict(recursive=True)) for row in error_sample_rows]

    rule_summary = _compute_rule_summary(invalid_df)

    return {
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


def validate_checks(params: dict) -> dict:
    """Validate a list of DQX check definitions for correctness."""
    from databricks.labs.dqx.engine import DQEngine

    checks = params["checks"]
    status = DQEngine.validate_checks(checks)
    return {
        "valid": not status.has_errors,
        "errors": status.errors,
    }


# COMMAND ----------


def list_available_checks(params: dict) -> dict:
    """List all built-in DQX check functions with signatures and descriptions."""
    import inspect
    from databricks.labs.dqx.rule import CHECK_FUNC_REGISTRY
    from databricks.labs.dqx.checks_resolver import resolve_check_function

    checks = []
    for name, func_type in sorted(CHECK_FUNC_REGISTRY.items()):
        func = resolve_check_function(name, fail_on_missing=False)
        if func is None:
            continue
        sig = inspect.signature(func)
        func_params = [
            {"name": p.name, "type": str(p.annotation) if p.annotation != inspect.Parameter.empty else "Any"}
            for p in sig.parameters.values()
        ]
        doc = inspect.getdoc(func) or ""
        first_line = doc.split("\n")[0] if doc else ""
        checks.append(
            {
                "name": name,
                "type": func_type,
                "signature": f"{name}{sig}",
                "description": first_line,
                "parameters": func_params,
            }
        )

    return {"checks": checks, "count": len(checks)}


# COMMAND ----------


def _grant_table_access(table_name, principal) -> bool:
    """Best-effort grant of full access on a table to the calling user. Returns True on success.

    Grants ``ALL PRIVILEGES`` (read + modify the data) **and** ``MANAGE`` (edit, drop, and manage
    grants on the table), so the user has full lifecycle control of MCP-created outputs outside
    the MCP — without transferring **ownership**. Keeping the app service principal as owner means
    a later overwrite run still works and does not depend on a grant the user could revoke.
    (Exercising MANAGE to drop also requires USE CATALOG / USE SCHEMA on the parents, which the
    user already has for tables they can reference.) Best-effort: logs and returns False on
    failure (e.g. the table pre-existed and is owned by someone else, or there is no caller).
    """
    import re

    if not principal or not table_name:
        return False
    # Validate before embedding in SQL — the principal comes from the OBO header and the table
    # from the tool arguments. Reject anything that is not a plain principal / 3-part identifier.
    if not re.match(r"^[A-Za-z0-9._%+\-@]+$", str(principal)):
        logger.warning(f"Skipping grant: unexpected principal format {principal!r}")
        return False
    parts = str(table_name).split(".")
    if len(parts) != 3 or not all(re.match(r"^[A-Za-z0-9_]+$", p) for p in parts):
        logger.warning(f"Skipping grant: not a 3-part table name: {table_name!r}")
        return False
    fqn = ".".join(f"`{p}`" for p in parts)
    try:
        spark.sql(f"GRANT ALL PRIVILEGES, MANAGE ON TABLE {fqn} TO `{principal}`")
        logger.info(f"Granted ALL PRIVILEGES + MANAGE on {table_name} to {principal}")
        return True
    except Exception:
        logger.warning(f"Could not grant on {table_name} to {principal}", exc_info=True)
        return False


# COMMAND ----------


def _drop_view_safe(view_name) -> None:
    """Drop the OBO-created temp view for this run. Best-effort: logs, never raises.

    Runs as the job's service principal, which owns the temp schema (see setup.py),
    so it can drop views created by any user via their OBO token. Doing this here —
    in the job that is guaranteed to run — means view cleanup no longer depends on the
    user polling get_run_result or on which app replica handles the poll.
    """
    import re

    if not view_name:
        return
    parts = str(view_name).split(".")
    if len(parts) != 3 or not all(re.match(r"^[A-Za-z0-9_]+$", p) for p in parts):
        logger.warning(f"Skipping drop of invalid view name: {view_name!r}")
        return
    safe_fqn = ".".join(f"`{p}`" for p in parts)
    try:
        spark.sql(f"DROP VIEW IF EXISTS {safe_fqn}")
        logger.info(f"Dropped temp view {view_name}")
    except Exception:
        logger.warning(f"Failed to drop temp view {view_name}", exc_info=True)


# COMMAND ----------

# Operation dispatch
OPERATIONS = {
    "profile_table": profile_table,
    "generate_rules": generate_rules,
    "generate_rules_from_contract": generate_rules_from_contract,
    "load_checks": load_checks,
    "save_checks": save_checks,
    "run_checks": run_checks,
    "apply_checks_and_save_to_table": apply_checks_and_save_to_table,
    "validate_checks": validate_checks,
    "list_available_checks": list_available_checks,
}

# COMMAND ----------

try:
    if operation not in OPERATIONS:
        result = {"error": f"Unknown operation: {operation}. Valid: {list(OPERATIONS.keys())}"}
    else:
        result = OPERATIONS[operation](params)
        # Echo the source table name so the client knows which table the result is for.
        # Done here (instead of re-attaching in the server) so the server needs no per-run state.
        if isinstance(result, dict) and params.get("table_name") and "table_name" not in result:
            result["table_name"] = params["table_name"]
except Exception as e:
    logger.error(f"Operation '{operation}' failed: {e}", exc_info=True)
    result = {"error": f"{type(e).__name__}: {str(e)}"}
finally:
    # Always drop the run's temp view, on success or failure.
    _drop_view_safe(params.get("view_name"))

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
