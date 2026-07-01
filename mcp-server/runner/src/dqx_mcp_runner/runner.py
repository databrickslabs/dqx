"""DQX MCP runner — serverless wheel-task entry point.

Runs a single DQX operation on Databricks compute and writes its JSON result to a file in the
MCP's results UC volume, keyed by the job run id. The MCP server (app) submits this as a
``python_wheel_task`` (see databricks.yml) and reads the result file back in ``get_run_result``.

Why a wheel task (not a notebook): the job runs as a dedicated, least-privilege workspace service
principal (``run_as``). A notebook_task would require that SP to have read access to the notebook
*object* in the deployer's workspace folder; a wheel ships as an environment dependency, so there
is no workspace-object ACL to manage. This mirrors the DQX Studio task runner.

Result passing: notebook ``dbutils.notebook.exit`` is unavailable to wheel tasks, so results go to
``{catalog}.{schema}.mcp_results`` (a UC volume) as ``<run_id>.json``. The app reads them via the
Files API (no SQL warehouse needed). On failure the entry point raises, so the job's result_state
becomes FAILED and the app surfaces the error from the run output.
"""

from __future__ import annotations

import argparse
import datetime
import io
import json
import logging
from decimal import Decimal
from typing import Any

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dqx-mcp-runner")

# Keep in sync with the app's read-side cap; notebook_output's 5MB limit is gone (files have no
# such limit) but very large payloads are still unhelpful to an agent, so cap defensively.
_MAX_RESULT_BYTES = 4_500_000


def _make_json_safe(value: Any) -> Any:
    """Recursively convert non-JSON-serializable values (Decimal, datetime)."""
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _make_json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_make_json_safe(v) for v in value]
    return value


def profile_table(spark: SparkSession, ws: WorkspaceClient, params: dict) -> dict:
    """Profile a view and return summary stats + profiles."""
    from dataclasses import asdict

    from databricks.labs.dqx.config import InputConfig
    from databricks.labs.dqx.profiler.profiler import DQProfiler

    view_name = params["view_name"]
    columns = params.get("columns")
    options = params.get("options")

    profiler = DQProfiler(workspace_client=ws, spark=spark)
    summary_stats, profiles = profiler.profile_table(InputConfig(location=view_name), columns=columns, options=options)

    return {
        "summary_stats": _make_json_safe(summary_stats),
        "profiles": _make_json_safe([asdict(p) for p in profiles]),
    }


def generate_rules(spark: SparkSession, ws: WorkspaceClient, params: dict) -> dict:
    """Generate DQX rules from profiling output."""
    from databricks.labs.dqx.profiler.generator import DQGenerator
    from databricks.labs.dqx.profiler.profiler import DQProfile

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

    generator = DQGenerator(workspace_client=ws, spark=spark)
    rules = generator.generate_dq_rules(dq_profiles, criticality=criticality)
    return {"rules": _make_json_safe(rules), "count": len(rules)}


def load_checks(spark: SparkSession, ws: WorkspaceClient, params: dict) -> dict:
    """Load DQX checks from a storage backend (table, UC volume, or workspace file)."""
    from databricks.labs.dqx.checks_storage import ChecksStorageHandlerFactory
    from databricks.labs.dqx.engine import DQEngine

    location = params["location"]
    run_config_name = params.get("run_config_name", "default")

    engine = DQEngine(workspace_client=ws, spark=spark)
    _handler, config = ChecksStorageHandlerFactory(ws, spark).create_for_location(location, run_config_name)
    checks = engine.load_checks(config)
    return {"checks": _make_json_safe(checks), "count": len(checks), "location": location}


def save_checks(spark: SparkSession, ws: WorkspaceClient, params: dict) -> dict:
    """Save DQX checks to a storage backend (table, UC volume, or workspace file)."""
    from databricks.labs.dqx.checks_storage import ChecksStorageHandlerFactory
    from databricks.labs.dqx.engine import DQEngine

    checks = params["checks"]
    location = params["location"]
    run_config_name = params.get("run_config_name", "default")
    mode = params.get("mode", "append")

    engine = DQEngine(workspace_client=ws, spark=spark)
    _handler, config = ChecksStorageHandlerFactory(ws, spark).create_for_location(location, run_config_name)
    if hasattr(config, "mode"):
        config.mode = mode
    engine.save_checks(checks, config)

    grant_to = params.get("grant_to")
    granted_to = None
    if grant_to and "/" not in location and location.count(".") == 2:
        if _grant_table_access(spark, location, grant_to):
            granted_to = grant_to

    return {"saved": True, "count": len(checks), "location": location, "access_granted_to": granted_to}


def apply_checks_and_save_to_table(spark: SparkSession, ws: WorkspaceClient, params: dict) -> dict:
    """Apply checks to a view and persist results to output (and optional quarantine) tables."""
    from databricks.labs.dqx.config import InputConfig, OutputConfig
    from databricks.labs.dqx.engine import DQEngine

    view_name = params["view_name"]
    checks = params["checks"]
    output_table = params["output_table"]
    quarantine_table = params.get("quarantine_table")
    mode = params.get("mode", "append")

    engine = DQEngine(workspace_client=ws, spark=spark)
    engine.apply_checks_by_metadata_and_save_in_table(
        input_config=InputConfig(location=view_name),
        output_config=OutputConfig(location=output_table, mode=mode),
        checks=checks,
        quarantine_config=OutputConfig(location=quarantine_table, mode=mode) if quarantine_table else None,
    )

    grant_to = params.get("grant_to")
    granted_tables = []
    if grant_to:
        for tbl in (output_table, quarantine_table):
            if tbl and _grant_table_access(spark, tbl, grant_to):
                granted_tables.append(tbl)

    result = {"output_table": output_table, "output_rows": spark.table(output_table).count()}
    if quarantine_table:
        result["quarantine_table"] = quarantine_table
        result["quarantine_rows"] = spark.table(quarantine_table).count()
    result["access_granted_to"] = grant_to if granted_tables else None
    result["granted_tables"] = granted_tables
    return result


def generate_rules_from_contract(spark: SparkSession, ws: WorkspaceClient, params: dict) -> dict:
    """Generate DQX rules from an ODCS data contract file (deterministic schema/quality rules)."""
    from databricks.labs.dqx.profiler.generator import DQGenerator

    contract_file = params["contract_file"]
    contract_format = params.get("contract_format", "odcs")
    process_text_rules = params.get("process_text_rules", False)
    default_criticality = params.get("default_criticality", "error")

    # The runner env installs the bare DQX wheel + [datacontract], not the [llm] extra (dspy). The
    # MCP tool is deterministic-only and never sets this; guard so a direct submission that sets it
    # fails clearly instead of with an opaque ImportError deep inside DQX.
    if process_text_rules:
        raise ValueError(
            "process_text_rules=True requires the DQX [llm] extra (dspy), which is not installed "
            "in the runner environment. Use process_text_rules=False, or add the [llm] extra to the "
            "runner environment in databricks.yml."
        )

    generator = DQGenerator(workspace_client=ws, spark=spark)
    rules = generator.generate_rules_from_contract(
        contract_file=contract_file,
        contract_format=contract_format,
        process_text_rules=process_text_rules,
        default_criticality=default_criticality,
    )
    return {"rules": _make_json_safe(rules), "count": len(rules)}


def run_checks(spark: SparkSession, ws: WorkspaceClient, params: dict) -> dict:
    """Run DQX checks against a view."""
    from databricks.labs.dqx.engine import DQEngine

    view_name = params["view_name"]
    checks = params["checks"]
    sample_size = params.get("sample_size", 50)

    engine = DQEngine(workspace_client=ws, spark=spark)
    df = spark.table(view_name)
    valid_df, invalid_df = engine.apply_checks_by_metadata_and_split(df, checks)

    error_sample = [_make_json_safe(r.asDict(recursive=True)) for r in invalid_df.limit(sample_size).collect()]
    return {
        "total_rows": df.count(),
        "valid_rows": valid_df.count(),
        "invalid_rows": invalid_df.count(),
        "error_sample": error_sample,
        "rule_summary": _compute_rule_summary(invalid_df),
    }


def validate_checks(spark: SparkSession, ws: WorkspaceClient, params: dict) -> dict:
    """Validate a list of DQX check definitions for correctness."""
    from databricks.labs.dqx.engine import DQEngine

    status = DQEngine.validate_checks(params["checks"])
    return {"valid": not status.has_errors, "errors": status.errors}


def list_available_checks(spark: SparkSession, ws: WorkspaceClient, params: dict) -> dict:
    """List all built-in DQX check functions with signatures and descriptions."""
    import inspect

    from databricks.labs.dqx.checks_resolver import resolve_check_function
    from databricks.labs.dqx.rule import CHECK_FUNC_REGISTRY

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
        checks.append(
            {
                "name": name,
                "type": func_type,
                "signature": f"{name}{sig}",
                "description": doc.split("\n")[0] if doc else "",
                "parameters": func_params,
            }
        )
    return {"checks": checks, "count": len(checks)}


def _compute_rule_summary(invalid_df) -> list:
    """Aggregate per-rule error/warning counts."""
    import pyspark.sql.functions as F

    summary: dict = {}
    for col_name in ("_errors", "_warnings"):
        if col_name not in invalid_df.columns:
            continue
        exploded = invalid_df.select(F.explode(F.col(col_name)).alias("item"))
        for row in exploded.groupBy("item.name").count().collect():
            rule_name = row["name"] or "unknown"
            summary.setdefault(rule_name, {"error_count": 0, "warning_count": 0})
            summary[rule_name]["error_count" if col_name == "_errors" else "warning_count"] = row["count"]
    return [{"rule_name": name, **counts} for name, counts in summary.items()]


def _grant_table_access(spark: SparkSession, table_name: str, principal: str) -> bool:
    """Best-effort grant of ALL PRIVILEGES + MANAGE on a table to the calling user. Returns success."""
    import re

    if not principal or not table_name:
        return False
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


def _drop_view_safe(spark: SparkSession, view_name) -> None:
    """Drop the OBO-created temp view for this run. Best-effort: logs, never raises."""
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


def _results_volume_path(results_volume: str, run_id: str) -> str:
    """Full UC-volume file path for a run's result JSON."""
    return f"{results_volume.rstrip('/')}/{run_id}.json"


def _write_result(ws: WorkspaceClient, results_volume: str, run_id: str, result: dict) -> None:
    """Write the result JSON to the results volume, keyed by run id. Caps oversized payloads."""
    payload = json.dumps(result)
    if len(payload.encode("utf-8")) > _MAX_RESULT_BYTES:
        payload = json.dumps({"error": "Output too large (>4.5MB). Try reducing sample_size.", "truncated": True})
    path = _results_volume_path(results_volume, run_id)
    # files.upload expects a binary file-like object (it calls .seekable()), not raw bytes.
    ws.files.upload(path, io.BytesIO(payload.encode("utf-8")), overwrite=True)
    logger.info(f"Wrote result to {path} ({len(payload)} bytes)")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DQX MCP runner")
    parser.add_argument("--operation", required=True, help="DQX operation name")
    parser.add_argument("--params", default="{}", help="JSON parameters for the operation")
    parser.add_argument("--run-id", required=True, help="Job run id (result file key)")
    parser.add_argument("--results-volume", required=True, help="UC volume path for result files")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    operation = args.operation
    params = json.loads(args.params)
    logger.info(f"Running operation: {operation}, params keys: {list(params.keys())}")

    spark = SparkSession.builder.getOrCreate()
    ws = WorkspaceClient()

    if operation not in OPERATIONS:
        raise ValueError(f"Unknown operation: {operation}. Valid: {list(OPERATIONS.keys())}")

    try:
        result = OPERATIONS[operation](spark, ws, params)
        # Echo the source table name so the client knows which table the result is for.
        if isinstance(result, dict) and params.get("table_name") and "table_name" not in result:
            result["table_name"] = params["table_name"]
    except Exception as e:
        # Do NOT write a result file — raising makes the job result_state FAILED, and the app
        # surfaces the error from the run output. Writing a success-shaped file would make the app
        # report the failed op as completed.
        logger.error(f"Operation '{operation}' failed: {e}", exc_info=True)
        raise
    finally:
        # Always drop the run's temp view, on success or failure.
        _drop_view_safe(spark, params.get("view_name"))

    _write_result(ws, args.results_volume, args.run_id, result)
    logger.info(f"Operation '{operation}' complete.")


if __name__ == "__main__":
    main()
