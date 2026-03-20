"""DQX Task Runner — executed inside a serverless Databricks Job.

This script is deployed as a workspace file via DABs and runs profiler
or dry-run operations on behalf of the DQX App.  The app creates a
temporary VIEW (using the user's OBO token) and then submits this job
(using SP credentials).  The job reads from the view, performs the
requested operation, writes results to a Delta table, and drops the
view in a ``finally`` block.

Parameters are received as command-line arguments (``--key value``).
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

logger = logging.getLogger("dqx_task_runner")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DQX Task Runner")
    parser.add_argument("--task_type", required=True, choices=["profile", "dryrun"])
    parser.add_argument("--view_fqn", required=True, help="Fully qualified view name")
    parser.add_argument("--result_catalog", required=True)
    parser.add_argument("--result_schema", required=True)
    parser.add_argument("--config_json", required=True, help="JSON config for the task")
    parser.add_argument("--run_id", required=True, help="App-generated run ID for tracking")
    parser.add_argument("--requesting_user", default="unknown", help="Email of the requesting user")
    return parser.parse_args()


def _run_profile(
    spark: SparkSession,
    ws: WorkspaceClient,
    view_fqn: str,
    config: dict,
    result_catalog: str,
    result_schema: str,
    run_id: str,
    requesting_user: str,
) -> None:
    """Run the profiler against the view and write results to Delta."""
    from databricks.labs.dqx.profiler.profiler import DQProfiler
    from databricks.labs.dqx.profiler.generator import DQGenerator

    sample_limit = config.get("sample_limit", 50_000)
    source_table_fqn = config.get("source_table_fqn", "")
    result_table = f"{result_catalog}.{result_schema}.dq_profiling_results"

    start = time.time()

    df = spark.table(view_fqn)
    if sample_limit:
        df = df.limit(sample_limit)

    profiler = DQProfiler(ws, spark)
    summary, profiles = profiler.profile(df)

    generator = DQGenerator(ws, spark)
    rules = generator.generate_dq_rules(profiles)

    duration = round(time.time() - start, 2)
    rows_profiled = df.count()
    columns_profiled = len(profiles) if profiles else 0

    # Write result row
    now = datetime.now(timezone.utc).isoformat()
    result_row = spark.createDataFrame(
        [
            (
                run_id,
                requesting_user,
                source_table_fqn,
                view_fqn,
                sample_limit,
                rows_profiled,
                columns_profiled,
                duration,
                json.dumps(summary) if summary else "{}",
                json.dumps(rules) if rules else "[]",
                "SUCCESS",
                None,
                now,
            )
        ],
        schema=(
            "run_id STRING, requesting_user STRING, source_table_fqn STRING, "
            "view_fqn STRING, sample_limit INT, rows_profiled INT, columns_profiled INT, "
            "duration_seconds DOUBLE, summary_json STRING, generated_rules_json STRING, "
            "status STRING, error_message STRING, created_at STRING"
        ),
    )
    result_row.write.mode("append").saveAsTable(result_table)
    logger.info("Profile results written to %s (run_id=%s)", result_table, run_id)


def _run_dryrun(
    spark: SparkSession,
    ws: WorkspaceClient,
    view_fqn: str,
    config: dict,
    result_catalog: str,
    result_schema: str,
    run_id: str,
    requesting_user: str,
) -> None:
    """Run a dry-run validation and write results to Delta."""
    from databricks.labs.dqx.engine import DQEngine

    checks = config.get("checks", [])
    sample_size = config.get("sample_size", 1000)
    source_table_fqn = config.get("source_table_fqn", "")
    result_table = f"{result_catalog}.{result_schema}.dq_validation_runs"

    df = spark.table(view_fqn).limit(sample_size)

    engine = DQEngine(workspace_client=ws, spark=spark)
    valid_df, invalid_df = engine.apply_checks_by_metadata_and_split(df, checks)

    total_rows = df.count()
    valid_rows = valid_df.count()
    invalid_rows = invalid_df.count()

    # Aggregate error summary
    error_summary: list[dict] = []
    if invalid_rows > 0:
        from pyspark.sql import functions as F

        errors_df = invalid_df.select(F.explode(F.col("_errors")).alias("error"))
        summary_rows = errors_df.groupBy("error").count().orderBy(F.desc("count")).limit(20).collect()
        error_summary = [{"error": str(row["error"]), "count": row["count"]} for row in summary_rows]

    # Collect sample invalid rows
    sample_invalid: list[dict] = []
    if invalid_rows > 0:
        sample_rows = invalid_df.limit(10).collect()
        sample_invalid = [row.asDict(recursive=True) for row in sample_rows]

    now = datetime.now(timezone.utc).isoformat()
    result_row = spark.createDataFrame(
        [
            (
                run_id,
                requesting_user,
                source_table_fqn,
                view_fqn,
                json.dumps(checks),
                sample_size,
                total_rows,
                valid_rows,
                invalid_rows,
                json.dumps(error_summary),
                json.dumps(sample_invalid),
                "SUCCESS",
                None,
                now,
            )
        ],
        schema=(
            "run_id STRING, requesting_user STRING, source_table_fqn STRING, "
            "view_fqn STRING, checks_json STRING, sample_size INT, "
            "total_rows INT, valid_rows INT, invalid_rows INT, "
            "error_summary_json STRING, sample_invalid_json STRING, "
            "status STRING, error_message STRING, created_at STRING"
        ),
    )
    result_row.write.mode("append").saveAsTable(result_table)
    logger.info("Dry-run results written to %s (run_id=%s)", result_table, run_id)


def _write_error(
    spark: SparkSession,
    task_type: str,
    result_catalog: str,
    result_schema: str,
    run_id: str,
    requesting_user: str,
    source_table_fqn: str,
    view_fqn: str,
    error_message: str,
) -> None:
    """Write a FAILED status row so the app can report the error."""
    now = datetime.now(timezone.utc).isoformat()
    if task_type == "profile":
        table = f"{result_catalog}.{result_schema}.dq_profiling_results"
        row = spark.createDataFrame(
            [
                (
                    run_id,
                    requesting_user,
                    source_table_fqn,
                    view_fqn,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    "FAILED",
                    error_message,
                    now,
                )
            ],
            schema=(
                "run_id STRING, requesting_user STRING, source_table_fqn STRING, "
                "view_fqn STRING, sample_limit INT, rows_profiled INT, columns_profiled INT, "
                "duration_seconds DOUBLE, summary_json STRING, generated_rules_json STRING, "
                "status STRING, error_message STRING, created_at STRING"
            ),
        )
    else:
        table = f"{result_catalog}.{result_schema}.dq_validation_runs"
        row = spark.createDataFrame(
            [
                (
                    run_id,
                    requesting_user,
                    source_table_fqn,
                    view_fqn,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    "FAILED",
                    error_message,
                    now,
                )
            ],
            schema=(
                "run_id STRING, requesting_user STRING, source_table_fqn STRING, "
                "view_fqn STRING, checks_json STRING, sample_size INT, "
                "total_rows INT, valid_rows INT, invalid_rows INT, "
                "error_summary_json STRING, sample_invalid_json STRING, "
                "status STRING, error_message STRING, created_at STRING"
            ),
        )
    row.write.mode("append").saveAsTable(table)
    logger.info("Error result written to %s (run_id=%s)", table, run_id)


def main() -> None:
    args = _parse_args()
    config = json.loads(args.config_json)
    source_table_fqn = config.get("source_table_fqn", "")

    spark = SparkSession.builder.getOrCreate()
    ws = WorkspaceClient()

    try:
        if args.task_type == "profile":
            _run_profile(
                spark,
                ws,
                args.view_fqn,
                config,
                args.result_catalog,
                args.result_schema,
                args.run_id,
                args.requesting_user,
            )
        elif args.task_type == "dryrun":
            _run_dryrun(
                spark,
                ws,
                args.view_fqn,
                config,
                args.result_catalog,
                args.result_schema,
                args.run_id,
                args.requesting_user,
            )
    except Exception as exc:
        logger.error("Task %s failed: %s", args.task_type, exc, exc_info=True)
        try:
            _write_error(
                spark,
                args.task_type,
                args.result_catalog,
                args.result_schema,
                args.run_id,
                args.requesting_user,
                source_table_fqn,
                args.view_fqn,
                str(exc),
            )
        except Exception as write_exc:
            logger.error("Failed to write error result: %s", write_exc, exc_info=True)
        sys.exit(1)
    finally:
        # Always drop the temporary view
        try:
            logger.info("Dropping temporary view: %s", args.view_fqn)
            spark.sql(f"DROP VIEW IF EXISTS {args.view_fqn}")  # noqa: S608
        except Exception as drop_exc:
            logger.warning("Failed to drop view %s: %s", args.view_fqn, drop_exc)


if __name__ == "__main__":
    main()
