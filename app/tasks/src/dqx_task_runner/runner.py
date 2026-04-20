"""DQX Task Runner — executed inside a serverless Databricks Job.

This module is deployed as a wheel on a Unity Catalog Volume and runs profiler
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
from datetime import datetime, timezone, date
from typing import Any

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

logger = logging.getLogger("dqx_task_runner")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles datetime objects."""

    def default(self, o: Any) -> Any:
        if isinstance(o, datetime):
            return o.isoformat()
        if isinstance(o, date):
            return o.isoformat()
        return super().default(o)


def _json_dumps(obj: Any) -> str:
    """Serialize object to JSON, handling datetime objects."""
    return json.dumps(obj, cls=DateTimeEncoder)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DQX Task Runner")
    parser.add_argument("--task_type", required=True, choices=["profile", "dryrun", "scheduled"])
    parser.add_argument("--view_fqn", required=True, help="Fully qualified view name")
    parser.add_argument("--result_catalog", required=True)
    parser.add_argument("--result_schema", required=True)
    parser.add_argument("--config_json", required=True, help="JSON config for the task")
    parser.add_argument("--run_id", required=True, help="App-generated run ID for tracking")
    parser.add_argument("--requesting_user", default="unknown", help="Email of the requesting user")
    return parser.parse_args()


def _read_view_with_retry(spark: SparkSession, view_fqn: str, max_retries: int = 5, delay: float = 2.0):
    """Read a view with retries to handle Unity Catalog metadata propagation delays."""
    last_error: Exception | None = None
    for attempt in range(max_retries):
        try:
            df = spark.table(view_fqn)
            _ = df.schema
            return df
        except Exception as e:
            last_error = e
            if attempt < max_retries - 1:
                logger.warning(
                    "View %s not accessible (attempt %d/%d), retrying in %.1fs: %s",
                    view_fqn,
                    attempt + 1,
                    max_retries,
                    delay,
                    e,
                )
                time.sleep(delay)
            else:
                logger.error("View %s not accessible after %d attempts", view_fqn, max_retries)
    if last_error is not None:
        raise last_error
    raise RuntimeError(f"View {view_fqn} not accessible after {max_retries} attempts")


def _run_profile(
    spark: SparkSession,
    ws: WorkspaceClient,
    view_fqn: str,
    config: dict[str, Any],
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
    columns = config.get("columns") or None
    profile_options = config.get("profile_options") or {}
    result_table = f"{result_catalog}.{result_schema}.dq_profiling_results"

    start = time.time()

    df = _read_view_with_retry(spark, view_fqn)
    if sample_limit:
        df = df.limit(sample_limit)

    profiler = DQProfiler(ws, spark)
    summary, profiles = profiler.profile(df, columns=columns, options=profile_options)

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
                _json_dumps(summary) if summary else "{}",
                _json_dumps(rules) if rules else "[]",
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
    result_row.writeTo(result_table).append()
    logger.info("Profile results written to %s (run_id=%s)", result_table, run_id)


def _run_dryrun(
    spark: SparkSession,
    ws: WorkspaceClient,
    view_fqn: str,
    config: dict[str, Any],
    result_catalog: str,
    result_schema: str,
    run_id: str,
    requesting_user: str,
) -> None:
    """Run a dry-run validation and write results to Delta."""
    checks = config.get("checks", [])
    sample_size = config.get("sample_size", 1000)
    source_table_fqn = config.get("source_table_fqn", "")
    result_table = f"{result_catalog}.{result_schema}.dq_validation_runs"

    if config.get("is_sql_check"):
        _run_dryrun_sql_check(
            spark,
            view_fqn,
            checks,
            sample_size,
            source_table_fqn,
            result_table,
            run_id,
            requesting_user,
            result_catalog,
            result_schema,
        )
        return

    from databricks.labs.dqx.engine import DQEngine

    df = _read_view_with_retry(spark, view_fqn).limit(sample_size)

    engine = DQEngine(workspace_client=ws, spark=spark)
    result = engine.apply_checks_by_metadata_and_split(df, checks)
    valid_df, invalid_df = result[0], result[1]

    total_rows = df.count()
    valid_rows = valid_df.count()
    invalid_rows = invalid_df.count()

    # Aggregate error summary
    error_summary: list[dict[str, Any]] = []
    if invalid_rows > 0:
        from pyspark.sql import functions as F

        errors_df = invalid_df.select(F.explode(F.col("_errors")).alias("error"))
        summary_rows = errors_df.groupBy("error").count().orderBy(F.desc("count")).limit(20).collect()
        error_summary = [{"error": str(row["error"]), "count": row["count"]} for row in summary_rows]

    # Collect sample invalid rows
    sample_invalid: list[dict[str, Any]] = []
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
                _json_dumps(checks),
                sample_size,
                total_rows,
                valid_rows,
                invalid_rows,
                _json_dumps(error_summary),
                _json_dumps(sample_invalid),
                "SUCCESS",
                None,
                now,
                "dryrun",
            )
        ],
        schema=(
            "run_id STRING, requesting_user STRING, source_table_fqn STRING, "
            "view_fqn STRING, checks_json STRING, sample_size INT, "
            "total_rows INT, valid_rows INT, invalid_rows INT, "
            "error_summary_json STRING, sample_invalid_json STRING, "
            "status STRING, error_message STRING, created_at STRING, "
            "run_type STRING"
        ),
    )
    result_row.writeTo(result_table).append()
    logger.info("Dry-run results written to %s (run_id=%s)", result_table, run_id)

    _write_quarantine_records(
        spark, invalid_df, run_id, source_table_fqn, requesting_user, result_catalog, result_schema
    )

    pass_rate = (valid_rows / total_rows * 100.0) if total_rows > 0 else 100.0
    _write_metrics_row(
        spark,
        run_id,
        source_table_fqn,
        "dryrun",
        total_rows,
        valid_rows,
        invalid_rows,
        pass_rate,
        error_summary,
        requesting_user,
        result_catalog,
        result_schema,
    )


def _run_dryrun_sql_check(
    spark: SparkSession,
    view_fqn: str,
    checks: list[dict[str, Any]],
    sample_size: int,
    source_table_fqn: str,
    result_table: str,
    run_id: str,
    requesting_user: str,
    result_catalog: str,
    result_schema: str,
) -> None:
    """Run a cross-table SQL check dry run.

    The view already contains the violation rows (created from the embedded
    SQL query).  Every row in the view is a violation.
    """
    df = _read_view_with_retry(spark, view_fqn)
    violation_df = df.limit(sample_size)

    invalid_rows = violation_df.count()
    total_rows = invalid_rows
    valid_rows = 0

    check_name = checks[0].get("name", source_table_fqn) if checks else source_table_fqn
    error_summary: list[dict[str, Any]] = []
    if invalid_rows > 0:
        error_summary = [
            {"error": f"SQL check '{check_name}' returned {invalid_rows} violation row(s)", "count": invalid_rows}
        ]

    sample_invalid: list[dict[str, Any]] = []
    if invalid_rows > 0:
        sample_rows = violation_df.limit(10).collect()
        sample_invalid = [row.asDict(recursive=True) for row in sample_rows]

    now = datetime.now(timezone.utc).isoformat()
    run_type = "scheduled" if sample_size == 0 else "dryrun"
    result_row = spark.createDataFrame(
        [
            (
                run_id,
                requesting_user,
                source_table_fqn,
                view_fqn,
                _json_dumps(checks),
                sample_size,
                total_rows,
                valid_rows,
                invalid_rows,
                _json_dumps(error_summary),
                _json_dumps(sample_invalid),
                "SUCCESS",
                None,
                now,
                run_type,
            )
        ],
        schema=(
            "run_id STRING, requesting_user STRING, source_table_fqn STRING, "
            "view_fqn STRING, checks_json STRING, sample_size INT, "
            "total_rows INT, valid_rows INT, invalid_rows INT, "
            "error_summary_json STRING, sample_invalid_json STRING, "
            "status STRING, error_message STRING, created_at STRING, "
            "run_type STRING"
        ),
    )
    result_row.writeTo(result_table).append()
    logger.info(
        "SQL-check %s results written to %s (run_id=%s, violations=%d)", run_type, result_table, run_id, invalid_rows
    )

    pass_rate = 0.0 if invalid_rows > 0 else 100.0
    _write_metrics_row(
        spark,
        run_id,
        source_table_fqn,
        "dryrun",
        total_rows,
        valid_rows,
        invalid_rows,
        pass_rate,
        error_summary,
        requesting_user,
        result_catalog,
        result_schema,
    )


def _write_quarantine_records(
    spark: SparkSession,
    invalid_df,
    run_id: str,
    source_table_fqn: str,
    requesting_user: str,
    result_catalog: str,
    result_schema: str,
) -> None:
    """Persist every invalid row to dq_quarantine_records."""
    from pyspark.sql import functions as F

    invalid_count = invalid_df.count()
    if invalid_count == 0:
        logger.info("No invalid rows to quarantine (run_id=%s)", run_id)
        return

    quarantine_table = f"{result_catalog}.{result_schema}.dq_quarantine_records"
    now = datetime.now(timezone.utc).isoformat()

    data_cols = [c for c in invalid_df.columns if c not in ("_warnings", "_errors", "_rule_name")]
    quarantine_df = (
        invalid_df.withColumn("quarantine_id", F.expr("uuid()"))
        .withColumn("run_id", F.lit(run_id))
        .withColumn("source_table_fqn", F.lit(source_table_fqn))
        .withColumn("requesting_user", F.lit(requesting_user))
        .withColumn("row_data", F.to_json(F.struct(*data_cols)))
        .withColumn("errors", F.to_json(F.col("_errors")))
        .withColumn("created_at", F.lit(now))
        .select("quarantine_id", "run_id", "source_table_fqn", "requesting_user", "row_data", "errors", "created_at")
    )
    quarantine_df.writeTo(quarantine_table).append()
    logger.info("Wrote %d quarantine rows to %s (run_id=%s)", invalid_count, quarantine_table, run_id)


def _write_metrics_row(
    spark: SparkSession,
    run_id: str,
    source_table_fqn: str,
    run_type: str,
    total_rows: int,
    valid_rows: int,
    invalid_rows: int,
    pass_rate: float,
    error_summary: list[dict[str, Any]],
    requesting_user: str,
    result_catalog: str,
    result_schema: str,
) -> None:
    """Write a single metrics snapshot to dq_metrics."""
    import uuid as _uuid

    metrics_table = f"{result_catalog}.{result_schema}.dq_metrics"
    now = datetime.now(timezone.utc).isoformat()
    row = spark.createDataFrame(
        [
            (
                str(_uuid.uuid4()),
                run_id,
                source_table_fqn,
                run_type,
                total_rows,
                valid_rows,
                invalid_rows,
                round(pass_rate, 4),
                _json_dumps(error_summary),
                requesting_user,
                now,
            )
        ],
        schema=(
            "metric_id STRING, run_id STRING, source_table_fqn STRING, "
            "run_type STRING, total_rows INT, valid_rows INT, invalid_rows INT, "
            "pass_rate DOUBLE, error_breakdown STRING, requesting_user STRING, "
            "created_at STRING"
        ),
    )
    row.writeTo(metrics_table).append()
    logger.info("Metrics row written to %s (run_id=%s)", metrics_table, run_id)


def _run_scheduled(
    spark: SparkSession,
    ws: WorkspaceClient,
    view_fqn: str,
    config: dict[str, Any],
    result_catalog: str,
    result_schema: str,
    run_id: str,
    requesting_user: str,
) -> None:
    """Run a scheduled validation -- single engine pass, then persist results + quarantine + metrics.

    For scheduled runs the scheduler passes the source table FQN directly
    (no temporary UC view) to avoid cross-compute metadata propagation issues.
    For SQL checks, the SQL query is included in ``config["sql_query"]`` and
    a Spark-local temp view is created here.
    """
    checks = config.get("checks", [])
    source_table_fqn = config.get("source_table_fqn", "")

    if config.get("is_sql_check"):
        sql_query = config.get("sql_query")
        if sql_query:
            from databricks.labs.dqx.utils import is_sql_query_safe
            from databricks.labs.dqx.errors import UnsafeSqlQueryError

            if not is_sql_query_safe(sql_query):
                raise UnsafeSqlQueryError("Refusing to execute unsafe sql_query in task runner")

            tmp_name = f"_dqx_scheduled_sql_{run_id}"
            spark.sql(f"CREATE OR REPLACE TEMP VIEW {tmp_name} AS {sql_query}")
            effective_view = tmp_name
        else:
            effective_view = view_fqn

        _run_dryrun_sql_check(
            spark,
            effective_view,
            checks,
            0,
            source_table_fqn,
            f"{result_catalog}.{result_schema}.dq_validation_runs",
            run_id,
            requesting_user,
            result_catalog,
            result_schema,
        )
        return

    from databricks.labs.dqx.engine import DQEngine
    from pyspark.sql import functions as F

    df = _read_view_with_retry(spark, view_fqn)

    engine = DQEngine(workspace_client=ws, spark=spark)
    result = engine.apply_checks_by_metadata_and_split(df, checks)
    valid_df, invalid_df = result[0], result[1]

    valid_rows = valid_df.count()
    invalid_rows = invalid_df.count()
    total_rows = valid_rows + invalid_rows
    pass_rate = (valid_rows / total_rows * 100.0) if total_rows > 0 else 100.0

    error_summary: list[dict[str, Any]] = []
    if invalid_rows > 0:
        errors_df = invalid_df.select(F.explode(F.col("_errors")).alias("error"))
        summary_rows = errors_df.groupBy("error").count().orderBy(F.desc("count")).limit(20).collect()
        error_summary = [{"error": str(r["error"]), "count": r["count"]} for r in summary_rows]

    now = datetime.now(timezone.utc).isoformat()
    result_table = f"{result_catalog}.{result_schema}.dq_validation_runs"
    result_row = spark.createDataFrame(
        [
            (
                run_id,
                requesting_user,
                source_table_fqn,
                view_fqn,
                _json_dumps(checks),
                None,
                total_rows,
                valid_rows,
                invalid_rows,
                _json_dumps(error_summary),
                None,
                "SUCCESS",
                None,
                now,
                "scheduled",
            )
        ],
        schema=(
            "run_id STRING, requesting_user STRING, source_table_fqn STRING, "
            "view_fqn STRING, checks_json STRING, sample_size INT, "
            "total_rows INT, valid_rows INT, invalid_rows INT, "
            "error_summary_json STRING, sample_invalid_json STRING, "
            "status STRING, error_message STRING, created_at STRING, "
            "run_type STRING"
        ),
    )
    result_row.writeTo(result_table).append()
    logger.info("Scheduled run results written to %s (run_id=%s)", result_table, run_id)

    _write_quarantine_records(
        spark,
        invalid_df,
        run_id,
        source_table_fqn,
        requesting_user,
        result_catalog,
        result_schema,
    )
    _write_metrics_row(
        spark,
        run_id,
        source_table_fqn,
        "scheduled",
        total_rows,
        valid_rows,
        invalid_rows,
        pass_rate,
        error_summary,
        requesting_user,
        result_catalog,
        result_schema,
    )


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
    checks: list[dict[str, Any]] | None = None,
) -> None:
    """Write a FAILED status row so the app can report the error."""
    now = datetime.now(timezone.utc).isoformat()
    checks_str = _json_dumps(checks) if checks else None
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
        error_run_type = "scheduled" if task_type == "scheduled" else "dryrun"
        row = spark.createDataFrame(
            [
                (
                    run_id,
                    requesting_user,
                    source_table_fqn,
                    view_fqn,
                    checks_str,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    "FAILED",
                    error_message,
                    now,
                    error_run_type,
                )
            ],
            schema=(
                "run_id STRING, requesting_user STRING, source_table_fqn STRING, "
                "view_fqn STRING, checks_json STRING, sample_size INT, "
                "total_rows INT, valid_rows INT, invalid_rows INT, "
                "error_summary_json STRING, sample_invalid_json STRING, "
                "status STRING, error_message STRING, created_at STRING, "
                "run_type STRING"
            ),
        )
    row.writeTo(table).append()
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
        elif args.task_type == "scheduled":
            _run_scheduled(
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
                checks=config.get("checks"),
            )
        except Exception as write_exc:
            logger.error("Failed to write error result: %s", write_exc, exc_info=True)
        sys.exit(1)
    finally:
        if args.task_type != "scheduled":
            try:
                from databricks_labs_dqx_app.backend.sql_utils import validate_fqn, quote_fqn

                validate_fqn(args.view_fqn)
                logger.info("Dropping temporary view: %s", args.view_fqn)
                spark.sql(f"DROP VIEW IF EXISTS {quote_fqn(args.view_fqn)}")
            except Exception as drop_exc:
                logger.warning("Failed to drop view %s: %s", args.view_fqn, drop_exc)


if __name__ == "__main__":
    main()
