"""DQX Task Runner — executed inside a serverless Databricks Job.

This module is deployed as a wheel on a Unity Catalog Volume and runs profiler
or dry-run operations on behalf of DQX Studio.  The app creates a
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
import re
from datetime import datetime, timezone, date
from decimal import Decimal
from typing import Any, cast

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

logger = logging.getLogger("dqx_task_runner")

# Names of the spec-defined built-in metrics produced by ``DQMetricsObserver``.
# Anything not in this set is treated as a custom metric and persisted as-is.
_BUILTIN_METRIC_NAMES = frozenset(
    {"input_row_count", "error_row_count", "warning_row_count", "valid_row_count", "check_metrics"}
)

# Hard ceiling on rows persisted to ``dq_quarantine_records`` for cross-table
# SQL checks. Unlike row-level checks (where the violation count is naturally
# bounded by ``sample_size``), a SQL check's violation set can be the entire
# joined dataset and grow unboundedly. The export endpoint already enforces a
# 50K download cap (`backend/routes/v1/quarantine.py:_EXPORT_MAX_ROWS`); we
# keep some headroom here so admins querying the table directly still have
# more rows for offline debugging without us writing millions on a runaway
# rule. Truncation is logged at WARNING so it's visible in run logs, and the
# real violation count remains accurate in ``dq_metrics.error_row_count``.
_SQL_QUARANTINE_MAX_ROWS = 100_000

# User-facing observer names written to ``dq_metrics.run_name``. The
# internal ``run_type`` token (``dryrun`` / ``scheduled`` / ``preview``)
# is still used everywhere else (API filters, ``dq_validation_runs``,
# frontend grouping); only the metrics row's display name is rewritten
# here. ``dryrun`` runs that persist history are renamed to ``manual``
# because they're really ad-hoc manual runs from the UI, not throw-away
# previews — the literal preview path uses ``run_type="preview"``.
_RUN_NAME_BY_RUN_TYPE: dict[str, str] = {
    "dryrun": "dqx_app_manual",
    "scheduled": "dqx_app_scheduled",
    "preview": "dqx_app_preview",
}


def _observer_run_name(run_type: str) -> str:
    """Map an internal ``run_type`` to the observer name persisted in ``dq_metrics.run_name``."""
    return _RUN_NAME_BY_RUN_TYPE.get(run_type, f"dqx_app_{run_type}")


def _aggregate_rule_labels(checks: list[dict[str, Any]] | None) -> dict[str, str]:
    """Collapse the per-check ``user_metadata`` (rule labels) into one map.

    DQX writes a single ``user_metadata`` map per metric row in the
    ``dq_metrics`` table, but a run can validate many rules each with
    their own labels. We use **intersection-with-equal-values** here:
    a key is included iff every rule in the run carries that key with
    the *same* value. That makes ``dq_metrics.user_metadata`` a true
    description of the run-level cohort (e.g. ``team=finance`` if every
    rule in the run is tagged that team) and avoids ambiguous,
    last-write-wins style merges that would silently drop conflicts.

    Run provenance — ``run_type``, ``requesting_user`` — deliberately
    does NOT live here. Those are first-class columns on
    ``dq_validation_runs`` and the metrics route already joins on
    ``run_id`` to surface them, so duplicating them inside the
    user-metadata map would just pollute label-based dashboards.

    Returns ``{}`` (not ``None``) when there are no checks or no
    consistent labels — the caller can then opt to pass ``None`` to
    skip the column entirely if desired.
    """
    if not checks:
        return {}
    per_check_labels: list[dict[str, str]] = []
    for check in checks:
        if not isinstance(check, dict):
            continue
        labels = check.get("user_metadata") or {}
        if not isinstance(labels, dict):
            continue
        # Coerce values to strings to match the DQX map<string,string>
        # schema — the rules catalog stores everything as strings, but
        # be defensive against legacy rows that might have non-string
        # values (e.g. numeric ``weight``).
        per_check_labels.append({str(k): str(v) for k, v in labels.items() if k is not None})

    if not per_check_labels:
        return {}

    # Intersection: only keys present in every check, with identical
    # values across all of them.
    common_keys: set[str] = set(per_check_labels[0].keys())
    for labels in per_check_labels[1:]:
        common_keys &= labels.keys()
    if not common_keys:
        return {}

    result: dict[str, str] = {}
    for key in common_keys:
        values = {labels[key] for labels in per_check_labels}
        if len(values) == 1:
            result[key] = next(iter(values))
    return result


_FQN_PART_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_\-]*$")
_SQL_CHECK_RE = re.compile(r"^__sql_check__/[a-zA-Z0-9_\-]+$")
_RUN_ID_RE = re.compile(r"^[a-zA-Z0-9_\-]{1,64}$")


def _validate_fqn(fqn: str) -> str:
    if not fqn:
        raise ValueError("Fully qualified name must not be empty.")
    if _SQL_CHECK_RE.match(fqn):
        return fqn
    parts = fqn.split(".")
    if len(parts) != 3:
        raise ValueError(f"Invalid fully qualified name: '{fqn}'. Expected exactly three parts: catalog.schema.table")
    for part in parts:
        cleaned = part.strip("`")
        if not cleaned or not _FQN_PART_RE.match(cleaned):
            raise ValueError(f"Invalid fully qualified name part '{part}' in '{fqn}'.")
    return fqn


def _quote_fqn(fqn: str) -> str:
    return ".".join(f"`{p.strip('`')}`" for p in fqn.split("."))


def _validate_run_id(run_id: str) -> str:
    """Validate that a run_id is safe for use in SQL identifiers."""
    if not _RUN_ID_RE.match(run_id):
        raise ValueError(
            f"Invalid run_id: '{run_id}'. "
            "Must be 1-64 characters using only letters, digits, underscores, or hyphens."
        )
    return run_id


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles datetime objects."""

    def default(self, o: Any) -> Any:
        if isinstance(o, datetime):
            return o.isoformat()
        if isinstance(o, date):
            return o.isoformat()
        if isinstance(o, Decimal):
            return float(o)
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
    parser.add_argument("--warehouse_id", default="", help="SQL warehouse ID for view cleanup")
    return parser.parse_args()


def _read_view_with_retry(
    spark: SparkSession,
    view_fqn: str,
    max_retries: int = 10,
    initial_delay: float = 2.0,
    max_delay: float = 15.0,
):
    """Read a view with exponential-backoff retries for UC metadata propagation.

    Under concurrent load (e.g. batch profiling of multiple tables) the
    GRANT / view creation may take longer to propagate to serverless
    compute. The default budget is ~80 s which covers typical propagation
    delays.
    """
    last_error: Exception | None = None
    delay = initial_delay
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
                delay = min(delay * 1.5, max_delay)
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

    # Write result row. Profiling has no checks yet, but we still record a
    # null fingerprint slot so later pipeline stages that join on
    # rule_set_fingerprint don't have to special-case profile rows.
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
                None,
            )
        ],
        schema=(
            "run_id STRING, requesting_user STRING, source_table_fqn STRING, "
            "view_fqn STRING, sample_limit INT, rows_profiled INT, columns_profiled INT, "
            "duration_seconds DOUBLE, summary_json STRING, generated_rules_json STRING, "
            "status STRING, error_message STRING, created_at STRING, "
            "rule_set_fingerprint STRING"
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
    """Run a dry-run validation and write results to Delta.

    Metric collection uses ``DQMetricsObserver`` so the built-in counts
    (``input_row_count``, ``error_row_count``, ``warning_row_count``,
    ``valid_row_count``, ``check_metrics``) plus any configured custom
    metrics are emitted in a single Spark action — no second pass over
    the DataFrame.
    """
    checks = config.get("checks", [])
    sample_size = config.get("sample_size", 1000)
    source_table_fqn = config.get("source_table_fqn", "")
    custom_metrics = _validate_custom_metrics(config.get("custom_metrics") or [])
    run_type = "preview" if config.get("skip_history") else "dryrun"
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
            run_type=run_type,
            custom_metrics=custom_metrics,
        )
        return

    from databricks.labs.dqx.engine import DQEngine
    from databricks.labs.dqx.metrics_observer import DQMetricsObserver

    fingerprint = _compute_fingerprint(checks)

    observer = DQMetricsObserver(
        name=_observer_run_name(run_type),
        custom_metrics=custom_metrics or None,
        id_overwrite=run_id,
    )
    engine = DQEngine(workspace_client=ws, spark=spark, observer=observer)

    df = _read_view_with_retry(spark, view_fqn).limit(sample_size)
    # ``apply_checks_by_metadata_and_split`` returns ``(valid, invalid)``
    # when no observer is attached and ``(valid, invalid, observation)``
    # when one is. We always construct the engine with ``observer=...``
    # above, so the 3-tuple shape is guaranteed at runtime — the cast
    # tells pyright to pick that branch of the union.
    valid_df, invalid_df, observation = cast(
        "tuple[Any, Any, Any]",
        engine.apply_checks_by_metadata_and_split(df, checks),
    )

    # Quarantine pass — also serves as the action that materialises the
    # observation. ``invalid_df.count()`` triggers metric collection.
    invalid_rows = invalid_df.count()

    sample_invalid: list[dict[str, Any]] = []
    if invalid_rows > 0:
        sample_rows = invalid_df.limit(10).collect()
        sample_invalid = [row.asDict(recursive=True) for row in sample_rows]

    observed = observation.get if observation is not None else {}
    total_rows = int(observed.get("input_row_count", 0) or 0)
    valid_rows = int(observed.get("valid_row_count", 0) or 0)
    error_rows = int(observed.get("error_row_count", 0) or 0)
    warning_rows = int(observed.get("warning_row_count", 0) or 0)

    # Backwards-compatible error_summary derived from check_metrics.
    error_summary = _check_metrics_to_error_summary(observed.get("check_metrics"))

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
                run_type,
                fingerprint,
            )
        ],
        schema=(
            "run_id STRING, requesting_user STRING, source_table_fqn STRING, "
            "view_fqn STRING, checks_json STRING, sample_size INT, "
            "total_rows INT, valid_rows INT, invalid_rows INT, "
            "error_summary_json STRING, sample_invalid_json STRING, "
            "status STRING, error_message STRING, created_at STRING, "
            "run_type STRING, rule_set_fingerprint STRING"
        ),
    )
    result_row.writeTo(result_table).append()
    logger.info(
        "Dry-run results written to %s (run_id=%s, run_type=%s, errors=%d, warnings=%d)",
        result_table,
        run_id,
        run_type,
        error_rows,
        warning_rows,
    )

    if run_type != "preview":
        _write_quarantine_records(
            spark, invalid_df, run_id, source_table_fqn, requesting_user, result_catalog, result_schema
        )
        _persist_observed_metrics(
            spark=spark,
            observed=observed,
            run_id=run_id,
            run_name=observer.name,
            input_location=source_table_fqn,
            quarantine_location=f"{result_catalog}.{result_schema}.dq_quarantine_records",
            checks_location=f"{result_catalog}.{result_schema}.dq_quality_rules",
            rule_set_fingerprint=fingerprint,
            user_metadata=_aggregate_rule_labels(checks) or None,
            result_catalog=result_catalog,
            result_schema=result_schema,
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
    run_type: str = "dryrun",
    custom_metrics: (
        list[str] | None
    ) = None,  # noqa: ARG001 — accepted for API symmetry; SQL checks bypass the observer.
) -> None:
    """Run a cross-table SQL check dry run.

    The view already contains the violation rows (created from the embedded
    SQL query). Every row in the view is a violation. SQL checks bypass
    the row-level ``_errors``/``_warnings`` columns so we synthesise
    spec-compatible metric names manually instead of attaching an observer.
    """
    df = _read_view_with_retry(spark, view_fqn)
    violation_df = df.limit(sample_size) if sample_size else df

    invalid_rows = violation_df.count()
    total_rows = invalid_rows
    valid_rows = 0
    fingerprint = _compute_fingerprint(checks)

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
    if run_type == "dryrun" and sample_size == 0:
        run_type = "scheduled"
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
                fingerprint,
            )
        ],
        schema=(
            "run_id STRING, requesting_user STRING, source_table_fqn STRING, "
            "view_fqn STRING, checks_json STRING, sample_size INT, "
            "total_rows INT, valid_rows INT, invalid_rows INT, "
            "error_summary_json STRING, sample_invalid_json STRING, "
            "status STRING, error_message STRING, created_at STRING, "
            "run_type STRING, rule_set_fingerprint STRING"
        ),
    )
    result_row.writeTo(result_table).append()
    logger.info(
        "SQL-check %s results written to %s (run_id=%s, violations=%d)", run_type, result_table, run_id, invalid_rows
    )

    if run_type != "preview":
        # Persist the full violation set (capped — see ``_SQL_QUARANTINE_MAX_ROWS``)
        # so the run-history UI can offer a true CSV/Excel export instead of
        # the 10-row ``sample_invalid_json`` fallback. Note that for *manual*
        # SQL dry runs ``violation_df`` is already limited to ``sample_size``
        # rows further up; for *scheduled* SQL checks (``sample_size==0``)
        # the cap below is what bounds storage.
        persisted_quarantine = _write_sql_quarantine_records(
            spark=spark,
            violation_df=violation_df,
            run_id=run_id,
            source_table_fqn=source_table_fqn,
            requesting_user=requesting_user,
            check_name=str(check_name),
            invalid_count=invalid_rows,
            result_catalog=result_catalog,
            result_schema=result_schema,
        )

        # SQL checks treat each violation row as one error against a single
        # logical "check" (criticality 'error' by convention).
        synth_check_metrics = _json_dumps(
            [{"check_name": str(check_name), "error_count": invalid_rows, "warning_count": 0}]
        )
        observed = {
            "input_row_count": str(total_rows),
            "error_row_count": str(invalid_rows),
            "warning_row_count": "0",
            "valid_row_count": str(valid_rows),
            "check_metrics": synth_check_metrics,
        }
        # Stamp the metric row's ``quarantine_location`` honestly — only
        # populated when we actually persisted rows, so dashboards can
        # tell at a glance which runs have downloadable detail.
        quarantine_loc = f"{result_catalog}.{result_schema}.dq_quarantine_records" if persisted_quarantine > 0 else None
        _persist_observed_metrics(
            spark=spark,
            observed=observed,
            run_id=run_id,
            run_name=_observer_run_name(run_type),
            input_location=source_table_fqn,
            quarantine_location=quarantine_loc,
            checks_location=f"{result_catalog}.{result_schema}.dq_quality_rules",
            rule_set_fingerprint=fingerprint,
            user_metadata=_aggregate_rule_labels(checks) or None,
            result_catalog=result_catalog,
            result_schema=result_schema,
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


def _write_sql_quarantine_records(
    spark: SparkSession,
    violation_df,
    *,
    run_id: str,
    source_table_fqn: str,
    requesting_user: str,
    check_name: str,
    invalid_count: int,
    result_catalog: str,
    result_schema: str,
    max_rows: int = _SQL_QUARANTINE_MAX_ROWS,
) -> int:
    """Persist cross-table SQL-check violations to ``dq_quarantine_records``.

    Unlike row-level checks, SQL violations don't carry DQX's per-row
    ``_errors`` map — every row in ``violation_df`` *is* a violation
    against the single named check. We synthesise a small JSON errors
    payload using the same ``{check_name: message}`` shape DQX produces
    for column checks, so the quarantine schema and the downstream UI /
    export path stay uniform across check kinds.

    Volume safety: row-level checks are naturally bounded by
    ``sample_size`` on the input read, but a SQL check's violation set
    can be unbounded for scheduled runs (``sample_size=0``). We cap
    persistence at ``max_rows`` to avoid pathological writes when a rule
    matches everything. The true violation count is still recorded
    accurately in ``dq_metrics.error_row_count``.

    Returns the number of rows actually persisted (``0`` if ``invalid_count``
    is zero, ``min(invalid_count, max_rows)`` otherwise).
    """
    from pyspark.sql import functions as F

    if invalid_count <= 0:
        logger.info("No SQL violations to quarantine (run_id=%s)", run_id)
        return 0

    persisted_target = min(invalid_count, max_rows)
    capped_df = violation_df.limit(persisted_target) if persisted_target < invalid_count else violation_df

    quarantine_table = f"{result_catalog}.{result_schema}.dq_quarantine_records"
    now = datetime.now(timezone.utc).isoformat()

    # Every column on the violation view is part of the row payload —
    # there are no DQX-internal columns to strip (the ``not in`` filter
    # is just defensive in case a check author re-uses those names).
    data_cols = [c for c in capped_df.columns if c not in ("_warnings", "_errors", "_rule_name")]
    row_data_expr = F.to_json(F.struct(*data_cols)) if data_cols else F.lit("{}")
    errors_json = _json_dumps({check_name: "SQL check violation"})

    quarantine_df = (
        capped_df.withColumn("quarantine_id", F.expr("uuid()"))
        .withColumn("run_id", F.lit(run_id))
        .withColumn("source_table_fqn", F.lit(source_table_fqn))
        .withColumn("requesting_user", F.lit(requesting_user))
        .withColumn("row_data", row_data_expr)
        .withColumn("errors", F.lit(errors_json))
        .withColumn("created_at", F.lit(now))
        .select(
            "quarantine_id",
            "run_id",
            "source_table_fqn",
            "requesting_user",
            "row_data",
            "errors",
            "created_at",
        )
    )
    quarantine_df.writeTo(quarantine_table).append()

    if invalid_count > max_rows:
        logger.warning(
            "SQL-quarantine truncated for run_id=%s: %d violations exist, persisted %d (cap=%d). "
            "True count remains in dq_metrics.error_row_count.",
            run_id,
            invalid_count,
            persisted_target,
            max_rows,
        )
    else:
        logger.info(
            "Wrote %d SQL-quarantine row(s) to %s (run_id=%s, check=%s)",
            persisted_target,
            quarantine_table,
            run_id,
            check_name,
        )
    return persisted_target


def _compute_fingerprint(checks: list[dict[str, Any]]) -> str | None:
    """Compute a deterministic SHA-256 fingerprint of the rule set.

    Delegates to DQX's ``compute_rule_set_fingerprint_by_metadata`` so the
    value matches what the library produces in its own end-to-end paths,
    enabling cross-tool comparison. Returns ``None`` for an empty rule set.
    """
    if not checks:
        return None
    try:
        from databricks.labs.dqx.rule_fingerprint import compute_rule_set_fingerprint_by_metadata

        return compute_rule_set_fingerprint_by_metadata(checks)
    except Exception:
        # Older DQX versions (or unexpected check shapes) — fall back to a
        # stable local hash so metrics rows still get a fingerprint.
        import hashlib

        canon = json.dumps(checks, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canon.encode()).hexdigest()


_CUSTOM_METRIC_RE = re.compile(r"^[A-Za-z0-9_(),.\s'\"\-+*/=<>!|&%:?\[\]]+ as [A-Za-z_][A-Za-z0-9_]*$")


def _validate_custom_metrics(exprs: list[str]) -> list[str]:
    """Lightly validate user-supplied custom-metric SQL expressions.

    Each entry must look like ``<expr> as <alias>`` and survive DQX's
    ``is_sql_query_safe`` denylist. Anything that fails is dropped with a
    warning rather than aborting the whole run.
    """
    if not exprs:
        return []
    try:
        from databricks.labs.dqx.utils import is_sql_query_safe
    except Exception:
        is_sql_query_safe = None  # type: ignore[assignment]

    safe: list[str] = []
    for raw in exprs:
        if not isinstance(raw, str):
            continue
        expr = raw.strip()
        if not expr:
            continue
        if not _CUSTOM_METRIC_RE.match(expr):
            logger.warning("Dropping malformed custom metric: %r", raw)
            continue
        if is_sql_query_safe is not None and not is_sql_query_safe(expr):
            logger.warning("Dropping unsafe custom metric: %r", raw)
            continue
        safe.append(expr)
    return safe


def _check_metrics_to_error_summary(check_metrics_json: Any) -> list[dict[str, Any]]:
    """Convert the observer's ``check_metrics`` JSON into a top-N error summary.

    Preserves the historical ``error_summary_json`` shape on
    ``dq_validation_runs`` so the existing UI keeps working without a
    schema change.
    """
    if not check_metrics_json:
        return []
    try:
        items = json.loads(check_metrics_json) if isinstance(check_metrics_json, str) else check_metrics_json
    except (json.JSONDecodeError, TypeError):
        return []
    if not isinstance(items, list):
        return []
    rows: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        err = int(item.get("error_count") or 0)
        warn = int(item.get("warning_count") or 0)
        if err == 0 and warn == 0:
            continue
        name = item.get("check_name") or "unknown"
        rows.append({"error": str(name), "count": err + warn, "error_count": err, "warning_count": warn})
    rows.sort(key=lambda r: -int(r.get("count") or 0))
    return rows[:20]


def _persist_observed_metrics(
    *,
    spark: SparkSession,
    observed: dict[str, Any],
    run_id: str,
    run_name: str,
    input_location: str | None,
    quarantine_location: str | None,
    checks_location: str | None,
    rule_set_fingerprint: str | None,
    user_metadata: dict[str, str] | None,
    result_catalog: str,
    result_schema: str,
    output_location: str | None = None,
    error_column_name: str = "_errors",
    warning_column_name: str = "_warnings",
) -> None:
    """Persist an Observation as long-format rows in ``dq_metrics``.

    Uses :func:`DQMetricsObserver.build_metrics_df` so the row layout
    matches the public ``OBSERVATION_TABLE_SCHEMA`` exactly. Each metric
    name (built-in or custom) is written as its own row, mirroring the
    spec's ``metric_name`` / ``metric_value`` design.
    """
    if not observed:
        logger.info("No observed metrics to persist (run_id=%s)", run_id)
        return

    from databricks.labs.dqx.metrics_observer import DQMetricsObservation, DQMetricsObserver

    serialized: dict[str, Any] = {}
    for k, v in observed.items():
        if v is None:
            continue
        serialized[str(k)] = v if isinstance(v, str) else _json_dumps(v) if not isinstance(v, (int, float)) else str(v)

    obs = DQMetricsObservation(
        run_id=run_id,
        run_name=run_name,
        error_column_name=error_column_name,
        warning_column_name=warning_column_name,
        observed_metrics=serialized,
        input_location=input_location,
        output_location=output_location,
        quarantine_location=quarantine_location,
        checks_location=checks_location,
        rule_set_fingerprint=rule_set_fingerprint,
        user_metadata=user_metadata,
    )
    metrics_df = DQMetricsObserver.build_metrics_df(spark, obs)

    metrics_table = f"{result_catalog}.{result_schema}.dq_metrics"
    metrics_df.writeTo(metrics_table).append()

    builtin_count = sum(1 for k in serialized if k in _BUILTIN_METRIC_NAMES)
    custom_count = len(serialized) - builtin_count
    logger.info(
        "Wrote %d metric row(s) to %s (run_id=%s, builtin=%d, custom=%d)",
        len(serialized),
        metrics_table,
        run_id,
        builtin_count,
        custom_count,
    )


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
    custom_metrics = _validate_custom_metrics(config.get("custom_metrics") or [])

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
            custom_metrics=custom_metrics,
        )
        return

    from databricks.labs.dqx.engine import DQEngine
    from databricks.labs.dqx.metrics_observer import DQMetricsObserver

    fingerprint = _compute_fingerprint(checks)
    observer = DQMetricsObserver(
        name=_observer_run_name("scheduled"),
        custom_metrics=custom_metrics or None,
        id_overwrite=run_id,
    )
    engine = DQEngine(workspace_client=ws, spark=spark, observer=observer)

    df = _read_view_with_retry(spark, view_fqn)
    # See narrowing rationale on the dryrun call site — DQX returns the
    # 3-tuple shape iff the engine was constructed with an observer,
    # which we always do here.
    valid_df, invalid_df, observation = cast(
        "tuple[Any, Any, Any]",
        engine.apply_checks_by_metadata_and_split(df, checks),
    )

    invalid_rows = invalid_df.count()  # triggers the observation

    observed = observation.get if observation is not None else {}
    total_rows = int(observed.get("input_row_count", 0) or 0)
    valid_rows = int(observed.get("valid_row_count", 0) or 0)
    error_rows = int(observed.get("error_row_count", 0) or 0)
    warning_rows = int(observed.get("warning_row_count", 0) or 0)
    error_summary = _check_metrics_to_error_summary(observed.get("check_metrics"))

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
                fingerprint,
            )
        ],
        schema=(
            "run_id STRING, requesting_user STRING, source_table_fqn STRING, "
            "view_fqn STRING, checks_json STRING, sample_size INT, "
            "total_rows INT, valid_rows INT, invalid_rows INT, "
            "error_summary_json STRING, sample_invalid_json STRING, "
            "status STRING, error_message STRING, created_at STRING, "
            "run_type STRING, rule_set_fingerprint STRING"
        ),
    )
    result_row.writeTo(result_table).append()
    logger.info(
        "Scheduled run results written to %s (run_id=%s, errors=%d, warnings=%d)",
        result_table,
        run_id,
        error_rows,
        warning_rows,
    )

    _write_quarantine_records(
        spark,
        invalid_df,
        run_id,
        source_table_fqn,
        requesting_user,
        result_catalog,
        result_schema,
    )
    _persist_observed_metrics(
        spark=spark,
        observed=observed,
        run_id=run_id,
        run_name=observer.name,
        input_location=source_table_fqn,
        quarantine_location=f"{result_catalog}.{result_schema}.dq_quarantine_records",
        checks_location=f"{result_catalog}.{result_schema}.dq_quality_rules",
        rule_set_fingerprint=fingerprint,
        user_metadata=_aggregate_rule_labels(checks) or None,
        result_catalog=result_catalog,
        result_schema=result_schema,
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
    skip_history: bool = False,
) -> None:
    """Write a FAILED status row so the app can report the error."""
    now = datetime.now(timezone.utc).isoformat()
    checks_str = _json_dumps(checks) if checks else None
    fingerprint = _compute_fingerprint(checks or [])
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
                    fingerprint,
                )
            ],
            schema=(
                "run_id STRING, requesting_user STRING, source_table_fqn STRING, "
                "view_fqn STRING, sample_limit INT, rows_profiled INT, columns_profiled INT, "
                "duration_seconds DOUBLE, summary_json STRING, generated_rules_json STRING, "
                "status STRING, error_message STRING, created_at STRING, "
                "rule_set_fingerprint STRING"
            ),
        )
    else:
        table = f"{result_catalog}.{result_schema}.dq_validation_runs"
        if skip_history:
            error_run_type = "preview"
        elif task_type == "scheduled":
            error_run_type = "scheduled"
        else:
            error_run_type = "dryrun"
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
                    fingerprint,
                )
            ],
            schema=(
                "run_id STRING, requesting_user STRING, source_table_fqn STRING, "
                "view_fqn STRING, checks_json STRING, sample_size INT, "
                "total_rows INT, valid_rows INT, invalid_rows INT, "
                "error_summary_json STRING, sample_invalid_json STRING, "
                "status STRING, error_message STRING, created_at STRING, "
                "run_type STRING, rule_set_fingerprint STRING"
            ),
        )
    row.writeTo(table).append()
    logger.info("Error result written to %s (run_id=%s)", table, run_id)


def main() -> None:
    args = _parse_args()
    _validate_run_id(args.run_id)
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
                skip_history=bool(config.get("skip_history")),
            )
        except Exception as write_exc:
            logger.error("Failed to write error result: %s", write_exc, exc_info=True)
        sys.exit(1)
    finally:
        if args.task_type != "scheduled" and "tmp_view_" in args.view_fqn:
            _validate_fqn(args.view_fqn)
            drop_sql = f"DROP VIEW IF EXISTS {_quote_fqn(args.view_fqn)}"
            dropped = False

            if args.warehouse_id:
                try:
                    logger.info("Dropping view %s via SQL warehouse %s", args.view_fqn, args.warehouse_id)
                    ws.statement_execution.execute_statement(
                        warehouse_id=args.warehouse_id,
                        statement=drop_sql,
                        wait_timeout="30s",
                    )
                    logger.info("Dropped view %s via SQL warehouse", args.view_fqn)
                    dropped = True
                except Exception as sql_exc:
                    logger.warning("SQL warehouse DROP failed for %s: %s", args.view_fqn, sql_exc)

            if not dropped:
                try:
                    logger.info("Dropping view %s via spark.sql", args.view_fqn)
                    spark.sql(drop_sql)
                    logger.info("Dropped temporary view: %s", args.view_fqn)
                except Exception as spark_exc:
                    logger.warning("Failed to drop view %s: %s", args.view_fqn, spark_exc)


if __name__ == "__main__":
    main()
