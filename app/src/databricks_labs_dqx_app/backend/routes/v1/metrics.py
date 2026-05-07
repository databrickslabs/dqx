"""Metrics read API.

Reads the spec-compliant long-format ``dq_metrics`` table populated by
``DQMetricsObserver`` (one row per metric per run) and pivots it back
into the wide-format DTO the existing UI consumes. Two paths are
supported:

* ``GET /api/v1/metrics/{table_fqn}`` — trend snapshots for one table
  (newest first).
* ``GET /api/v1/metrics`` — latest snapshot per table.

Per-check breakdowns and admin-defined custom metrics are surfaced as
new optional fields on :class:`MetricSnapshotOut`.
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.config import AppConfig
from databricks_labs_dqx_app.backend.dependencies import (
    get_conf,
    get_sp_sql_executor,
    get_user_catalog_names,
    require_role,
)
from databricks_labs_dqx_app.backend.models import (
    CheckMetricBreakdown,
    MetricSnapshotOut,
    MetricsSummaryOut,
)
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor

logger = logging.getLogger(__name__)
router = APIRouter()

_ALL_ROLES = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR, UserRole.VIEWER]
_BUILTIN_METRIC_NAMES = frozenset(
    {"input_row_count", "error_row_count", "warning_row_count", "valid_row_count", "check_metrics"}
)


def _catalog_of(fqn: str) -> str:
    """Extract the catalog part from a fully qualified table name."""
    parts = fqn.split(".", 1)
    return parts[0] if parts else ""


def _safe_int(value: Any) -> int | None:
    """Best-effort string→int that tolerates ``None`` and decimal strings."""
    if value in (None, ""):
        return None
    try:
        # Accept '123', '123.0', 123, 123.0 — counts can be promoted to
        # bigint by Spark and arrive as strings.
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _parse_check_metrics(raw: Any) -> list[CheckMetricBreakdown]:
    """Parse the ``check_metrics`` JSON-string emitted by the observer."""
    if not raw:
        return []
    try:
        items = json.loads(raw) if isinstance(raw, str) else raw
    except (json.JSONDecodeError, TypeError):
        return []
    if not isinstance(items, list):
        return []
    out: list[CheckMetricBreakdown] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        out.append(
            CheckMetricBreakdown(
                check_name=str(item.get("check_name") or "unknown"),
                error_count=int(item.get("error_count") or 0),
                warning_count=int(item.get("warning_count") or 0),
            )
        )
    return out


def _check_metrics_to_error_breakdown(items: list[CheckMetricBreakdown]) -> list[dict[str, Any]] | None:
    """Convert per-check breakdown into the legacy ``error_breakdown`` shape.

    The frontend already renders ``error_breakdown`` as a sorted list of
    ``{error, count}`` pairs; preserving that shape avoids touching the
    UI while still backfilling the data from the observer's per-check
    metric.
    """
    if not items:
        return None
    rows = [
        {
            "error": it.check_name,
            "count": it.error_count + it.warning_count,
            "error_count": it.error_count,
            "warning_count": it.warning_count,
        }
        for it in items
        if (it.error_count + it.warning_count) > 0
    ]
    rows.sort(key=lambda r: -int(r["count"]))
    return rows[:20]


def _pivot_rows(rows: list[dict[str, Any]]) -> list[MetricSnapshotOut]:
    """Pivot long-format dq_metrics rows back into wide MetricSnapshotOut.

    ``rows`` is expected to be ordered by ``run_time DESC`` and grouped
    naturally by ``run_id`` — we preserve that order in the output so the
    trend chart remains newest-first.
    """
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    order: list[str] = []
    for r in rows:
        rid = r.get("run_id") or ""
        if rid not in grouped:
            order.append(rid)
        grouped[rid].append(r)

    out: list[MetricSnapshotOut] = []
    for rid in order:
        bucket = grouped[rid]
        first = bucket[0]
        # ``user_metadata`` arrives as a string-encoded map (Databricks
        # SQL serialises MAP<STRING,STRING> as ``{key=val, ...}``). We
        # don't need to parse it for the existing UI; just pull
        # well-known fields out via the dedicated columns we already
        # encode them under (run_type comes from the run_type column on
        # dq_validation_runs).
        metrics: dict[str, str] = {}
        for r in bucket:
            name = r.get("metric_name") or ""
            value = r.get("metric_value")
            if name:
                metrics[name] = value if value is not None else ""

        total = _safe_int(metrics.get("input_row_count"))
        valid = _safe_int(metrics.get("valid_row_count"))
        errors = _safe_int(metrics.get("error_row_count"))
        warnings = _safe_int(metrics.get("warning_row_count"))
        invalid = errors  # 'invalid' in the legacy DTO == 'error_row_count'

        pass_rate = (valid / total * 100.0) if total and total > 0 and valid is not None else None
        if pass_rate is not None:
            pass_rate = round(pass_rate, 4)

        check_metrics = _parse_check_metrics(metrics.get("check_metrics"))
        custom_metrics = {k: v for k, v in metrics.items() if k not in _BUILTIN_METRIC_NAMES} or None

        out.append(
            MetricSnapshotOut(
                metric_id=rid,
                run_id=rid,
                source_table_fqn=first.get("input_location") or "",
                run_type=first.get("run_type"),
                total_rows=total,
                valid_rows=valid,
                invalid_rows=invalid,
                error_row_count=errors,
                warning_row_count=warnings,
                pass_rate=pass_rate,
                error_breakdown=_check_metrics_to_error_breakdown(check_metrics),
                check_metrics=check_metrics or None,
                custom_metrics=custom_metrics,
                rule_set_fingerprint=first.get("rule_set_fingerprint"),
                requesting_user=first.get("requesting_user"),
                created_at=first.get("created_at"),
            )
        )
    return out


@router.get(
    "/{table_fqn:path}",
    operation_id="getMetricsTrend",
    dependencies=[require_role(*_ALL_ROLES)],
)
def get_metrics_trend(
    table_fqn: str,
    sql: Annotated[SqlExecutor, Depends(get_sp_sql_executor)],
    app_conf: Annotated[AppConfig, Depends(get_conf)],
    user_catalogs: Annotated[frozenset[str], Depends(get_user_catalog_names)],
    limit: int = Query(50, ge=1, le=200),
) -> list[MetricSnapshotOut]:
    """Return quality metric snapshots for a specific table, ordered by time (newest first).

    Joins ``dq_metrics`` (long-format observations) to ``dq_validation_runs``
    so we recover ``run_type``, ``requesting_user`` and ``created_at`` from
    the lifecycle table without duplicating them across every metric row.
    """
    from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string, validate_fqn

    try:
        validate_fqn(table_fqn)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if _catalog_of(table_fqn) not in user_catalogs:
        raise HTTPException(status_code=403, detail="You do not have access to this table's catalog")

    metrics_table = f"{app_conf.catalog}.{app_conf.schema_name}.dq_metrics"
    runs_table = f"{app_conf.catalog}.{app_conf.schema_name}.dq_validation_runs"
    e_fqn = escape_sql_string(table_fqn)

    # Pull the latest ``limit`` runs for this table (DESC by run_time)
    # then fetch every metric row attached to those runs in one go.
    stmt = (
        f"WITH recent_runs AS ("
        f"  SELECT DISTINCT m.run_id, m.run_time "
        f"  FROM {metrics_table} m WHERE m.input_location = '{e_fqn}' "  # noqa: S608
        f"  ORDER BY m.run_time DESC LIMIT {limit}"
        f") "
        f"SELECT m.run_id, m.input_location, m.metric_name, m.metric_value, "
        f"       m.rule_set_fingerprint, r.run_type, r.requesting_user, r.created_at "
        f"FROM {metrics_table} m "
        f"JOIN recent_runs rr ON rr.run_id = m.run_id "
        f"LEFT JOIN {runs_table} r ON r.run_id = m.run_id "
        f"ORDER BY rr.run_time DESC, m.run_id, m.metric_name"
    )
    try:
        rows = sql.query_dicts(stmt)
    except Exception as exc:
        logger.exception("Failed to read metrics for %s", table_fqn)
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return _pivot_rows(rows)


@router.get(
    "",
    operation_id="getMetricsSummary",
    dependencies=[require_role(*_ALL_ROLES)],
)
def get_metrics_summary(
    sql: Annotated[SqlExecutor, Depends(get_sp_sql_executor)],
    app_conf: Annotated[AppConfig, Depends(get_conf)],
    user_catalogs: Annotated[frozenset[str], Depends(get_user_catalog_names)],
) -> list[MetricsSummaryOut]:
    """Return the latest pass-rate per tracked table.

    Computes pass rate inline from ``valid_row_count`` and
    ``input_row_count`` so we don't need a stored ``pass_rate`` column.
    """
    metrics_table = f"{app_conf.catalog}.{app_conf.schema_name}.dq_metrics"
    runs_table = f"{app_conf.catalog}.{app_conf.schema_name}.dq_validation_runs"

    # For each (input_location), find the most recent run_id, then pull
    # input/valid counts plus run_type/created_at from the runs table.
    stmt = (
        f"WITH latest AS ("
        f"  SELECT input_location, run_id, run_time, "
        f"         ROW_NUMBER() OVER (PARTITION BY input_location ORDER BY run_time DESC) AS rn "
        f"  FROM {metrics_table}"  # noqa: S608
        f"), latest_run AS ("
        f"  SELECT input_location, run_id, run_time FROM latest WHERE rn = 1"
        f") "
        f"SELECT lr.input_location AS source_table_fqn, lr.run_id, "
        f"       MAX(CASE WHEN m.metric_name='input_row_count' THEN m.metric_value END) AS total, "
        f"       MAX(CASE WHEN m.metric_name='valid_row_count' THEN m.metric_value END) AS valid, "
        f"       r.run_type, r.created_at "
        f"FROM latest_run lr "
        f"JOIN {metrics_table} m ON m.run_id = lr.run_id "
        f"LEFT JOIN {runs_table} r ON r.run_id = lr.run_id "
        f"GROUP BY lr.input_location, lr.run_id, lr.run_time, r.run_type, r.created_at "
        f"ORDER BY lr.input_location LIMIT 500"
    )
    try:
        rows = sql.query_dicts(stmt)
    except Exception as exc:
        logger.exception("Failed to read metrics summary")
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    out: list[MetricsSummaryOut] = []
    for r in rows:
        fqn = r.get("source_table_fqn") or ""
        if _catalog_of(fqn) not in user_catalogs:
            continue
        total = _safe_int(r.get("total"))
        valid = _safe_int(r.get("valid"))
        pass_rate = (valid / total * 100.0) if total and total > 0 and valid is not None else None
        out.append(
            MetricsSummaryOut(
                source_table_fqn=fqn,
                latest_pass_rate=round(pass_rate, 4) if pass_rate is not None else None,
                latest_run_id=r.get("run_id"),
                latest_run_type=r.get("run_type"),
                latest_created_at=r.get("created_at"),
            )
        )
    return out
