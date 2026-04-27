from __future__ import annotations

import json
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.config import AppConfig
from databricks_labs_dqx_app.backend.dependencies import (
    get_conf,
    get_sp_sql_executor,
    get_user_catalog_names,
    require_role,
)
from databricks_labs_dqx_app.backend.models import MetricSnapshotOut, MetricsSummaryOut
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor

router = APIRouter()

_ALL_ROLES = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR, UserRole.VIEWER]


def _catalog_of(fqn: str) -> str:
    """Extract the catalog part from a fully qualified table name."""
    parts = fqn.split(".", 1)
    return parts[0] if parts else ""


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
    """Return quality metric snapshots for a specific table, ordered by time (newest first)."""
    from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string, validate_fqn

    try:
        validate_fqn(table_fqn)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if _catalog_of(table_fqn) not in user_catalogs:
        raise HTTPException(status_code=403, detail="You do not have access to this table's catalog")

    table = f"{app_conf.catalog}.{app_conf.schema_name}.dq_metrics"
    et = escape_sql_string(table_fqn)

    stmt = (
        f"SELECT metric_id, run_id, source_table_fqn, run_type, "  # noqa: S608
        f"total_rows, valid_rows, invalid_rows, pass_rate, "
        f"error_breakdown, requesting_user, created_at "
        f"FROM {table} WHERE source_table_fqn = '{et}' "
        f"ORDER BY created_at DESC LIMIT {limit}"
    )
    try:
        rows = sql.query_dicts(stmt)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    results: list[MetricSnapshotOut] = []
    for r in rows:
        eb = None
        raw_eb = r.get("error_breakdown")
        if raw_eb:
            try:
                eb = json.loads(raw_eb)
            except (json.JSONDecodeError, TypeError):
                eb = None
        results.append(
            MetricSnapshotOut(
                metric_id=r.get("metric_id", "") or "",
                run_id=r.get("run_id", "") or "",
                source_table_fqn=r.get("source_table_fqn", "") or "",
                run_type=r.get("run_type"),
                total_rows=int(v) if (v := r.get("total_rows")) else None,
                valid_rows=int(v) if (v := r.get("valid_rows")) else None,
                invalid_rows=int(v) if (v := r.get("invalid_rows")) else None,
                pass_rate=float(v) if (v := r.get("pass_rate")) else None,
                error_breakdown=eb,
                requesting_user=r.get("requesting_user"),
                created_at=r.get("created_at"),
            )
        )
    return results


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
    """Return the latest pass rate for each tracked table."""
    table = f"{app_conf.catalog}.{app_conf.schema_name}.dq_metrics"

    stmt = (
        f"SELECT source_table_fqn, pass_rate, run_id, run_type, created_at "  # noqa: S608
        f"FROM ("
        f"  SELECT *, ROW_NUMBER() OVER ("
        f"    PARTITION BY source_table_fqn ORDER BY created_at DESC"
        f"  ) AS rn FROM {table}"
        f") WHERE rn = 1 ORDER BY source_table_fqn LIMIT 500"
    )
    try:
        rows = sql.query_dicts(stmt)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return [
        MetricsSummaryOut(
            source_table_fqn=r.get("source_table_fqn", "") or "",
            latest_pass_rate=float(v) if (v := r.get("pass_rate")) else None,
            latest_run_id=r.get("run_id"),
            latest_run_type=r.get("run_type"),
            latest_created_at=r.get("created_at"),
        )
        for r in rows
        if _catalog_of(r.get("source_table_fqn") or "") in user_catalogs
    ]
