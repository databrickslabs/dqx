from __future__ import annotations

import json
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query

from databricks_labs_dqx_app.backend.config import AppConfig
from databricks_labs_dqx_app.backend.dependencies import get_conf, get_job_service
from databricks_labs_dqx_app.backend.models import MetricSnapshotOut, MetricsSummaryOut
from databricks_labs_dqx_app.backend.services.job_service import JobService

router = APIRouter()


@router.get("/{table_fqn:path}", operation_id="getMetricsTrend")
def get_metrics_trend(
    table_fqn: str,
    job_svc: Annotated[JobService, Depends(get_job_service)],
    app_conf: Annotated[AppConfig, Depends(get_conf)],
    limit: int = Query(50, ge=1, le=200),
) -> list[MetricSnapshotOut]:
    """Return quality metric snapshots for a specific table, ordered by time (newest first)."""
    from databricks.sdk.service.sql import Disposition, Format, StatementState
    from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string, validate_fqn

    try:
        validate_fqn(table_fqn)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    table = f"{app_conf.catalog}.{app_conf.schema_name}.dq_metrics"
    et = escape_sql_string(table_fqn)

    sql = (
        f"SELECT metric_id, run_id, source_table_fqn, run_type, "  # noqa: S608
        f"total_rows, valid_rows, invalid_rows, pass_rate, "
        f"error_breakdown, requesting_user, created_at "
        f"FROM {table} WHERE source_table_fqn = '{et}' "
        f"ORDER BY created_at DESC LIMIT {limit}"
    )
    try:
        resp = job_svc._ws.statement_execution.execute_statement(
            warehouse_id=job_svc._warehouse_id,
            statement=sql,
            catalog=app_conf.catalog,
            schema=app_conf.schema_name,
            disposition=Disposition.INLINE,
            format=Format.JSON_ARRAY,
        )
        if resp.status and resp.status.state == StatementState.FAILED:
            msg = resp.status.error.message if resp.status.error else "Unknown error"
            raise RuntimeError(f"Metrics query failed: {msg}")

        if not resp.result or not resp.result.data_array:
            return []

        columns = [
            col.name or ""
            for col in ((resp.manifest.schema.columns if resp.manifest and resp.manifest.schema else None) or [])
        ]
        rows = [dict(zip(columns, row)) for row in resp.result.data_array]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    results: list[MetricSnapshotOut] = []
    for r in rows:
        eb = None
        if r.get("error_breakdown"):
            try:
                eb = json.loads(r["error_breakdown"])
            except (json.JSONDecodeError, TypeError):
                eb = None
        results.append(
            MetricSnapshotOut(
                metric_id=r.get("metric_id", ""),
                run_id=r.get("run_id", ""),
                source_table_fqn=r.get("source_table_fqn", ""),
                run_type=r.get("run_type"),
                total_rows=int(r["total_rows"]) if r.get("total_rows") else None,
                valid_rows=int(r["valid_rows"]) if r.get("valid_rows") else None,
                invalid_rows=int(r["invalid_rows"]) if r.get("invalid_rows") else None,
                pass_rate=float(r["pass_rate"]) if r.get("pass_rate") else None,
                error_breakdown=eb,
                requesting_user=r.get("requesting_user"),
                created_at=r.get("created_at"),
            )
        )
    return results


@router.get("", operation_id="getMetricsSummary")
def get_metrics_summary(
    job_svc: Annotated[JobService, Depends(get_job_service)],
    app_conf: Annotated[AppConfig, Depends(get_conf)],
) -> list[MetricsSummaryOut]:
    """Return the latest pass rate for each tracked table."""
    from databricks.sdk.service.sql import Disposition, Format, StatementState

    table = f"{app_conf.catalog}.{app_conf.schema_name}.dq_metrics"

    sql = (
        f"SELECT source_table_fqn, pass_rate, run_id, run_type, created_at "  # noqa: S608
        f"FROM ("
        f"  SELECT *, ROW_NUMBER() OVER ("
        f"    PARTITION BY source_table_fqn ORDER BY created_at DESC"
        f"  ) AS rn FROM {table}"
        f") WHERE rn = 1 ORDER BY source_table_fqn LIMIT 500"
    )
    try:
        resp = job_svc._ws.statement_execution.execute_statement(
            warehouse_id=job_svc._warehouse_id,
            statement=sql,
            catalog=app_conf.catalog,
            schema=app_conf.schema_name,
            disposition=Disposition.INLINE,
            format=Format.JSON_ARRAY,
        )
        if resp.status and resp.status.state == StatementState.FAILED:
            msg = resp.status.error.message if resp.status.error else "Unknown error"
            raise RuntimeError(f"Metrics summary query failed: {msg}")

        if not resp.result or not resp.result.data_array:
            return []

        columns = [
            col.name or ""
            for col in ((resp.manifest.schema.columns if resp.manifest and resp.manifest.schema else None) or [])
        ]
        rows = [dict(zip(columns, row)) for row in resp.result.data_array]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return [
        MetricsSummaryOut(
            source_table_fqn=r.get("source_table_fqn", ""),
            latest_pass_rate=float(r["pass_rate"]) if r.get("pass_rate") else None,
            latest_run_id=r.get("run_id"),
            latest_run_type=r.get("run_type"),
            latest_created_at=r.get("created_at"),
        )
        for r in rows
    ]
