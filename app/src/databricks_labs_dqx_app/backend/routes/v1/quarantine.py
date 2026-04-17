from __future__ import annotations

import csv
import io
import json
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse

from databricks_labs_dqx_app.backend.config import AppConfig
from databricks_labs_dqx_app.backend.dependencies import get_conf, get_job_service
from databricks_labs_dqx_app.backend.models import QuarantineListOut, QuarantineRecordOut
from databricks_labs_dqx_app.backend.services.job_service import JobService

router = APIRouter()


def _query_quarantine(
    job_svc: JobService,
    app_conf: AppConfig,
    run_id: str,
    offset: int = 0,
    limit: int = 50,
) -> tuple[list[dict[str, Any]], int]:
    """Query quarantine records for a given run_id with pagination."""
    from databricks.sdk.service.sql import Disposition, Format, StatementState
    from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

    table = f"{app_conf.catalog}.{app_conf.schema_name}.dq_quarantine_records"
    er = escape_sql_string(run_id)

    count_sql = f"SELECT COUNT(*) AS cnt FROM {table} WHERE run_id = '{er}'"  # noqa: S608
    resp = job_svc._ws.statement_execution.execute_statement(
        warehouse_id=job_svc._warehouse_id,
        statement=count_sql,
        catalog=app_conf.catalog,
        schema=app_conf.schema_name,
        disposition=Disposition.INLINE,
        format=Format.JSON_ARRAY,
    )
    total_count = 0
    if resp.result and resp.result.data_array and resp.result.data_array[0]:
        total_count = int(resp.result.data_array[0][0] or 0)

    data_sql = (
        f"SELECT quarantine_id, run_id, source_table_fqn, requesting_user, "  # noqa: S608
        f"row_data, errors, created_at "
        f"FROM {table} WHERE run_id = '{er}' "
        f"ORDER BY created_at DESC LIMIT {limit} OFFSET {offset}"
    )
    resp2 = job_svc._ws.statement_execution.execute_statement(
        warehouse_id=job_svc._warehouse_id,
        statement=data_sql,
        catalog=app_conf.catalog,
        schema=app_conf.schema_name,
        disposition=Disposition.INLINE,
        format=Format.JSON_ARRAY,
    )

    if resp2.status and resp2.status.state == StatementState.FAILED:
        msg = resp2.status.error.message if resp2.status.error else "Unknown error"
        raise RuntimeError(f"Quarantine query failed: {msg}")

    if not resp2.result or not resp2.result.data_array:
        return [], total_count

    columns = [
        col.name or ""
        for col in ((resp2.manifest.schema.columns if resp2.manifest and resp2.manifest.schema else None) or [])
    ]
    rows = [dict(zip(columns, row)) for row in resp2.result.data_array]
    return rows, total_count


def _row_to_record(row: dict[str, Any]) -> QuarantineRecordOut:
    row_data = None
    if row.get("row_data"):
        try:
            row_data = json.loads(row["row_data"])
        except (json.JSONDecodeError, TypeError):
            row_data = {"raw": row["row_data"]}

    errors = None
    if row.get("errors"):
        try:
            errors = json.loads(row["errors"])
        except (json.JSONDecodeError, TypeError):
            errors = [row["errors"]]

    return QuarantineRecordOut(
        quarantine_id=row.get("quarantine_id", ""),
        run_id=row.get("run_id", ""),
        source_table_fqn=row.get("source_table_fqn", ""),
        requesting_user=row.get("requesting_user"),
        row_data=row_data,
        errors=errors,
        created_at=row.get("created_at"),
    )


@router.get("/runs/{run_id}", operation_id="listQuarantineRecords")
def list_quarantine_records(
    run_id: str,
    job_svc: Annotated[JobService, Depends(get_job_service)],
    app_conf: Annotated[AppConfig, Depends(get_conf)],
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
) -> QuarantineListOut:
    try:
        rows, total_count = _query_quarantine(job_svc, app_conf, run_id, offset, limit)
        records = [_row_to_record(r) for r in rows]
        return QuarantineListOut(records=records, total_count=total_count, offset=offset, limit=limit)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/runs/{run_id}/count", operation_id="getQuarantineCount")
def get_quarantine_count(
    run_id: str,
    job_svc: Annotated[JobService, Depends(get_job_service)],
    app_conf: Annotated[AppConfig, Depends(get_conf)],
) -> dict[str, int]:
    try:
        _, total_count = _query_quarantine(job_svc, app_conf, run_id, 0, 0)
        return {"count": total_count}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


_EXPORT_MAX_ROWS = 50_000


@router.get("/runs/{run_id}/export", operation_id="exportQuarantineRecords")
def export_quarantine_records(
    run_id: str,
    job_svc: Annotated[JobService, Depends(get_job_service)],
    app_conf: Annotated[AppConfig, Depends(get_conf)],
    format: str = Query("csv", regex="^(csv|json)$"),
    max_rows: int = Query(_EXPORT_MAX_ROWS, ge=1, le=_EXPORT_MAX_ROWS),
) -> StreamingResponse:
    """Export quarantine records for a run as CSV or JSON download (capped)."""
    from databricks.sdk.service.sql import Disposition, Format, StatementState
    from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

    table = f"{app_conf.catalog}.{app_conf.schema_name}.dq_quarantine_records"
    er = escape_sql_string(run_id)

    sql = (
        f"SELECT quarantine_id, run_id, source_table_fqn, requesting_user, "  # noqa: S608
        f"row_data, errors, created_at "
        f"FROM {table} WHERE run_id = '{er}' ORDER BY created_at DESC "
        f"LIMIT {int(max_rows)}"
    )
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
        raise HTTPException(status_code=500, detail=f"Export query failed: {msg}")

    columns = [
        col.name or ""
        for col in ((resp.manifest.schema.columns if resp.manifest and resp.manifest.schema else None) or [])
    ]
    raw_rows = resp.result.data_array if resp.result and resp.result.data_array else []
    rows = [dict(zip(columns, row)) for row in raw_rows]

    if format == "json":
        records = [_row_to_record(r).model_dump() for r in rows]
        content = json.dumps(records, indent=2)
        return StreamingResponse(
            io.BytesIO(content.encode("utf-8")),
            media_type="application/json",
            headers={"Content-Disposition": f'attachment; filename="quarantine_{run_id}.json"'},
        )

    buf = io.StringIO()
    if rows:
        all_data_keys: set[str] = set()
        for r in rows:
            if r.get("row_data"):
                try:
                    all_data_keys.update(json.loads(r["row_data"]).keys())
                except (json.JSONDecodeError, TypeError):
                    pass
        data_keys = sorted(all_data_keys)
        fieldnames = ["quarantine_id", "run_id", "source_table_fqn", "errors"] + data_keys + ["created_at"]
        writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            flat: dict[str, str] = {
                "quarantine_id": r.get("quarantine_id", ""),
                "run_id": r.get("run_id", ""),
                "source_table_fqn": r.get("source_table_fqn", ""),
                "errors": r.get("errors", ""),
                "created_at": r.get("created_at", ""),
            }
            if r.get("row_data"):
                try:
                    flat.update(json.loads(r["row_data"]))
                except (json.JSONDecodeError, TypeError):
                    pass
            writer.writerow(flat)

    return StreamingResponse(
        io.BytesIO(buf.getvalue().encode("utf-8")),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="quarantine_{run_id}.csv"'},
    )
