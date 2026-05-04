from __future__ import annotations

import csv
import io
import json
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.config import AppConfig
from databricks_labs_dqx_app.backend.dependencies import get_conf, get_sp_sql_executor, require_role
from databricks_labs_dqx_app.backend.models import QuarantineListOut, QuarantineRecordOut
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor

router = APIRouter()

_APPROVERS_AND_ABOVE = [UserRole.ADMIN, UserRole.RULE_APPROVER]


def _query_quarantine(
    sql: SqlExecutor,
    app_conf: AppConfig,
    run_id: str,
    offset: int = 0,
    limit: int = 50,
) -> tuple[list[dict[str, Any]], int]:
    """Query quarantine records for a given run_id with pagination."""
    from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

    table = f"{app_conf.catalog}.{app_conf.schema_name}.dq_quarantine_records"
    er = escape_sql_string(run_id)

    count_sql = f"SELECT COUNT(*) AS cnt FROM {table} WHERE run_id = '{er}'"  # noqa: S608
    count_rows = sql.query(count_sql)
    total_count = int(count_rows[0][0] or 0) if count_rows and count_rows[0] else 0

    data_sql = (
        f"SELECT quarantine_id, run_id, source_table_fqn, requesting_user, "
        f"row_data, errors, created_at "
        f"FROM {table} WHERE run_id = '{er}' "
        f"ORDER BY created_at DESC LIMIT {limit} OFFSET {offset}"
    )
    rows = sql.query_dicts(data_sql)
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


@router.get(
    "/runs/{run_id}",
    operation_id="listQuarantineRecords",
    dependencies=[require_role(*_APPROVERS_AND_ABOVE)],
)
def list_quarantine_records(
    run_id: str,
    sql: Annotated[SqlExecutor, Depends(get_sp_sql_executor)],
    app_conf: Annotated[AppConfig, Depends(get_conf)],
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
) -> QuarantineListOut:
    try:
        rows, total_count = _query_quarantine(sql, app_conf, run_id, offset, limit)
        records = [_row_to_record(r) for r in rows]
        return QuarantineListOut(records=records, total_count=total_count, offset=offset, limit=limit)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get(
    "/runs/{run_id}/count",
    operation_id="getQuarantineCount",
    dependencies=[require_role(*_APPROVERS_AND_ABOVE)],
)
def get_quarantine_count(
    run_id: str,
    sql: Annotated[SqlExecutor, Depends(get_sp_sql_executor)],
    app_conf: Annotated[AppConfig, Depends(get_conf)],
) -> dict[str, int]:
    try:
        _, total_count = _query_quarantine(sql, app_conf, run_id, 0, 0)
        return {"count": total_count}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


_EXPORT_MAX_ROWS = 50_000


@router.get(
    "/runs/{run_id}/export",
    operation_id="exportQuarantineRecords",
    dependencies=[require_role(*_APPROVERS_AND_ABOVE)],
)
def export_quarantine_records(
    run_id: str,
    sql: Annotated[SqlExecutor, Depends(get_sp_sql_executor)],
    app_conf: Annotated[AppConfig, Depends(get_conf)],
    format: str = Query("csv", pattern="^(csv|json|xlsx)$"),
    max_rows: int = Query(_EXPORT_MAX_ROWS, ge=1, le=_EXPORT_MAX_ROWS),
) -> StreamingResponse:
    """Export quarantine records for a run as CSV or JSON download (capped)."""
    from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

    table = f"{app_conf.catalog}.{app_conf.schema_name}.dq_quarantine_records"
    er = escape_sql_string(run_id)

    stmt = (
        f"SELECT quarantine_id, run_id, source_table_fqn, requesting_user, "
        f"row_data, errors, created_at "
        f"FROM {table} WHERE run_id = '{er}' ORDER BY created_at DESC "  # noqa: S608
        f"LIMIT {int(max_rows)}"
    )
    rows = sql.query_dicts(stmt)

    if format == "json":
        records = [_row_to_record(r).model_dump() for r in rows]
        content = json.dumps(records, indent=2)
        return StreamingResponse(
            io.BytesIO(content.encode("utf-8")),
            media_type="application/json",
            headers={"Content-Disposition": f'attachment; filename="quarantine_{run_id}.json"'},
        )

    if format == "xlsx":
        from openpyxl import Workbook

        wb = Workbook()
        ws = wb.active
        assert ws is not None
        ws.title = "Quarantine"

        all_data_keys: set[str] = set()
        for r in rows:
            raw_rd = r.get("row_data")
            if raw_rd:
                try:
                    all_data_keys.update(json.loads(raw_rd).keys())
                except (json.JSONDecodeError, TypeError):
                    pass
        data_keys = sorted(all_data_keys)
        headers = ["quarantine_id", "run_id", "source_table_fqn", "errors"] + data_keys + ["created_at"]
        ws.append(headers)

        for r in rows:
            flat: dict[str, str] = {
                "quarantine_id": r.get("quarantine_id", "") or "",
                "run_id": r.get("run_id", "") or "",
                "source_table_fqn": r.get("source_table_fqn", "") or "",
                "errors": r.get("errors", "") or "",
                "created_at": r.get("created_at", "") or "",
            }
            raw_rd = r.get("row_data")
            if raw_rd:
                try:
                    flat.update(json.loads(raw_rd))
                except (json.JSONDecodeError, TypeError):
                    pass
            ws.append([str(flat.get(h, "")) for h in headers])

        xlsx_buf = io.BytesIO()
        wb.save(xlsx_buf)
        xlsx_buf.seek(0)
        return StreamingResponse(
            xlsx_buf,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f'attachment; filename="quarantine_{run_id}.xlsx"'},
        )

    buf = io.StringIO()
    if rows:
        csv_data_keys: set[str] = set()
        for r in rows:
            raw_rd = r.get("row_data")
            if raw_rd:
                try:
                    csv_data_keys.update(json.loads(raw_rd).keys())
                except (json.JSONDecodeError, TypeError):
                    pass
        csv_dk = sorted(csv_data_keys)
        fieldnames = ["quarantine_id", "run_id", "source_table_fqn", "errors"] + csv_dk + ["created_at"]
        writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            csv_flat: dict[str, str] = {
                "quarantine_id": r.get("quarantine_id", "") or "",
                "run_id": r.get("run_id", "") or "",
                "source_table_fqn": r.get("source_table_fqn", "") or "",
                "errors": r.get("errors", "") or "",
                "created_at": r.get("created_at", "") or "",
            }
            raw_rd = r.get("row_data")
            if raw_rd:
                try:
                    csv_flat.update(json.loads(raw_rd))
                except (json.JSONDecodeError, TypeError):
                    pass
            writer.writerow(csv_flat)

    return StreamingResponse(
        io.BytesIO(buf.getvalue().encode("utf-8")),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="quarantine_{run_id}.csv"'},
    )
