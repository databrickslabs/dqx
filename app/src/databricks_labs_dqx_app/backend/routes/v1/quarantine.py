from __future__ import annotations

import csv
import io
import json
import re
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

# Match DQX check identifiers exactly: leading letter / underscore,
# letters / digits / underscores after that. Validating up front keeps
# the value safe to inline into a Spark SQL literal further down without
# needing additional escaping.
_CHECK_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")


def _check_name_predicate(check_name: str) -> str:
    """Build a Spark SQL predicate that matches quarantine rows whose
    ``errors`` or ``warnings`` VARIANT array contains a struct with the
    given ``name``.

    We project the VARIANT to a typed ``array<struct<name:string>>`` via
    ``from_json(to_json(...))`` so the higher-order ``exists`` can run.
    The ``or`` covers warning-level rules (which DQX writes to the
    sibling ``warnings`` column starting in migration v4).
    """
    return (
        "(EXISTS(from_json(to_json(errors), 'array<struct<name:string>>'), "
        f"e -> e.name = '{check_name}') "
        "OR EXISTS(from_json(to_json(warnings), 'array<struct<name:string>>'), "
        f"w -> w.name = '{check_name}'))"
    )


def _query_quarantine(
    sql: SqlExecutor,
    app_conf: AppConfig,
    run_id: str,
    offset: int = 0,
    limit: int = 50,
    check_name: str | None = None,
) -> tuple[list[dict[str, Any]], int]:
    """Query quarantine records for a given run_id with pagination.

    When ``check_name`` is supplied, only rows where that DQX check
    appears in the ``errors`` or ``warnings`` VARIANT payload are
    returned.  Both the COUNT and the data query apply the filter so
    pagination stays consistent.
    """
    from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

    table = f"{app_conf.catalog}.{app_conf.schema_name}.dq_quarantine_records"
    er = escape_sql_string(run_id)

    where = f"run_id = '{er}'"
    if check_name:
        if not _CHECK_NAME_RE.match(check_name):
            # Caller-controlled input; refuse anything that doesn't look
            # like a normal DQX identifier rather than risk injection.
            raise ValueError(f"Invalid check_name: '{check_name}'")
        where = f"{where} AND {_check_name_predicate(check_name)}"

    count_sql = f"SELECT COUNT(*) AS cnt FROM {table} WHERE {where}"  # noqa: S608
    count_rows = sql.query(count_sql)
    total_count = int(count_rows[0][0] or 0) if count_rows and count_rows[0] else 0

    # row_data / errors / warnings are VARIANT — render as JSON strings for
    # the existing _row_to_record parser. created_at is TIMESTAMP — cast to
    # STRING so query_dicts returns an ISO-formatted value.
    data_sql = (
        f"SELECT quarantine_id, run_id, source_table_fqn, requesting_user, "
        f"to_json(row_data) AS row_data, to_json(errors) AS errors, "
        f"to_json(warnings) AS warnings, "
        f"CAST(created_at AS STRING) AS created_at "
        f"FROM {table} WHERE {where} "  # noqa: S608
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
            parsed_errors = json.loads(row["errors"])
        except (json.JSONDecodeError, TypeError):
            errors = [row["errors"]]
        else:
            # Normalise to a list. DQX's ``dq_result_item_schema`` (and
            # the current SQL-check writer) emit a list of ``{name,
            # message, ...}`` structs, but legacy SQL-check rows written
            # before that fix used a single ``{check_name: message}``
            # dict. Coerce the legacy shape so those rows still display
            # — and so Pydantic's ``list[Any]`` validation doesn't 500
            # the whole endpoint when one historical row is wrong.
            if isinstance(parsed_errors, list):
                errors = parsed_errors
            elif isinstance(parsed_errors, dict):
                errors = [{"name": k, "message": v} for k, v in parsed_errors.items()]
            elif parsed_errors is not None:
                errors = [parsed_errors]

    # ``warnings`` is missing on rows written before migration v4; the
    # column is ``null`` for SQL-check quarantines.
    warnings: list[Any] | None = None
    raw_warnings = row.get("warnings")
    if raw_warnings and raw_warnings != "null":
        try:
            parsed = json.loads(raw_warnings)
            if parsed is not None:
                warnings = parsed if isinstance(parsed, list) else [parsed]
        except (json.JSONDecodeError, TypeError):
            warnings = [raw_warnings]

    return QuarantineRecordOut(
        quarantine_id=row.get("quarantine_id", ""),
        run_id=row.get("run_id", ""),
        source_table_fqn=row.get("source_table_fqn", ""),
        requesting_user=row.get("requesting_user"),
        row_data=row_data,
        errors=errors,
        warnings=warnings,
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
    check_name: str | None = Query(
        None,
        description="Filter to rows that failed only this DQX check (matches either errors or warnings).",
        max_length=128,
    ),
) -> QuarantineListOut:
    try:
        rows, total_count = _query_quarantine(sql, app_conf, run_id, offset, limit, check_name)
        records = [_row_to_record(r) for r in rows]
        return QuarantineListOut(records=records, total_count=total_count, offset=offset, limit=limit)
    except ValueError as exc:
        # _query_quarantine raises ValueError for malformed check_name.
        raise HTTPException(status_code=400, detail=str(exc)) from exc
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
    check_name: str | None = Query(
        None,
        description="Filter to rows that failed only this DQX check (matches either errors or warnings).",
        max_length=128,
    ),
) -> dict[str, int]:
    try:
        _, total_count = _query_quarantine(sql, app_conf, run_id, 0, 0, check_name)
        return {"count": total_count}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
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
    check_name: str | None = Query(
        None,
        description="Filter the export to rows that failed only this DQX check.",
        max_length=128,
    ),
) -> StreamingResponse:
    """Export quarantine records for a run as CSV or JSON download (capped)."""
    from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

    table = f"{app_conf.catalog}.{app_conf.schema_name}.dq_quarantine_records"
    er = escape_sql_string(run_id)

    where = f"run_id = '{er}'"
    if check_name:
        if not _CHECK_NAME_RE.match(check_name):
            raise HTTPException(status_code=400, detail=f"Invalid check_name: '{check_name}'")
        where = f"{where} AND {_check_name_predicate(check_name)}"

    stmt = (
        f"SELECT quarantine_id, run_id, source_table_fqn, requesting_user, "
        f"to_json(row_data) AS row_data, to_json(errors) AS errors, "
        f"to_json(warnings) AS warnings, "
        f"CAST(created_at AS STRING) AS created_at "
        f"FROM {table} WHERE {where} ORDER BY created_at DESC "  # noqa: S608
        f"LIMIT {int(max_rows)}"
    )
    rows = sql.query_dicts(stmt)

    # Reflect the filter in the downloaded filename so an exported file
    # is self-describing once it's left the UI.
    filename_suffix = f"_check_{check_name}" if check_name else ""

    if format == "json":
        records = [_row_to_record(r).model_dump() for r in rows]
        content = json.dumps(records, indent=2)
        return StreamingResponse(
            io.BytesIO(content.encode("utf-8")),
            media_type="application/json",
            headers={"Content-Disposition": (f'attachment; filename="quarantine_{run_id}{filename_suffix}.json"')},
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
        headers = ["quarantine_id", "run_id", "source_table_fqn", "errors", "warnings"] + data_keys + ["created_at"]
        ws.append(headers)

        for r in rows:
            raw_warn = r.get("warnings")
            warn_str = "" if not raw_warn or raw_warn == "null" else raw_warn
            flat: dict[str, str] = {
                "quarantine_id": r.get("quarantine_id", "") or "",
                "run_id": r.get("run_id", "") or "",
                "source_table_fqn": r.get("source_table_fqn", "") or "",
                "errors": r.get("errors", "") or "",
                "warnings": warn_str or "",
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
            headers={"Content-Disposition": (f'attachment; filename="quarantine_{run_id}{filename_suffix}.xlsx"')},
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
        fieldnames = ["quarantine_id", "run_id", "source_table_fqn", "errors", "warnings"] + csv_dk + ["created_at"]
        writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            raw_warn_csv = r.get("warnings")
            warn_str_csv = "" if not raw_warn_csv or raw_warn_csv == "null" else raw_warn_csv
            csv_flat: dict[str, str] = {
                "quarantine_id": r.get("quarantine_id", "") or "",
                "run_id": r.get("run_id", "") or "",
                "source_table_fqn": r.get("source_table_fqn", "") or "",
                "errors": r.get("errors", "") or "",
                "warnings": warn_str_csv or "",
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
        headers={"Content-Disposition": (f'attachment; filename="quarantine_{run_id}{filename_suffix}.csv"')},
    )
