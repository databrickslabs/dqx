"""Row-level failing-sample read API.

Returns actual row values from the shared *dq_quarantine_records* table,
so its permission model is stricter than the aggregate score endpoints —
see ``services/quarantine_sample_service.py`` for the checks it enforces.

SECURITY MODEL — the order below is load-bearing:

1. Validate the FQN (400 on malformed input, before any backend call).
2. Live OBO SELECT self-check on the SOURCE table, as the CALLER. On
   failure return HTTP 200 with an empty records list — never a 403/404
   that would confirm or deny the table's existence to an unauthorized
   caller.
3. Fine-grained-control check (row filter / column mask) via the
   caller's OBO metadata read; if present — or unknowable — suppress the
   sample entirely. Runs AFTER the SELECT check so a user without access
   cannot even learn whether the table carries fine-grained controls.
4. Only then does the app's service principal read the quarantine table
   (which only the SP can read).
"""

from __future__ import annotations

import json
import logging
from typing import Annotated

from databricks.sdk import WorkspaceClient
from fastapi import APIRouter, Depends, HTTPException, Query

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.config import AppConfig
from databricks_labs_dqx_app.backend.dependencies import (
    get_conf,
    get_obo_ws,
    get_preview_sql_executor,
    get_sp_sql_executor,
    require_role,
)
from databricks_labs_dqx_app.backend.models import (
    FailingRecordFailureOut,
    FailingRecordOut,
    FailingRecordsOut,
)
from databricks_labs_dqx_app.backend.services.quarantine_sample_service import (
    QuarantineSampleService,
)
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor
from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string, validate_fqn

logger = logging.getLogger(__name__)
router = APIRouter()

_ALL_ROLES = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR, UserRole.VIEWER]


@router.get(
    "/{table_fqn:path}",
    operation_id="getQuarantineSample",
    dependencies=[require_role(*_ALL_ROLES)],
)
def get_quarantine_sample(
    table_fqn: str,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    obo_sql: Annotated[SqlExecutor, Depends(get_preview_sql_executor)],
    sp_sql: Annotated[SqlExecutor, Depends(get_sp_sql_executor)],
    app_conf: Annotated[AppConfig, Depends(get_conf)],
    limit: int = Query(50, ge=1, le=200),
) -> FailingRecordsOut:
    """Return the latest failing rows recorded for *table_fqn* (OBO-gated)."""
    try:
        validate_fqn(table_fqn)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    # (2) Cheap denial, as the caller. Empty 200 — not 403/404 — so an
    # unauthorized caller cannot confirm or deny the table's existence.
    if not QuarantineSampleService.user_can_select(obo_sql, table_fqn):
        return FailingRecordsOut(source_table_fqn=table_fqn, records=[], suppressed=False)

    # (3) Fine-grained controls (or an unverifiable state) suppress the
    # sample entirely — copied quarantine rows can't replicate the policy.
    if QuarantineSampleService.has_fine_grained_access_control(obo_ws, table_fqn):
        return FailingRecordsOut(source_table_fqn=table_fqn, records=[], suppressed=True)

    # (4) SP-side fetch of the precomputed failing rows. VARIANT columns are
    # rendered via to_json(...) — same convention as _query_quarantine in
    # routes/v1/quarantine.py.
    quarantine_table = f"{app_conf.catalog}.{app_conf.schema_name}.dq_quarantine_records"
    e_fqn = escape_sql_string(table_fqn)
    stmt = (
        f"SELECT quarantine_id, run_id, to_json(row_data) AS row_data, "
        f"to_json(errors) AS errors, to_json(warnings) AS warnings, "
        f"CAST(created_at AS STRING) AS created_at "
        f"FROM {quarantine_table} WHERE source_table_fqn = '{e_fqn}' "  # noqa: S608
        f"ORDER BY created_at DESC LIMIT {int(limit)}"
    )
    try:
        rows = sp_sql.query_dicts(stmt)
    except Exception as exc:
        # Only reachable after both OBO checks passed, so this 500 leaks
        # nothing to unauthorized callers.
        logger.exception(f"Failed to load quarantine sample for {table_fqn}")
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return FailingRecordsOut(
        source_table_fqn=table_fqn,
        records=[_to_failing_record(r) for r in rows],
        suppressed=False,
    )


def _parse_json_or_none(raw: str | None) -> object:
    """Parse a to_json(...)-rendered VARIANT column; None when absent/corrupt."""
    if not raw or raw == "null":
        return None
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None


def _to_failing_record(row: dict[str, str | None]) -> FailingRecordOut:
    """Transform a dq_quarantine_records row into the UI's failure-highlight shape.

    The quarantine table nests the quarantined source row under the VARIANT
    *row_data* column, with DQX's result structs (*name*, *message*,
    *columns* — see schema/dq_result_schema.py) under the sibling VARIANT
    *errors*/*warnings* columns. All three arrive here as JSON text.
    Malformed payloads degrade to empty values rather than failing the
    whole response.
    """
    parsed_row = _parse_json_or_none(row.get("row_data"))
    row_values: dict[str, str | None] = {}
    if isinstance(parsed_row, dict):
        row_values = {str(k): (None if v is None else str(v)) for k, v in parsed_row.items()}

    failures: list[FailingRecordFailureOut] = []
    for col_name in ("errors", "warnings"):
        parsed = _parse_json_or_none(row.get(col_name))
        if isinstance(parsed, dict):
            # Legacy SQL-check rows wrote a single {check_name: message}
            # dict (see _row_to_record in routes/v1/quarantine.py).
            failures.extend(
                FailingRecordFailureOut(rule_name=str(k), message=str(v), columns=[])
                for k, v in parsed.items()
            )
            continue
        if not isinstance(parsed, list):
            continue
        for entry in parsed:
            if not isinstance(entry, dict):
                continue
            columns = entry.get("columns")
            failures.append(
                FailingRecordFailureOut(
                    rule_name=str(entry["name"]) if entry.get("name") is not None else None,
                    message=str(entry["message"]) if entry.get("message") is not None else None,
                    columns=[str(c) for c in columns] if isinstance(columns, list) else [],
                )
            )

    failed_columns = sorted({c for f in failures for c in f.columns})
    return FailingRecordOut(
        record_key=str(row.get("quarantine_id") or ""),
        row_values=row_values,
        failed_columns=failed_columns,
        failures=failures,
    )
