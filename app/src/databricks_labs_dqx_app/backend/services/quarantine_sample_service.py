"""Row-level failing-sample access, gated by live per-request OBO checks.

No new UC grants anywhere: the shared *dq_quarantine_records* table is
always read via the app's service principal (which already owns it).
Before any row is returned, the *requesting user's own* OBO credentials
must pass two live checks — a check that needs no elevated privilege,
since verifying your own access never requires MANAGE/ownership:

1. :meth:`QuarantineSampleService.user_can_select` — a zero-row SELECT
   probe against the source table, issued through the caller's
   OBO-scoped SQL executor (the same mechanism as the View Data
   preview, see *dependencies.get_preview_sql_executor*).
2. :meth:`QuarantineSampleService.has_fine_grained_access_control` — a
   metadata read (row filter / column masks) via the caller's OBO
   WorkspaceClient. If the source table carries fine-grained access
   controls, the sample is suppressed entirely: we cannot faithfully
   replicate those policies on copied quarantine data.

Both checks fail closed. See
docs/superpowers/specs/2026-07-10-dq-score-results-design.md §3.
"""

from __future__ import annotations

import json
import logging

from databricks.sdk import WorkspaceClient

from databricks_labs_dqx_app.backend.models import (
    FailedRowFailureOut,
    FailingRecordFailureOut,
    FailingRecordOut,
)
from databricks_labs_dqx_app.backend.services.dq_results_service import CheckAttribution
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor
from databricks_labs_dqx_app.backend.sql_utils import quote_fqn, validate_fqn

logger = logging.getLogger(__name__)


def parse_json_or_none(raw: str | None) -> object:
    """Parse a to_json(...)-rendered VARIANT column; None when absent/corrupt."""
    if not raw or raw == "null":
        return None
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None


def to_failing_record(row: dict[str, str | None]) -> FailingRecordOut:
    """Transform a dq_quarantine_records row into the UI's failure-highlight shape.

    The quarantine table nests the quarantined source row under the VARIANT
    *row_data* column, with DQX's result structs (*name*, *message*,
    *columns* — see schema/dq_result_schema.py) under the sibling VARIANT
    *errors*/*warnings* columns. All three arrive here as JSON text.
    Malformed payloads degrade to empty values rather than failing the
    whole response.
    """
    parsed_row = parse_json_or_none(row.get("row_data"))
    row_values: dict[str, str | None] = {}
    if isinstance(parsed_row, dict):
        row_values = {str(k): (None if v is None else str(v)) for k, v in parsed_row.items()}

    failures: list[FailingRecordFailureOut] = []
    for col_name in ("errors", "warnings"):
        parsed = parse_json_or_none(row.get(col_name))
        if isinstance(parsed, dict):
            # Legacy SQL-check rows wrote a single {check_name: message}
            # dict (see _row_to_record in routes/v1/quarantine.py).
            failures.extend(
                FailingRecordFailureOut(rule_name=str(k), message=str(v), columns=[]) for k, v in parsed.items()
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


def enrich_failures(
    failures: list[FailingRecordFailureOut],
    attribution: dict[str, CheckAttribution],
) -> list[FailedRowFailureOut]:
    """Join each failure's rule name to the binding's applied-rule metadata.

    Produces the dqlake FailureOut shape: rule_id / quality_dimension /
    severity come from the applied-rule attribution (None for checks not
    attributable to a registry rule application).
    """
    out: list[FailedRowFailureOut] = []
    for failure in failures:
        attr = attribution.get(failure.rule_name) if failure.rule_name else None
        out.append(
            FailedRowFailureOut(
                rule_id=attr.rule_id if attr else None,
                rule_name=failure.rule_name,
                quality_dimension=attr.dimension if attr else None,
                severity=attr.severity if attr else None,
                message=failure.message,
                columns=list(failure.columns),
            )
        )
    return out


class QuarantineSampleService:
    """Live per-request permission checks for the failing-sample endpoint."""

    @staticmethod
    def user_can_select(obo_sql: SqlExecutor, table_fqn: str) -> bool:
        """Live self-check: can the calling user currently SELECT this table.

        Runs a zero-row probe (no data returned, cheap) via the caller's own
        OBO-scoped SQL executor — never an elevated/service-principal call.
        Fails closed: ANY failure (permission denied, table missing,
        warehouse hiccup) reads as "no access".

        Args:
            obo_sql: SQL executor authenticated with the caller's OBO token.
            table_fqn: Three-part source-table name; validated before use.

        Returns:
            True only when the probe executes successfully as the caller.
        """
        validate_fqn(table_fqn)
        try:
            obo_sql.query(f"SELECT 1 FROM {quote_fqn(table_fqn)} LIMIT 0")
            return True
        except Exception:
            # table_fqn is validated above (no control characters), so it is
            # safe to interpolate into the log message.
            logger.info(f"OBO SELECT self-check denied for {table_fqn}", exc_info=True)
            return False

    @staticmethod
    def has_fine_grained_access_control(obo_ws: WorkspaceClient, table_fqn: str) -> bool:
        """True if the source table has a row filter or any column mask.

        The metadata read runs via the caller's OBO client. Fails closed:
        when the read errors we cannot verify the *absence* of fine-grained
        controls, so we report them as present and the caller suppresses
        the sample rather than risking a policy bypass.

        Args:
            obo_ws: WorkspaceClient authenticated with the caller's OBO token.
            table_fqn: Three-part source-table name; validated before use.

        Returns:
            True when a row filter or column mask is present, or when the
            metadata read fails.
        """
        validate_fqn(table_fqn)
        try:
            table_info = obo_ws.tables.get(table_fqn)
        except Exception:
            logger.warning(
                f"Fine-grained-control metadata read failed for {table_fqn}; suppressing sample",
                exc_info=True,
            )
            return True
        if table_info.row_filter is not None:
            return True
        return any(col.mask is not None for col in (table_info.columns or []))

    @staticmethod
    def row_matches_filters(
        failures: list[FailedRowFailureOut],
        failed_columns: list[str],
        *,
        dimensions: tuple[str, ...] = (),
        severities: tuple[str, ...] = (),
        rules: tuple[str, ...] = (),
        columns: tuple[str, ...] = (),
    ) -> bool:
        """Server-side failure filter for the filtered failed-rows endpoint.

        Mirrors dqlake's SQL predicates over ``failed_rows_latest``: each
        facet is satisfied when ANY failure on the row matches ANY of its
        values (``exists(failures, f -> f.<field> = v)``), the column facet
        is a membership test over the row's *failed_columns*, and the
        facets are ANDed together. Untagged failures (None fields) never
        match an active dimension/severity facet.
        """
        if dimensions and not any(f.quality_dimension in dimensions for f in failures):
            return False
        if severities and not any(f.severity in severities for f in failures):
            return False
        if rules and not any(f.rule_name in rules for f in failures):
            return False
        if columns and not any(c in columns for c in failed_columns):
            return False
        return True
