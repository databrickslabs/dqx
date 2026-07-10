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

import logging

from databricks.sdk import WorkspaceClient

from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor
from databricks_labs_dqx_app.backend.sql_utils import quote_fqn, validate_fqn

logger = logging.getLogger(__name__)


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
