"""Job submission service for delegating compute to serverless Databricks Jobs.

All operations use the **SP WorkspaceClient** (``rt.ws``) so that the
app's service principal submits and polls job runs.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from databricks.sdk import WorkspaceClient
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class RunStatus(BaseModel):
    """Lightweight view of a job run's current state."""

    state: str  # PENDING, RUNNING, TERMINATED, SKIPPED, INTERNAL_ERROR
    result_state: str | None = None  # SUCCESS, FAILED, TIMEDOUT, CANCELED
    message: str | None = None


class JobService:
    """Submit and poll serverless job runs for profiler / dry-run tasks."""

    def __init__(
        self,
        ws: WorkspaceClient,
        job_id: str,
        catalog: str,
        schema: str,
        warehouse_id: str,
    ) -> None:
        self._ws = ws
        self._job_id = int(job_id) if job_id else 0
        self._catalog = catalog
        self._schema = schema
        self._warehouse_id = warehouse_id

    def submit_run(
        self,
        task_type: str,
        view_fqn: str,
        config: dict[str, Any],
        run_id: str,
        requesting_user: str,
    ) -> int:
        """Submit a job run with the given parameters.

        Returns the Databricks job ``run_id`` (not the app-level run_id).
        """
        if not self._job_id:
            raise RuntimeError("DQX_JOB_ID is not configured — cannot submit job runs")

        run = self._ws.jobs.run_now(
            job_id=self._job_id,
            job_parameters={
                "task_type": task_type,
                "view_fqn": view_fqn,
                "result_catalog": self._catalog,
                "result_schema": self._schema,
                "config_json": json.dumps(config),
                "run_id": run_id,
                "requesting_user": requesting_user,
            },
        )
        logger.info(
            "Submitted job run %s (job_id=%s, task_type=%s, app_run_id=%s)",
            run.run_id,
            self._job_id,
            task_type,
            run_id,
        )
        return run.run_id  # type: ignore[return-value]

    def cancel_run(self, job_run_id: int) -> None:
        """Cancel a running job."""
        self._ws.jobs.cancel_run(job_run_id)
        logger.info("Cancelled job run %s", job_run_id)

    def get_run_status(self, job_run_id: int) -> RunStatus:
        """Get the current status of a job run."""
        run = self._ws.jobs.get_run(job_run_id)
        state = run.state
        return RunStatus(
            state=state.life_cycle_state.value if state and state.life_cycle_state else "UNKNOWN",
            result_state=state.result_state.value if state and state.result_state else None,
            message=state.state_message if state else None,
        )

    def _record_running_placeholder(
        self,
        table: str,
        run_id: str,
        requesting_user: str,
        source_table_fqn: str,
        view_fqn: str,
        size_column: str,
        size_value: int,
        run_type: str | None = None,
        job_run_id: int | None = None,
    ) -> None:
        """Insert a RUNNING placeholder row. Non-fatal on failure."""
        from databricks.sdk.service.sql import Disposition, Format, StatementState
        from datetime import datetime, timezone
        from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

        now = datetime.now(timezone.utc).isoformat()
        er = escape_sql_string(run_id)
        eu = escape_sql_string(requesting_user)
        ef = escape_sql_string(source_table_fqn)
        ev = escape_sql_string(view_fqn)

        cols = f"run_id, requesting_user, source_table_fqn, view_fqn, {size_column}, status, created_at"
        vals = f"'{er}', '{eu}', '{ef}', '{ev}', {int(size_value)}, 'RUNNING', '{now}'"
        if run_type:
            ert = escape_sql_string(run_type)
            cols += ", run_type"
            vals += f", '{ert}'"
        if job_run_id is not None:
            cols += ", job_run_id"
            vals += f", {int(job_run_id)}"

        sql = f"INSERT INTO {table} ({cols}) VALUES ({vals})"
        try:
            resp = self._ws.statement_execution.execute_statement(
                warehouse_id=self._warehouse_id,
                statement=sql,
                catalog=self._catalog,
                schema=self._schema,
                disposition=Disposition.INLINE,
                format=Format.JSON_ARRAY,
            )
            if resp.status and resp.status.state == StatementState.FAILED:
                msg = resp.status.error.message if resp.status.error else "Unknown error"
                logger.warning("Failed to record run started for %s: %s", run_id, msg)
        except Exception as exc:
            logger.warning("Failed to record run started for %s: %s", run_id, exc)

    def record_run_started(
        self,
        table: str,
        run_id: str,
        requesting_user: str,
        source_table_fqn: str,
        view_fqn: str,
        sample_limit: int,
        job_run_id: int | None = None,
    ) -> None:
        """Insert a RUNNING placeholder for a profiler run."""
        self._record_running_placeholder(
            table,
            run_id,
            requesting_user,
            source_table_fqn,
            view_fqn,
            "sample_limit",
            sample_limit,
            job_run_id=job_run_id,
        )

    def record_dryrun_started(
        self,
        table: str,
        run_id: str,
        requesting_user: str,
        source_table_fqn: str,
        view_fqn: str,
        sample_size: int,
        run_type: str = "dryrun",
        job_run_id: int | None = None,
    ) -> None:
        """Insert a RUNNING placeholder for a dry-run or scheduled run."""
        self._record_running_placeholder(
            table,
            run_id,
            requesting_user,
            source_table_fqn,
            view_fqn,
            "sample_size",
            sample_size,
            run_type=run_type,
            job_run_id=job_run_id,
        )

    def _list_deduplicated_rows(
        self,
        table: str,
        select_cols: str,
        limit: int = 100,
    ) -> list[dict[str, str | None]]:
        """Read the most recent result rows from a Delta table, newest first.

        Deduplicates by run_id -- if both a RUNNING placeholder and a terminal
        row exist for the same run_id, only the terminal row is returned.
        """
        from databricks.sdk.service.sql import Disposition, Format, StatementState

        sql = (
            f"SELECT {select_cols} "  # noqa: S608
            f"FROM ("
            f"  SELECT *, ROW_NUMBER() OVER ("
            f"    PARTITION BY run_id "
            f"    ORDER BY CASE WHEN status = 'RUNNING' THEN 1 ELSE 0 END ASC, created_at DESC"
            f"  ) AS rn "
            f"  FROM {table}"
            f") WHERE rn = 1 "
            f"ORDER BY created_at DESC LIMIT {int(limit)}"
        )
        resp = self._ws.statement_execution.execute_statement(
            warehouse_id=self._warehouse_id,
            statement=sql,
            catalog=self._catalog,
            schema=self._schema,
            disposition=Disposition.INLINE,
            format=Format.JSON_ARRAY,
        )

        if resp.status and resp.status.state == StatementState.FAILED:
            msg = resp.status.error.message if resp.status.error else "Unknown error"
            raise RuntimeError(f"List query failed: {msg}")

        if not resp.result or not resp.result.data_array:
            return []

        columns = [
            col.name or ""
            for col in ((resp.manifest.schema.columns if resp.manifest and resp.manifest.schema else None) or [])
        ]
        return [dict(zip(columns, row)) for row in resp.result.data_array]

    _PROFILE_COLS = (
        "run_id, requesting_user, source_table_fqn, view_fqn, sample_limit, "
        "rows_profiled, columns_profiled, duration_seconds, summary_json, "
        "generated_rules_json, status, error_message, canceled_by, updated_at, created_at"
    )

    _DRYRUN_COLS = (
        "run_id, requesting_user, source_table_fqn, sample_size, "
        "total_rows, valid_rows, invalid_rows, "
        "status, error_message, canceled_by, updated_at, created_at, "
        "COALESCE(run_type, 'dryrun') AS run_type, "
        "checks_json"
    )

    def list_run_rows(self, table: str, limit: int = 100) -> list[dict[str, str | None]]:
        """Read the most recent profiler result rows."""
        return self._list_deduplicated_rows(table, self._PROFILE_COLS, limit)

    def list_dryrun_rows(self, table: str, limit: int = 100) -> list[dict[str, str | None]]:
        """Read the most recent dry-run result rows."""
        return self._list_deduplicated_rows(table, self._DRYRUN_COLS, limit)

    def get_run_result_row(self, table: str, run_id: str) -> dict[str, str | None] | None:
        """Read a result row from a Delta table by run_id.

        Uses the SP WorkspaceClient and SQL Statement Execution API.
        Returns a dict keyed by column name, or None if no row found.
        """
        from databricks.sdk.service.sql import Disposition, Format, StatementState

        from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

        er = escape_sql_string(run_id)
        sql = f"SELECT * FROM {table} WHERE run_id = '{er}' AND status != 'RUNNING' LIMIT 1"  # noqa: S608
        resp = self._ws.statement_execution.execute_statement(
            warehouse_id=self._warehouse_id,
            statement=sql,
            catalog=self._catalog,
            schema=self._schema,
            disposition=Disposition.INLINE,
            format=Format.JSON_ARRAY,
        )

        if resp.status and resp.status.state == StatementState.FAILED:
            msg = resp.status.error.message if resp.status.error else "Unknown error"
            raise RuntimeError(f"Results query failed: {msg}")

        if not resp.result or not resp.result.data_array or not resp.result.data_array:
            return None

        # Build dict from manifest column names + first row values
        columns = [
            col.name or ""
            for col in ((resp.manifest.schema.columns if resp.manifest and resp.manifest.schema else None) or [])
        ]
        row = resp.result.data_array[0]
        return dict(zip(columns, row))
