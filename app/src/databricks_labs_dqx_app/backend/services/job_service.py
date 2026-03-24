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

    def get_run_status(self, job_run_id: int) -> RunStatus:
        """Get the current status of a job run."""
        run = self._ws.jobs.get_run(job_run_id)
        state = run.state
        return RunStatus(
            state=state.life_cycle_state.value if state and state.life_cycle_state else "UNKNOWN",
            result_state=state.result_state.value if state and state.result_state else None,
            message=state.state_message if state else None,
        )

    def list_run_rows(self, table: str, limit: int = 100) -> list[dict[str, str | None]]:
        """Read the most recent result rows from a Delta table, newest first.

        Returns a list of dicts keyed by column name.
        """
        from databricks.sdk.service.sql import Disposition, Format, StatementState

        sql = f"SELECT * FROM {table} ORDER BY created_at DESC LIMIT {limit}"  # noqa: S608
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

    def get_run_result_row(self, table: str, run_id: str) -> dict[str, str | None] | None:
        """Read a result row from a Delta table by run_id.

        Uses the SP WorkspaceClient and SQL Statement Execution API.
        Returns a dict keyed by column name, or None if no row found.
        """
        from databricks.sdk.service.sql import Disposition, Format, StatementState

        sql = f"SELECT * FROM {table} WHERE run_id = '{run_id}' LIMIT 1"  # noqa: S608
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
