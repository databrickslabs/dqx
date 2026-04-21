"""Background scheduler that triggers approved rule runs on a configured cadence.

The scheduler runs as an asyncio background task inside the FastAPI process.
It polls ``dq_schedule_configs`` for schedule entries with a non-manual
frequency, and fires ``JobService.submit_run(task_type="scheduled")`` when
a schedule is due.

Persistence of *last run / next run* timestamps lives in
``dq_schedule_runs`` so the scheduler survives app restarts without
re-triggering runs that already completed.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Disposition, Format, StatementState

from databricks_labs_dqx_app.backend.logger import get_logger

_TERMINAL_STATES = {StatementState.SUCCEEDED, StatementState.FAILED, StatementState.CANCELED, StatementState.CLOSED}

logger = get_logger("scheduler")

_SQL_CHECK_PREFIX = "__sql_check__/"


class SchedulerService:
    """Manages a background loop that checks schedule configs and triggers runs."""

    def __init__(
        self,
        ws: WorkspaceClient,
        warehouse_id: str,
        catalog: str,
        schema: str,
        tmp_schema: str,
        job_id: str,
    ) -> None:
        self._ws = ws
        self._warehouse_id = warehouse_id
        self._catalog = catalog
        self._schema = schema
        self._tmp_schema = tmp_schema
        self._job_id = job_id
        self._task: asyncio.Task[None] | None = None
        self._reload_event = asyncio.Event()
        self._force_recalc = False
        self._table = f"{catalog}.{schema}.dq_schedule_runs"
        self._configs_table = f"{catalog}.{schema}.dq_schedule_configs"
        self._settings_table = f"{catalog}.{schema}.dq_app_settings"
        self._rules_table = f"{catalog}.{schema}.dq_quality_rules"

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Spawn the background scheduler loop."""
        if self._task is not None:
            return
        self._task = asyncio.create_task(self._loop())
        logger.info(
            "Scheduler started (warehouse=%s, catalog=%s, schema=%s)", self._warehouse_id, self._catalog, self._schema
        )

    async def stop(self) -> None:
        """Cancel the background scheduler loop."""
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None
        logger.info("Scheduler stopped")

    def reload(self) -> None:
        """Signal the loop to recalculate schedules on its next iteration."""
        self._force_recalc = True
        self._reload_event.set()

    # ------------------------------------------------------------------
    # Background loop
    # ------------------------------------------------------------------

    async def _loop(self) -> None:
        """Check schedules every 60 seconds and trigger due runs."""
        while True:
            try:
                recalc = self._force_recalc
                self._force_recalc = False
                await self._tick(recalc=recalc)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Scheduler tick failed")

            try:
                await asyncio.wait_for(self._reload_event.wait(), timeout=60)
                self._reload_event.clear()
            except asyncio.TimeoutError:
                pass

    async def _tick(self, *, recalc: bool = False) -> None:
        """Single scheduler iteration: load configs, check each, trigger if due.

        When *recalc* is True (after a config save), ``next_run_at`` is
        recomputed from the current config so schedule time changes take
        effect immediately rather than waiting for the old ``next_run_at``
        to expire.
        """
        configs = await asyncio.to_thread(self._load_schedule_configs)
        if not configs:
            logger.info("Scheduler tick: no schedule configs found")
            return

        logger.info("Scheduler tick: found %d config(s), recalc=%s", len(configs), recalc)

        now = datetime.now(timezone.utc)

        for name, cfg in configs.items():
            freq = cfg.get("frequency", "manual")
            if freq == "manual":
                continue

            try:
                tracker = await asyncio.to_thread(self._get_tracker, name)
                next_run = tracker.get("next_run_at") if tracker else None

                if next_run is None or recalc:
                    computed = self._compute_next_run(cfg, now - timedelta(seconds=1))
                    if next_run is not None:
                        old_dt = self._parse_ts(next_run)
                        if old_dt is not None and old_dt != computed:
                            logger.info(
                                "Schedule '%s' next_run_at recalculated: %s → %s",
                                name,
                                next_run,
                                computed.isoformat(),
                            )
                    last_run = tracker.get("last_run_at") if tracker else None
                    last_id = tracker.get("last_run_id") if tracker else None
                    last_dt = self._parse_ts(last_run) if last_run else None
                    await asyncio.to_thread(
                        self._upsert_tracker,
                        name,
                        last_dt,
                        computed,
                        last_id,
                        "pending",
                    )
                    if computed <= now:
                        next_run = computed.isoformat()
                    else:
                        continue

                next_run_dt = self._parse_ts(next_run) if isinstance(next_run, str) else next_run
                if next_run_dt is None or next_run_dt > now:
                    continue

                run_id = uuid4().hex[:16]
                logger.info("Schedule '%s' is due (next_run_at=%s), triggering run %s", name, next_run, run_id)

                errors = await asyncio.to_thread(self._trigger_run, name, cfg, run_id)

                new_next = self._compute_next_run(cfg, now)
                status = "success" if not errors else "partial_failure"
                if errors:
                    logger.warning("Schedule '%s' run %s had errors: %s", name, run_id, errors)

                await asyncio.to_thread(self._upsert_tracker, name, now, new_next, run_id, status)
            except Exception:
                logger.exception("Scheduler failed processing schedule '%s'", name)

    # ------------------------------------------------------------------
    # Config loading
    # ------------------------------------------------------------------

    def _load_schedule_configs(self) -> dict[str, dict[str, Any]]:
        """Read schedule configs from dq_schedule_configs table.

        Falls back to the legacy dq_app_settings blob if the new table
        has no rows (backward compatibility during migration).
        """
        result: dict[str, dict[str, Any]] = {}

        try:
            sql = f"SELECT schedule_name, config_json FROM {self._configs_table}"
            rows = self._query(sql)
            for row in rows:
                name = row[0] or ""
                if not name:
                    continue
                try:
                    cfg = json.loads(row[1]) if row[1] else {}
                    if isinstance(cfg, dict) and "frequency" in cfg:
                        result[name] = cfg
                except (json.JSONDecodeError, TypeError):
                    logger.warning("Invalid JSON in dq_schedule_configs for '%s'", name)
        except Exception:
            logger.debug("dq_schedule_configs table not available, trying legacy path", exc_info=True)

        if result:
            return result

        # Legacy fallback: read from dq_app_settings blob
        try:
            sql = f"SELECT setting_value FROM {self._settings_table} WHERE setting_key = 'workspace_config'"
            rows = self._query(sql)
            if not rows:
                return {}
            data = json.loads(rows[0][0])
            run_configs = data.get("run_configs") or []
            for rc in run_configs:
                name = rc.get("name", "")
                loc = rc.get("checks_location", "")
                if not name or not loc:
                    continue
                try:
                    cfg = json.loads(loc)
                    if isinstance(cfg, dict) and "frequency" in cfg:
                        result[name] = cfg
                except (json.JSONDecodeError, TypeError):
                    pass
        except Exception:
            logger.debug("Legacy config load also failed", exc_info=True)

        return result

    # ------------------------------------------------------------------
    # Tracker (dq_schedule_runs)
    # ------------------------------------------------------------------

    def _get_tracker(self, name: str) -> dict[str, str] | None:
        from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

        escaped = escape_sql_string(name)
        sql = (
            f"SELECT schedule_name, CAST(last_run_at AS STRING), CAST(next_run_at AS STRING), "
            f"last_run_id, status "
            f"FROM {self._table} WHERE schedule_name = '{escaped}'"
        )
        rows = self._query(sql)
        if not rows:
            return None
        row = rows[0]
        return {
            "schedule_name": row[0],
            "last_run_at": row[1],
            "next_run_at": row[2],
            "last_run_id": row[3],
            "status": row[4],
        }

    def _upsert_tracker(
        self,
        name: str,
        last_run_at: datetime | None,
        next_run_at: datetime | None,
        last_run_id: str | None,
        status: str,
    ) -> None:
        from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

        escaped_name = escape_sql_string(name)
        escaped_status = escape_sql_string(status)
        last_str = f"'{last_run_at.isoformat()}'" if last_run_at else "NULL"
        next_str = f"'{next_run_at.isoformat()}'" if next_run_at else "NULL"
        run_id_str = f"'{escape_sql_string(last_run_id)}'" if last_run_id else "NULL"

        sql = (
            f"MERGE INTO {self._table} AS target "
            f"USING (SELECT '{escaped_name}' AS schedule_name) AS source "
            "ON target.schedule_name = source.schedule_name "
            "WHEN MATCHED THEN UPDATE SET "
            f"  last_run_at = {last_str}, next_run_at = {next_str}, "
            f"  last_run_id = {run_id_str}, status = '{escaped_status}' "
            "WHEN NOT MATCHED THEN INSERT (schedule_name, last_run_at, next_run_at, last_run_id, status) "
            f"VALUES ('{escaped_name}', {last_str}, {next_str}, {run_id_str}, '{escaped_status}')"
        )
        self._execute(sql)

    # ------------------------------------------------------------------
    # Trigger run
    # ------------------------------------------------------------------

    def _trigger_run(self, schedule_name: str, cfg: dict[str, Any], run_id_prefix: str) -> list[str]:
        """Resolve scope and submit jobs for a schedule. Returns error list.

        Scheduled runs pass the source table FQN directly to the task runner
        instead of creating temporary views.  This avoids Unity Catalog metadata
        propagation delays that caused TABLE_OR_VIEW_NOT_FOUND errors when the
        SQL-warehouse-created view wasn't yet visible to the serverless job compute.
        For SQL checks the embedded query is passed in config_json and the runner
        creates a Spark-local temp view.
        """
        table_fqns = self._resolve_scope(cfg)
        if not table_fqns:
            logger.info("Schedule '%s': no approved rules matched scope", schedule_name)
            return [f"No approved rules matched scope for schedule '{schedule_name}'"]

        sample_size = cfg.get("sample_size") or 0
        errors: list[str] = []

        for i, table_fqn in enumerate(table_fqns):
            try:
                entry = self._get_approved_rule(table_fqn)
                if entry is None:
                    continue

                run_id = f"{run_id_prefix}_{i}"
                is_sql_check = table_fqn.startswith(_SQL_CHECK_PREFIX)

                sql_query: str | None = None
                if is_sql_check:
                    sql_query = self._extract_sql_query(entry["checks"])
                    if not sql_query:
                        errors.append(f"{table_fqn}: SQL check has no query")
                        continue

                config = {
                    "checks": entry["checks"],
                    "sample_size": sample_size,
                    "source_table_fqn": table_fqn,
                    "is_sql_check": is_sql_check,
                }

                if sql_query is not None:
                    config["sql_query"] = sql_query

                self._ws.jobs.run_now(
                    job_id=int(self._job_id),
                    job_parameters={
                        "task_type": "scheduled",
                        "view_fqn": table_fqn,
                        "result_catalog": self._catalog,
                        "result_schema": self._schema,
                        "config_json": json.dumps(config),
                        "run_id": run_id,
                        "requesting_user": f"scheduler:{schedule_name}",
                    },
                )
                logger.info("Schedule '%s': submitted run for %s (run_id=%s)", schedule_name, table_fqn, run_id)
            except Exception as e:
                logger.error("Schedule '%s': failed for %s: %s", schedule_name, table_fqn, e)
                errors.append(f"{table_fqn}: {e}")

        return errors

    # ------------------------------------------------------------------
    # Scope resolution
    # ------------------------------------------------------------------

    def _resolve_scope(self, cfg: dict[str, Any]) -> list[str]:
        """Return list of unique table_fqn matching the schedule's scope from approved rules."""
        mode = cfg.get("scope_mode", "all")
        sql = f"SELECT DISTINCT table_fqn FROM {self._rules_table} WHERE status = 'approved'"
        rows = self._query(sql)
        fqns = [r[0] for r in rows if r[0]]

        if mode == "all":
            return fqns

        if mode == "catalog":
            catalogs = set(cfg.get("scope_catalogs") or [])
            return [f for f in fqns if self._fqn_part(f, 0) in catalogs]

        if mode == "schema":
            schemas = set(cfg.get("scope_schemas") or [])
            return [f for f in fqns if self._fqn_schema(f) in schemas]

        if mode == "tables":
            tables = set(cfg.get("scope_tables") or [])
            return [f for f in fqns if f in tables]

        return fqns

    @staticmethod
    def _fqn_part(fqn: str, idx: int) -> str:
        parts = fqn.split(".")
        return parts[idx] if idx < len(parts) else ""

    @staticmethod
    def _fqn_schema(fqn: str) -> str:
        parts = fqn.split(".")
        return f"{parts[0]}.{parts[1]}" if len(parts) >= 2 else ""

    # ------------------------------------------------------------------
    # Rule helpers
    # ------------------------------------------------------------------

    def _get_approved_rule(self, table_fqn: str) -> dict[str, Any] | None:
        """Get merged checks from all approved rule rows for a table."""
        from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

        escaped = escape_sql_string(table_fqn)
        sql = (
            f"SELECT table_fqn, checks FROM {self._rules_table} "
            f"WHERE table_fqn = '{escaped}' AND status = 'approved'"
        )
        rows = self._query(sql)
        if not rows:
            return None
        merged_checks: list[dict[str, Any]] = []
        for row in rows:
            try:
                parsed = json.loads(row[1], strict=False) if row[1] else []
                if isinstance(parsed, list):
                    merged_checks.extend(parsed)
            except json.JSONDecodeError:
                continue
        if not merged_checks:
            return None
        return {"table_fqn": rows[0][0], "checks": merged_checks}

    @staticmethod
    def _extract_sql_query(checks: list[dict[str, Any]]) -> str | None:
        for check in checks:
            fn = (check.get("check") or {}).get("function", "")
            if fn == "sql_query":
                return (check.get("check") or {}).get("arguments", {}).get("query")
        return None

    # ------------------------------------------------------------------
    # View creation (SP credentials)
    # ------------------------------------------------------------------

    def _create_view(self, source_table_fqn: str) -> str:
        from databricks_labs_dqx_app.backend.sql_utils import quote_fqn

        view_id = uuid4().hex[:12]
        view_name = f"{self._catalog}.{self._tmp_schema}.tmp_view_{view_id}"
        quoted_view = quote_fqn(view_name)
        quoted_source = quote_fqn(source_table_fqn)
        self._ensure_tmp_schema()
        sql = f"CREATE OR REPLACE VIEW {quoted_view} AS SELECT * FROM {quoted_source}"
        self._execute(sql)
        self._grant_view(view_name)
        if not self._view_exists(view_name):
            raise RuntimeError(f"Scheduler: view creation succeeded but view not found: {view_name}")
        return view_name

    def _create_view_from_sql(self, sql_query: str) -> str:
        from databricks.labs.dqx.utils import is_sql_query_safe
        from databricks.labs.dqx.errors import UnsafeSqlQueryError
        from databricks_labs_dqx_app.backend.sql_utils import quote_fqn

        if not is_sql_query_safe(sql_query):
            raise UnsafeSqlQueryError(
                "The SQL query contains prohibited statements and cannot be used to create a view."
            )

        view_id = uuid4().hex[:12]
        view_name = f"{self._catalog}.{self._tmp_schema}.tmp_view_{view_id}"
        quoted_view = quote_fqn(view_name)
        self._ensure_tmp_schema()
        sql = f"CREATE OR REPLACE VIEW {quoted_view} AS {sql_query}"
        self._execute(sql)
        self._grant_view(view_name)
        if not self._view_exists(view_name):
            raise RuntimeError(f"Scheduler: view creation succeeded but view not found: {view_name}")
        return view_name

    def _grant_view(self, view_name: str) -> None:
        from databricks_labs_dqx_app.backend.sql_utils import quote_fqn

        try:
            self._execute(f"GRANT SELECT ON VIEW {quote_fqn(view_name)} TO `account users`")
        except Exception as e:
            logger.warning("Failed to grant SELECT on %s: %s", view_name, e)

    _tmp_schema_ensured = False

    def _ensure_tmp_schema(self) -> None:
        if self._tmp_schema_ensured:
            return
        cat = self._catalog.replace("`", "")
        schema = self._tmp_schema.replace("`", "")
        self._execute(f"CREATE SCHEMA IF NOT EXISTS `{cat}`.`{schema}`")
        self._tmp_schema_ensured = True

    # ------------------------------------------------------------------
    # Polling / verification helpers
    # ------------------------------------------------------------------

    def _wait_for_completion(self, statement_id: str, timeout_seconds: int) -> StatementState:
        """Poll statement status until it reaches a terminal state."""
        import time

        start_time = time.time()
        poll_interval = 2.0

        while time.time() - start_time < timeout_seconds:
            status = self._ws.statement_execution.get_statement(statement_id)
            state = status.status.state if status.status else None
            if state in _TERMINAL_STATES:
                logger.info("Statement %s completed with state: %s", statement_id, state)
                return state
            logger.debug("Statement %s still in state %s, waiting...", statement_id, state)
            time.sleep(poll_interval)

        raise RuntimeError(f"Scheduler SQL statement {statement_id} timed out after {timeout_seconds}s")

    def _view_exists(self, view_fqn: str) -> bool:
        """Check if a view exists in Unity Catalog."""
        from databricks_labs_dqx_app.backend.sql_utils import quote_fqn

        try:
            self._execute(f"DESCRIBE TABLE {quote_fqn(view_fqn)}")
            return True
        except Exception as e:
            logger.warning("View existence check failed for %s: %s", view_fqn, e)
            return False

    # ------------------------------------------------------------------
    # Schedule computation
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_next_run(cfg: dict[str, Any], after: datetime) -> datetime:
        """Compute the next run time after *after* based on frequency settings."""
        freq = cfg.get("frequency", "manual")
        hour = int(cfg.get("hour") or 0)
        minute = int(cfg.get("minute") or 0)

        if freq == "hourly":
            candidate = after.replace(minute=minute, second=0, microsecond=0)
            if candidate <= after:
                candidate += timedelta(hours=1)
            return candidate

        if freq == "daily":
            candidate = after.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if candidate <= after:
                candidate += timedelta(days=1)
            return candidate

        if freq == "weekly":
            dow = int(cfg.get("day_of_week") or 0)
            candidate = after.replace(hour=hour, minute=minute, second=0, microsecond=0)
            days_ahead = (dow - candidate.weekday()) % 7
            if days_ahead == 0 and candidate <= after:
                days_ahead = 7
            candidate += timedelta(days=days_ahead)
            return candidate

        if freq == "monthly":
            dom = int(cfg.get("day_of_month") or 1)
            candidate = after.replace(day=min(dom, 28), hour=hour, minute=minute, second=0, microsecond=0)
            if candidate <= after:
                if after.month == 12:
                    candidate = candidate.replace(year=after.year + 1, month=1)
                else:
                    candidate = candidate.replace(month=after.month + 1)
            return candidate

        return after + timedelta(hours=1)

    @staticmethod
    def _parse_ts(value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except (ValueError, TypeError):
            return None

    # ------------------------------------------------------------------
    # SQL helpers
    # ------------------------------------------------------------------

    def _execute(self, sql: str, timeout_seconds: int = 120) -> None:
        resp = self._ws.statement_execution.execute_statement(
            warehouse_id=self._warehouse_id,
            statement=sql,
            catalog=self._catalog,
            disposition=Disposition.INLINE,
            format=Format.JSON_ARRAY,
            wait_timeout="30s",
        )
        if not resp.status:
            raise RuntimeError(f"Scheduler SQL returned no status\nSQL: {sql}")

        state = resp.status.state
        statement_id = resp.statement_id

        if state not in _TERMINAL_STATES and statement_id:
            logger.info("Scheduler statement %s in state %s, polling...", statement_id, state)
            state = self._wait_for_completion(statement_id, timeout_seconds)

        if state == StatementState.SUCCEEDED:
            return
        if state == StatementState.FAILED:
            msg = resp.status.error.message if resp.status.error else "Unknown error"
            raise RuntimeError(f"Scheduler SQL failed: {msg}\nSQL: {sql}")
        raise RuntimeError(f"Scheduler SQL ended in unexpected state {state}\nSQL: {sql}")

    def _query(self, sql: str, timeout_seconds: int = 120) -> list[list[str]]:
        resp = self._ws.statement_execution.execute_statement(
            warehouse_id=self._warehouse_id,
            statement=sql,
            catalog=self._catalog,
            disposition=Disposition.INLINE,
            format=Format.JSON_ARRAY,
            wait_timeout="30s",
        )
        if not resp.status:
            raise RuntimeError(f"Scheduler SQL query returned no status\nSQL: {sql}")

        state = resp.status.state
        statement_id = resp.statement_id

        if state not in _TERMINAL_STATES and statement_id:
            logger.info("Scheduler query %s in state %s, polling...", statement_id, state)
            state = self._wait_for_completion(statement_id, timeout_seconds)
            if state == StatementState.SUCCEEDED and statement_id:
                resp = self._ws.statement_execution.get_statement(statement_id)

        if state == StatementState.FAILED:
            msg = resp.status.error.message if resp.status and resp.status.error else "Unknown error"
            raise RuntimeError(f"Scheduler SQL query failed: {msg}\nSQL: {sql}")
        if state != StatementState.SUCCEEDED:
            raise RuntimeError(f"Scheduler SQL query ended in unexpected state {state}\nSQL: {sql}")

        if resp.result and resp.result.data_array:
            return resp.result.data_array
        return []
