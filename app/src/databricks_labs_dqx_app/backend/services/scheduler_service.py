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
import re
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

from databricks.sdk import WorkspaceClient

from databricks_labs_dqx_app.backend.logger import get_logger
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor

logger = get_logger("scheduler")

_SQL_CHECK_PREFIX = "__sql_check__/"

_VALID_TRACKER_STATUSES = {"pending", "success", "partial_failure", "failed"}

# Length of the hex suffix on ``tmp_view_*`` names. ``uuid4().hex`` is
# always 32 lowercase hex chars; we slice to keep schema-qualified
# names short. Centralised so the GC regex below, the creation paths
# (:meth:`SchedulerService._create_view` /
# :meth:`SchedulerService._create_view_from_sql`), and the unit test
# that ties them together all read from the same constant.
_TMP_VIEW_ID_LEN = 12

# Strict gate for the orphan-view GC: anything we drop must match this
# pattern AND live in the configured tmp schema. Belt-and-suspenders.
#
# IMPORTANT — keep this in sync with the generator. View names are
# produced by :meth:`SchedulerService._generate_tmp_view_id` (which
# returns ``uuid4().hex[:_TMP_VIEW_ID_LEN]``). If you change the
# suffix length or charset there, this regex will silently start
# excluding new views from GC. The range ``{8,32}`` intentionally
# tolerates a small drift around ``_TMP_VIEW_ID_LEN`` (12) so an
# accidental length change doesn't immediately break cleanup, but the
# round-trip is enforced by ``test_regex_matches_generator_output``
# in ``tests/test_scheduler_service.py``.
_TMP_VIEW_NAME_RE = re.compile(r"^tmp_view_[a-f0-9]{8,32}$")

# Weekly orphan-view GC cadence. Saturday is ``datetime.weekday() == 5``.
_GC_WEEKDAY_SAT = 5
_GC_HOUR_UTC = 1
# Minimum age before a tmp view is eligible for cleanup. Bumped from
# 24h → 48h after review feedback: long-running validation jobs (large
# tables, busy warehouses, retried runs) can keep a view "in use" well
# past a single day, and the per-run ``finally`` cleanup already
# handles the common case. 48h gives a generous safety margin so the
# weekly GC almost never fights a still-active workload, at the cost
# of orphans living one extra day before being reaped.
_GC_AGE_HOURS = 48
_GC_MAX_DROPS_PER_RUN = 500


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
        self._job_id = job_id
        self._catalog = catalog
        self._schema = schema
        self._tmp_schema = tmp_schema
        self._sql = SqlExecutor(ws=ws, warehouse_id=warehouse_id, catalog=catalog, schema=schema)
        self._tmp_sql = SqlExecutor(ws=ws, warehouse_id=warehouse_id, catalog=catalog, schema=tmp_schema)
        self._task: asyncio.Task[None] | None = None
        self._reload_event = asyncio.Event()
        self._force_recalc = False
        self._table = f"{catalog}.{schema}.dq_schedule_runs"
        self._configs_table = f"{catalog}.{schema}.dq_schedule_configs"
        self._settings_table = f"{catalog}.{schema}.dq_app_settings"
        self._rules_table = f"{catalog}.{schema}.dq_quality_rules"

        # Orphan-tmp-view GC: fires every Saturday at 01:00 UTC. Held in
        # process memory rather than persisted — a missed Saturday (e.g.
        # the app was redeploying at exactly 01:00 UTC) is fine because
        # the per-run ``finally`` cleanup already handles 99% of cases
        # and orphans only accumulate slowly.
        self._next_view_gc_at: datetime = self._next_saturday_01_utc(datetime.now(timezone.utc))

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Spawn the background scheduler loop."""
        if self._task is not None:
            return
        self._task = asyncio.create_task(self._loop())
        logger.info("Scheduler started (catalog=%s, schema=%s)", self._catalog, self._schema)

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
                await self._maybe_gc_orphan_views(datetime.now(timezone.utc))
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
            rows = self._sql.query(sql)
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
            rows = self._sql.query(sql)
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
        from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string, validate_schedule_name

        validate_schedule_name(name)
        escaped = escape_sql_string(name)
        sql = (
            f"SELECT schedule_name, CAST(last_run_at AS STRING), CAST(next_run_at AS STRING), "
            f"last_run_id, status "
            f"FROM {self._table} WHERE schedule_name = '{escaped}'"
        )
        rows = self._sql.query(sql)
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
        from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string, validate_schedule_name

        validate_schedule_name(name)
        if status not in _VALID_TRACKER_STATUSES:
            raise ValueError(f"Invalid tracker status: '{status}'. Must be one of {_VALID_TRACKER_STATUSES}")
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
        self._sql.execute(sql)

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

        # Fetch custom metrics once per schedule tick — they apply
        # globally to every dq_metrics row produced by this trigger.
        custom_metrics = self._load_custom_metrics()

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

                if custom_metrics:
                    config["custom_metrics"] = custom_metrics

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
        rows = self._sql.query(sql)
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

        e_fqn = escape_sql_string(table_fqn)
        sql = (
            f"SELECT table_fqn, checks FROM {self._rules_table} " f"WHERE table_fqn = '{e_fqn}' AND status = 'approved'"
        )
        rows = self._sql.query(sql)
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

    def _load_custom_metrics(self) -> list[str]:
        """Fetch admin-configured custom metric SQL expressions.

        We deliberately query the settings table directly rather than
        instantiating ``AppSettingsService`` — the scheduler runs on the
        app's SP credentials and we want to keep this hot path tight.
        Failures are swallowed so a missing/blank setting never blocks a
        scheduled run.
        """
        try:
            from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

            key = escape_sql_string("custom_metrics_v1")
            sql = f"SELECT setting_value FROM {self._settings_table} WHERE setting_key = '{key}'"  # noqa: S608
            rows = self._sql.query(sql)
            if not rows or rows[0][0] is None:
                return []
            parsed = json.loads(rows[0][0])
            if not isinstance(parsed, list):
                return []
            return [s for s in parsed if isinstance(s, str) and s.strip()]
        except Exception:
            logger.exception("Failed to load custom_metrics_v1; continuing with empty list")
            return []

    @staticmethod
    def _extract_sql_query(checks: list[dict[str, Any]]) -> str | None:
        for check in checks:
            fn = (check.get("check") or {}).get("function", "")
            if fn == "sql_query":
                return (check.get("check") or {}).get("arguments", {}).get("query")
        return None

    # ------------------------------------------------------------------
    # Orphan tmp-view GC (weekly, Saturday 01:00 UTC)
    # ------------------------------------------------------------------

    @staticmethod
    def _next_saturday_01_utc(now: datetime) -> datetime:
        """Return the next datetime > now landing on Saturday at 01:00:00 UTC."""
        target = now.replace(hour=_GC_HOUR_UTC, minute=0, second=0, microsecond=0)
        days_ahead = (_GC_WEEKDAY_SAT - target.weekday()) % 7
        target = target + timedelta(days=days_ahead)
        if target <= now:
            target = target + timedelta(days=7)
        return target

    async def _maybe_gc_orphan_views(self, now: datetime) -> None:
        """Run the weekly GC if we've crossed the next-fire boundary.

        The check is cheap (one comparison per scheduler tick) and the
        actual work runs in a thread so it never blocks the loop.
        Failures are logged but not fatal — the next Saturday will try
        again.
        """
        if now < self._next_view_gc_at:
            return

        scheduled_for = self._next_view_gc_at
        # Advance the timer first so a slow GC pass can't double-fire if
        # the loop completes before it returns.
        self._next_view_gc_at = self._next_saturday_01_utc(now)
        logger.info(
            "View GC: triggering weekly cleanup (was due at %s); next run scheduled for %s",
            scheduled_for.isoformat(),
            self._next_view_gc_at.isoformat(),
        )
        try:
            await asyncio.to_thread(self._gc_orphan_views)
        except Exception:
            logger.exception("View GC failed (non-fatal)")

    def _gc_orphan_views(self) -> None:
        """Drop ``tmp_view_*`` views in the tmp schema older than ``_GC_AGE_HOURS``.

        The age threshold (currently 48h) is intentionally generous —
        long-running validation jobs on big tables can keep a view
        "in use" for many hours and the per-run ``finally`` cleanup
        already handles the common case, so we'd rather under-clean
        than race a still-active workload.

        Cross-checks against ``status = 'RUNNING'`` rows in
        ``dq_validation_runs`` and ``dq_profiling_results`` so an
        in-flight (but slow) run is never killed. All operations are
        idempotent; failures on individual views are logged and
        skipped.
        """
        from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string, quote_fqn

        list_sql = (
            f"SELECT table_name "
            f"FROM `{self._catalog}`.information_schema.views "
            f"WHERE table_schema = '{escape_sql_string(self._tmp_schema)}' "
            f"  AND table_name LIKE 'tmp\\_view\\_%' ESCAPE '\\\\' "
            f"  AND created_at < current_timestamp() - INTERVAL {_GC_AGE_HOURS} HOUR "
            f"ORDER BY created_at ASC "
            f"LIMIT {_GC_MAX_DROPS_PER_RUN}"
        )
        try:
            rows = self._tmp_sql.query(list_sql)
        except Exception as exc:
            logger.warning("View GC: failed to list candidates: %s", exc)
            return

        candidates: list[str] = []
        for row in rows:
            name = row[0] if row else None
            if isinstance(name, str) and _TMP_VIEW_NAME_RE.match(name):
                candidates.append(name)

        if not candidates:
            logger.info(
                "View GC: no orphan tmp views older than %dh in %s.%s",
                _GC_AGE_HOURS,
                self._catalog,
                self._tmp_schema,
            )
            return

        in_use_sql = (
            f"SELECT view_fqn FROM `{self._catalog}`.`{self._schema}`.dq_validation_runs WHERE status = 'RUNNING' "
            f"UNION ALL "
            f"SELECT view_fqn FROM `{self._catalog}`.`{self._schema}`.dq_profiling_results WHERE status = 'RUNNING'"
        )
        in_use: set[str] = set()
        try:
            for row in self._sql.query(in_use_sql):
                fqn = row[0] if row else None
                if isinstance(fqn, str) and fqn:
                    in_use.add(fqn.rsplit(".", 1)[-1].strip("`"))
        except Exception as exc:
            logger.warning("View GC: could not read RUNNING rows (%s) - proceeding with age threshold only", exc)

        targets = [n for n in candidates if n not in in_use]
        skipped = len(candidates) - len(targets)

        dropped = 0
        failed = 0
        for name in targets:
            view_fqn = f"{self._catalog}.{self._tmp_schema}.{name}"
            try:
                self._tmp_sql.execute(f"DROP VIEW IF EXISTS {quote_fqn(view_fqn)}")
                dropped += 1
            except Exception as exc:
                logger.warning("View GC: failed to drop %s: %s", view_fqn, exc)
                failed += 1

        logger.info(
            "View GC complete: candidates=%d dropped=%d failed=%d skipped=%d",
            len(candidates),
            dropped,
            failed,
            skipped,
        )

    # ------------------------------------------------------------------
    # View creation (SP credentials)
    # ------------------------------------------------------------------

    @staticmethod
    def _generate_tmp_view_id() -> str:
        """Return a fresh hex suffix for a ``tmp_view_<id>`` name.

        Single source of truth for the suffix shape. Centralised so the
        GC regex (:data:`_TMP_VIEW_NAME_RE`) and the unit test
        ``test_regex_matches_generator_output`` can both reason about
        exactly what creation paths emit. If you change the slice
        length or switch away from ``uuid4().hex``, update
        :data:`_TMP_VIEW_ID_LEN` and the regex above as well.
        """
        return uuid4().hex[:_TMP_VIEW_ID_LEN]

    def _create_view(self, source_table_fqn: str) -> str:
        from databricks_labs_dqx_app.backend.sql_utils import quote_fqn

        view_id = self._generate_tmp_view_id()
        view_name = f"{self._catalog}.{self._tmp_schema}.tmp_view_{view_id}"
        quoted_view = quote_fqn(view_name)
        quoted_source = quote_fqn(source_table_fqn)
        self._ensure_tmp_schema()
        sql = f"CREATE OR REPLACE VIEW {quoted_view} AS SELECT * FROM {quoted_source}"
        self._tmp_sql.execute(sql)
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

        view_id = self._generate_tmp_view_id()
        view_name = f"{self._catalog}.{self._tmp_schema}.tmp_view_{view_id}"
        quoted_view = quote_fqn(view_name)
        self._ensure_tmp_schema()
        sql = f"CREATE OR REPLACE VIEW {quoted_view} AS {sql_query}"
        self._tmp_sql.execute(sql)
        self._grant_view(view_name)
        if not self._view_exists(view_name):
            raise RuntimeError(f"Scheduler: view creation succeeded but view not found: {view_name}")
        return view_name

    def _grant_view(self, view_name: str) -> None:
        from databricks_labs_dqx_app.backend.sql_utils import quote_fqn

        try:
            self._tmp_sql.execute(f"GRANT SELECT ON VIEW {quote_fqn(view_name)} TO `account users`")
        except Exception as e:
            logger.warning("Failed to grant SELECT on %s: %s", view_name, e)

    _tmp_schema_ensured = False

    def _ensure_tmp_schema(self) -> None:
        if self._tmp_schema_ensured:
            return
        cat = self._catalog.replace("`", "")
        schema = self._tmp_schema.replace("`", "")
        self._tmp_sql.execute_no_schema(f"CREATE SCHEMA IF NOT EXISTS `{cat}`.`{schema}`")
        self._tmp_schema_ensured = True

    def _view_exists(self, view_fqn: str) -> bool:
        """Check if a view exists in Unity Catalog."""
        from databricks_labs_dqx_app.backend.sql_utils import quote_fqn

        try:
            self._tmp_sql.execute(f"DESCRIBE TABLE {quote_fqn(view_fqn)}")
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
