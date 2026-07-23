"""Background scheduler that triggers approved rule runs on a configured cadence.

The scheduler runs as an asyncio background task inside the FastAPI process.
It polls ``dq_schedule_configs`` for schedule entries with a non-manual
frequency, and fires ``JobService.submit_run(task_type="scheduled")`` when
a schedule is due.

Persistence of *last run / next run* timestamps lives in
``dq_schedule_runs`` so the scheduler survives app restarts without
re-triggering runs that already completed.

**Data Products product ticks (design spec §4.3, Task 5):** each tick also
polls ``dq_data_products`` for approved products with a non-null
``schedule_cron`` and fires ``DataProductService.run(...)`` for due ones.
This is a SECOND, independent source of due-ness — it runs after the
scope-config loop above completes and never mutates any state the
scope-config path reads, so that path's behaviour is unaffected. See
:meth:`SchedulerService._tick_products` for the full contract.
"""

from __future__ import annotations

import asyncio
import calendar
import json
import re
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from databricks.sdk import WorkspaceClient

from databricks_labs_dqx_app.backend.logger import get_logger
from databricks_labs_dqx_app.backend.services.binding_run_service import (
    BindingRunError,
    BindingRunService,
)
from databricks_labs_dqx_app.backend.services.data_product_service import (
    DataProductService,
    NoRunnableMembersError,
)
from databricks_labs_dqx_app.backend.services.metadata_dim_service import MetadataDimService
from databricks_labs_dqx_app.backend.services.monitored_table_service import MonitoredTableService
from databricks_labs_dqx_app.backend.services.score_cache_service import ScoreCacheService
from databricks_labs_dqx_app.backend.services.tag_reconcile_service import TagReconcileService
from databricks_labs_dqx_app.backend.sql_executor import OltpExecutorProtocol, RawSql, SqlExecutor

logger = get_logger("scheduler")

_SQL_CHECK_PREFIX = "__sql_check__/"

_VALID_TRACKER_STATUSES = {"pending", "success", "partial_failure", "failed"}

# Schedule scope (B2-52): what a due schedule actually runs. Mirrors the
# ``schedule_kind`` column on ``dq_monitored_tables`` / ``dq_data_products``.
# A missing/unknown value falls back to the default (both), matching the
# column defaults and the model parsers, so a legacy NULL row still runs.
_SCHEDULE_KIND_DEFAULT = "dq_only"
_KINDS_WITH_DQ = frozenset({"dq_only", "profiling_and_dq"})
_KINDS_WITH_PROFILING = frozenset({"profiling_only", "profiling_and_dq"})
# Row cap sampled by a scheduler-launched profiling run — matches the profiler
# route's ``ProfileRunIn.sample_limit`` default.
_PROFILE_SAMPLE_LIMIT = 50_000


def _normalize_schedule_kind(value: object) -> str:
    """Return a valid schedule-kind string, defaulting unknown/None to both."""
    if value in _KINDS_WITH_PROFILING or value in _KINDS_WITH_DQ:
        return str(value)
    return _SCHEDULE_KIND_DEFAULT


# Fallback gap used to push ``next_run_at`` into the future when a schedule's
# trigger fails *and* its next occurrence cannot be computed. Prevents a
# deterministic failure from re-firing the schedule on every tick.
_FAILURE_BACKOFF = timedelta(hours=1)

# How long a scheduler-launched run stays tracked for the completion
# score-cache refresh (:meth:`SchedulerService._refresh_scores_for_completed_runs`)
# while waiting for its ``dq_validation_runs`` terminal row. A run whose
# job dies before the runner writes any terminal result would otherwise
# be re-checked on every tick forever; 24h comfortably outlives any real
# validation run.
_SCORE_REFRESH_TTL = timedelta(hours=24)

# Recent-run-set sweep bounds (P5.3). Every tick, one bounded OLTP query
# lists the member run ids of run sets created inside this window so
# runs launched OUTSIDE the scheduler (manual UI runs with the tab
# closed) still get a score-cache refresh when they complete. 1 day
# matches _SCORE_REFRESH_TTL; the row cap keeps a pathological burst of
# run sets from ballooning the statement or the in-memory tracking.
_RUN_SET_SWEEP_WINDOW_DAYS = 1
_RUN_SET_SWEEP_MAX_RUNS = 2000

# Retry budget for the startup score-cache reconcile (P5.3). The
# reconcile is best-effort: a transient warehouse failure right after
# boot retries on the next tick, but a persistently broken warehouse
# must not turn the 60s tick into an indefinite retry storm — after
# this many failed attempts the reconcile is skipped for the rest of
# the boot (run completions and browser refreshes still heal scores).
_SCORE_RECONCILE_MAX_ATTEMPTS = 3

# ------------------------------------------------------------------
# Data Products cron evaluation (design spec §4.3, Task 5)
# ------------------------------------------------------------------
#
# Standard 5-field cron day-of-week names, used only by the ``dow`` field
# of :meth:`SchedulerService._compute_next_cron_run`. ``0`` and ``7`` both
# mean Sunday in POSIX cron; :meth:`_compute_next_cron_run` normalises ``7``
# to ``0`` after parsing.
_CRON_WEEKDAY_NAMES: dict[str, int] = {
    "SUN": 0,
    "MON": 1,
    "TUE": 2,
    "WED": 3,
    "THU": 4,
    "FRI": 5,
    "SAT": 6,
}

# Bound on the number of coarse steps ``_compute_next_cron_run`` will take
# before giving up on an expression with no near-term occurrence (e.g. a
# day-of-month that never exists, such as ``31`` combined with a month
# field that never lands on a 31-day month). Each step advances the
# candidate by at least one unit (month/day/hour/minute), so this bound
# comfortably covers any real cron schedule while still terminating fast
# on unsatisfiable input instead of looping forever.
_CRON_MAX_STEPS = 100_000

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

# Hourly sweep for tmp views whose runs finished (or were abandoned) but
# whose per-run status poll never fired ``drop_view``. This is the main
# safety net — the weekly age-based GC below is belt-and-braces.
_TMP_VIEW_SWEEP_INTERVAL_HOURS = 1
_TMP_VIEW_SWEEP_MAX_RUNS = 50

# Retention sweep — daily DELETE pass against the high-volume tables to
# keep them from growing without bound. Each (table, time-column) pair
# in :data:`_RETENTION_TABLES` is trimmed to ``RETENTION_DAYS`` worth of
# history. Defaults to 90 days; configurable via the ``retention_days``
# setting in ``dq_app_settings``. The sweep runs at most once per
# ``_RETENTION_INTERVAL_HOURS`` so the warehouse isn't billed repeatedly
# for the same DELETE.
_RETENTION_DAYS_DEFAULT = 90
_RETENTION_DAYS_MIN = 7
_RETENTION_INTERVAL_HOURS = 24

# ``dq_quarantine_records`` is the only table that holds full row
# payloads (the source row + ``_errors`` / ``_warnings`` blobs).  Those
# rows are PII-sensitive and tend to drive most of the Studio's storage
# growth, so we expose a *separate* retention knob with a tighter
# default (30 days) instead of subjecting them to the same window as
# trend tables like ``dq_metrics`` (which dashboards expect to look
# back ~3 months on).  Set via the ``quarantine_retention_days`` key
# in ``dq_app_settings``; falls back here when unset.
_QUARANTINE_RETENTION_DAYS_DEFAULT = 30
_QUARANTINE_TABLE_NAME = "dq_quarantine_records"

# The rule + monitored-table metadata dims (``dim_dq_rules`` /
# ``dim_dq_monitored_tables``) are full-refreshed from the Rules Registry
# once per ``_METADATA_DIM_REFRESH_INTERVAL_HOURS`` so the Genie space's
# authoring/ownership data sources stay current between deploys. Hourly
# (vs. retention's daily) because registry edits are user-facing and cheap
# to re-materialize at page scale.
_METADATA_DIM_REFRESH_INTERVAL_HOURS = 1

# Apply-on-tag reconcile sweep (Task 7): a low-frequency pass that re-attaches
# every published tag-mapped rule across all monitored tables, catching tag
# changes on already-monitored tables (the publish/register route hooks handle
# the immediate cases). A 6h cadence is deliberately coarse — the sweep is a
# safety net for out-of-band tag edits, not the primary trigger, and each pass
# reads every monitored table's columns via the SP client. A no-op when no
# ``tag_reconcile_service`` was wired or when tag-auto-apply is off.
_TAG_RECONCILE_INTERVAL_HOURS = 6

# System attribution for scheduler-initiated writes (mirrors the
# ``user_email="scheduler"`` the product/table run ticks already use).
_SCHEDULER_SYSTEM_USER = "scheduler"

# Retention is split per-backend: analytical (Delta) tables are
# trimmed via the SQL warehouse executor, OLTP tables via the OLTP
# executor (Lakebase if enabled, Delta otherwise).  Both lists are
# walked on every retention sweep.  ``dq_quarantine_records`` is in
# this list but resolves its cutoff via :meth:`_resolve_quarantine_retention_days`
# instead of the global :meth:`_resolve_retention_days`.
_DELTA_RETENTION_TABLES: tuple[tuple[str, str], ...] = (
    ("dq_validation_runs", "created_at"),
    ("dq_profiling_results", "created_at"),
    (_QUARANTINE_TABLE_NAME, "created_at"),
    ("dq_metrics", "run_time"),
)
_OLTP_RETENTION_TABLES: tuple[tuple[str, str], ...] = (
    ("dq_quality_rules_history", "changed_at"),
    ("dq_schedule_configs_history", "changed_at"),
)


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
        oltp_sql: OltpExecutorProtocol | None = None,
        data_product_service: DataProductService | None = None,
        binding_run_service: BindingRunService | None = None,
        score_cache_service: ScoreCacheService | None = None,
        monitored_table_service: MonitoredTableService | None = None,
        metadata_dim_service: MetadataDimService | None = None,
        tag_reconcile_service: TagReconcileService | None = None,
        reconcile_scores_on_start: bool = False,
    ) -> None:
        """Construct the scheduler.

        Parameters
        ----------
        oltp_sql:
            Executor used for OLTP-table operations (schedule
            tracking, schedule configs, app settings, rule reads).
            When ``None`` (legacy mode, no Lakebase) the same Delta
            executor is used for everything.  When Lakebase is enabled,
            callers pass a :class:`backend.pg_executor.PgExecutor` so
            the high-frequency reads/writes hit Postgres.  Typed as
            :class:`OltpExecutorProtocol` so both concrete executors
            are accepted without a runtime cast — the Protocol is the
            structural contract every OLTP call site relies on.
        data_product_service:
            Optional collaborator that fans a Data Product run out to
            its members (design spec §4.2). When ``None`` (legacy
            deployments, or unit tests that only exercise the
            scope-config path), :meth:`_tick_products` is a no-op —
            the scope-config scheduling path is entirely unaffected
            either way.
        binding_run_service:
            Optional collaborator that submits a single monitored
            table's run (P21 item 14). When ``None``,
            :meth:`_tick_monitored_tables` is a no-op — a THIRD,
            independent due-ness source that never touches state the
            scope-config or product paths read.
        score_cache_service:
            Optional collaborator that recomputes the Lakebase
            ``dq_score_cache`` rows. When set, every run the scheduler
            launches is tracked in memory and, once its
            ``dq_validation_runs`` terminal row lands, the affected
            tables' scores are refreshed best-effort on the next tick
            (:meth:`_refresh_scores_for_completed_runs`) — closing the
            gap where the browser-side refresh-scores POST never fires
            because no browser observed the scheduled run complete.
            When ``None`` the refresh step is a no-op.
        monitored_table_service:
            Optional collaborator that denormalizes each completed table's
            ``last_run_at`` / ``last_profiled_at`` into its OLTP
            ``dq_monitored_tables`` row (T-perf / B2-15), alongside the score
            refresh above and in the startup reconcile — so the overview
            "Last run" column and table-space last-run stay current for runs
            no browser observed, without the list path ever touching the
            warehouse. When ``None`` the timestamp write is skipped.
        metadata_dim_service:
            Optional collaborator that full-refreshes the rule +
            monitored-table metadata dims (``dim_dq_rules`` /
            ``dim_dq_monitored_tables``) the Genie space queries. When set,
            :meth:`_maybe_refresh_metadata_dims` re-materializes them once
            per ``_METADATA_DIM_REFRESH_INTERVAL_HOURS`` so registry edits
            reach Genie without a redeploy. When ``None`` (legacy
            deployments, unit tests) the tick is a no-op — a fourth,
            independent timer that touches no state the other ticks read.
        tag_reconcile_service:
            Optional apply-on-tag orchestrator (Task 7). When set,
            :meth:`_maybe_run_tag_reconcile` runs a full reconcile sweep once
            per ``_TAG_RECONCILE_INTERVAL_HOURS`` so tag changes on
            already-monitored tables re-attach their matching published rules
            without a publish/register event. When ``None`` (legacy
            deployments, unit tests) the tick is a no-op — a fifth independent
            timer that touches no state the other ticks read. The sweep is
            itself a no-op when the ``tag_auto_apply`` setting is off.
        reconcile_scores_on_start:
            When True (production wiring — set by the app lifespan),
            the first score-refresh pass after boot recomputes EVERY
            monitored table's cached score in one batched warehouse
            query (then products + global) instead of only the runs it
            observed complete — healing rows left stale or NULL by
            semantic changes and cold deployments. Runs at most once
            per boot (the "reconciled this boot" flag), best-effort
            with a small retry budget. Default False keeps legacy /
            unit-test constructions on the pure per-run refresh.
        """
        self._ws = ws
        self._job_id = job_id
        self._catalog = catalog
        self._schema = schema
        self._tmp_schema = tmp_schema
        self._sql = SqlExecutor(ws=ws, warehouse_id=warehouse_id, catalog=catalog, schema=schema)
        self._tmp_sql = SqlExecutor(ws=ws, warehouse_id=warehouse_id, catalog=catalog, schema=tmp_schema)
        # OLTP executor — either a PgExecutor (Lakebase) or the same
        # Delta executor (legacy mode). All schedule / settings /
        # rule access goes through this; only analytical table
        # operations (retention sweep, orphan view GC) use ``self._sql``.
        # No cast: ``SqlExecutor`` structurally satisfies
        # :class:`OltpExecutorProtocol`, so basedpyright validates
        # every ``self._oltp_sql.foo()`` call against the same
        # Protocol surface regardless of which concrete executor was
        # injected.
        self._oltp_sql: OltpExecutorProtocol = oltp_sql if oltp_sql is not None else self._sql
        self._task: asyncio.Task[None] | None = None
        self._reload_event = asyncio.Event()
        self._force_recalc = False
        # Both backend layouts qualify the table differently — let the
        # OLTP executor's catalog/schema decide.
        self._table = self._oltp_sql.fqn("dq_schedule_runs")
        self._configs_table = self._oltp_sql.fqn("dq_schedule_configs")
        self._settings_table = self._oltp_sql.fqn("dq_app_settings")
        self._rules_table = self._oltp_sql.fqn("dq_quality_rules")
        self._products_table = self._oltp_sql.fqn("dq_data_products")
        self._monitored_tables_table = self._oltp_sql.fqn("dq_monitored_tables")
        self._data_product_service = data_product_service
        self._binding_run_service = binding_run_service
        self._score_cache_service = score_cache_service
        self._monitored_table_service = monitored_table_service
        self._metadata_dim_service = metadata_dim_service
        self._tag_reconcile_service = tag_reconcile_service
        # Scheduler-launched runs awaiting their dq_validation_runs
        # terminal row, run_id -> launch time (UTC). In-memory only:
        # the scheduler is file-locked to one worker, and a run lost to
        # an app restart is covered by the browser-side refresh or the
        # next scheduled completion. Entries expire after
        # :data:`_SCORE_REFRESH_TTL`.
        self._pending_score_runs: dict[str, datetime] = {}
        self._runs_table = self._sql.fqn("dq_validation_runs")
        # Run-set sweep state (P5.3): run ids already tracked or
        # processed this boot, so the recurring 24h-window query never
        # re-tracks a run it has already handled. Pruned every sweep to
        # (window ∪ pending) so it stays bounded across long uptimes.
        self._seen_score_runs: set[str] = set()
        self._run_sets_table = self._oltp_sql.fqn("dq_run_sets")
        self._run_set_members_table = self._oltp_sql.fqn("dq_run_set_members")
        # Startup reconcile state (P5.3): whether this boot has healed
        # the whole score cache yet, and how many attempts it has spent
        # trying. Both in-memory only — the reconcile is deliberately
        # per-boot (each deploy may ship semantic changes that
        # invalidate cached rows).
        self._reconcile_scores_on_start = reconcile_scores_on_start
        self._scores_reconciled = False
        self._score_reconcile_attempts = 0

        # Orphan-tmp-view GC: fires every Saturday at 01:00 UTC. Held in
        # process memory rather than persisted — a missed Saturday (e.g.
        # the app was redeploying at exactly 01:00 UTC) is fine because
        # the per-run ``finally`` cleanup already handles 99% of cases
        # and orphans only accumulate slowly.
        self._next_view_gc_at: datetime = self._next_saturday_01_utc(datetime.now(timezone.utc))

        # Hourly tmp-view sweep: drops views for terminal/abandoned runs.
        # Fires on the first scheduler tick after boot so a redeploy
        # quickly reaps anything a browser never polled to completion.
        self._next_tmp_view_sweep_at: datetime = datetime.now(timezone.utc)

        # Retention sweep: fires every ``_RETENTION_INTERVAL_HOURS``
        # (default 24h). Held in process memory like the view GC; a
        # missed sweep is harmless since the next one catches up.
        self._next_retention_at: datetime = datetime.now(timezone.utc) + timedelta(hours=_RETENTION_INTERVAL_HOURS)

        # Metadata-dim refresh: fires every
        # ``_METADATA_DIM_REFRESH_INTERVAL_HOURS`` (default 1h). Held in
        # process memory like the retention sweep; the app also refreshes once
        # at startup, so a missed tick is harmless.
        self._next_metadata_dim_refresh_at: datetime = datetime.now(timezone.utc) + timedelta(
            hours=_METADATA_DIM_REFRESH_INTERVAL_HOURS
        )

        # Apply-on-tag reconcile sweep: fires every
        # ``_TAG_RECONCILE_INTERVAL_HOURS`` (default 6h). Held in process
        # memory like the retention sweep; a missed tick is harmless since the
        # next one catches up and the sweep is idempotent.
        self._next_tag_reconcile_at: datetime = datetime.now(timezone.utc) + timedelta(
            hours=_TAG_RECONCILE_INTERVAL_HOURS
        )

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
                await self._maybe_sweep_stale_tmp_views(datetime.now(timezone.utc))
                await self._maybe_run_retention(datetime.now(timezone.utc))
                await self._maybe_run_tag_reconcile(datetime.now(timezone.utc))
                await self._maybe_refresh_metadata_dims(datetime.now(timezone.utc))
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

        After the scope-config loop below completes (unconditionally, and
        regardless of whether any config exists), :meth:`_tick_products`
        runs as a SECOND, independent due-ness source over approved Data
        Products (design spec §4.3). It never reads or mutates any state
        the scope-config loop touches, so scope-config behaviour is
        unaffected either way.
        """
        now = datetime.now(timezone.utc)
        configs = await asyncio.to_thread(self._load_schedule_configs)
        if not configs:
            logger.info("Scheduler tick: no schedule configs found")
        else:
            logger.info("Scheduler tick: found %d config(s), recalc=%s", len(configs), recalc)

        for name, cfg in configs.items():
            freq = cfg.get("frequency", "manual")
            if freq == "manual":
                continue
            # ``paused`` is a soft kill switch toggled from the Schedules
            # list. Skipping here (rather than at config-save time) means a
            # paused schedule keeps its existing ``next_run_at`` tracker, so
            # resuming it does not retroactively fire any missed runs — the
            # next tick simply picks the schedule back up.
            if cfg.get("paused"):
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

                try:
                    errors = await asyncio.to_thread(self._trigger_run, name, cfg, run_id)

                    new_next = self._compute_next_run(cfg, now)
                    status = "success" if not errors else "partial_failure"
                    if errors:
                        logger.warning("Schedule '%s' run %s had errors: %s", name, run_id, errors)

                    await asyncio.to_thread(self._upsert_tracker, name, now, new_next, run_id, status)
                except Exception:
                    # A hard failure inside ``_trigger_run`` (e.g. ``_resolve_scope``,
                    # ``_load_custom_metrics``, or DB access) — anything outside the
                    # per-table try that returns ``errors`` — would otherwise skip the
                    # ``_upsert_tracker`` above and leave ``next_run_at`` in the past.
                    # The schedule then stays "due" and re-fires every tick, turning a
                    # deterministic error into a tight retry loop that keeps submitting
                    # jobs. Advance ``next_run_at`` (persisting a ``failed`` status) so
                    # the schedule resumes at its next occurrence instead of hammering.
                    logger.exception(
                        "Schedule '%s' run %s failed to trigger; advancing next_run_at to avoid a retry loop",
                        name,
                        run_id,
                    )
                    await asyncio.to_thread(self._advance_after_failure, name, cfg, now, run_id)
            except Exception:
                logger.exception("Scheduler failed processing schedule '%s'", name)

        # Second, independent due-ness source (design spec §4.3). Runs
        # after every config has already been processed above — a
        # product-tick failure is fully isolated inside
        # :meth:`_tick_products` and cannot roll back or skip anything
        # the config loop already did.
        try:
            await asyncio.to_thread(self._tick_products, now)
        except Exception:
            logger.exception("Scheduler failed processing Data Product schedules")

        # Third, independent due-ness source (P21 item 14): monitored
        # tables with an approved snapshot (``version > 0``) carrying a
        # cron — not gated on current review ``status``, see
        # :meth:`_load_scheduled_tables`. Fully isolated inside
        # :meth:`_tick_monitored_tables` like the product tick above.
        try:
            await asyncio.to_thread(self._tick_monitored_tables, now)
        except Exception:
            logger.exception("Scheduler failed processing monitored-table schedules")

        # Completion observation: refresh the Lakebase score cache for any
        # scheduler-launched run whose terminal ``dq_validation_runs`` row
        # has landed since the last tick. Piggybacks on the 60s tick (no
        # extra loop) and is fully best-effort — a failure here never
        # affects the due-ness sources above.
        try:
            await asyncio.to_thread(self._refresh_scores_for_completed_runs, now)
        except Exception:
            logger.exception("Scheduler failed refreshing the score cache for completed runs")

    def _advance_after_failure(self, name: str, cfg: dict[str, Any], now: datetime, run_id: str) -> None:
        """Persist a failed run and push ``next_run_at`` forward after a trigger failure.

        Computes the next scheduled occurrence so a deterministic failure does not
        re-fire every tick. If even the next-run computation fails, falls back to a
        fixed backoff so the schedule still moves off "due".
        """
        try:
            new_next = self._compute_next_run(cfg, now)
        except Exception:
            logger.exception(
                "Schedule '%s': could not compute next_run_at after a failure; using backoff",
                name,
            )
            new_next = now + _FAILURE_BACKOFF
        try:
            self._upsert_tracker(name, now, new_next, run_id, "failed")
        except Exception:
            logger.exception("Schedule '%s': failed to persist tracker after a trigger failure", name)

    # ------------------------------------------------------------------
    # Data Products product ticks (design spec §4.3, Task 5)
    # ------------------------------------------------------------------
    #
    # A SECOND, independent due-ness source alongside the scope-config loop
    # above. Bookkeeping reuses the same ``dq_schedule_runs`` table and the
    # same ``_get_tracker``/``_upsert_tracker`` helpers, keyed by
    # ``schedule_name = f"product:{product_id}"`` so product schedules and
    # scope-config schedules can never collide in the tracker table.
    #
    # Cron evaluation deliberately does NOT reuse ``_compute_next_run``
    # (the scope-config path's frequency-dict evaluator): that method has
    # no cron-expression branch today — an unrecognised ``frequency`` value
    # (including a bare ``"cron"``) falls through to its generic
    # ``after + timedelta(hours=1)`` fallback. Reusing it as-is would
    # silently truncate every Data Product schedule to hourly; extending it
    # to understand raw cron strings would be a behavioural change to the
    # method the scope-config path depends on, which the "byte-identical"
    # requirement rules out. ``_compute_next_cron_run`` is therefore a new,
    # additive evaluator for the standard 5-field cron dialect the Schedule
    # tab authors (no third-party cron library — plain calendar/datetime
    # arithmetic, same as ``_compute_next_run`` itself).
    #
    # Timezone: the scope-config path always evaluates ``hour``/``minute``
    # against naive UTC wall-clock values (see ``_compute_next_run`` and the
    # UTC-labelled schedule-preview copy in the UI, e.g. "Daily at 09:00
    # UTC") — there is no per-config timezone field. A Data Product's cron
    # is instead evaluated in its own ``schedule_tz`` (an IANA zone name,
    # e.g. ``"America/Sao_Paulo"``); an unset or unrecognised zone falls
    # back to UTC, matching the scope-config path's behaviour for the
    # common case where no timezone was configured.

    def _tick_products(self, now: datetime) -> None:
        """Check every cron-scheduled Table Space with an approved snapshot and trigger due ones.

        Eligibility (see :meth:`_load_scheduled_products`) is
        ``version > 0``, not the space's current review ``status`` — a
        space pending re-approval keeps its schedule and resolves each
        member per its pin / latest-approved version as normal, same as
        an approved space.

        No-op when the scheduler was constructed without a
        :class:`DataProductService` (legacy deployments, or unit tests that
        only exercise the scope-config path) — this keeps the method safe
        to call unconditionally from :meth:`_tick`.
        """
        if self._data_product_service is None:
            return

        products = self._load_scheduled_products()
        if not products:
            return

        logger.info("Scheduler tick: found %d scheduled data product(s)", len(products))

        for product in products:
            try:
                self._tick_one_product(product, now)
            except Exception:
                logger.exception("Scheduler failed processing product schedule 'product:%s'", product["product_id"])

    def _load_scheduled_products(self) -> list[dict[str, Any]]:
        """Return cron-scheduled Table Spaces that have an approved (frozen) snapshot.

        Eligibility is ``schedule_cron IS NOT NULL AND version > 0`` — NOT
        ``status = 'approved'``. Ruling (user's words): "keep the schedule
        running with the old frozen version. Because it's frozen so who
        cares?" A space that has been approved at least once keeps ticking
        on schedule even while it sits in ``pending_approval`` (e.g. rolled
        back to review by a followed rule's republish) or ``rejected`` —
        the run resolves each member per its pin / latest-approved binding
        version exactly as :meth:`DataProductService.run` always has, so it
        is the already-reviewed frozen content that executes, never
        unreviewed draft edits. Only a space that has NEVER been approved
        (``version == 0``) is excluded, matching
        :func:`data_product_service._is_runnable`'s member-level gate.

        Best-effort like :meth:`_load_schedule_configs`: a missing
        ``dq_data_products`` table (a deployment predating Data Products, or
        a migration that hasn't run yet) yields an empty list rather than
        raising.
        """
        try:
            sql = (
                f"SELECT product_id, schedule_cron, schedule_tz, schedule_kind FROM {self._products_table} "
                f"WHERE schedule_cron IS NOT NULL AND version > 0"
            )
            rows = self._oltp_sql.query(sql)
        except Exception:
            logger.debug("dq_data_products table not available; skipping product schedules", exc_info=True)
            return []
        return [
            {
                "product_id": row[0],
                "schedule_cron": row[1],
                "schedule_tz": row[2],
                "schedule_kind": _normalize_schedule_kind(row[3] if len(row) > 3 else None),
            }
            for row in rows
            if row and row[0] and row[1]
        ]

    def _tick_one_product(self, product: dict[str, Any], now: datetime) -> None:
        """Check due-ness for one product and fire its run if due.

        Mirrors the scope-config due-ness/tracker dance in :meth:`_tick`
        (first tick after a schedule is created seeds ``next_run_at``
        without firing unless it's already in the past; each due firing
        advances ``next_run_at`` to the following occurrence). Every branch
        that fires a run persists a tracker row so a deterministic failure
        (including zero runnable members) cannot turn into a tight
        every-tick retry loop.
        """
        product_id = product["product_id"]
        cron_expr = product["schedule_cron"]
        tz_name = product.get("schedule_tz")
        schedule_name = f"product:{product_id}"

        tracker = self._get_tracker(schedule_name)
        next_run = tracker.get("next_run_at") if tracker else None

        if next_run is None:
            last_run = tracker.get("last_run_at") if tracker else None
            last_id = tracker.get("last_run_id") if tracker else None
            last_dt = self._parse_ts(last_run) if last_run else None
            try:
                computed = self._compute_next_cron_run(cron_expr, now - timedelta(seconds=1), tz_name)
            except Exception:
                # A malformed ``schedule_cron`` must not raise here: this
                # branch runs on every tick until a tracker row exists, so
                # an unguarded raise means a full stack trace logged
                # forever with next_run_at never advancing. Seed a backoff
                # tracker instead, mirroring
                # :meth:`_advance_product_after_failure`'s fallback, so the
                # schedule retries on the :data:`_FAILURE_BACKOFF` cadence.
                # Only the first encounter (no tracker row yet) gets a full
                # exception log; subsequent ticks just warn to avoid spam.
                if tracker is None:
                    logger.exception(
                        "Product schedule '%s': could not compute initial next_run_at for cron '%s'; "
                        "seeding backoff tracker",
                        schedule_name,
                        cron_expr,
                    )
                else:
                    logger.warning(
                        "Product schedule '%s': could not compute next_run_at for cron '%s'; seeding backoff tracker",
                        schedule_name,
                        cron_expr,
                    )
                computed = now + _FAILURE_BACKOFF
                self._upsert_tracker(schedule_name, last_dt, computed, last_id, "pending")
                return
            self._upsert_tracker(schedule_name, last_dt, computed, last_id, "pending")
            if computed <= now:
                next_run = computed.isoformat()
            else:
                return

        next_run_dt = self._parse_ts(next_run) if isinstance(next_run, str) else next_run
        if next_run_dt is None or next_run_dt > now:
            return

        run_id = uuid4().hex[:16]
        kind = _normalize_schedule_kind(product.get("schedule_kind"))
        logger.info(
            "Product schedule '%s' is due (next_run_at=%s, kind=%s), triggering run %s",
            schedule_name,
            next_run,
            kind,
            run_id,
        )

        assert self._data_product_service is not None  # guarded by _tick_products

        # Branch by schedule scope (B2-52): the DQ fan-out (DataProductService.run)
        # and/or a profiling run per member table, folded into one combined
        # tracker status so the dedupe/advance bookkeeping stays a single row
        # per firing.
        any_succeeded = False
        any_failed = False

        if kind in _KINDS_WITH_DQ:
            try:
                result = self._data_product_service.run(
                    product_id,
                    source="approved",
                    user_email="scheduler",
                    trigger="scheduled",
                )
                logger.info(
                    "Product schedule '%s': submitted run set %s (%d member(s), %d skipped)",
                    schedule_name,
                    result.run_set_id,
                    len(result.submitted),
                    len(result.skipped),
                )
                for submission in result.submitted:
                    self._track_run_for_score_refresh(submission.run_id)
                any_succeeded = True
                if result.skipped:
                    any_failed = True  # some members skipped → partial
            except NoRunnableMembersError as e:
                # Zero runnable members (all drafts / never approved) maps to a
                # 409 at the manual-trigger route, but a scheduled tick must
                # not treat it as a hard failure that retries every tick.
                logger.warning("Product schedule '%s': no runnable members: %s", schedule_name, e)
                any_failed = True
            except Exception:
                logger.exception("Product schedule '%s' DQ run failed to trigger", schedule_name)
                any_failed = True

        if kind in _KINDS_WITH_PROFILING:
            try:
                member_fqns = self._data_product_service.member_table_fqns(product_id)
            except Exception:
                logger.exception("Product schedule '%s': failed to enumerate members for profiling", schedule_name)
                member_fqns = []
                any_failed = True
            for fqn in member_fqns:
                try:
                    prof_run_id = self._submit_profile_run(fqn, f"scheduler:{schedule_name}")
                    logger.info(
                        "Product schedule '%s': submitted profiling run %s for %s", schedule_name, prof_run_id, fqn
                    )
                    any_succeeded = True
                except Exception:
                    logger.exception("Product schedule '%s': profiling run failed for %s", schedule_name, fqn)
                    any_failed = True

        self._finish_schedule_firing(schedule_name, cron_expr, tz_name, now, run_id, any_succeeded, any_failed)

    # ------------------------------------------------------------------
    # Monitored-table ticks (P21 item 14)
    # ------------------------------------------------------------------
    #
    # A THIRD, independent due-ness source alongside the scope-config and
    # product loops. Bookkeeping reuses the same ``dq_schedule_runs`` table
    # and helpers, keyed by ``schedule_name = f"table:{binding_id}"`` so
    # table schedules can never collide with product (``product:``) or
    # user-authored scope-config schedules — the ``table:`` prefix is
    # reserved in ``schedule_config_service`` exactly like ``product:``.
    # Cron evaluation reuses :meth:`_compute_next_cron_run` (same 5-field
    # POSIX dialect + per-table ``schedule_tz`` the product path uses).
    #
    # Eligibility is ``version > 0``, not ``status = 'approved'`` — see
    # :meth:`_load_scheduled_tables` for the ruling and rationale. The
    # scheduler runs the frozen, already-reviewed snapshot regardless of
    # whether the binding is currently mid-review for NEWER content.

    def _tick_monitored_tables(self, now: datetime) -> None:
        """Check every cron-scheduled monitored table with an approved snapshot and trigger due ones.

        No-op when the scheduler was constructed without a
        :class:`BindingRunService` (legacy deployments, or unit tests that
        only exercise the other paths) — safe to call unconditionally from
        :meth:`_tick`.
        """
        if self._binding_run_service is None:
            return

        tables = self._load_scheduled_tables()
        if not tables:
            return

        logger.info("Scheduler tick: found %d scheduled monitored table(s)", len(tables))

        for table in tables:
            try:
                self._tick_one_table(table, now)
            except Exception:
                logger.exception("Scheduler failed processing table schedule 'table:%s'", table["binding_id"])

    def _load_scheduled_tables(self) -> list[dict[str, Any]]:
        """Return cron-scheduled monitored tables that have an approved (frozen) snapshot.

        Eligibility is ``schedule_cron IS NOT NULL AND version > 0`` — NOT
        ``status = 'approved'``. Ruling (user's words): "keep the schedule
        running with the old frozen version. Because it's frozen so who
        cares?" A following table that gets rolled to ``pending_approval``
        because a rule it follows republished (auto-upgrade OFF) must keep
        running its existing schedule: :meth:`_tick_one_table` calls
        ``BindingRunService.run_binding(..., source="approved", version=None)``,
        which always resolves the latest APPROVED snapshot
        (``binding.version``) — an immutable, already-reviewed artifact —
        regardless of the binding's current review *status*. Pending review
        of new content is no reason to stop running the old, frozen version.
        A table with ``version == 0`` (never approved) or ``rejected`` with
        a prior approved version behave symmetrically: v0 stays excluded
        here (nothing to run); ``rejected``-with-vN keeps firing vN, exactly
        like ``pending_approval``-with-vN.

        Best-effort like :meth:`_load_scheduled_products`: a missing
        ``dq_monitored_tables`` table (a deployment predating the schedule
        columns, or a migration that hasn't run yet) yields an empty list
        rather than raising.
        """
        try:
            sql = (
                f"SELECT binding_id, schedule_cron, schedule_tz, table_fqn, schedule_kind "
                f"FROM {self._monitored_tables_table} "
                f"WHERE schedule_cron IS NOT NULL AND version > 0"
            )
            rows = self._oltp_sql.query(sql)
        except Exception:
            logger.debug("dq_monitored_tables schedule columns not available; skipping", exc_info=True)
            return []
        return [
            {
                "binding_id": row[0],
                "schedule_cron": row[1],
                "schedule_tz": row[2],
                "table_fqn": row[3] if len(row) > 3 else None,
                "schedule_kind": _normalize_schedule_kind(row[4] if len(row) > 4 else None),
            }
            for row in rows
            if row and row[0] and row[1]
        ]

    def _tick_one_table(self, table: dict[str, Any], now: datetime) -> None:
        """Check due-ness for one monitored table and fire its run if due.

        Mirrors :meth:`_tick_one_product` exactly (seed-without-firing on the
        first tick, single catch-up on a missed window, malformed-cron backoff
        that never turns into an every-tick retry loop), differing only in the
        collaborator it fires (``BindingRunService.run_binding`` for one table
        rather than a product fan-out).
        """
        binding_id = table["binding_id"]
        cron_expr = table["schedule_cron"]
        tz_name = table.get("schedule_tz")
        schedule_name = f"table:{binding_id}"

        tracker = self._get_tracker(schedule_name)
        next_run = tracker.get("next_run_at") if tracker else None

        if next_run is None:
            last_run = tracker.get("last_run_at") if tracker else None
            last_id = tracker.get("last_run_id") if tracker else None
            last_dt = self._parse_ts(last_run) if last_run else None
            try:
                computed = self._compute_next_cron_run(cron_expr, now - timedelta(seconds=1), tz_name)
            except Exception:
                # Mirror the product path: a malformed cron must seed a
                # backoff tracker instead of raising on every tick.
                if tracker is None:
                    logger.exception(
                        "Table schedule '%s': could not compute initial next_run_at for cron '%s'; "
                        "seeding backoff tracker",
                        schedule_name,
                        cron_expr,
                    )
                else:
                    logger.warning(
                        "Table schedule '%s': could not compute next_run_at for cron '%s'; seeding backoff tracker",
                        schedule_name,
                        cron_expr,
                    )
                computed = now + _FAILURE_BACKOFF
                self._upsert_tracker(schedule_name, last_dt, computed, last_id, "pending")
                return
            self._upsert_tracker(schedule_name, last_dt, computed, last_id, "pending")
            if computed <= now:
                next_run = computed.isoformat()
            else:
                return

        next_run_dt = self._parse_ts(next_run) if isinstance(next_run, str) else next_run
        if next_run_dt is None or next_run_dt > now:
            return

        run_id = uuid4().hex[:16]
        kind = _normalize_schedule_kind(table.get("schedule_kind"))
        table_fqn = table.get("table_fqn")
        logger.info(
            "Table schedule '%s' is due (next_run_at=%s, kind=%s), triggering run %s",
            schedule_name,
            next_run,
            kind,
            run_id,
        )

        assert self._binding_run_service is not None  # guarded by _tick_monitored_tables

        # Branch by schedule scope (B2-52): DQ (BindingRunService), profiling
        # (a "profile" task on the shared job), or both. Each attempted action
        # contributes to a single combined tracker status so the existing
        # dedupe/advance bookkeeping stays intact — one tracker row per due
        # firing regardless of how many sub-runs it launches.
        any_succeeded = False
        any_failed = False

        if kind in _KINDS_WITH_DQ:
            try:
                result = self._binding_run_service.run_binding(
                    binding_id,
                    source="approved",
                    version=None,
                    user_email="scheduler",
                    trigger="scheduled",
                )
                logger.info(
                    "Table schedule '%s': submitted DQ run %s (run_set %s)",
                    schedule_name,
                    result.run_id,
                    result.run_set_id,
                )
                self._track_run_for_score_refresh(result.run_id)
                any_succeeded = True
            except BindingRunError as e:
                # An expected, deterministic domain failure (never approved,
                # missing snapshot, empty checks) — record and advance rather
                # than hard-retrying every tick, mirroring the product
                # NoRunnableMembers path.
                logger.warning("Table schedule '%s': DQ not runnable: %s", schedule_name, e)
                any_failed = True
            except Exception:
                logger.exception("Table schedule '%s' DQ run failed to trigger", schedule_name)
                any_failed = True

        if kind in _KINDS_WITH_PROFILING:
            if not table_fqn:
                logger.warning("Table schedule '%s': cannot profile — no table_fqn on the schedule row", schedule_name)
                any_failed = True
            else:
                try:
                    profile_run_id = self._submit_profile_run(table_fqn, f"scheduler:{schedule_name}")
                    logger.info(
                        "Table schedule '%s': submitted profiling run %s for %s",
                        schedule_name,
                        profile_run_id,
                        table_fqn,
                    )
                    any_succeeded = True
                except Exception:
                    logger.exception(
                        "Table schedule '%s' profiling run failed to trigger for %s", schedule_name, table_fqn
                    )
                    any_failed = True

        self._finish_schedule_firing(schedule_name, cron_expr, tz_name, now, run_id, any_succeeded, any_failed)

    def _finish_schedule_firing(
        self,
        schedule_name: str,
        cron_expr: str,
        tz_name: str | None,
        now: datetime,
        run_id: str,
        any_succeeded: bool,
        any_failed: bool,
    ) -> None:
        """Advance ``next_run_at`` and persist one combined tracker status (B2-52).

        Shared by the table and product ticks: a due firing may launch a DQ
        run, a profiling run, or both. Exactly ONE tracker row is written per
        firing so the existing dedupe bookkeeping is untouched — ``success``
        when nothing failed, ``partial_failure`` when at least one sub-run
        succeeded and at least one failed, ``failed`` when nothing succeeded.
        ``next_run_at`` always moves forward (falling back to
        :data:`_FAILURE_BACKOFF` if the next occurrence can't be computed) so a
        deterministic failure never becomes an every-tick retry loop.
        """
        try:
            new_next = self._compute_next_cron_run(cron_expr, now, tz_name)
        except Exception:
            logger.exception("Schedule '%s': could not compute next_run_at after firing; using backoff", schedule_name)
            new_next = now + _FAILURE_BACKOFF
        if not any_succeeded and any_failed:
            status = "failed"
        elif any_failed:
            status = "partial_failure"
        else:
            status = "success"
        try:
            self._upsert_tracker(schedule_name, now, new_next, run_id, status)
        except Exception:
            logger.exception("Schedule '%s': failed to persist tracker after firing", schedule_name)

    def _submit_profile_run(self, source_table_fqn: str, requesting_user: str) -> str:
        """Launch a profiling run for one table via the shared task-runner job (B2-52).

        Mirrors the profiler route's submit path but runs entirely as the app
        service principal (the scheduler has no OBO token): create a temp view
        with :meth:`_create_view` (SP credentials), then fire a ``profile``
        task on the same job the scope-config/DQ paths use. The frozen profiler
        runner writes the ``dq_profiling_results`` row itself and drops the temp
        view on completion (``task_type == 'profile'``), so no placeholder row
        is recorded here — matching the fire-and-forget scope-config path. On a
        submission failure the just-created view is dropped so a half-submitted
        run never leaks a temp view.

        Returns the app-level ``run_id``. Raises if the job id is unset or the
        submission fails.
        """
        if not self._job_id:
            raise RuntimeError("DQX_JOB_ID is not configured — cannot submit profiling runs")

        run_id = uuid4().hex[:16]
        view_fqn = self._create_view(source_table_fqn)
        try:
            config = {
                "sample_limit": _PROFILE_SAMPLE_LIMIT,
                "source_table_fqn": source_table_fqn,
                "columns": None,
                "profile_options": None,
            }
            self._ws.jobs.run_now(
                job_id=int(self._job_id),
                job_parameters={
                    "task_type": "profile",
                    "view_fqn": view_fqn,
                    "result_catalog": self._catalog,
                    "result_schema": self._schema,
                    "config_json": json.dumps(config),
                    "run_id": run_id,
                    "requesting_user": requesting_user,
                },
            )
        except Exception:
            try:
                from databricks_labs_dqx_app.backend.sql_utils import quote_fqn

                self._tmp_sql.execute(f"DROP VIEW IF EXISTS {quote_fqn(view_fqn)}")
            except Exception as cleanup_err:
                logger.warning(
                    "Failed to drop temp view %s after profiling submit failure for %s: %s",
                    view_fqn,
                    source_table_fqn,
                    cleanup_err,
                )
            raise
        return run_id

    # ------------------------------------------------------------------
    # Score-cache refresh on observed run completion
    # ------------------------------------------------------------------
    #
    # The ``dq_score_cache`` refresh otherwise only fires from the browser
    # (the results-invalidation POST when a user watches a run complete).
    # A run finishing with no browser open would leave the list scores
    # stale/NULL forever, so the scheduler tracks every run it launches
    # PLUS (P5.3) every run minted into a recent run set by the manual UI
    # paths, and refreshes the affected tables' scores when it observes
    # the run's terminal ``dq_validation_runs`` row — piggybacking on the
    # 60s tick, one bounded OLTP query + one batched Delta lookup per
    # tick, no new loop. The first pass after boot additionally
    # reconciles the whole cache (see ``_reconcile_scores``).

    def _track_run_for_score_refresh(self, run_id: str) -> None:
        """Remember a scheduler-launched run so its completion refreshes the score cache.

        No-op without a :class:`ScoreCacheService` collaborator so the
        launch paths can call it unconditionally.
        """
        if self._score_cache_service is None:
            return
        self._pending_score_runs[run_id] = datetime.now(timezone.utc)
        # Mark seen so the run-set sweep never re-tracks the same run
        # (scheduler-launched product/table runs also mint run sets).
        self._seen_score_runs.add(run_id)

    def _refresh_scores_for_completed_runs(self, now: datetime) -> None:
        """Refresh the score cache for tracked runs that reached a terminal state.

        One batched ``dq_validation_runs`` lookup over the pending run ids:
        any run with a non-RUNNING row has completed (the runner appends
        its terminal row next to the app's RUNNING placeholder — see
        :meth:`RunSetService._fetch_validation_rows`). Completed runs are
        dropped from tracking and their ``source_table_fqn``s fed to
        :meth:`ScoreCacheService.refresh_all_for_tables` in a single call.
        Fully best-effort: a refresh failure is logged and the runs stay
        untracked (the browser-side refresh or the run's next completion
        catches up) so a warehouse hiccup can never wedge the tick into a
        retry loop. Runs whose terminal row never lands (job died before
        the runner wrote it) expire after :data:`_SCORE_REFRESH_TTL`.

        Startup reconcile (P5.3): while this boot has not yet reconciled
        (and the retry budget isn't spent), the per-run refresh is
        replaced by :meth:`_reconcile_scores` — one batched recompute of
        the union of ALL monitored tables and any completed-run tables,
        so boot never performs the same warehouse recompute twice.
        """
        if self._score_cache_service is None:
            return

        try:
            self._sweep_recent_run_sets(now)
        except Exception:
            logger.exception(
                "Run-set sweep for the score-cache refresh failed; continuing with in-memory tracking only"
            )

        fqns = self._collect_completed_score_run_fqns(now)

        if self._reconcile_due():
            self._reconcile_scores(fqns)
            return
        if not fqns:
            return

        try:
            refreshed_tables, refreshed_products = self._score_cache_service.refresh_all_for_tables(sorted(fqns))
            logger.info(
                "Score cache refreshed after run completion: %d table(s), %d product(s)",
                refreshed_tables,
                refreshed_products,
            )
        except Exception:
            logger.exception(
                "Score-cache refresh after run completion failed; "
                "scores stay stale until the next completion or browser refresh"
            )
        self._refresh_run_timestamps(sorted(fqns))

    def _refresh_run_timestamps(self, fqns: list[str]) -> None:
        """Denormalize ``last_run_at`` / ``last_profiled_at`` for *fqns* (best-effort).

        The server-side counterpart of the browser's refresh-scores timestamp
        write (T-perf / B2-15): keeps the overview "Last run" column and
        table-space last-run current for completed runs no browser observed.
        A failure only leaves those columns stale until the next completion or
        reconcile, so it never wedges the tick. No-op without a
        :class:`MonitoredTableService` collaborator or when *fqns* is empty.
        """
        if self._monitored_table_service is None or not fqns:
            return
        try:
            self._monitored_table_service.refresh_run_timestamps(fqns)
        except Exception:
            logger.exception(
                "Monitored-table run-timestamp refresh failed; "
                "last-run columns stay stale until the next completion or reconcile"
            )

    def _sweep_recent_run_sets(self, now: datetime) -> None:
        """Track every unseen run from run sets created in the last 24h.

        Runs launched outside the scheduler — a user clicking Run on a
        monitored table or table space and closing the tab — mint
        ``dq_run_sets`` / ``dq_run_set_members`` rows but were invisible
        to the in-memory tracking, so their completion never refreshed
        the score cache. One bounded OLTP query per tick reads the
        window's member run ids; unseen ones join ``_pending_score_runs``
        and ride the existing batched terminal lookup. ``source =
        'approved'`` only: draft runs can never move published scores.
        The seen set guarantees each run is processed at most once per
        boot, and is pruned to (window ∪ pending) so it stays bounded.
        """
        interval = self._oltp_sql.interval_days_expr(_RUN_SET_SWEEP_WINDOW_DAYS)
        stmt = (
            f"SELECT m.run_id FROM {self._run_set_members_table} m "  # noqa: S608
            f"JOIN {self._run_sets_table} rs ON rs.run_set_id = m.run_set_id "
            f"WHERE rs.source = 'approved' "
            f"AND rs.created_at >= current_timestamp - {interval} "
            f"LIMIT {_RUN_SET_SWEEP_MAX_RUNS}"
        )
        rows = self._oltp_sql.query(stmt)
        window_ids = {row[0] for row in rows if row and row[0]}
        for run_id in window_ids:
            if run_id in self._seen_score_runs:
                continue
            self._pending_score_runs[run_id] = now
            self._seen_score_runs.add(run_id)
        self._seen_score_runs &= window_ids | set(self._pending_score_runs)

    def _collect_completed_score_run_fqns(self, now: datetime) -> set[str]:
        """Pop every tracked run with a terminal row; return their table FQNs.

        The expiry + batched terminal lookup extracted from
        :meth:`_refresh_scores_for_completed_runs` so the reconcile pass
        can fold the completed runs' tables into its own recompute.
        """
        from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

        if not self._pending_score_runs:
            return set()

        expired = [rid for rid, started in self._pending_score_runs.items() if now - started > _SCORE_REFRESH_TTL]
        for rid in expired:
            del self._pending_score_runs[rid]
            logger.warning("Run %s never reached a terminal state; dropping its score-refresh tracking", rid)
        if not self._pending_score_runs:
            return set()

        in_list = ", ".join(f"'{escape_sql_string(rid)}'" for rid in self._pending_score_runs)
        sql = (
            f"SELECT DISTINCT run_id, source_table_fqn FROM {self._runs_table} "  # noqa: S608
            f"WHERE run_id IN ({in_list}) AND UPPER(status) <> 'RUNNING'"
        )
        rows = self._sql.query(sql)

        fqns: set[str] = set()
        for row in rows:
            run_id = row[0] if row else None
            if not run_id:
                continue
            self._pending_score_runs.pop(run_id, None)
            fqn = row[1]
            if fqn and not fqn.startswith(_SQL_CHECK_PREFIX):
                fqns.add(fqn)
        return fqns

    def _reconcile_due(self) -> bool:
        """Whether this pass should run the startup score-cache reconcile."""
        return (
            self._reconcile_scores_on_start
            and not self._scores_reconciled
            and self._score_reconcile_attempts < _SCORE_RECONCILE_MAX_ATTEMPTS
        )

    def _reconcile_scores(self, completed_fqns: set[str]) -> None:
        """Recompute the cached score of EVERY monitored table (once per boot).

        Heals ``dq_score_cache`` rows left stale or NULL by semantic
        changes shipped in a deploy (e.g. the run_mode reclassification)
        and cold deployments where nothing has recomputed since boot —
        and, transitively, the product and global means derived from
        them. Runs on the scheduler's first refresh pass (single worker,
        seconds after startup, after the lifespan ensured the score
        views), bounded by :data:`~.score_cache_service.RECONCILE_MAX_TABLES`
        and merged with *completed_fqns* so a boot-backlog run completing
        on the same pass shares the ONE batched warehouse query. Success
        sets the reconciled-this-boot flag; failure retries next tick up
        to :data:`_SCORE_RECONCILE_MAX_ATTEMPTS` attempts.
        """
        if self._score_cache_service is None:  # pragma: no cover — caller guards
            return
        self._score_reconcile_attempts += 1
        try:
            monitored = self._score_cache_service.list_monitored_table_fqns()
            fqns = sorted(set(monitored) | completed_fqns)
            refreshed_tables, refreshed_products = self._score_cache_service.refresh_all_for_tables(fqns)
            self._scores_reconciled = True
            logger.info(
                "Startup score-cache reconcile complete: %d table(s), %d product(s), global",
                refreshed_tables,
                refreshed_products,
            )
            # Backfill the denormalized run/profile timestamps for the same set
            # (T-perf / B2-15) so tables whose runs completed before this deploy
            # — or with no browser watching — get a real "Last run" on the next
            # list load. Shares the reconcile's once-per-boot cadence.
            self._refresh_run_timestamps(fqns)
        except Exception:
            if self._score_reconcile_attempts >= _SCORE_RECONCILE_MAX_ATTEMPTS:
                logger.exception(
                    "Startup score-cache reconcile failed %d time(s); giving up for this boot "
                    "(run completions and browser refreshes still heal scores)",
                    self._score_reconcile_attempts,
                )
            else:
                logger.exception(
                    "Startup score-cache reconcile failed (attempt %d/%d); retrying next tick",
                    self._score_reconcile_attempts,
                    _SCORE_RECONCILE_MAX_ATTEMPTS,
                )

    @staticmethod
    def _resolve_cron_token(token: str, names: dict[str, int] | None) -> int:
        """Resolve one cron token to an int, honouring an optional name map (weekdays)."""
        token = token.strip()
        if names is not None:
            upper = token.upper()
            if upper in names:
                return names[upper]
        try:
            return int(token)
        except ValueError as exc:
            raise ValueError(f"Invalid cron token: '{token}'") from exc

    @staticmethod
    def _parse_cron_field(raw: str, lo: int, hi: int, names: dict[str, int] | None = None) -> set[int]:
        """Parse one standard 5-field-cron field into its concrete matching values.

        Supports the syntax the Schedule tab's raw-cron input accepts:
        ``*``, comma-separated lists, ``a-b`` ranges, and ``*/n`` / ``a-b/n``
        steps. *names* optionally maps case-insensitive tokens (weekday
        abbreviations ``MON``..``SUN``) to their numeric value for the
        day-of-week field.
        """
        values: set[int] = set()
        for part in raw.strip().split(","):
            part = part.strip()
            if not part:
                continue
            base, _, step_s = part.partition("/")
            step = int(step_s) if step_s else 1
            if step <= 0:
                raise ValueError(f"Invalid cron step: '{part}'")
            if base == "*":
                start, end = lo, hi
            elif "-" in base:
                start_s, end_s = base.split("-", 1)
                start = SchedulerService._resolve_cron_token(start_s, names)
                end = SchedulerService._resolve_cron_token(end_s, names)
            else:
                start = end = SchedulerService._resolve_cron_token(base, names)
            if not (lo <= start <= hi and lo <= end <= hi and start <= end):
                raise ValueError(f"Cron field value out of range [{lo}, {hi}]: '{part}'")
            values.update(v for v in range(start, end + 1) if (v - start) % step == 0)
        if not values:
            raise ValueError(f"Invalid cron field: '{raw}'")
        return values

    @staticmethod
    def _compute_next_cron_run(cron_expr: str, after: datetime, tz_name: str | None) -> datetime:
        """Compute the next UTC occurrence of a standard 5-field cron expression after *after*.

        Field order: ``minute hour day-of-month month day-of-week``
        (standard POSIX cron order). Day-of-week accepts ``0``-``7`` (both
        ``0`` and ``7`` mean Sunday) and ``MON``-``SUN`` names. Day
        matching follows the standard POSIX rule: when BOTH day-of-month
        and day-of-week are restricted (neither is ``*``), a day matches if
        EITHER field matches; when only one is restricted, only that one
        need match.

        *after* must be timezone-aware; the return value is UTC-aware.
        *tz_name* (an IANA zone name, e.g. ``"America/Sao_Paulo"``) is the
        zone the cron's wall-clock fields are interpreted in — see the
        module-level note above on why this diverges from the
        UTC-only scope-config path. An unset or unrecognised zone falls
        back to UTC rather than raising.
        """
        fields = cron_expr.split()
        if len(fields) != 5:
            raise ValueError(f"Cron expression must have exactly 5 fields: '{cron_expr}'")
        minute_f, hour_f, dom_f, month_f, dow_f = fields

        minutes = SchedulerService._parse_cron_field(minute_f, 0, 59)
        hours = SchedulerService._parse_cron_field(hour_f, 0, 23)
        doms = SchedulerService._parse_cron_field(dom_f, 1, 31)
        months = SchedulerService._parse_cron_field(month_f, 1, 12)
        raw_dows = SchedulerService._parse_cron_field(dow_f, 0, 7, _CRON_WEEKDAY_NAMES)
        dows = {0 if v == 7 else v for v in raw_dows}
        dom_wild = dom_f.strip() == "*"
        dow_wild = dow_f.strip() == "*"

        try:
            tz = ZoneInfo(tz_name) if tz_name else timezone.utc
        except (ZoneInfoNotFoundError, ValueError):
            logger.warning("Unknown schedule_tz '%s'; evaluating cron in UTC", tz_name)
            tz = timezone.utc

        candidate = (after.astimezone(tz) + timedelta(minutes=1)).replace(second=0, microsecond=0)

        for _ in range(_CRON_MAX_STEPS):
            if candidate.month not in months:
                year = candidate.year + (1 if candidate.month == 12 else 0)
                month = 1 if candidate.month == 12 else candidate.month + 1
                candidate = candidate.replace(year=year, month=month, day=1, hour=0, minute=0)
                continue

            cron_dow = candidate.isoweekday() % 7  # Mon=1..Sat=6, Sun=0 — matches cron numbering
            if dom_wild and dow_wild:
                day_ok = True
            elif dom_wild:
                day_ok = cron_dow in dows
            elif dow_wild:
                day_ok = candidate.day in doms
            else:
                day_ok = candidate.day in doms or cron_dow in dows
            if not day_ok:
                candidate = (candidate + timedelta(days=1)).replace(hour=0, minute=0)
                continue

            if candidate.hour not in hours:
                candidate = (candidate + timedelta(hours=1)).replace(minute=0)
                continue

            if candidate.minute not in minutes:
                candidate = candidate + timedelta(minutes=1)
                continue

            return candidate.astimezone(timezone.utc)

        raise ValueError(f"Could not find next occurrence for cron '{cron_expr}' within lookahead window")

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
            rows = self._oltp_sql.query(sql)
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
            rows = self._oltp_sql.query(sql)
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
        ts = self._oltp_sql.ts_text
        sql = (
            f"SELECT schedule_name, {ts('last_run_at')}, {ts('next_run_at')}, "
            f"last_run_id, status "
            f"FROM {self._table} WHERE schedule_name = '{escaped}'"
        )
        rows = self._oltp_sql.query(sql)
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
        from databricks_labs_dqx_app.backend.sql_utils import validate_schedule_name

        validate_schedule_name(name)
        if status not in _VALID_TRACKER_STATUSES:
            raise ValueError(f"Invalid tracker status: '{status}'. Must be one of {_VALID_TRACKER_STATUSES}")

        # Render datetimes as portable TIMESTAMP literals.  The
        # ``TIMESTAMP'<iso>'`` form is ANSI SQL and works in both
        # Delta and Postgres without modification.  PgExecutor's
        # upsert renderer treats ``RawSql("current_timestamp()")``
        # specially and rewrites it to ``CURRENT_TIMESTAMP`` so the
        # same call works for both backends.
        def _ts(dt: datetime | None) -> RawSql:
            if dt is None:
                return RawSql("NULL")
            return RawSql(f"TIMESTAMP'{dt.isoformat()}'")

        self._oltp_sql.upsert(
            self._table,
            key_cols={"schedule_name": name},
            value_cols={
                "last_run_at": _ts(last_run_at),
                "next_run_at": _ts(next_run_at),
                "last_run_id": last_run_id,
                "status": status,
                "updated_at": RawSql("current_timestamp()"),
            },
        )

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
        from databricks_labs_dqx_app.backend.sql_utils import fqn_needs_quoting, quote_fqn

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
                # The synthetic ``__sql_check__/<name>`` namespace holds
                # cross-table SQL checks only: the task runner builds a Spark
                # temp view from the embedded query, so ``view_fqn`` can stay
                # as the synthetic key. Reference checks (``has_valid_schema``
                # / ``foreign_key``) carry a real target-table FQN, which the
                # runner reads directly through the standard row-level path
                # (``is_sql_check=False``). ``source_table_fqn`` keeps the
                # original key so run history groups under the rule.
                is_synthetic = table_fqn.startswith(_SQL_CHECK_PREFIX)
                sql_query: str | None = None

                if is_synthetic:
                    sql_query = self._extract_sql_query(entry["checks"])
                    if sql_query is None:
                        errors.append(f"{table_fqn}: cross-table rule is missing its sql_query")
                        continue

                config = {
                    "checks": entry["checks"],
                    "sample_size": sample_size,
                    "source_table_fqn": table_fqn,
                    # Only cross-table SQL queries take the SQL fast-path in
                    # the runner; everything else uses the row-level engine.
                    "is_sql_check": sql_query is not None,
                }

                if custom_metrics:
                    config["custom_metrics"] = custom_metrics

                if sql_query is not None:
                    config["sql_query"] = sql_query

                # The runner does ``spark.table(view_fqn)`` for the row-level
                # (non-SQL-check) path, so an exotic real table name (quotes,
                # spaces, …) must arrive backtick-quoted or Spark fails to
                # parse it and every scheduled run for that table records
                # FAILED. Synthetic ``__sql_check__/<name>`` keys are never
                # ``spark.table``'d (the runner builds a temp view from the
                # embedded query) and simple names parse fine unquoted, so we
                # quote *only* exotic real FQNs — normal names stay
                # byte-identical in the stored ``view_fqn`` column.
                view_fqn_param = table_fqn
                if not is_synthetic and fqn_needs_quoting(table_fqn):
                    view_fqn_param = quote_fqn(table_fqn)

                self._ws.jobs.run_now(
                    job_id=int(self._job_id),
                    job_parameters={
                        "task_type": "scheduled",
                        "view_fqn": view_fqn_param,
                        "result_catalog": self._catalog,
                        "result_schema": self._schema,
                        "config_json": json.dumps(config),
                        "run_id": run_id,
                        "requesting_user": f"scheduler:{schedule_name}",
                    },
                )
                logger.info("Schedule '%s': submitted run for %s (run_id=%s)", schedule_name, table_fqn, run_id)
                # Synthetic cross-table keys never carry a real table FQN,
                # so there is no score-cache row to refresh for them.
                if not is_synthetic:
                    self._track_run_for_score_refresh(run_id)
            except Exception as e:
                logger.error("Schedule '%s': failed for %s: %s", schedule_name, table_fqn, e)
                errors.append(f"{table_fqn}: {e}")

        return errors

    # ------------------------------------------------------------------
    # Scope resolution
    # ------------------------------------------------------------------

    def _resolve_scope(self, cfg: dict[str, Any]) -> list[str]:
        """Return list of unique table_fqn matching the schedule's scope from approved rules.

        Two orthogonal filters intersected:
          * ``scope_mode`` / ``scope_catalogs|schemas|tables`` — FQN-based.
          * ``scope_labels`` — keep only FQNs that have at least one approved
            check carrying a matching ``user_metadata`` label.
        """
        mode = cfg.get("scope_mode", "all")
        sql = f"SELECT DISTINCT table_fqn FROM {self._rules_table} WHERE status = 'approved'"
        rows = self._oltp_sql.query(sql)
        fqns = [r[0] for r in rows if r[0]]

        if mode == "catalog":
            catalogs = set(cfg.get("scope_catalogs") or [])
            fqns = [f for f in fqns if self._fqn_part(f, 0) in catalogs]
        elif mode == "schema":
            schemas = set(cfg.get("scope_schemas") or [])
            fqns = [f for f in fqns if self._fqn_schema(f) in schemas]
        elif mode == "tables":
            tables = set(cfg.get("scope_tables") or [])
            fqns = [f for f in fqns if f in tables]

        label_filter = self._parse_scope_labels(cfg.get("scope_labels"))
        if label_filter:
            fqns = [f for f in fqns if self._fqn_has_matching_label(f, label_filter)]

        return fqns

    @staticmethod
    def _parse_scope_labels(raw: Any) -> set[tuple[str, str]]:
        """Normalise the persisted ``scope_labels`` field to a set of (key, value).

        Accepts the canonical ``[{key, value}, ...]`` shape produced by the
        UI plus a lenient ``["key=value", ...]`` shorthand for hand-edited
        configs. Invalid entries are silently dropped — a malformed label
        filter must never block a scheduled run.
        """
        if not isinstance(raw, list):
            return set()
        out: set[tuple[str, str]] = set()
        for entry in raw:
            if isinstance(entry, dict):
                key = entry.get("key")
                if isinstance(key, str) and key:
                    out.add((key, str(entry.get("value") or "")))
            elif isinstance(entry, str):
                if not entry:
                    continue
                idx = entry.find("=")
                if idx < 0:
                    out.add((entry, ""))
                else:
                    out.add((entry[:idx], entry[idx + 1 :]))
        return out

    def _fqn_has_matching_label(
        self,
        table_fqn: str,
        label_filter: set[tuple[str, str]],
    ) -> bool:
        """True iff any approved check on ``table_fqn`` carries a matching label."""
        rule = self._get_approved_rule(table_fqn)
        if rule is None:
            return False
        for check in rule.get("checks") or []:
            md = self._check_user_metadata(check)
            for key, value in md.items():
                if (key, value) in label_filter:
                    return True
        return False

    @staticmethod
    def _check_user_metadata(check: Any) -> dict[str, str]:
        """Pull the ``user_metadata`` map off a check payload regardless of shape.

        Mirrors the front-end ``getUserMetadata`` helper — checks come in
        either as a top-level dict with ``user_metadata`` directly on them,
        or wrapped under a ``check`` key (legacy export shape).
        """
        if not isinstance(check, dict):
            return {}
        candidate = check.get("user_metadata")
        if not isinstance(candidate, dict):
            inner = check.get("check")
            candidate = inner.get("user_metadata") if isinstance(inner, dict) else None
        if not isinstance(candidate, dict):
            return {}
        return {str(k): str(v) for k, v in candidate.items() if k}

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
        """Get merged checks from all approved rule rows for a table.

        After the v1 baseline split, each row stores a single check in
        the VARIANT/JSONB ``check`` column rather than an array of
        checks. The scheduler still presents one merged ``checks`` list
        downstream (the task runner expects an array) so we collect
        each row's bare object and append it.
        """
        from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

        e_fqn = escape_sql_string(table_fqn)
        check_col = self._oltp_sql.q("check")
        # Dialect-agnostic JSON projection via the executor's
        # :meth:`select_json_text` — ``to_json(col)`` on Delta,
        # bare column on Postgres (PgExecutor._to_text JSON-encodes
        # JSONB cells on the way out).
        check_text = self._oltp_sql.select_json_text(check_col)
        sql = (
            f"SELECT table_fqn, {check_text} AS check_json FROM {self._rules_table} "
            f"WHERE table_fqn = '{e_fqn}' AND status = 'approved'"
        )
        rows = self._oltp_sql.query(sql)
        if not rows:
            return None
        merged_checks: list[dict[str, Any]] = []
        for row in rows:
            try:
                parsed = json.loads(row[1], strict=False) if row[1] else None
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, dict):
                merged_checks.append(parsed)
            elif isinstance(parsed, list):
                # Defensive: pre-baseline rows wrapped the single check
                # in a one-element list.  Tolerate that on read so a
                # workspace that hasn't run ``DROP SCHEMA CASCADE``
                # against legacy data doesn't lose its rules.
                merged_checks.extend([c for c in parsed if isinstance(c, dict)])
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
            rows = self._oltp_sql.query(sql)
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
    # Stale tmp-view sweep (hourly)
    # ------------------------------------------------------------------

    async def _maybe_sweep_stale_tmp_views(self, now: datetime) -> None:
        """Drop tmp views whose runs finished but were never polled to cleanup."""
        if now < self._next_tmp_view_sweep_at:
            return
        self._next_tmp_view_sweep_at = now + timedelta(hours=_TMP_VIEW_SWEEP_INTERVAL_HOURS)
        try:
            await asyncio.to_thread(self._sweep_stale_tmp_views)
        except Exception:
            logger.exception("Tmp-view sweep failed (non-fatal)")

    def _sweep_stale_tmp_views(self) -> None:
        """Reap tmp views left behind when status polling never ran ``drop_view``.

        Three sources:
        1. Runs already marked terminal in ``dq_profiling_results`` /
           ``dq_validation_runs`` but whose view still exists.
        2. Rows still ``RUNNING`` in those tables whose Databricks job has
           already reached a terminal lifecycle state (abandoned poll).
        3. Age-based orphans (delegates to :meth:`_gc_orphan_views` logic
           with a shorter threshold) — catches views with no metadata row.
        """
        from databricks_labs_dqx_app.backend.run_status_manager import update_run_status
        from databricks_labs_dqx_app.backend.sql_utils import quote_fqn

        views_to_drop: set[str] = set()

        for table_name in ("dq_profiling_results", "dq_validation_runs"):
            table = f"`{self._catalog}`.`{self._schema}`.{table_name}"
            terminal_sql = (
                f"SELECT DISTINCT view_fqn FROM {table} "
                f"WHERE view_fqn IS NOT NULL AND status IN ('SUCCESS', 'FAILED', 'CANCELED')"
            )
            try:
                for row in self._sql.query(terminal_sql) or []:
                    fqn = row[0] if row else None
                    if isinstance(fqn, str) and fqn.strip():
                        views_to_drop.add(fqn.strip())
            except Exception as exc:
                logger.warning("Tmp-view sweep: failed to list terminal views from %s: %s", table_name, exc)

            running_sql = (
                f"SELECT run_id, view_fqn, CAST(job_run_id AS STRING) FROM {table} "
                f"WHERE status = 'RUNNING' AND view_fqn IS NOT NULL AND job_run_id IS NOT NULL "
                f"ORDER BY created_at DESC LIMIT {_TMP_VIEW_SWEEP_MAX_RUNS}"
            )
            try:
                for row in self._sql.query(running_sql) or []:
                    if not row or len(row) < 3:
                        continue
                    run_id, view_fqn, job_run_id_raw = row[0], row[1], row[2]
                    if not isinstance(view_fqn, str) or not view_fqn.strip():
                        continue
                    try:
                        job_run_id = int(job_run_id_raw)
                    except (TypeError, ValueError):
                        continue
                    try:
                        run = self._ws.jobs.get_run(job_run_id)
                        state = run.state
                        lifecycle = state.life_cycle_state.value if state and state.life_cycle_state else "UNKNOWN"
                    except Exception as exc:
                        logger.warning(
                            "Tmp-view sweep: could not fetch job status for run %s (job_run_id=%s): %s",
                            run_id,
                            job_run_id,
                            exc,
                        )
                        continue
                    if lifecycle not in {"TERMINATED", "INTERNAL_ERROR", "SKIPPED"}:
                        continue
                    views_to_drop.add(view_fqn.strip())
                    result_state = state.result_state.value if state and state.result_state else None
                    if result_state != "SUCCESS" and isinstance(run_id, str) and run_id:
                        new_status = "CANCELED" if result_state == "CANCELED" else "FAILED"
                        message = (state.state_message if state else None) or f"Run finished with state: {lifecycle}"
                        try:
                            from databricks_labs_dqx_app.backend.config import AppConfig

                            update_run_status(
                                self._sql,
                                AppConfig(catalog=self._catalog, schema_name=self._schema),
                                table_name,
                                run_id,
                                status=new_status,
                                error_message=message,
                            )
                        except Exception as exc:
                            logger.warning(
                                "Tmp-view sweep: failed to reconcile RUNNING row %s in %s: %s",
                                run_id,
                                table_name,
                                exc,
                            )
            except Exception as exc:
                logger.warning("Tmp-view sweep: failed to list RUNNING views from %s: %s", table_name, exc)

        dropped = 0
        failed = 0
        for view_fqn in sorted(views_to_drop):
            try:
                self._tmp_sql.execute(f"DROP VIEW IF EXISTS {quote_fqn(view_fqn)}")
                dropped += 1
            except Exception as exc:
                failed += 1
                logger.warning("Tmp-view sweep: failed to drop %s: %s", view_fqn, exc)

        if dropped or failed:
            logger.info("Tmp-view sweep complete: targeted=%d dropped=%d failed=%d", len(views_to_drop), dropped, failed)

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
    # Retention — daily DELETE sweep against high-volume tables
    # ------------------------------------------------------------------

    def _resolve_retention_days(self) -> int:
        """Return the configured retention window in days (>= 7).

        Looks up ``retention_days`` in ``dq_app_settings`` and falls back
        to :data:`_RETENTION_DAYS_DEFAULT` (90 days) when unset or
        unparseable. Capped at the lower bound :data:`_RETENTION_DAYS_MIN`
        so a misconfiguration can't accidentally wipe live data.
        """
        return self._resolve_setting_days("retention_days", _RETENTION_DAYS_DEFAULT)

    def _resolve_quarantine_retention_days(self) -> int:
        """Return the quarantine-specific retention window in days (>= 7).

        Quarantine rows hold the full source row payload (PII surface)
        so we maintain a separate, tighter default
        (:data:`_QUARANTINE_RETENTION_DAYS_DEFAULT`, 30 days) than the
        global retention.  Configurable via ``quarantine_retention_days``
        in ``dq_app_settings``.  Same min-floor protection as the global
        resolver.
        """
        return self._resolve_setting_days(
            "quarantine_retention_days",
            _QUARANTINE_RETENTION_DAYS_DEFAULT,
        )

    def _resolve_setting_days(self, key: str, default: int) -> int:
        """Read an integer-day setting from ``dq_app_settings``.

        Shared parsing/floor logic for the global and quarantine
        retention knobs.  Any read or parse failure falls back to
        *default*; the returned value is always >= :data:`_RETENTION_DAYS_MIN`
        so a misconfiguration can never wipe data inside the safety floor.
        """
        try:
            from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

            escaped_key = escape_sql_string(key)
            sql = f"SELECT setting_value FROM {self._settings_table} WHERE setting_key = '{escaped_key}'"  # noqa: S608
            rows = self._oltp_sql.query(sql)
            if rows and rows[0] and rows[0][0]:
                value = int(rows[0][0])
                return max(_RETENTION_DAYS_MIN, value)
        except Exception:
            logger.debug("Failed to read %s setting; falling back to default", key, exc_info=True)
        return default

    async def _maybe_run_retention(self, now: datetime) -> None:
        """Run the retention sweep if the daily timer has elapsed.

        Cheap to skip (one comparison) and runs in a background thread
        so it doesn't block the loop. Failures are logged but never
        fatal — the next tick re-tries.
        """
        if now < self._next_retention_at:
            return

        scheduled_for = self._next_retention_at
        # Advance the timer first so a slow sweep can't double-fire.
        self._next_retention_at = now + timedelta(hours=_RETENTION_INTERVAL_HOURS)
        logger.info(
            "Retention sweep: triggering daily cleanup (was due at %s); next run scheduled for %s",
            scheduled_for.isoformat(),
            self._next_retention_at.isoformat(),
        )
        try:
            await asyncio.to_thread(self._run_retention)
        except Exception:
            logger.exception("Retention sweep failed (non-fatal)")

    async def _maybe_run_tag_reconcile(self, now: datetime) -> None:
        """Run the apply-on-tag reconcile sweep if the timer has elapsed.

        No-op when no ``tag_reconcile_service`` was wired (legacy deployments,
        unit tests) or when the timer hasn't elapsed. The sweep is itself a
        no-op when the ``tag_auto_apply`` setting is off. Runs in a background
        thread so it doesn't block the loop. Failures are logged but never
        fatal — the next tick re-tries.
        """
        if self._tag_reconcile_service is None:
            return
        if now < self._next_tag_reconcile_at:
            return

        scheduled_for = self._next_tag_reconcile_at
        # Advance the timer first so a slow sweep can't double-fire.
        self._next_tag_reconcile_at = now + timedelta(hours=_TAG_RECONCILE_INTERVAL_HOURS)
        logger.info(
            "Tag-reconcile sweep: triggering apply-on-tag reconcile (was due at %s); next run scheduled for %s",
            scheduled_for.isoformat(),
            self._next_tag_reconcile_at.isoformat(),
        )
        try:
            await asyncio.to_thread(self._tag_reconcile_service.sweep, _SCHEDULER_SYSTEM_USER)
        except Exception:
            logger.exception("Tag-reconcile sweep failed (non-fatal)")

    async def _maybe_refresh_metadata_dims(self, now: datetime) -> None:
        """Full-refresh the metadata dims if the hourly timer has elapsed.

        No-op when no ``metadata_dim_service`` was wired (legacy deployments,
        unit tests). Cheap to skip (one comparison) and runs in a background
        thread so it doesn't block the loop. Failures are logged but never
        fatal — the next tick re-tries.
        """
        if self._metadata_dim_service is None:
            return
        if now < self._next_metadata_dim_refresh_at:
            return

        scheduled_for = self._next_metadata_dim_refresh_at
        # Advance the timer first so a slow refresh can't double-fire.
        self._next_metadata_dim_refresh_at = now + timedelta(hours=_METADATA_DIM_REFRESH_INTERVAL_HOURS)
        logger.info(
            "Metadata-dim refresh: triggering hourly rebuild (was due at %s); next run scheduled for %s",
            scheduled_for.isoformat(),
            self._next_metadata_dim_refresh_at.isoformat(),
        )
        try:
            await asyncio.to_thread(self._metadata_dim_service.refresh)
        except Exception:
            logger.exception("Metadata-dim refresh failed (non-fatal)")

    def _run_retention(self) -> None:
        """DELETE rows older than ``retention_days`` from each high-volume table.

        Each table is processed independently — a failure on one
        doesn't abort the others. The DELETE predicate uses an
        INTERVAL literal so the backend stamps the cutoff against its
        own clock (no Python-side time skew).

        Tables are split between the analytical Delta executor and
        the OLTP executor (Lakebase or Delta-fallback) because the
        ``INTERVAL`` syntax differs between dialects: Delta uses
        ``INTERVAL N DAY`` (no quotes); Postgres uses
        ``INTERVAL '<N> days'``.
        """
        days = self._resolve_retention_days()
        quarantine_days = self._resolve_quarantine_retention_days()
        logger.info(
            "Retention sweep: deleting rows older than %d days (quarantine: %d days)",
            days,
            quarantine_days,
        )

        total_deleted = 0
        # Delta tables — quoted with backticks so a future
        # special-character schema name doesn't break the DELETE.
        # ``dq_quarantine_records`` honours its own cutoff so PII row
        # payloads can be aged out faster than the trend tables.
        for table_name, time_col in _DELTA_RETENTION_TABLES:
            table = f"`{self._catalog}`.`{self._schema}`.{table_name}"
            cutoff = quarantine_days if table_name == _QUARANTINE_TABLE_NAME else days
            stmt = f"DELETE FROM {table} WHERE {time_col} < current_timestamp() - INTERVAL {cutoff} DAY"
            try:
                self._sql.execute(stmt)
                logger.info("Retention sweep (Delta): cleaned %s (cutoff=%dd)", table_name, cutoff)
                total_deleted += 1
            except Exception as exc:
                logger.warning("Retention sweep: %s failed (%s); continuing", table_name, exc)

        # OLTP tables — fqn(), q(), and the INTERVAL literal are all
        # delegated to the executor so the body stays dialect-agnostic.
        interval = self._oltp_sql.interval_days_expr(days)
        for table_name, time_col in _OLTP_RETENTION_TABLES:
            table = self._oltp_sql.fqn(table_name)
            stmt = f"DELETE FROM {table} WHERE {time_col} < CURRENT_TIMESTAMP - {interval}"
            try:
                self._oltp_sql.execute(stmt)
                logger.info("Retention sweep (OLTP): cleaned %s (cutoff=%dd)", table_name, days)
                total_deleted += 1
            except Exception as exc:
                logger.warning("Retention sweep: %s failed (%s); continuing", table_name, exc)

        logger.info("Retention sweep complete: %d table(s) processed", total_deleted)

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

            # Clamp the configured day to the actual number of days in the
            # target month (e.g. day 31 → 30 in April, 28/29 in February)
            # rather than always capping at 28 — capping made schedules set
            # for the 29th–31st silently fire on the 28th every month while
            # the UI still showed the configured day.
            def _clamp_day(year: int, month: int) -> int:
                return min(dom, calendar.monthrange(year, month)[1])

            candidate = after.replace(
                day=_clamp_day(after.year, after.month),
                hour=hour,
                minute=minute,
                second=0,
                microsecond=0,
            )
            if candidate <= after:
                if after.month == 12:
                    year, month = after.year + 1, 1
                else:
                    year, month = after.year, after.month + 1
                # Re-clamp for the next month before setting the day so a 31 →
                # 30/28 rollover doesn't raise ValueError on a short month.
                candidate = candidate.replace(year=year, month=month, day=_clamp_day(year, month))
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
