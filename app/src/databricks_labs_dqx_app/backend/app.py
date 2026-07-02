import asyncio
import os
from contextlib import asynccontextmanager
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import Environment
from databricks.sdk.service.jobs import JobEnvironment, JobSettings
from fastapi import FastAPI

from ._scheduler_registry import get_scheduler, set_scheduler
from .builtin_rules_seed import seed_builtin_rules_if_absent
from .config import conf
from .dependencies import get_sp_ws, set_oltp_executor
from .logger import logger
from .migrations import MigrationRunner

# ``PgMigrationRunner`` is safe to import at module load: its module
# (``migrations.postgres``) now imports the trust-boundary helpers
# from the psycopg-free :mod:`backend.pg_cursor_helpers` module
# rather than from :mod:`backend.pg_executor`, so loading it does
# NOT transitively pull in :mod:`psycopg`. The remaining lazy import
# (``build_pg_executor``) inside :func:`lifespan` is the one that
# genuinely needs psycopg at runtime, and stays lazy for the
# Delta-only test environments that don't install it.
from .migrations.postgres import PgMigrationRunner
from .routes import api_router
from .services.app_settings_service import AppSettingsService
from .services.registry_service import RegistryService
from .services.scheduler_service import SchedulerService
from .services.view_service import mark_tmp_schema_ready
from .sql_executor import SqlExecutor
from .utils import add_not_found_handler

_SCHEDULER_LOCK_PATH = Path("/tmp/.dqx_scheduler.lock")  # noqa: S108

# Module-level reference that pins the lock file descriptor for the
# process lifetime. The fd MUST stay live as long as we hold the
# advisory lock — letting it get garbage-collected would silently
# close the fd and release the flock, after which a second uvicorn
# worker could grab the lease and we'd end up with two schedulers
# fighting over the same job. Stored as a plain module global rather
# than via ``globals()[...]`` so the dependency is greppable and the
# type-checker can see it.
_scheduler_lock_fd: int | None = None


def _try_acquire_scheduler_lease() -> bool:
    """Use an exclusive file lock so only one uvicorn worker runs the scheduler.

    The lock file is held for the lifetime of the process; when the worker
    exits the OS releases it automatically.

    .. WARNING::
       This lease is **process-local**: ``fcntl.flock`` only excludes other
       processes on the same host. If DQX Studio is ever scaled to more
       than one Databricks App replica/container, every replica will
       acquire its own lock and run ``_tick()`` independently — the same
       due schedule will be submitted N times. The current deployment
       relies on Databricks Apps running a single container; if that ever
       changes, replace this with a cross-replica lease (e.g. a Postgres
       advisory lock on ``DQX_LAKEBASE_SCHEMA_NAME`` keyed by
       ``hashtext('dqx-scheduler')`` with ``pg_try_advisory_lock``, or a
       lease row in ``dq_app_settings`` with TTL + ``FOR UPDATE
       SKIP LOCKED``). See backend audit finding B1. The file lock is
       still needed as a fallback for multi-worker uvicorn within one
       container.
    """
    import fcntl

    global _scheduler_lock_fd
    try:
        fd = os.open(str(_SCHEDULER_LOCK_PATH), os.O_CREAT | os.O_RDWR)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        _scheduler_lock_fd = fd
        return True
    except OSError:
        return False


def _find_wheels() -> list[Path]:
    """Return wheel files found in candidate directories.

    Databricks Apps sets the working directory to the source code directory, but
    we also walk up from this file's location as a fallback (handles the case
    where the package is installed in a venv inside the source tree).
    For local dev (``make app-start-dev`` / ``scripts/dev.py``), the DAB build
    places DQX wheels in .build/ relative to the app source root rather than
    in the cwd itself.
    """
    cwd = Path.cwd()
    search_roots: list[Path] = [cwd]
    if (cwd / ".build").is_dir():
        search_roots.append(cwd / ".build")
    for parent in Path(__file__).resolve().parents:
        if parent not in search_roots and (parent / "requirements.txt").exists():
            search_roots.append(parent)
            if (parent / ".build").is_dir():
                search_roots.append(parent / ".build")
            break

    logger.info("Searching for wheel files in: %s", [str(p) for p in search_roots])

    seen: set[Path] = set()
    wheels: list[Path] = []
    for root in search_roots:
        for wheel in sorted(root.glob("databricks_labs_dqx-*.whl")):
            if wheel not in seen:
                seen.add(wheel)
                wheels.append(wheel)
        for wheel in sorted(root.glob("tasks/databricks_labs_dqx_task_runner-*.whl")):
            if wheel not in seen:
                seen.add(wheel)
                wheels.append(wheel)
    return wheels


def _compute_wheels_hash(wheels: list[Path]) -> str:
    """Return a hex digest that changes when any wheel file changes."""
    import hashlib

    h = hashlib.sha256()
    for wheel_path in sorted(wheels):
        h.update(wheel_path.name.encode())
        h.update(wheel_path.stat().st_size.to_bytes(8, "big"))
        with open(wheel_path, "rb") as f:
            while chunk := f.read(1 << 16):
                h.update(chunk)
    return h.hexdigest()


def _read_remote_hash(sp_ws: WorkspaceClient, volume_path: str) -> str | None:
    """Read the previously uploaded wheels hash from a marker file on the volume."""
    marker = f"{volume_path}/.wheels_hash"
    try:
        resp = sp_ws.files.download(marker)
        if resp.contents is None:
            return None
        return resp.contents.read().decode().strip()
    except Exception:
        return None


def _write_remote_hash(sp_ws: WorkspaceClient, volume_path: str, digest: str) -> None:
    """Write the wheels hash marker file to the volume."""
    import io

    marker = f"{volume_path}/.wheels_hash"
    sp_ws.files.upload(marker, io.BytesIO(digest.encode()), overwrite=True)


async def _upload_wheels_to_volume(sp_ws: WorkspaceClient, volume_path: str) -> tuple[list[str], bool]:
    """Upload DQX wheel files to a UC Volume using their real versioned filenames.

    Skips upload when the local wheel content hash matches the remote marker.
    Returns (volume_paths, changed) — *changed* is False when the upload was skipped.
    """
    wheels = _find_wheels()
    if not wheels:
        logger.warning("No wheel files found — skipping volume upload (cwd: %s)", Path.cwd())
        return [], False

    local_hash = _compute_wheels_hash(wheels)
    remote_hash = await asyncio.to_thread(_read_remote_hash, sp_ws, volume_path)

    if local_hash == remote_hash:
        logger.info("Wheel content hash unchanged (%s…) — skipping upload", local_hash[:12])
        return [f"{volume_path}/{w.name}" for w in wheels], False

    uploaded: list[str] = []
    for wheel_path in wheels:
        dest = f"{volume_path}/{wheel_path.name}"
        logger.info("Uploading %s → %s", wheel_path.name, dest)
        with open(wheel_path, "rb") as f:
            await asyncio.to_thread(sp_ws.files.upload, dest, f, overwrite=True)
        logger.info("Uploaded %s", dest)
        uploaded.append(dest)

    await asyncio.to_thread(_write_remote_hash, sp_ws, volume_path, local_hash)
    logger.info("Wrote wheels hash marker: %s…", local_hash[:12])

    return uploaded, True


async def _update_job_wheels(sp_ws: WorkspaceClient, job_id: str, wheel_paths: list[str]) -> None:
    """Update the task-runner job environment to install wheels from the volume.

    Called after every successful wheel upload so the job always references the
    version that matches the running app.
    """
    env = JobEnvironment(
        environment_key="default",
        spec=Environment(client="5", dependencies=wheel_paths),
    )
    await asyncio.to_thread(
        sp_ws.jobs.update,
        job_id=int(job_id),
        new_settings=JobSettings(environments=[env]),
    )
    logger.info("Updated job %s environment with wheels: %s", job_id, wheel_paths)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"Starting app with configuration:\n{conf.model_dump_json(indent=2)}")

    # Service-principal auth and database migrations are required for the app to
    # function correctly.  Failure here is fatal: a partial-state app silently
    # surfaces confusing SQL errors on every request, so we'd rather fail loudly
    # at startup and let the platform restart us / page the operator.
    sp_ws = await get_sp_ws()
    wh_id = os.environ.get("DATABRICKS_WAREHOUSE_ID") or os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID") or ""
    sp_sql = SqlExecutor(ws=sp_ws, warehouse_id=wh_id, catalog=conf.catalog, schema=conf.schema_name)

    # ------------------------------------------------------------------
    # Lakebase (optional) — open the pool, run Postgres migrations, and
    # register the executor as the OLTP backend used by service DI.
    #
    # When ``lakebase_enabled`` is true the operator has explicitly
    # chosen Lakebase as the OLTP store: rules, schedules, RBAC,
    # comments, and app settings live in the Postgres schema, NOT in
    # Delta. Silently falling back to Delta on a transient init
    # failure (network blip, OAuth issuance, momentary
    # CAN_CONNECT_AND_CREATE drop) would split-brain the deployment —
    # writes during the flap land in the empty Delta fallback tables
    # while the canonical Postgres rows become invisible, and prior
    # data reappears after the next restart with anything written in
    # the interim orphaned. That's silent data loss, not graceful
    # degradation, so we follow the same fail-loud-and-let-the-
    # platform-restart-us pattern as the SP/migrations block above:
    # raise, let the Databricks Apps platform restart the container,
    # and surface the underlying problem via restart-loop alerting.
    # The opt-out is intentional and explicit (unset
    # ``DQX_LAKEBASE_INSTANCE_NAME``); a flap is not an opt-out.
    # The legitimate Delta-only path runs in the ``else`` branch below.
    # ------------------------------------------------------------------
    pg_executor = None
    if conf.lakebase_enabled:
        try:
            # ``build_pg_executor`` lives in :mod:`backend.pg_executor`,
            # which imports :mod:`psycopg` at module load. That import
            # is fine in production (the Lakebase-enabled path) but
            # would break Delta-only test environments that don't
            # install psycopg, so we defer it to this branch.
            # ``PgMigrationRunner`` itself is already imported at
            # module load — it routes through the psycopg-free
            # :mod:`backend.pg_cursor_helpers` module.
            from .pg_executor import build_pg_executor

            pg_executor = await asyncio.to_thread(
                build_pg_executor,
                sp_ws,
                instance_name=conf.lakebase_instance_name,
                database=conf.lakebase_database_name,
                schema=conf.lakebase_schema_name,
                token_refresh_minutes=conf.lakebase_token_refresh_minutes,
                token_refresh_retry_seconds=conf.lakebase_token_refresh_retry_seconds,
                token_refresh_retry_jitter=conf.lakebase_token_refresh_retry_jitter,
                token_refresh_max_failures=conf.lakebase_token_refresh_max_failures,
                pool_min_size=conf.lakebase_pool_min_size,
                pool_max_size=conf.lakebase_pool_max_size,
            )
            pg_runner = PgMigrationRunner(pg_executor)
            pg_applied = await asyncio.to_thread(pg_runner.run_all)
            if pg_applied:
                logger.info("Applied %d Lakebase migration(s)", pg_applied)
            else:
                logger.info("Lakebase schema is up to date")
            set_oltp_executor(pg_executor)
            logger.info(
                "Lakebase OLTP routing enabled (instance=%s, database=%s, schema=%s)",
                conf.lakebase_instance_name,
                conf.lakebase_database_name,
                conf.lakebase_schema_name,
            )
        except Exception:
            # Close any partially-built pool before re-raising so a
            # restart loop doesn't accumulate orphaned server-side
            # Postgres connections on every flap. ``close()`` is
            # idempotent and best-effort (see pg_executor.close).
            if pg_executor is not None:
                try:
                    await asyncio.to_thread(pg_executor.close)
                except Exception:
                    # Best-effort cleanup inside the OUTER ``except`` —
                    # we're already about to re-raise the init failure,
                    # so a close-time exception here would mask the real
                    # root cause from the operator. Log and let the
                    # outer raise propagate. (Same resilience contract
                    # as :meth:`pg_executor.PgExecutor.close`; see the
                    # BLE001 policy in pyproject.toml.)
                    logger.warning("Error closing Lakebase pool during init failure", exc_info=True)
            set_oltp_executor(None)
            logger.exception(
                "Lakebase initialisation failed (instance=%s, database=%s, schema=%s). "
                "Refusing to start — silent fallback to Delta would split OLTP writes across "
                "two physical stores and orphan prior Lakebase data on every flap. "
                "Common causes: the database_instance is not provisioned, the app SP lacks "
                "CAN_CONNECT_AND_CREATE on the bound database, OAuth token issuance is failing, "
                "or the Lakebase endpoint is transiently unreachable. Fix the underlying issue "
                "and the platform will restart this container automatically. To intentionally "
                "run on Delta only, unset DQX_LAKEBASE_INSTANCE_NAME.",
                conf.lakebase_instance_name,
                conf.lakebase_database_name,
                conf.lakebase_schema_name,
            )
            raise
    else:
        logger.info("Lakebase not configured (DQX_LAKEBASE_INSTANCE_NAME is empty). OLTP tables will live on Delta.")
        set_oltp_executor(None)

    # Delta migrations always run, but the OLTP fallback DDL is
    # skipped when Lakebase owns those tables — the same data model
    # is created in Postgres above.
    runner = MigrationRunner(sql=sp_sql)
    applied = runner.run_all(include_oltp_fallback=pg_executor is None)
    if applied:
        logger.info("Applied %d Delta migration(s)", applied)
    else:
        logger.info("Delta schema is up to date")

    # Best-effort below — the app can recover from these failing.

    # Seed the run-review-status catalogue once, here at startup, rather
    # than lazily on first read. This keeps ``get_run_review_statuses``
    # (called on the Runs listing GET path) side-effect free. Best-effort:
    # if the write fails the read path still returns the seed virtually,
    # so the feature degrades gracefully until an admin saves the list.
    try:
        oltp_for_seed = pg_executor if pg_executor is not None else sp_sql
        settings_for_seed = AppSettingsService(sql=oltp_for_seed)
        settings_for_seed.seed_run_review_statuses_if_absent()
    except Exception as seed_e:
        logger.warning("Could not seed default run_review_statuses: %s", seed_e, exc_info=True)

    # Seed the reserved dimension/severity label-definition keys (Rules
    # Registry Phase 1 — dimensions & severity are TAGS in the existing
    # ``label_definitions`` catalog, not new tables). Idempotent and
    # best-effort for the same reason as run-review-statuses above.
    try:
        oltp_for_label_seed = pg_executor if pg_executor is not None else sp_sql
        AppSettingsService(sql=oltp_for_label_seed).seed_reserved_label_definitions_if_absent()
    except Exception as seed_e:
        logger.warning("Could not seed reserved label definitions: %s", seed_e, exc_info=True)

    # Seed every built-in DQX check function as a pre-published Rules
    # Registry rule (Rules Registry Phase 2C). Idempotent — re-running never
    # duplicates or overwrites an admin-edited built-in rule, see
    # ``builtin_rules_seed.seed_builtin_rules_if_absent``. Best-effort for
    # the same reason as the seeds above: the registry list/create routes
    # still function without built-ins present, just with an empty catalog
    # until the next successful restart.
    try:
        oltp_for_registry_seed = pg_executor if pg_executor is not None else sp_sql
        seeded_count = seed_builtin_rules_if_absent(RegistryService(sql=oltp_for_registry_seed))
        if seeded_count:
            logger.info("Seeded %d built-in registry rule(s) at startup", seeded_count)
    except Exception as seed_e:
        logger.warning("Could not seed built-in registry rules: %s", seed_e, exc_info=True)

    try:
        tmp_cat = conf.catalog.replace("`", "")
        tmp_sch = conf.tmp_schema_name.replace("`", "")
        sp_sql.execute_no_schema(f"CREATE SCHEMA IF NOT EXISTS `{tmp_cat}`.`{tmp_sch}`")
        mark_tmp_schema_ready()
        logger.info("Ensured tmp schema exists: %s.%s", tmp_cat, tmp_sch)
    except Exception as tmp_e:
        logger.warning("Could not create tmp schema %s.%s: %s", conf.catalog, conf.tmp_schema_name, tmp_e)

    try:
        cat = conf.catalog.replace("`", "")
        sp_sql.execute_no_schema(f"GRANT USE CATALOG ON CATALOG `{cat}` TO `account users`")
        logger.info("Granted USE CATALOG on %s to account users", cat)
    except Exception as grant_e:
        logger.warning(
            "Could not grant USE CATALOG on %s: %s (users may need this granted manually)", conf.catalog, grant_e
        )

    if not (conf.wheels_volume and conf.job_id):
        msg = (
            "DQX_WHEELS_VOLUME or DQX_JOB_ID is not set — profiler, dry-run, "
            "and scheduler features will be unavailable"
        )
        if conf.require_task_runner:
            # Production-style deploy (bundle sets DQX_REQUIRE_TASK_RUNNER=1):
            # treat a missing binding as a fatal misconfiguration so we
            # crash loop with an actionable error instead of silently
            # serving a half-broken app.
            raise RuntimeError(
                f"{msg}. Both env vars are required when DQX_REQUIRE_TASK_RUNNER=1. "
                "Check the bundle resource bindings (dqx-task-runner-job, dqx-wheels) "
                "and re-run `databricks bundle deploy`."
            )
        logger.warning("%s (set DQX_REQUIRE_TASK_RUNNER=1 in production to fail fast)", msg)
    else:
        wheel_volume_paths: list[str] = []
        try:
            wheel_volume_paths, _ = await _upload_wheels_to_volume(sp_ws, conf.wheels_volume)
        except Exception as e:
            logger.warning(
                "Could not upload wheels to volume — job environment will not be updated: %s", e, exc_info=True
            )

        if wheel_volume_paths:
            try:
                await _update_job_wheels(sp_ws, conf.job_id, wheel_volume_paths)
            except Exception as e:
                logger.warning("Could not update job environment: %s", e, exc_info=True)

    if not conf.job_id:
        logger.info("Scheduler not started (missing JOB_ID)")
    elif os.environ.get("DQX_SCHEDULER_DISABLED") == "1":
        logger.info("Scheduler disabled via DQX_SCHEDULER_DISABLED=1")
    elif not _try_acquire_scheduler_lease():
        logger.info("Scheduler lease held by another worker — skipping")
    else:
        try:
            _scheduler = SchedulerService(
                ws=sp_ws,
                warehouse_id=os.environ.get("DATABRICKS_WAREHOUSE_ID")
                or os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID")
                or "",
                catalog=conf.catalog,
                schema=conf.schema_name,
                tmp_schema=conf.tmp_schema_name,
                job_id=conf.job_id,
                oltp_sql=pg_executor,
            )
            set_scheduler(_scheduler)
            _scheduler.start()
            logger.info(
                "Scheduler background task started (job_id=%s, warehouse=%s)",
                conf.job_id,
                os.environ.get("DATABRICKS_WAREHOUSE_ID") or os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID") or "(empty)",
            )
        except Exception as e:
            logger.warning("Could not start scheduler: %s", e, exc_info=True)

    yield

    sched = get_scheduler()
    if sched is not None:
        await sched.stop()
        set_scheduler(None)

    # Close the Lakebase pool last so any in-flight writes from
    # ``sched.stop()`` finish first.
    if pg_executor is not None:
        try:
            await asyncio.to_thread(pg_executor.close)
            logger.info("Lakebase connection pool closed")
        except Exception:
            # Lifespan shutdown: same resilience contract as
            # :meth:`pg_executor.PgExecutor.close` itself. A raise here
            # would prevent the ``set_oltp_executor(None)`` reset below
            # from running, leaving a closed pool wired in for the next
            # request and producing a confusing "operation on closed
            # pool" error instead of the underlying close failure.
            # See the BLE001 policy block in pyproject.toml.
            logger.warning("Error closing Lakebase pool", exc_info=True)
        set_oltp_executor(None)


app = FastAPI(title=f"{conf.app_name}", lifespan=lifespan)

# Configure route for the backend API (/api) using fastapi
app.include_router(api_router)

# Configure route for the UI (static files)
# Mount static files for the UI only if the dist directory exists (e.g., after build)
# This allows the API to work in test environments without requiring a frontend build
if conf.static_assets_path.exists():
    from .spa_static import SPAStaticFiles

    ui = SPAStaticFiles(directory=conf.static_assets_path, html=True)
    app.mount("/", ui)
else:
    logger.warning(f"Static assets path {conf.static_assets_path} not found. UI will not be available.")

add_not_found_handler(app)
