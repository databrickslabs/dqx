import asyncio
import os
from contextlib import asynccontextmanager
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import Environment
from databricks.sdk.service.jobs import JobEnvironment, JobSettings
from fastapi import FastAPI

from ._scheduler_registry import get_scheduler, set_scheduler
from .config import conf
from .dependencies import get_sp_ws, set_oltp_executor
from .logger import logger
from .migrations import MigrationRunner
from .migrations.postgres import PgMigrationRunner
from .routes import api_router
from .services.scheduler_service import SchedulerService
from .services.view_service import mark_tmp_schema_ready
from .sql_executor import SqlExecutor
from .utils import add_not_found_handler

_SCHEDULER_LOCK_PATH = Path("/tmp/.dqx_scheduler.lock")  # noqa: S108


def _try_acquire_scheduler_lease() -> bool:
    """Use an exclusive file lock so only one uvicorn worker runs the scheduler.

    The lock file is held for the lifetime of the process; when the worker
    exits the OS releases it automatically.
    """
    import fcntl

    try:
        fd = os.open(str(_SCHEDULER_LOCK_PATH), os.O_CREAT | os.O_RDWR)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        # Keep fd open (and thus the lock held) for the process lifetime.
        # Store on module so it isn't garbage-collected.
        globals()["_scheduler_lock_fd"] = fd
        return True
    except OSError:
        return False


def _find_wheels() -> list[Path]:
    """Return wheel files found in candidate directories.

    Databricks Apps sets the working directory to the source code directory, but
    we also walk up from this file's location as a fallback (handles the case
    where the package is installed in a venv inside the source tree).
    For local dev (apx dev start), the DAB build places DQX wheels in .build/
    relative to the app source root rather than in the cwd itself.
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
    # The block is entirely best-effort: if Lakebase is configured but
    # the instance is misconfigured/down, we log loudly and fall back
    # to UC-only mode so the app still serves. This matches how the
    # rest of startup degrades (volume sync, scheduler) — partial
    # functionality beats a hard crash loop.
    # ------------------------------------------------------------------
    pg_executor = None
    if conf.lakebase_enabled:
        try:
            from .pg_executor import build_pg_executor

            pg_executor = await asyncio.to_thread(
                build_pg_executor,
                sp_ws,
                instance_name=conf.lakebase_instance_name,
                database=conf.lakebase_database_name,
                schema=conf.lakebase_schema_name,
                token_refresh_minutes=conf.lakebase_token_refresh_minutes,
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
            logger.exception(
                "Lakebase initialisation failed — falling back to Delta for OLTP tables. "
                "Verify the database_instance is provisioned and the app SP has CAN_CONNECT_AND_CREATE."
            )
            pg_executor = None
            set_oltp_executor(None)
    else:
        logger.info("Lakebase not configured (DQX_LAKEBASE_INSTANCE_NAME is empty). " "OLTP tables will live on Delta.")
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
        logger.warning("DQX_WHEELS_VOLUME or DQX_JOB_ID not set — task-runner job wheels will not be synced")
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
        except Exception:  # noqa: BLE001
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
