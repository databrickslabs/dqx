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
from .dependencies import (
    get_app_settings_service,
    get_binding_run_service,
    get_data_product_service,
    get_job_service,
    get_materializer,
    get_monitored_table_service,
    get_monitored_table_version_service,
    get_registry_service,
    get_rules_catalog_service,
    get_run_set_service,
    get_sp_ws,
    get_view_service,
    set_oltp_executor,
)
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
from .services.binding_run_service import BindingRunService
from .services.data_product_service import DataProductService
from .services.entitlement_service import FAILING_ROWS_VIEW_NAME, EntitlementService
from .services.metadata_dim_service import MetadataDimService
from .services.monitored_table_service import MonitoredTableService
from .services.registry_service import RegistryService
from .services.rule_embeddings import RuleEmbeddingsService
from .services.scheduler_service import SchedulerService
from .services.score_cache_service import ScoreCacheService
from .services.score_view_service import (
    ASOF_VIEW_NAME,
    ATTRIBUTION_VIEW_NAME,
    METRIC_VIEW_NAME,
    SHAPING_VIEW_NAME,
    ScoreViewService,
)
from .services.vector_store import VectorStoreProvisioner
from .services.view_service import mark_tmp_schema_ready
from .sql_executor import OltpExecutorProtocol, SqlExecutor
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


async def _build_scheduler_data_product_service(
    sp_ws: WorkspaceClient,
    sp_sql: SqlExecutor,
    oltp: OltpExecutorProtocol,
) -> tuple[DataProductService, BindingRunService]:
    """Wire the scheduler's product-tick + table-tick collaborators (Task 5 / P21 item 14).

    Mirrors the FastAPI dependency chain in ``dependencies.py``
    (``get_data_product_service`` and its transitive collaborators) but
    calls the factories directly with explicit arguments — there is no
    per-request/OBO context at startup. ``get_view_service`` normally
    splits view-creation credentials (OBO) from schema-DDL credentials
    (SP); here both legs are pinned to the SP executor, matching how the
    existing scope-config scheduler path already creates its own views
    with SP credentials (``SchedulerService._create_view``/
    ``_create_view_from_sql``) rather than a calling user's OBO token.
    """
    monitored_tables = await get_monitored_table_service(sql=oltp, profiling_sql=sp_sql)
    registry = await get_registry_service(sql=oltp)
    app_settings = await get_app_settings_service(sql=oltp)
    rules_catalog = await get_rules_catalog_service(sql=oltp)
    materializer = await get_materializer(
        sql=oltp, registry=registry, monitored_tables=monitored_tables, app_settings=app_settings
    )
    version_service = await get_monitored_table_version_service(
        sql=oltp, monitored_tables=monitored_tables, rules_catalog=rules_catalog
    )
    view_service = await get_view_service(sql=sp_sql, sp_sql=sp_sql)
    job_service = await get_job_service(sp_ws=sp_ws, sql=sp_sql)
    run_set_service = await get_run_set_service(sql=oltp, validation_sql=sp_sql)
    binding_run_service = await get_binding_run_service(
        monitored_tables=monitored_tables,
        version_service=version_service,
        materializer=materializer,
        view_service=view_service,
        job_service=job_service,
        run_set_service=run_set_service,
        settings_service=app_settings,
        sp_sql=sp_sql,
    )
    data_product_service = await get_data_product_service(
        sql=oltp,
        monitored_tables=monitored_tables,
        run_set_service=run_set_service,
        binding_run_service=binding_run_service,
        version_service=version_service,
        app_settings=app_settings,
    )
    return data_product_service, binding_run_service


def _ensure_score_views(sp_sql: SqlExecutor) -> None:
    """Create/refresh the DQ score shaping + metric views (best-effort).

    Runs after the Delta migrations so ``dq_metrics`` is guaranteed to
    exist, and uses CREATE OR REPLACE on every startup so view
    definition changes ship with the app. Best-effort: a warehouse that
    cannot create metric views (or a transient DDL failure) degrades to
    failing dq-score endpoints rather than a crash-looping app — same
    contract as the other post-migration startup steps.
    """
    try:
        service = ScoreViewService(sql=sp_sql)
        service.ensure_views()
        logger.info(
            "Ensured DQ score views exist: %s and %s",
            service.shaping_view_fqn_quoted,
            service.metric_view_fqn_quoted,
        )
    except Exception as e:
        logger.warning(
            "Could not create the DQ score views over dq_metrics — the dq-score "
            "endpoints will fail until the next successful startup: %s",
            e,
            exc_info=True,
        )


def _ensure_metadata_dims(sp_sql: SqlExecutor, oltp: OltpExecutorProtocol) -> None:
    """Full-refresh the rule + monitored-table metadata dims (best-effort).

    Runs after ``_ensure_score_views`` so the Genie space's authoring/
    ownership data sources (``dim_dq_rules`` / ``dim_dq_monitored_tables``)
    exist and are populated from the Rules Registry — same best-effort
    contract as the score views: a warehouse hiccup or transient DDL failure
    degrades to stale/empty dims (and Genie answering those questions less
    well) rather than a crash-looping app. The scheduler re-refreshes them
    hourly thereafter.
    """
    try:
        registry = RegistryService(sql=oltp)
        monitored_tables = MonitoredTableService(sql=oltp, profiling_sql=sp_sql)
        MetadataDimService(sp_sql=sp_sql, registry=registry, monitored_tables=monitored_tables).refresh()
        logger.info("Ensured DQ metadata dims exist and are refreshed")
    except Exception as e:
        logger.warning(
            "Could not refresh the DQ metadata dims — Genie authoring/ownership "
            "questions may be stale until the next successful refresh: %s",
            e,
            exc_info=True,
        )


def _ensure_entitlement_objects(sp_sql: SqlExecutor) -> None:
    """Create/refresh the entitlement cache table + gated failing-rows view (best-effort).

    Runs after the Delta migrations (``dq_quarantine_records`` must exist for
    the view) alongside ``_ensure_score_views`` — same best-effort contract:
    a DDL failure degrades to Genie row-level questions returning nothing
    rather than a crash-looping app. The entitlement table MUST be a UC
    Delta object (the dynamic view references it with definer's rights), so
    it is deliberately NOT part of the Lakebase/OLTP data model.
    """
    try:
        service = EntitlementService(sql=sp_sql)
        service.ensure_objects()
        logger.info(
            "Ensured entitlement objects exist: %s and %s",
            service.entitlements_table_fqn_quoted,
            service.failing_rows_view_fqn_quoted,
        )
    except Exception as e:
        logger.warning(
            "Could not create the entitlement table / gated failing-rows view — "
            "Genie row-level access will stay closed until the next successful startup: %s",
            e,
            exc_info=True,
        )


# The user-facing read surface for OBO Genie: the score views (including
# the as-of expansion behind average-over-time questions) + the gated
# failing-rows view. The entitlement table is deliberately absent — it is
# SP-only (the dynamic view reads it with definer's rights; user emails
# inside are not for general reading).
_USER_READABLE_VIEWS = (
    METRIC_VIEW_NAME,
    SHAPING_VIEW_NAME,
    ASOF_VIEW_NAME,
    ATTRIBUTION_VIEW_NAME,
    FAILING_ROWS_VIEW_NAME,
)


def _grant_user_view_access(sp_sql: SqlExecutor) -> None:
    """GRANT the read path for OBO Genie to ``account users`` (best-effort).

    Once Genie conversations run as the calling user (Phase 4), every user
    needs USE SCHEMA on the app schema plus SELECT on the five views the
    space queries — same precedent as the startup ``GRANT USE CATALOG``
    below. Row-level protection does NOT depend on these grants: the
    failing-rows view carries its own current_user() entitlement gate, and
    the other four views are aggregate-only by design. Each statement is
    individually best-effort so one failing grant cannot block the rest.
    """
    cat = conf.catalog.replace("`", "")
    sch = conf.schema_name.replace("`", "")
    statements = [
        f"GRANT USE SCHEMA ON SCHEMA `{cat}`.`{sch}` TO `account users`",
        *(
            f"GRANT SELECT ON TABLE `{cat}`.`{sch}`.{view_name} TO `account users`"
            for view_name in _USER_READABLE_VIEWS
        ),
    ]
    for stmt in statements:
        try:
            sp_sql.execute_no_schema(stmt)
        except Exception as grant_e:
            logger.warning("Startup grant failed (%s): %s (users may need this granted manually)", stmt, grant_e)


def _ensure_genie_space(sp_ws: WorkspaceClient, warehouse_id: str, settings_sql: OltpExecutorProtocol) -> None:
    """Provision (or update) the Ask-Genie space over the score views (best-effort).

    Runs after ``_ensure_score_views`` so the objects the space points at
    exist. ``ensure_dq_genie_space`` itself is idempotent (config-hash no-op /
    in-place PATCH / find-or-create by title) and never raises; this wrapper
    only guards the collaborator wiring around it. Requires a warehouse to
    bind a freshly-created space to — skipped (with a log) when none is
    bound, same contract as the other best-effort startup steps.
    """
    try:
        from .services.genie_space_service import ensure_dq_genie_space

        if not warehouse_id:
            logger.info("Genie space provisioning skipped: no SQL warehouse bound (DATABRICKS_WAREHOUSE_ID)")
            return
        settings = AppSettingsService(sql=settings_sql)
        try:
            parent_path = f"/Users/{sp_ws.current_user.me().user_name}"
        except Exception:
            # Best-effort: the parent folder is cosmetic — fall back to a
            # location every workspace has rather than skip provisioning.
            parent_path = "/Shared"
        ensure_dq_genie_space(
            settings=settings,
            ws=sp_ws,
            warehouse_id=warehouse_id,
            parent_path=parent_path,
            catalog=conf.catalog,
            schema=conf.schema_name,
        )
    except Exception as e:
        logger.warning("Could not provision the DQ Genie space: %s", e, exc_info=True)


def _maybe_start_vector_store_provisioning(
    app: FastAPI,
    *,
    sp_ws: WorkspaceClient,
    sp_sql: SqlExecutor,
    pg_executor: OltpExecutorProtocol | None,
) -> None:
    """Fire-and-forget kick-off of Vector Search endpoint/index provisioning.

    Best-effort, non-blocking (Rules Registry Phase 7F; auto-derived settings
    since Phase 8B). ``ensure_vector_store`` itself never raises, but it's
    additionally fired via ``create_task`` (not awaited) so a slow or
    unreachable Vector Search control plane can never delay startup. Gated on
    the AI kill-switch (not just "settings configured", since embedding/VS
    names now always resolve to an auto-derived default) so a fresh deploy
    with AI left off never creates Vector Search infrastructure nobody asked
    for. The suggester keeps reporting ``available=False`` until the index
    reports ONLINE. The task is stashed on ``app.state`` (not just a local
    variable) so it isn't garbage-collected mid-flight — same rationale as
    ``CacheFactory.set_fire_and_forget``.

    *pg_executor* is typed as ``OltpExecutorProtocol`` (rather than the
    concrete ``PgExecutor``) so this module never needs to import ``psycopg``
    at load time — the same rationale as ``SchedulerService.__init__``'s
    ``oltp_sql`` parameter.
    """
    try:
        oltp_for_vs = pg_executor if pg_executor is not None else sp_sql
        vs_app_settings = AppSettingsService(sql=oltp_for_vs)
        if vs_app_settings.get_ai_enabled():
            vs_embeddings = RuleEmbeddingsService(sql=oltp_for_vs, sp_ws=sp_ws, app_settings=vs_app_settings)
            vs_registry = RegistryService(sql=oltp_for_vs)
            vs_provisioner = VectorStoreProvisioner(
                sp_ws=sp_ws, app_settings=vs_app_settings, embeddings=vs_embeddings, registry=vs_registry
            )
            app.state.vector_store_startup_task = asyncio.create_task(vs_provisioner.ensure_vector_store())
        else:
            logger.debug("AI features disabled; skipping Vector Search auto-provisioning at startup")
    except Exception as e:
        logger.warning("Could not kick off Vector Search auto-provisioning: %s", e, exc_info=True)


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
    # ``DQX_LAKEBASE_ENDPOINT``); a flap is not an opt-out.
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
                endpoint=conf.lakebase_endpoint,
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
                "Lakebase OLTP routing enabled (endpoint=%s, database=%s, schema=%s)",
                conf.lakebase_endpoint,
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
                "Lakebase initialisation failed (endpoint=%s, database=%s, schema=%s). "
                "Refusing to start — silent fallback to Delta would split OLTP writes across "
                "two physical stores and orphan prior Lakebase data on every flap. "
                "Common causes: the project branch/endpoint is not provisioned, the app SP "
                "lacks a Postgres role on the branch, OAuth token issuance is failing, "
                "or the Lakebase endpoint is transiently unreachable. Fix the underlying issue "
                "and the platform will restart this container automatically. To intentionally "
                "run on Delta only, unset DQX_LAKEBASE_ENDPOINT.",
                conf.lakebase_endpoint,
                conf.lakebase_database_name,
                conf.lakebase_schema_name,
            )
            raise
    else:
        logger.info("Lakebase not configured (DQX_LAKEBASE_ENDPOINT is empty). OLTP tables will live on Delta.")
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

    # DQ score views (shaping view + UC metric view over dq_metrics) —
    # must come after the Delta migrations above so dq_metrics exists.
    _ensure_score_views(sp_sql)

    # Rule + monitored-table metadata dims (P8.1) — SP-owned UC tables the
    # Genie space queries for authoring/ownership questions, full-refreshed
    # from the Rules Registry (Genie cannot reach Lakebase directly). After
    # the migrations so the registry tables exist; the scheduler re-refreshes
    # hourly from here on.
    _ensure_metadata_dims(sp_sql, pg_executor if pg_executor is not None else sp_sql)

    # Entitlement cache + gated failing-rows view (P4.1) — after the Delta
    # migrations (dq_quarantine_records) — then the user-facing grants, which
    # need every view to exist first.
    _ensure_entitlement_objects(sp_sql)
    _grant_user_view_access(sp_sql)

    # Ask-Genie space over the score views — after the views so a freshly
    # created space points at objects that exist. Blocking Genie REST calls
    # run in a thread; the ensure itself is idempotent + best-effort.
    await asyncio.to_thread(_ensure_genie_space, sp_ws, wh_id, pg_executor if pg_executor is not None else sp_sql)

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

    # NOTE: the Rules Registry deliberately starts EMPTY. We used to seed every
    # built-in DQX check function as a pre-published registry rule at startup,
    # but that cluttered a fresh install with ~78 auto-provisioned rules the
    # user never asked for. The registry now begins empty and is populated only
    # by rules authors create (or import) themselves. The seeding helper
    # ``builtin_rules_seed.seed_builtin_rules_if_absent`` is retained for a
    # potential future opt-in admin action, but is not invoked on startup.
    #
    # Clearing any built-in rules a previous version of the app already
    # auto-seeded is a manual, one-off developer cleanup action
    # (``RegistryService.delete_builtin_rules()``) — not a routine migration
    # step, so it is intentionally not invoked here on every startup.

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
            "DQX_WHEELS_VOLUME or DQX_JOB_ID is not set — profiler, dry-run, and scheduler features will be unavailable"
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
            oltp_for_scheduler = pg_executor if pg_executor is not None else sp_sql
            try:
                data_product_service, binding_run_service = await _build_scheduler_data_product_service(
                    sp_ws, sp_sql, oltp_for_scheduler
                )
            except Exception as dp_e:
                # Best-effort: the scope-config scheduling path must still
                # start even if the Data Products / monitored-table
                # collaborator chain can't be wired (e.g. a table from an
                # unapplied migration). The scheduler simply skips product
                # and table ticks in that case — see
                # ``SchedulerService._tick_products`` /
                # ``_tick_monitored_tables``.
                logger.warning("Could not wire scheduler product/table tick collaborators: %s", dp_e, exc_info=True)
                data_product_service = None
                binding_run_service = None

            # Metadata-dim refresher (P8.1): the scheduler re-materializes the
            # rule + monitored-table dims hourly so Genie's authoring/ownership
            # data sources stay fresh long after the startup refresh above.
            # Same OLTP executor as the other scheduler collaborators; the
            # dims themselves are SP-owned UC tables written via sp_sql.
            scheduler_monitored_tables = MonitoredTableService(sql=oltp_for_scheduler, profiling_sql=sp_sql)
            metadata_dim_service = MetadataDimService(
                sp_sql=sp_sql,
                registry=RegistryService(sql=oltp_for_scheduler),
                monitored_tables=scheduler_monitored_tables,
            )

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
                data_product_service=data_product_service,
                binding_run_service=binding_run_service,
                metadata_dim_service=metadata_dim_service,
                # Same construction as dependencies.get_score_cache_service:
                # the cache lives on the OLTP executor, the published-score
                # recompute reads the metric view via the SP warehouse
                # executor. Lets the scheduler refresh list scores when it
                # observes a launched run complete server-side (no browser).
                score_cache_service=ScoreCacheService(oltp=oltp_for_scheduler, warehouse_sql=sp_sql),
                # Denormalize each completed table's last_run_at/last_profiled_at
                # server-side too (T-perf / B2-15), sharing the score-refresh and
                # reconcile cadence so runs no browser observed still update the
                # overview "Last run" without the list path hitting the warehouse.
                monitored_table_service=scheduler_monitored_tables,
                # Startup reconcile (P5.3): the scheduler's first refresh
                # pass recomputes EVERY monitored table's cached score
                # (then products + global), healing rows left stale/NULL
                # by semantic changes or cold deploys. Lives here — not a
                # lifespan task — so the file-lock lease guarantees it
                # runs exactly once per host even with multiple uvicorn
                # workers, it naturally sequences after _ensure_score_views
                # above, and its failure can never block startup.
                reconcile_scores_on_start=True,
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

    _maybe_start_vector_store_provisioning(app, sp_ws=sp_ws, sp_sql=sp_sql, pg_executor=pg_executor)

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
