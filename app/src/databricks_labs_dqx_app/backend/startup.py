"""Post-migration activation and shutdown boundary for DQX Studio."""

import asyncio
import hashlib
import io
import inspect
import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path

from databricks.sdk import WorkspaceClient
from fastapi import FastAPI

from databricks_labs_dqx_app.backend._scheduler_registry import get_scheduler, set_scheduler
from databricks_labs_dqx_app.backend.config import conf
from databricks_labs_dqx_app.backend.dependencies import (
    _build_column_reader,
    get_app_settings_service,
    get_binding_run_service,
    get_data_product_service,
    get_job_service,
    get_materializer,
    get_monitored_table_service,
    get_monitored_table_version_service,
    get_permissions_service,
    get_registry_service,
    get_rules_catalog_service,
    get_run_set_service,
    get_sp_ws,
    get_view_service,
    set_oltp_executor,
)
from databricks_labs_dqx_app.backend.logger import logger
from databricks_labs_dqx_app.backend.migrations import MigrationRunner
from databricks_labs_dqx_app.backend.migrations.postgres import PgMigrationRunner
from databricks_labs_dqx_app.backend.pg_executor import PgExecutor, build_pg_executor_from_connection
from databricks_labs_dqx_app.backend.runtime import Runtime
from databricks_labs_dqx_app.backend.runtime import rt as application_runtime
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.ai_bootstrap import AiBootstrap
from databricks_labs_dqx_app.backend.services.apply_rules_service import ApplyRulesService
from databricks_labs_dqx_app.backend.services.binding_run_service import BindingRunService
from databricks_labs_dqx_app.backend.services.compute_service import ComputeService
from databricks_labs_dqx_app.backend.services.data_product_service import DataProductService
from databricks_labs_dqx_app.backend.services.entitlement_service import FAILING_ROWS_VIEW_NAME, EntitlementService
from databricks_labs_dqx_app.backend.services.metadata_dim_service import MetadataDimService
from databricks_labs_dqx_app.backend.services.monitored_table_service import MonitoredTableService
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService
from databricks_labs_dqx_app.backend.services.rule_embeddings import RuleEmbeddingsService
from databricks_labs_dqx_app.backend.services.scheduler_service import SchedulerService
from databricks_labs_dqx_app.backend.services.score_cache_service import ScoreCacheService
from databricks_labs_dqx_app.backend.services.score_view_service import (
    ASOF_VIEW_NAME,
    ATTRIBUTION_VIEW_NAME,
    METRIC_VIEW_NAME,
    SHAPING_VIEW_NAME,
    ScoreViewService,
)
from databricks_labs_dqx_app.backend.services.tag_reconcile_service import TagReconcileService
from databricks_labs_dqx_app.backend.services.view_service import mark_tmp_schema_ready
from databricks_labs_dqx_app.backend.setup.checks import ResourceCheckers
from databricks_labs_dqx_app.backend.setup.job_manager import TaskRunnerJobManager
from databricks_labs_dqx_app.backend.setup.models import (
    SetupActionId,
    SetupReport,
    SetupState,
    SetupStep,
    SetupStepId,
    StepState,
)
from databricks_labs_dqx_app.backend.setup.orchestrator import SetupOrchestrator, StudioActivation
from databricks_labs_dqx_app.backend.setup.resources import ActiveResources
from databricks_labs_dqx_app.backend.setup.resources import parse_volume_path, resolve_lakebase_connection
from databricks_labs_dqx_app.backend.setup.runtime import setup_runtime
from databricks_labs_dqx_app.backend.sql_executor import OltpExecutorProtocol, SqlExecutor

StartupHook = Callable[[], Awaitable[None] | None]


@dataclass
class StartupContext:
    """Resources and injected side effects owned by one FastAPI lifespan."""

    resources: ActiveResources
    runtime: Runtime
    oltp_executor: OltpExecutorProtocol
    register_oltp: Callable[[OltpExecutorProtocol | None], None]
    activation_hooks: tuple[StartupHook, ...] = ()
    background_hooks: tuple[StartupHook, ...] = ()
    shutdown_hooks: tuple[StartupHook, ...] = ()
    opened: bool = False
    active: bool = False
    background_started: bool = False
    completed_background_hooks: set[int] = field(default_factory=set)
    completed_shutdown_hooks: set[int] = field(default_factory=set)


async def activate_studio(context: StartupContext) -> None:
    """Publish ready resources and run foreground post-migration hooks once."""
    if context.active:
        return
    context.register_oltp(context.oltp_executor)
    context.runtime.activate(context.resources)
    context.opened = True
    for hook in context.activation_hooks:
        await _invoke(hook)
    context.active = True


async def start_studio_background(context: StartupContext) -> None:
    """Start background services only after setup readiness is published."""
    if context.background_started:
        return
    if not context.active:
        raise RuntimeError("DQX Studio foreground activation is incomplete")
    for index, hook in enumerate(context.background_hooks):
        if index in context.completed_background_hooks:
            continue
        await _invoke(hook)
        context.completed_background_hooks.add(index)
    context.background_started = True


async def deactivate_studio(context: StartupContext) -> None:
    """Stop background services and clear shared executors once."""
    if not context.opened:
        return
    failures: list[BaseException] = []
    for index, hook in enumerate(context.shutdown_hooks):
        if index in context.completed_shutdown_hooks:
            continue
        try:
            await _invoke(hook)
        except BaseException as error:
            failures.append(error)
        finally:
            context.completed_shutdown_hooks.add(index)
    try:
        context.register_oltp(None)
    except BaseException as error:
        failures.append(error)
    try:
        context.runtime.deactivate()
    except BaseException as error:
        failures.append(error)
    finally:
        context.background_started = False
        context.active = False
        context.opened = False
    _raise_cleanup_failures(failures)


def _raise_cleanup_failures(failures: list[BaseException]) -> None:
    if not failures:
        return
    if any(isinstance(error, asyncio.CancelledError) for error in failures):
        raise asyncio.CancelledError from None
    if len(failures) == 1:
        raise RuntimeError("DQX Studio shutdown cleanup failed") from None
    sanitized_failures = [RuntimeError("A DQX Studio shutdown cleanup action failed") for _ in failures]
    raise BaseExceptionGroup("DQX Studio shutdown cleanup failed", sanitized_failures) from None


async def _invoke(hook: StartupHook) -> None:
    result = hook()
    if inspect.isawaitable(result):
        await result


@dataclass
class _Activation(StudioActivation):
    context: StartupContext

    async def activate(self) -> None:
        await activate_studio(self.context)

    async def start_background(self) -> None:
        await start_studio_background(self.context)


async def start_studio(app: FastAPI) -> StartupContext | None:
    """Construct setup collaborators, reconcile readiness, and never abort lifespan."""
    setup_runtime.publish(SetupReport(state=SetupState.CHECKING, steps=()))
    resources = _resolve_resources()
    if resources is None:
        return None

    pg_executor: PgExecutor | None = None
    try:
        sp_ws = await get_sp_ws()
        sp_sql = SqlExecutor(
            ws=sp_ws,
            warehouse_id=resources.warehouse_id,
            catalog=resources.volume.catalog,
            schema=resources.volume.schema,
        )
        pg_executor = await asyncio.to_thread(
            build_pg_executor_from_connection,
            sp_ws,
            resources.lakebase,
            token_refresh_minutes=conf.lakebase_token_refresh_minutes,
            token_refresh_retry_seconds=conf.lakebase_token_refresh_retry_seconds,
            token_refresh_retry_jitter=conf.lakebase_token_refresh_retry_jitter,
            token_refresh_max_failures=conf.lakebase_token_refresh_max_failures,
            pool_min_size=conf.lakebase_pool_min_size,
            pool_max_size=conf.lakebase_pool_max_size,
        )
    except Exception:
        if pg_executor is not None:
            await asyncio.to_thread(pg_executor.close)
        _publish_unavailable(
            SetupStepId.LAKEBASE,
            "lakebase_connection_unavailable",
            "Could not open the required Lakebase connection.",
        )
        return None

    context = StartupContext(
        resources=resources,
        runtime=application_runtime,
        oltp_executor=pg_executor,
        register_oltp=set_oltp_executor,
        activation_hooks=(lambda: _run_post_migration_startup(app, sp_ws, sp_sql, pg_executor, resources),),
        background_hooks=(
            lambda: _start_scheduler(sp_ws, sp_sql, pg_executor, resources),
            lambda: _maybe_start_ai_bootstrap(app, sp_ws, sp_sql, pg_executor),
        ),
        shutdown_hooks=(lambda: _stop_background_services(app, pg_executor),),
        opened=True,
    )
    try:
        app_settings = AppSettingsService(sql=pg_executor)
        compute = ComputeService(sp_ws=sp_ws, app_settings=app_settings)

        async def publish_wheels() -> list[str]:
            return await publish_wheels_to_volume(sp_ws, resources.volume.path)

        orchestrator = SetupOrchestrator(
            runtime=setup_runtime,
            resources=resources,
            checkers=ResourceCheckers(
                resources=resources,
                workspace=sp_ws,
                sql=sp_sql,
                pg=pg_executor,
                compute=compute,
            ),
            jobs=TaskRunnerJobManager(sp_ws),
            pg_migrations=PgMigrationRunner(pg_executor),
            delta_migrations=MigrationRunner(sp_sql),
            app_settings=app_settings,
            publish_wheels=publish_wheels,
            activation=_Activation(context),
            app_sp_id=compute.sp_application_id(),
        )
        app.state.setup_orchestrator = orchestrator
        await orchestrator.reconcile()
        return context
    except BaseException as startup_error:
        cleanup_error: BaseException | None = None
        try:
            await deactivate_studio(context)
        except BaseException as error:
            cleanup_error = error
        if isinstance(startup_error, asyncio.CancelledError):
            raise asyncio.CancelledError from None
        if cleanup_error is not None and isinstance(startup_error, Exception):
            raise RuntimeError("DQX Studio startup and cleanup failed") from None
        raise


async def stop_studio(context: StartupContext | None) -> None:
    """Clean up every resource opened by *start_studio*."""
    if context is not None:
        await deactivate_studio(context)


def _resolve_resources() -> ActiveResources | None:
    try:
        volume = parse_volume_path(conf.wheels_volume)
    except Exception:
        _publish_unavailable(
            SetupStepId.VOLUME,
            "wheels_volume_invalid",
            "A valid bound Unity Catalog wheels volume is required.",
        )
        return None

    try:
        lakebase = resolve_lakebase_connection(conf, os.environ)
    except Exception:
        lakebase = None
    if lakebase is None:
        _publish_unavailable(
            SetupStepId.LAKEBASE,
            "lakebase_binding_missing",
            "A Lakebase connection is required for DQX Studio.",
        )
        return None

    warehouse_id = (
        os.environ.get("DATABRICKS_WAREHOUSE_ID") or os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID") or ""
    ).strip()
    if not warehouse_id:
        _publish_unavailable(
            SetupStepId.WAREHOUSE,
            "warehouse_binding_missing",
            "A SQL warehouse binding is required for DQX Studio.",
        )
        return None

    return ActiveResources(
        volume=volume,
        lakebase=lakebase,
        warehouse_id=warehouse_id,
        job_id=conf.job_id.strip() or None,
        tmp_schema=conf.tmp_schema_name,
        genie_schema=conf.genie_schema_name,
    )


def _publish_unavailable(step_id: SetupStepId, code: str, summary: str) -> None:
    setup_runtime.publish(
        SetupReport(
            state=SetupState.SETUP_REQUIRED,
            current_step=step_id,
            steps=(
                SetupStep(
                    id=step_id,
                    state=StepState.ACTION_REQUIRED,
                    code=code,
                    summary=summary,
                    actions=(SetupActionId.VERIFY_AGAIN,),
                ),
            ),
        )
    )


def _find_wheels() -> list[Path]:
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

    seen: set[Path] = set()
    wheels: list[Path] = []
    for root in search_roots:
        for pattern in ("databricks_labs_dqx-*.whl", "tasks/databricks_labs_dqx_task_runner-*.whl"):
            for wheel in sorted(root.glob(pattern)):
                if wheel not in seen:
                    seen.add(wheel)
                    wheels.append(wheel)
    return wheels


def _compute_wheels_hash(wheels: list[Path]) -> str:
    digest = hashlib.sha256()
    for wheel_path in sorted(wheels):
        digest.update(wheel_path.name.encode())
        digest.update(wheel_path.stat().st_size.to_bytes(8, "big"))
        with open(wheel_path, "rb") as wheel_file:
            while chunk := wheel_file.read(1 << 16):
                digest.update(chunk)
    return digest.hexdigest()


def _read_remote_hash(workspace: WorkspaceClient, volume_path: str) -> str | None:
    try:
        response = workspace.files.download(f"{volume_path}/.wheels_hash")
        if response.contents is None:
            return None
        return response.contents.read().decode().strip()
    except Exception:
        return None


def _write_remote_hash(workspace: WorkspaceClient, volume_path: str, digest: str) -> None:
    workspace.files.upload(f"{volume_path}/.wheels_hash", io.BytesIO(digest.encode()), overwrite=True)


async def publish_wheels_to_volume(workspace: WorkspaceClient, volume_path: str) -> list[str]:
    """Publish build wheels to a Unity Catalog volume when their content changed.

    Args:
        workspace: Authenticated workspace client for the bound volume.
        volume_path: Bound Unity Catalog volume path receiving the wheels.

    Returns:
        Published wheel paths, or an empty list when no build wheels exist.
    """
    wheels = _find_wheels()
    if not wheels:
        return []
    local_hash = _compute_wheels_hash(wheels)
    remote_hash = await asyncio.to_thread(_read_remote_hash, workspace, volume_path)
    paths = [f"{volume_path}/{wheel.name}" for wheel in wheels]
    if local_hash == remote_hash:
        return paths

    for wheel_path, destination in zip(wheels, paths, strict=True):
        with open(wheel_path, "rb") as wheel_file:
            await asyncio.to_thread(workspace.files.upload, destination, wheel_file, overwrite=True)
    await asyncio.to_thread(_write_remote_hash, workspace, volume_path, local_hash)
    return paths


async def _run_post_migration_startup(
    app: FastAPI,
    workspace: WorkspaceClient,
    delta_sql: SqlExecutor,
    oltp: OltpExecutorProtocol,
    resources: ActiveResources,
) -> None:
    _ensure_score_views(delta_sql, resources)
    _ensure_metadata_dims(delta_sql, oltp, resources)
    _ensure_entitlement_objects(delta_sql, resources)
    _grant_user_view_access(delta_sql, resources)
    await asyncio.to_thread(_ensure_genie_space, workspace, resources, oltp)

    settings = AppSettingsService(sql=oltp)
    try:
        settings.seed_run_review_statuses_if_absent()
    except Exception:
        logger.warning("Could not seed default run review statuses")
    try:
        settings.seed_reserved_label_definitions_if_absent()
    except Exception:
        logger.warning("Could not seed reserved label definitions")

    mark_tmp_schema_ready()


def _ensure_score_views(delta_sql: SqlExecutor, resources: ActiveResources) -> None:
    try:
        ScoreViewService(sql=delta_sql, genie_schema=resources.genie_schema).ensure_views()
    except Exception:
        logger.warning("Could not create the DQ score views")


def _ensure_metadata_dims(
    delta_sql: SqlExecutor,
    oltp: OltpExecutorProtocol,
    resources: ActiveResources,
) -> None:
    try:
        MetadataDimService(
            sp_sql=delta_sql,
            registry=RegistryService(sql=oltp),
            monitored_tables=MonitoredTableService(sql=oltp, profiling_sql=delta_sql),
            genie_schema=resources.genie_schema,
        ).refresh()
    except Exception:
        logger.warning("Could not refresh the DQ metadata dimensions")


def _ensure_entitlement_objects(delta_sql: SqlExecutor, resources: ActiveResources) -> None:
    try:
        EntitlementService(sql=delta_sql, genie_schema=resources.genie_schema).ensure_objects()
    except Exception:
        logger.warning("Could not create the entitlement objects")


_USER_READABLE_VIEWS = (
    METRIC_VIEW_NAME,
    SHAPING_VIEW_NAME,
    ASOF_VIEW_NAME,
    ATTRIBUTION_VIEW_NAME,
    FAILING_ROWS_VIEW_NAME,
)


def _grant_user_view_access(delta_sql: SqlExecutor, resources: ActiveResources) -> None:
    catalog = delta_sql.q(resources.volume.catalog)
    schema = delta_sql.q(resources.genie_schema)
    statements = [
        f"GRANT USE SCHEMA ON SCHEMA {catalog}.{schema} TO `account users`",
        *(f"GRANT SELECT ON TABLE {catalog}.{schema}.{name} TO `account users`" for name in _USER_READABLE_VIEWS),
    ]
    for statement in statements:
        try:
            delta_sql.execute_no_schema(statement)
        except Exception:
            logger.warning("Could not grant account users access to a Studio view")


def _ensure_genie_space(
    workspace: WorkspaceClient,
    resources: ActiveResources,
    oltp: OltpExecutorProtocol,
) -> None:
    try:
        from databricks_labs_dqx_app.backend.services.genie_space_service import ensure_dq_genie_space

        try:
            parent_path = f"/Users/{workspace.current_user.me().user_name}"
        except Exception:
            parent_path = "/Shared"
        ensure_dq_genie_space(
            settings=AppSettingsService(sql=oltp),
            ws=workspace,
            warehouse_id=resources.warehouse_id,
            parent_path=parent_path,
            catalog=resources.volume.catalog,
            schema=resources.genie_schema,
        )
    except Exception:
        logger.warning("Could not provision the DQ Genie space")


async def _build_scheduler_data_product_service(
    workspace: WorkspaceClient,
    delta_sql: SqlExecutor,
    oltp: OltpExecutorProtocol,
) -> tuple[DataProductService, BindingRunService]:
    app_settings = await get_app_settings_service(sql=oltp)
    permissions = await get_permissions_service(sql=oltp, app_settings=app_settings)
    monitored_tables = await get_monitored_table_service(
        sql=oltp,
        profiling_sql=delta_sql,
        perms=permissions,
        sp_ws=workspace,
    )
    registry = await get_registry_service(sql=oltp, perms=permissions, sp_ws=workspace)
    rules_catalog = await get_rules_catalog_service(sql=oltp)
    materializer = await get_materializer(
        sql=oltp,
        registry=registry,
        monitored_tables=monitored_tables,
        app_settings=app_settings,
    )
    version_service = await get_monitored_table_version_service(
        sql=oltp,
        monitored_tables=monitored_tables,
        rules_catalog=rules_catalog,
        materializer=materializer,
    )
    view_service = await get_view_service(sql=delta_sql, sp_sql=delta_sql)
    job_service = await get_job_service(sp_ws=workspace, sql=delta_sql, app_settings=app_settings)
    run_sets = await get_run_set_service(sql=oltp, validation_sql=delta_sql)
    binding_runs = await get_binding_run_service(
        monitored_tables=monitored_tables,
        version_service=version_service,
        materializer=materializer,
        view_service=view_service,
        job_service=job_service,
        run_set_service=run_sets,
        settings_service=app_settings,
        sp_sql=delta_sql,
    )
    products = await get_data_product_service(
        sql=oltp,
        monitored_tables=monitored_tables,
        run_set_service=run_sets,
        binding_run_service=binding_runs,
        version_service=version_service,
        app_settings=app_settings,
        materializer=materializer,
        perms=permissions,
        sp_ws=workspace,
    )
    return products, binding_runs


async def _start_scheduler(
    workspace: WorkspaceClient,
    delta_sql: SqlExecutor,
    oltp: OltpExecutorProtocol,
    resources: ActiveResources,
) -> None:
    if os.environ.get("DQX_SCHEDULER_DISABLED") == "1":
        return
    from databricks_labs_dqx_app.backend import app as app_module

    if not app_module._try_acquire_scheduler_lease():
        return
    try:
        try:
            data_products, binding_runs = await _build_scheduler_data_product_service(workspace, delta_sql, oltp)
        except Exception:
            logger.warning("Could not wire scheduler product collaborators")
            data_products = None
            binding_runs = None

        monitored_tables = MonitoredTableService(sql=oltp, profiling_sql=delta_sql)
        registry = RegistryService(sql=oltp)
        settings = AppSettingsService(sql=oltp)
        metadata_dims = MetadataDimService(
            sp_sql=delta_sql,
            registry=registry,
            monitored_tables=monitored_tables,
            genie_schema=resources.genie_schema,
        )
        tag_reconcile = TagReconcileService(
            registry=registry,
            monitored_tables=monitored_tables,
            apply_rules=ApplyRulesService(sql=oltp, registry=registry, app_settings=settings),
            app_settings=settings,
            read_columns=_build_column_reader(workspace, delta_sql),
        )
        scheduler = SchedulerService(
            ws=workspace,
            warehouse_id=resources.warehouse_id,
            catalog=resources.volume.catalog,
            schema=resources.volume.schema,
            tmp_schema=resources.tmp_schema,
            job_id=str(setup_runtime.require_job_id()),
            oltp_sql=oltp,
            data_product_service=data_products,
            binding_run_service=binding_runs,
            metadata_dim_service=metadata_dims,
            score_cache_service=ScoreCacheService(
                oltp=oltp,
                warehouse_sql=delta_sql,
                genie_schema=resources.genie_schema,
            ),
            monitored_table_service=monitored_tables,
            tag_reconcile_service=tag_reconcile,
            reconcile_scores_on_start=True,
        )
        set_scheduler(scheduler)
        scheduler.start()
    except Exception:
        logger.warning("Could not start the Studio scheduler")


def _maybe_start_ai_bootstrap(
    app: FastAPI,
    workspace: WorkspaceClient,
    delta_sql: SqlExecutor,
    oltp: OltpExecutorProtocol,
) -> None:
    try:
        app_settings = AppSettingsService(sql=oltp)
        if not app_settings.get_ai_enabled():
            return
        bootstrap = AiBootstrap(
            sp_ws=workspace,
            app_settings=app_settings,
            embeddings=RuleEmbeddingsService(sql=oltp, sp_ws=workspace, app_settings=app_settings),
            registry=RegistryService(sql=oltp),
        )
        app.state.ai_bootstrap_startup_task = asyncio.create_task(bootstrap.ensure_ai_ready())
    except Exception:
        logger.warning("Could not start the Studio AI bootstrap")


async def _stop_background_services(app: FastAPI, pg_executor: PgExecutor) -> None:
    failures: list[BaseException] = []
    scheduler = get_scheduler()
    if scheduler is not None:
        try:
            await scheduler.stop()
        except BaseException as error:
            failures.append(error)
        finally:
            set_scheduler(None)

    task = getattr(app.state, "ai_bootstrap_startup_task", None)
    if isinstance(task, asyncio.Task) and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        except BaseException as error:
            failures.append(error)

    try:
        await asyncio.to_thread(pg_executor.close)
    except BaseException as error:
        failures.append(error)
    _raise_cleanup_failures(failures)
