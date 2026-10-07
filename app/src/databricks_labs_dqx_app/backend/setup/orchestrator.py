"""Serialized, deployment-agnostic setup reconciliation for DQX Studio."""

import asyncio
import logging
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import partial
from typing import Literal, Protocol

from databricks.sdk import WorkspaceClient

from databricks_labs_dqx_app.backend.setup.configuration import (
    ConfigurationSource,
    ResolvedConfiguration,
    SetupChoices,
    SetupConfigurationStore,
)
from databricks_labs_dqx_app.backend.setup.errors import RequiredViewSetupError
from databricks_labs_dqx_app.backend.setup.job_manager import ResolvedJob
from databricks_labs_dqx_app.backend.setup.models import (
    SetupActionId,
    SetupConfigurationView,
    SetupReport,
    SetupState,
    SetupStep,
    SetupStepId,
    StepState,
)
from databricks_labs_dqx_app.backend.setup.resources import (
    ActiveResources,
    BootstrapResources,
    build_active_resources,
)
from databricks_labs_dqx_app.backend.setup.runtime import SetupRuntime
from databricks_labs_dqx_app.backend.sanitization import replace_control_characters
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor

logger = logging.getLogger(__name__)

_STEP_ORDER = tuple(SetupStepId)
_BLOCKING_STATES = frozenset({StepState.ACTION_REQUIRED, StepState.FAILED})
_CONFIGURED_SUMMARY = "Studio storage and audience are configured."
_SOURCE_VIEWS: dict[ConfigurationSource, Literal["deployment", "saved", "none"]] = {
    ConfigurationSource.DEPLOYMENT: "deployment",
    ConfigurationSource.SAVED: "saved",
    ConfigurationSource.NONE: "none",
}


class BootstrapChecks(Protocol):
    """Checks that run before any Studio storage is configured."""

    def check_app_identity(self) -> SetupStep: ...

    def check_lakebase(self) -> SetupStep: ...

    def ensure_lakebase_schema(self) -> SetupStep: ...


class BoundChecks(Protocol):
    """Checks that need resolved Studio storage."""

    def check_unity_catalog(self, reader_sql: SqlExecutor | None = None) -> SetupStep: ...

    def ensure_storage(self, *, provision: bool) -> SetupStep: ...

    def check_warehouse(
        self,
        warehouse_id: str | None = None,
        reader_ws: WorkspaceClient | None = None,
    ) -> SetupStep: ...

    def check_runner_access(
        self,
        job_id: int,
        reader_ws: WorkspaceClient | None = None,
        *,
        reader_sql: SqlExecutor | None = None,
        include_outputs: bool = False,
    ) -> SetupStep: ...


class AccessChecks(Protocol):
    """Audience access and app-sharing checks that run after activation."""

    def reconcile_access(
        self,
        reader_sql: SqlExecutor | None = None,
        reader_ws: WorkspaceClient | None = None,
    ) -> SetupStep: ...

    def check_app_sharing(self, reader_ws: WorkspaceClient | None = None) -> SetupStep: ...


class SetupJobs(Protocol):
    """Task-runner management surface consumed by the orchestrator."""

    def resolve(self, configured_job_id: str | None) -> ResolvedJob: ...

    def grant_setup_admin(self, job_id: int, user_name: str) -> None: ...

    def validate_run_as(self, job_id: int, app_sp_id: str) -> SetupStep: ...

    def configure(self, job_id: int, wheel_paths: list[str]) -> None: ...


class SetupMigrations(Protocol):
    """Idempotent migration runner surface."""

    def run_all(self) -> int: ...


class SetupCompletionStore(Protocol):
    """Post-migration setup completion persistence surface."""

    def record_setup_completion(
        self,
        job_id: int,
        completed_at: datetime,
        user_name: str | None,
    ) -> None: ...


class StudioActivation(Protocol):
    """Post-migration activation surface."""

    async def activate(self) -> None: ...

    async def start_background(self) -> None: ...


@dataclass(frozen=True)
class BoundSetup:
    """Collaborators bound to one resolved Studio storage and audience.

    Args:
        resources: Active resources the collaborators were built for.
        checkers: Storage-dependent capability checks.
        access: Audience access and app-sharing checks.
        delta_migrations: Delta migration runner for the bound main schema.
        publish_wheels: Publishes application wheels to the bound volume.
        activation: Activates Studio for the bound resources.
    """

    resources: ActiveResources
    checkers: BoundChecks
    access: AccessChecks
    delta_migrations: SetupMigrations
    publish_wheels: Callable[[], Awaitable[list[str]]]
    activation: StudioActivation


class StorageBinder(Protocol):
    """Build storage-bound collaborators for resolved resources.

    Binding is asynchronous so an implementation can release collaborators bound to
    previously resolved resources (for example, deactivate a running Studio) first.
    """

    async def bind(self, resources: ActiveResources) -> BoundSetup: ...


class ConfigurationResolver(Protocol):
    """Resolve and lock the Studio storage and audience configuration."""

    def resolve(self) -> ResolvedConfiguration: ...

    def lock(self, *, user_email: str | None) -> None: ...


class SetupOrchestrator:
    """Reconcile Studio-owned setup actions in order.

    Lakebase is bootstrapped first so saved setup choices can be read before any
    Unity Catalog storage exists; storage-bound collaborators are created once the
    configuration resolves.
    """

    def __init__(
        self,
        *,
        runtime: SetupRuntime,
        bootstrap: BootstrapResources,
        bootstrap_checks: BootstrapChecks,
        pg_migrations: SetupMigrations,
        configuration: ConfigurationResolver,
        binder: StorageBinder,
        jobs: SetupJobs,
        app_settings: SetupCompletionStore,
        app_sp_id: str,
    ) -> None:
        self.runtime = runtime
        self.bootstrap = bootstrap
        self.bootstrap_checks = bootstrap_checks
        self.pg_migrations = pg_migrations
        self.configuration = configuration
        self.binder = binder
        self.jobs = jobs
        self.app_settings = app_settings
        self.app_sp_id = _sanitize_identity(app_sp_id) or ""
        self.bound: BoundSetup | None = None
        self._resolved: ResolvedConfiguration | None = None

    def configuration_view(self) -> SetupConfigurationView:
        """Return a sanitized view of the most recently resolved configuration."""
        resolved = self._resolved
        if resolved is None:
            return SetupConfigurationView(source="none")
        choices = resolved.choices
        storage = resolved.storage
        audience = resolved.audience
        if choices is not None:
            catalog, prefix, audience_group = choices.catalog, choices.prefix, choices.audience_group
        else:
            catalog = storage.catalog if storage is not None else ""
            prefix = resolved.deployment_prefix
            audience_group = ", ".join(audience.groups) if audience is not None else ""
        return SetupConfigurationView(
            source=_SOURCE_VIEWS[resolved.source],
            catalog=_display(catalog),
            prefix=_display(prefix),
            audience_group=_display(audience_group),
            schemas=tuple(_display(schema) for schema in storage.schemas) if storage is not None else (),
            broad_audience=audience.broad if audience is not None else False,
            locked=resolved.locked,
        )

    async def save_configuration(
        self,
        store: SetupConfigurationStore,
        choices: SetupChoices,
        *,
        user_email: str | None,
    ) -> Literal["saved", "unchanged", "locked"]:
        """Persist setup choices unless storage was already provisioned.

        The lock state is re-checked under the activation lock so a concurrent
        reconcile cannot provision storage for different choices than the ones saved.

        Args:
            store: Persistence for the setup choices.
            choices: Validated, normalized choices to save.
            user_email: Administrator performing the change.

        Returns:
            *saved* when written, *unchanged* when locked with identical choices,
            *locked* when locked with different choices.
        """
        async with self.runtime.activation_lock:
            if await asyncio.to_thread(store.is_locked):
                saved = await asyncio.to_thread(store.load)
                return "unchanged" if saved == choices else "locked"
            await asyncio.to_thread(partial(store.save, choices, user_email=user_email))
            return "saved"

    async def reconcile(
        self,
        setup_user: str | None = None,
        reader_ws: WorkspaceClient | None = None,
        reader_sql: SqlExecutor | None = None,
    ) -> SetupReport:
        """Run retry-safe setup actions serially and activate only after migrations.

        A READY installation is returned unchanged for unattended calls. When an
        administrator re-verifies a READY installation, every step is re-run while
        the READY report stays visible; a blocking result publishes SETUP_REQUIRED.

        Args:
            setup_user: Authenticated administrator performing setup, if present.
            reader_ws: Request-scoped administrator client for read-only privilege
                inspection. Jobs operations and writes remain app-authenticated.
            reader_sql: Request-scoped administrator SQL executor for inspecting grants.
        """
        async with self.runtime.activation_lock:
            actor = _sanitize_identity(setup_user)
            if self.runtime.report().state == SetupState.READY:
                if actor is None:
                    return self.runtime.report()
                await self._refresh_job_admin(actor)
                return await self._run_steps(
                    actor=actor, grant_user=None, reader_ws=reader_ws, reader_sql=reader_sql, progress=False
                )
            self.runtime.publish(SetupReport(state=SetupState.CHECKING, steps=()))
            return await self._run_steps(
                actor=actor, grant_user=actor, reader_ws=reader_ws, reader_sql=reader_sql, progress=True
            )

    async def _run_steps(
        self,
        *,
        actor: str | None,
        grant_user: str | None,
        reader_ws: WorkspaceClient | None,
        reader_sql: SqlExecutor | None,
        progress: bool,
    ) -> SetupReport:
        steps: list[SetupStep] = []

        def advance(step: SetupStep) -> SetupReport | None:
            return self._record(steps, step, progress=progress)

        step = await asyncio.to_thread(self.bootstrap_checks.check_app_identity)
        if stopped := advance(step):
            return stopped

        for lakebase_check in (self.bootstrap_checks.check_lakebase, self.bootstrap_checks.ensure_lakebase_schema):
            step = await asyncio.to_thread(lakebase_check)
            if stopped := advance(step):
                return stopped
        if stopped := advance(await self._run_postgres_migrations()):
            return stopped

        resolved, step = await self._resolve_configuration()
        if stopped := advance(step):
            return stopped
        bound = self.bound
        if resolved is None or bound is None:
            raise RuntimeError("Unreachable setup configuration state")

        step = await asyncio.to_thread(bound.checkers.check_unity_catalog, reader_sql)
        if stopped := advance(step):
            return stopped

        step = await self._ensure_storage(bound, resolved, actor)
        if step.state == StepState.PASSED and not resolved.locked:
            # Storage is now locked, so the saved choices can no longer be edited.
            self._record(steps, _passed(SetupStepId.CONFIGURATION, _CONFIGURED_SUMMARY), progress=False)
        if stopped := advance(step):
            return stopped

        step = await asyncio.to_thread(bound.checkers.check_warehouse)
        if stopped := advance(step):
            return stopped

        if stopped := advance(await self._reconcile_job(bound, grant_user, reader_ws, reader_sql)):
            return stopped

        if stopped := advance(await self._reconcile_wheels(bound)):
            return stopped

        if stopped := advance(await self._run_delta_migrations(bound)):
            return stopped

        job_id = self.runtime.require_job_id()
        step = await asyncio.to_thread(
            bound.checkers.check_runner_access,
            job_id,
            reader_ws=reader_ws,
            reader_sql=reader_sql,
            include_outputs=True,
        )
        if stopped := advance(step):
            return stopped
        try:
            await asyncio.to_thread(
                self.app_settings.record_setup_completion,
                job_id,
                datetime.now(timezone.utc),
                actor,
            )
        except Exception:
            step = _failed(
                SetupStepId.MIGRATIONS,
                "setup_completion_persistence_failed",
                "Could not persist setup completion after database migrations.",
            )
            if stopped := advance(step):
                return stopped
            raise RuntimeError("Unreachable setup persistence state") from None

        if stopped := advance(await self._activate(bound)):
            return stopped

        step = await asyncio.to_thread(bound.access.reconcile_access, reader_sql, reader_ws)
        if stopped := advance(step):
            return stopped

        step = await asyncio.to_thread(bound.access.check_app_sharing, reader_ws)
        if stopped := advance(step):
            return stopped

        report = SetupReport(state=SetupState.READY, steps=tuple(steps))
        self.runtime.publish(report)
        try:
            await bound.activation.start_background()
        except Exception:
            logger.warning(
                "Could not start Studio background services; the activated application remains available.",
                exc_info=True,
            )
        return report

    async def _refresh_job_admin(self, setup_user: str) -> None:
        try:
            await asyncio.to_thread(self.jobs.grant_setup_admin, self.runtime.require_job_id(), setup_user)
        except Exception:
            logger.warning(
                "Could not refresh setup administrator access to the Studio task-runner job; "
                "the ready application remains available."
            )

    async def _resolve_configuration(self) -> tuple[ResolvedConfiguration | None, SetupStep]:
        try:
            resolved = await asyncio.to_thread(self.configuration.resolve)
        except Exception as error:
            logger.warning(f"Could not resolve the Studio setup configuration ({type(error).__name__})")
            return None, _failed(
                SetupStepId.CONFIGURATION,
                "configuration_resolution_failed",
                "Could not read the Studio storage and audience configuration.",
            )
        self._resolved = resolved
        try:
            return resolved, await self._configuration_step(resolved)
        except Exception as error:
            logger.warning(f"Could not bind Studio setup collaborators ({type(error).__name__})")
            return None, _failed(
                SetupStepId.CONFIGURATION,
                "configuration_binding_failed",
                "Could not prepare Studio collaborators for the configured storage.",
            )

    async def _configuration_step(self, resolved: ResolvedConfiguration) -> SetupStep:
        if resolved.error is not None:
            return SetupStep(
                id=SetupStepId.CONFIGURATION,
                state=StepState.ACTION_REQUIRED,
                code=resolved.error,
                summary="The Studio storage or audience configuration is invalid.",
                actions=(SetupActionId.CONFIGURE,) if resolved.source != ConfigurationSource.DEPLOYMENT else (),
            )
        if resolved.storage is None or resolved.audience is None:
            return SetupStep(
                id=SetupStepId.CONFIGURATION,
                state=StepState.ACTION_REQUIRED,
                code="configuration_required",
                summary="Choose the catalog, storage prefix and audience group for DQX Studio.",
                actions=(SetupActionId.CONFIGURE,),
            )
        resources = build_active_resources(self.bootstrap, resolved.storage, resolved.audience)
        if self.bound is None or self.bound.resources != resources:
            # Clear first so a failed bind never leaves collaborators whose activation was released.
            self.bound = None
            self.bound = await self.binder.bind(resources)
        if resolved.source == ConfigurationSource.SAVED and not resolved.locked:
            # Saved choices stay editable until storage is provisioned and locked, so a
            # storage collision or missing catalog privilege is never a dead end.
            return SetupStep(
                id=SetupStepId.CONFIGURATION,
                state=StepState.PASSED,
                summary=_CONFIGURED_SUMMARY,
                actions=(SetupActionId.CONFIGURE,),
            )
        return _passed(SetupStepId.CONFIGURATION, _CONFIGURED_SUMMARY)

    async def _ensure_storage(self, bound: BoundSetup, resolved: ResolvedConfiguration, actor: str | None) -> SetupStep:
        step = await asyncio.to_thread(
            partial(bound.checkers.ensure_storage, provision=resolved.source == ConfigurationSource.SAVED)
        )
        if step.state != StepState.PASSED or resolved.locked:
            return step
        try:
            await asyncio.to_thread(partial(self.configuration.lock, user_email=actor))
        except Exception as error:
            logger.warning(f"Could not lock the Studio setup configuration ({type(error).__name__})")
            return _failed(
                SetupStepId.STORAGE,
                "configuration_lock_failed",
                "Could not record that Studio storage has been provisioned.",
            )
        return step

    async def _activate(self, bound: BoundSetup) -> SetupStep:
        try:
            await bound.activation.activate()
        except RequiredViewSetupError:
            return SetupStep(
                id=SetupStepId.ACTIVATION,
                state=StepState.FAILED,
                code="required_views_creation_failed",
                summary=(
                    "Could not create the required score, entitlement or metadata dimension objects "
                    "in the main and Genie schemas."
                ),
                instructions=(
                    "Verify the app service principal has USE CATALOG, USE SCHEMA, and CREATE TABLE "
                    "on the application and Genie schemas, and can replace existing Studio views "
                    "and metadata dimension tables.",
                ),
                actions=(SetupActionId.RECONCILE,),
            )
        except Exception:
            return _failed(
                SetupStepId.ACTIVATION,
                "studio_activation_failed",
                "Could not initialize required Studio application objects.",
            )
        return _passed(SetupStepId.ACTIVATION, "DQX Studio is active.")

    async def _reconcile_job(
        self,
        bound: BoundSetup,
        setup_user: str | None,
        reader_ws: WorkspaceClient | None,
        reader_sql: SqlExecutor | None,
    ) -> SetupStep:
        configured = str(self.runtime.job_id) if self.runtime.job_id is not None else self.bootstrap.job_id
        try:
            resolved = await asyncio.to_thread(self.jobs.resolve, configured)
            self.runtime.job_id = resolved.job_id
            if setup_user is not None:
                await asyncio.to_thread(self.jobs.grant_setup_admin, resolved.job_id, setup_user)
            step = await asyncio.to_thread(self.jobs.validate_run_as, resolved.job_id, self.app_sp_id)
            if step.state != StepState.PASSED:
                return step
            return await asyncio.to_thread(
                bound.checkers.check_runner_access, resolved.job_id, reader_ws=reader_ws, reader_sql=reader_sql
            )
        except Exception:
            return _failed(
                SetupStepId.TASK_RUNNER,
                "task_runner_reconciliation_failed",
                "Could not reconcile the Studio task-runner job.",
            )

    async def _reconcile_wheels(self, bound: BoundSetup) -> SetupStep:
        try:
            wheel_paths = await bound.publish_wheels()
            if not wheel_paths:
                return _failed(
                    SetupStepId.WHEELS,
                    "application_wheels_missing",
                    "Application wheel files are not available for the task runner.",
                )
            await asyncio.to_thread(self.jobs.configure, self.runtime.require_job_id(), wheel_paths)
            return _passed(SetupStepId.WHEELS, "Application wheels and task-runner configuration are current.")
        except Exception:
            return _failed(
                SetupStepId.WHEELS,
                "wheel_publication_failed",
                "Could not publish application wheels to the bound volume.",
            )

    async def _run_postgres_migrations(self) -> SetupStep:
        try:
            await asyncio.to_thread(self.pg_migrations.run_all)
        except Exception as error:
            logger.error(f"Lakebase migration failed ({_diagnostic(error)})")
            return _failed(
                SetupStepId.LAKEBASE,
                "lakebase_migration_failed",
                "Could not apply the required Lakebase database migrations.",
            )
        return _passed(SetupStepId.LAKEBASE, "Lakebase is available and its migrations are current.")

    async def _run_delta_migrations(self, bound: BoundSetup) -> SetupStep:
        try:
            await asyncio.to_thread(bound.delta_migrations.run_all)
        except Exception as error:
            logger.error(f"Delta migration failed ({_diagnostic(error)})")
            return _failed(
                SetupStepId.MIGRATIONS,
                "delta_migration_failed",
                "Could not apply the required Delta database migrations.",
            )
        return _passed(SetupStepId.MIGRATIONS, "Delta migrations are current.")

    def _record(self, steps: list[SetupStep], step: SetupStep, *, progress: bool) -> SetupReport | None:
        """Record *step*, replacing an earlier result for the same step, and stop when it blocks."""
        existing_index = next((index for index, existing in enumerate(steps) if existing.id == step.id), None)
        if existing_index is not None:
            steps[existing_index] = step
        else:
            steps.append(step)
        if step.state in _BLOCKING_STATES:
            report = SetupReport(state=SetupState.SETUP_REQUIRED, current_step=step.id, steps=tuple(steps))
            self.runtime.publish(report)
            return report
        if progress:
            self.runtime.publish(
                SetupReport(state=SetupState.INITIALIZING, current_step=_next_step(step.id), steps=tuple(steps))
            )
        return None


def _next_step(step_id: SetupStepId) -> SetupStepId | None:
    index = _STEP_ORDER.index(step_id) + 1
    return _STEP_ORDER[index] if index < len(_STEP_ORDER) else None


def _diagnostic(error: Exception) -> str:
    sqlstate = getattr(error, "sqlstate", None)
    suffix = f", SQLSTATE {sqlstate}" if isinstance(sqlstate, str) and re.fullmatch(r"[0-9A-Z]{5}", sqlstate) else ""
    return f"{type(error).__name__}{suffix}"


def _display(value: str) -> str:
    return replace_control_characters(value).strip()


def _passed(step_id: SetupStepId, summary: str) -> SetupStep:
    return SetupStep(id=step_id, state=StepState.PASSED, summary=summary)


def _failed(step_id: SetupStepId, code: str, summary: str) -> SetupStep:
    return SetupStep(
        id=step_id,
        state=StepState.FAILED,
        code=code,
        summary=summary,
        actions=(SetupActionId.RECONCILE,),
    )


def _sanitize_identity(value: str | None) -> str | None:
    if value is None:
        return None
    sanitized = replace_control_characters(value)
    return sanitized.strip() or None
