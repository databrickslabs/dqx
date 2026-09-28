"""Serialized, deployment-agnostic setup reconciliation for DQX Studio."""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Protocol

from databricks.sdk import WorkspaceClient

from databricks_labs_dqx_app.backend.setup.job_manager import ResolvedJob
from databricks_labs_dqx_app.backend.setup.models import (
    SetupActionId,
    SetupReport,
    SetupState,
    SetupStep,
    SetupStepId,
    StepState,
)
from databricks_labs_dqx_app.backend.setup.resources import ActiveResources
from databricks_labs_dqx_app.backend.setup.runtime import SetupRuntime
from databricks_labs_dqx_app.backend.sanitization import replace_control_characters

logger = logging.getLogger(__name__)

_STEP_ORDER = (
    SetupStepId.IDENTITY,
    SetupStepId.VOLUME,
    SetupStepId.UNITY_CATALOG,
    SetupStepId.SCHEMAS,
    SetupStepId.LAKEBASE,
    SetupStepId.WAREHOUSE,
    SetupStepId.TASK_RUNNER,
    SetupStepId.WHEELS,
    SetupStepId.MIGRATIONS,
    SetupStepId.ACTIVATION,
)


class SetupCheckers(Protocol):
    """Capability-checking surface consumed by the orchestrator."""

    def check_app_identity(self) -> SetupStep: ...

    def check_volume(self) -> SetupStep: ...

    def check_unity_catalog(self) -> SetupStep: ...

    def ensure_sibling_schemas(self) -> SetupStep: ...

    def check_lakebase(self) -> SetupStep: ...

    def ensure_lakebase_schema(self) -> SetupStep: ...

    def check_warehouse(
        self,
        warehouse_id: str | None = None,
        reader_ws: WorkspaceClient | None = None,
    ) -> SetupStep: ...


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


class SetupOrchestrator:
    """Reconcile Studio-owned setup actions in order."""

    def __init__(
        self,
        *,
        runtime: SetupRuntime,
        resources: ActiveResources,
        checkers: SetupCheckers,
        jobs: SetupJobs,
        pg_migrations: SetupMigrations,
        delta_migrations: SetupMigrations,
        app_settings: SetupCompletionStore,
        publish_wheels: Callable[[], Awaitable[list[str]]],
        activation: StudioActivation,
        app_sp_id: str,
    ) -> None:
        self.runtime = runtime
        self.resources = resources
        self.checkers = checkers
        self.jobs = jobs
        self.pg_migrations = pg_migrations
        self.delta_migrations = delta_migrations
        self.app_settings = app_settings
        self.publish_wheels = publish_wheels
        self.activation = activation
        self.app_sp_id = _sanitize_identity(app_sp_id) or ""

    async def reconcile(self, setup_user: str | None = None) -> SetupReport:
        """Run retry-safe setup actions serially and activate only after migrations."""
        async with self.runtime.activation_lock:
            actor = _sanitize_identity(setup_user)
            current_report = self.runtime.report()
            if current_report.state == SetupState.READY:
                return await self._reconcile_ready(current_report, actor)

            steps: list[SetupStep] = []
            self.runtime.publish(SetupReport(state=SetupState.CHECKING, steps=()))

            for check in (
                self.checkers.check_app_identity,
                self.checkers.check_volume,
                self.checkers.check_unity_catalog,
            ):
                step = await asyncio.to_thread(check)
                if stopped := self._append_and_stop(steps, step):
                    return stopped

            step = await asyncio.to_thread(self.checkers.ensure_sibling_schemas)
            if stopped := self._append_and_stop(steps, step):
                return stopped

            step = await asyncio.to_thread(self.checkers.check_lakebase)
            if stopped := self._append_and_stop(steps, step):
                return stopped
            step = await asyncio.to_thread(self.checkers.ensure_lakebase_schema)
            if stopped := self._replace_and_stop(steps, step):
                return stopped

            step = await asyncio.to_thread(self.checkers.check_warehouse)
            if stopped := self._append_and_stop(steps, step):
                return stopped

            task_step = await self._reconcile_job(actor)
            if stopped := self._append_and_stop(steps, task_step):
                return stopped

            wheel_step = await self._reconcile_wheels()
            if stopped := self._append_and_stop(steps, wheel_step):
                return stopped

            migration_step = await self._run_migrations()
            if stopped := self._append_and_stop(steps, migration_step):
                return stopped

            job_id = self.runtime.require_job_id()
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
                stopped = self._replace_and_stop(steps, step)
                if stopped is not None:
                    return stopped
                raise RuntimeError("Unreachable setup persistence state")

            try:
                await self.activation.activate()
            except Exception:
                activation_step = _failed(
                    SetupStepId.ACTIVATION,
                    "studio_activation_failed",
                    "Could not activate Studio background services.",
                )
                return self._publish_stopped([*steps, activation_step], SetupStepId.ACTIVATION)

            steps.append(_passed(SetupStepId.ACTIVATION, "DQX Studio is active."))
            report = SetupReport(state=SetupState.READY, steps=tuple(steps))
            self.runtime.publish(report)
            try:
                await self.activation.start_background()
            except Exception:
                logger.warning(
                    "Could not start Studio background services; the activated application remains available.",
                    exc_info=True,
                )
            return report

    async def _reconcile_ready(self, report: SetupReport, setup_user: str | None) -> SetupReport:
        if setup_user is None:
            return report
        try:
            await asyncio.to_thread(self.jobs.grant_setup_admin, self.runtime.require_job_id(), setup_user)
        except Exception:
            logger.warning(
                "Could not refresh setup administrator access to the Studio task-runner job; "
                "the ready application remains available."
            )
        return report

    async def _reconcile_job(self, setup_user: str | None) -> SetupStep:
        configured = str(self.runtime.job_id) if self.runtime.job_id is not None else self.resources.job_id
        try:
            resolved = await asyncio.to_thread(self.jobs.resolve, configured)
            self.runtime.job_id = resolved.job_id
            if setup_user is not None:
                await asyncio.to_thread(self.jobs.grant_setup_admin, resolved.job_id, setup_user)
            return await asyncio.to_thread(self.jobs.validate_run_as, resolved.job_id, self.app_sp_id)
        except Exception:
            return _failed(
                SetupStepId.TASK_RUNNER,
                "task_runner_reconciliation_failed",
                "Could not reconcile the Studio task-runner job.",
            )

    async def _reconcile_wheels(self) -> SetupStep:
        try:
            wheel_paths = await self.publish_wheels()
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

    async def _run_migrations(self) -> SetupStep:
        try:
            await asyncio.to_thread(self.pg_migrations.run_all)
            await asyncio.to_thread(self.delta_migrations.run_all)
            return _passed(SetupStepId.MIGRATIONS, "Postgres and Delta migrations are current.")
        except Exception:
            return _failed(
                SetupStepId.MIGRATIONS,
                "database_migration_failed",
                "Could not apply required database migrations.",
            )

    def _append_and_stop(self, steps: list[SetupStep], step: SetupStep) -> SetupReport | None:
        steps.append(step)
        if step.state != StepState.PASSED:
            return self._publish_stopped(steps, step.id)
        self._publish_initializing(steps, self._next_step(step.id))
        return None

    def _replace_and_stop(self, steps: list[SetupStep], step: SetupStep) -> SetupReport | None:
        if steps and steps[-1].id == step.id:
            steps[-1] = step
        else:
            steps.append(step)
        if step.state != StepState.PASSED:
            return self._publish_stopped(steps, step.id)
        self._publish_initializing(steps, self._next_step(step.id))
        return None

    def _publish_initializing(self, steps: list[SetupStep], current_step: SetupStepId | None) -> None:
        self.runtime.publish(SetupReport(state=SetupState.INITIALIZING, current_step=current_step, steps=tuple(steps)))

    def _publish_stopped(self, steps: list[SetupStep], current_step: SetupStepId) -> SetupReport:
        report = SetupReport(state=SetupState.SETUP_REQUIRED, current_step=current_step, steps=tuple(steps))
        self.runtime.publish(report)
        return report

    @staticmethod
    def _next_step(step_id: SetupStepId) -> SetupStepId | None:
        index = _STEP_ORDER.index(step_id) + 1
        return _STEP_ORDER[index] if index < len(_STEP_ORDER) else None


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
