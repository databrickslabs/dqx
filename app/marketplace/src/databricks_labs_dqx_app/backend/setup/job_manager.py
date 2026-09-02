"""Idempotent management of the DQX Studio task-runner job."""

from dataclasses import dataclass

from databricks.labs.dqx.errors import InvalidParameterError
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import Environment
from databricks.sdk.service.jobs import (
    JobAccessControlRequest,
    JobEnvironment,
    JobParameterDefinition,
    JobPermissionLevel,
    JobSettings,
    PythonWheelTask,
    Task,
)

from databricks_labs_dqx_app.backend.setup.models import SetupStep, SetupStepId, StepState

_MANAGED_TAGS = {"dqx_studio_managed": "true", "dqx_component": "task_runner"}
_JOB_NAME = "dqx-studio-task-runner"
_TASK_KEY = "run_task"
_ENVIRONMENT_KEY = "default"


@dataclass(frozen=True)
class ResolvedJob:
    """The job selected or created during setup reconciliation."""

    job_id: int
    created: bool


class TaskRunnerJobManager:
    """Resolve and safely configure the Studio-owned portions of a job."""

    def __init__(self, workspace: WorkspaceClient, setup_user: str | None = None) -> None:
        self.workspace = workspace
        self._setup_user = (setup_user or "").strip() or None

    def resolve(self, configured_job_id: str | None) -> ResolvedJob:
        """Return the configured or tagged task-runner job, creating one if needed."""
        if configured_job_id:
            return ResolvedJob(job_id=_parse_job_id(configured_job_id), created=False)

        for job in self.workspace.jobs.list(expand_tasks=True):
            settings = getattr(job, "settings", None)
            tags = getattr(settings, "tags", None) or {}
            if all(tags.get(key) == value for key, value in _MANAGED_TAGS.items()):
                job_id = getattr(job, "job_id", None)
                if isinstance(job_id, int):
                    return ResolvedJob(job_id=job_id, created=False)

        created = self.workspace.jobs.create(
            name=_JOB_NAME,
            max_concurrent_runs=10,
            tags=dict(_MANAGED_TAGS),
            tasks=[_task_runner_task()],
            parameters=_task_runner_parameters(),
            environments=[_task_runner_environment([])],
            access_control_list=[_setup_admin_access(self._setup_user)] if self._setup_user else None,
        )
        job_id = getattr(created, "job_id", None)
        if not isinstance(job_id, int):
            raise RuntimeError("Databricks did not return a task-runner job ID.")
        return ResolvedJob(job_id=job_id, created=True)

    def grant_setup_admin(self, job_id: int, user_name: str) -> None:
        """Give the setup administrator management access to a task-runner job."""
        permissions = self.workspace.jobs.get_permissions(str(job_id))
        for access in permissions.access_control_list or []:
            if (access.user_name or "").casefold() != user_name.casefold():
                continue
            granted_levels = {permission.permission_level for permission in access.all_permissions or []}
            if granted_levels & {JobPermissionLevel.CAN_MANAGE, JobPermissionLevel.IS_OWNER}:
                return
        self.workspace.jobs.update_permissions(
            str(job_id),
            access_control_list=[_setup_admin_access(user_name)],
        )

    def validate_run_as(self, job_id: int, app_sp_id: str) -> SetupStep:
        """Report whether a task-runner job uses a distinct service principal."""
        job = self.workspace.jobs.get(job_id)
        run_as = getattr(getattr(job, "settings", None), "run_as", None)
        service_principal = getattr(run_as, "service_principal_name", None)
        if service_principal and service_principal != app_sp_id:
            return SetupStep(
                id=SetupStepId.TASK_RUNNER,
                state=StepState.PASSED,
                code="task_runner_run_as_ready",
                summary="The task-runner job uses a separate service principal.",
            )
        return SetupStep(
            id=SetupStepId.TASK_RUNNER,
            state=StepState.ACTION_REQUIRED,
            code=_run_as_action_code(run_as, app_sp_id),
            summary="Assign a service principal distinct from the app identity as this job's run_as identity.",
        )

    def configure(self, job_id: int, wheel_paths: list[str]) -> None:
        """Update only Studio-managed task, parameter, tag, and environment settings."""
        job = self.workspace.jobs.get(job_id)
        existing_tags = getattr(getattr(job, "settings", None), "tags", None) or {}
        self.workspace.jobs.update(
            job_id=job_id,
            new_settings=JobSettings(
                tags={**existing_tags, **_MANAGED_TAGS},
                tasks=[_task_runner_task()],
                parameters=_task_runner_parameters(),
                environments=[_task_runner_environment(wheel_paths)],
            ),
        )


def _parse_job_id(configured_job_id: str) -> int:
    try:
        job_id = int(configured_job_id)
    except (TypeError, ValueError):
        raise InvalidParameterError("The configured task-runner job ID must be a positive integer.") from None
    if job_id <= 0:
        raise InvalidParameterError("The configured task-runner job ID must be a positive integer.")
    return job_id


def _setup_admin_access(user_name: str) -> JobAccessControlRequest:
    return JobAccessControlRequest(user_name=user_name, permission_level=JobPermissionLevel.CAN_MANAGE)


def _task_runner_task() -> Task:
    parameter_names = (
        "task_type",
        "view_fqn",
        "result_catalog",
        "result_schema",
        "config_json",
        "run_id",
        "requesting_user",
        "warehouse_id",
    )
    parameters = [item for name in parameter_names for item in (f"--{name}", f"{{{{job.parameters.{name}}}}}")]
    parameters.extend(["--job_run_id", "{{job.run_id}}"])
    return Task(
        task_key=_TASK_KEY,
        python_wheel_task=PythonWheelTask(
            package_name="databricks_labs_dqx_task_runner",
            entry_point="dqx-task-runner",
            parameters=parameters,
        ),
        environment_key=_ENVIRONMENT_KEY,
    )


def _task_runner_parameters() -> list[JobParameterDefinition]:
    return [
        JobParameterDefinition(name="task_type", default="profile"),
        JobParameterDefinition(name="view_fqn", default=""),
        JobParameterDefinition(name="result_catalog", default=""),
        JobParameterDefinition(name="result_schema", default=""),
        JobParameterDefinition(name="config_json", default="{}"),
        JobParameterDefinition(name="run_id", default=""),
        JobParameterDefinition(name="requesting_user", default="unknown"),
        JobParameterDefinition(name="warehouse_id", default=""),
    ]


def _task_runner_environment(wheel_paths: list[str]) -> JobEnvironment:
    return JobEnvironment(
        environment_key=_ENVIRONMENT_KEY,
        spec=Environment(client="5", dependencies=wheel_paths),
    )


def _run_as_action_code(run_as: object, app_sp_id: str) -> str:
    if run_as is None:
        return "task_runner_run_as_missing"
    if getattr(run_as, "service_principal_name", None) == app_sp_id:
        return "task_runner_run_as_matches_app"
    return "task_runner_run_as_not_service_principal"
