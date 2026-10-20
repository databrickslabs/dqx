"""Idempotent management of the DQX Studio task-runner job."""

import logging
import uuid
from dataclasses import dataclass

from databricks.labs.dqx.errors import InvalidParameterError
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.compute import Environment
from databricks.sdk.service.jobs import (
    JobAccessControlRequest,
    JobEnvironment,
    JobParameterDefinition,
    JobPermissionLevel,
    JobRunAs,
    JobSettings,
    PythonWheelTask,
    Task,
)

from databricks_labs_dqx_app.backend.sanitization import replace_control_characters
from databricks_labs_dqx_app.backend.setup.models import (
    SetupActionId,
    SetupStep,
    SetupStepId,
    StepState,
    TaskRunnerPrincipal,
    TaskRunnerSpPermission,
)

logger = logging.getLogger(__name__)

_DISCOVERY_TAGS = {"dqx_studio_managed": "true", "dqx_component": "task_runner"}
_MANAGED_TAGS = {"app": "dqx-studio", **_DISCOVERY_TAGS}
_JOB_NAME = "dqx-studio-task-runner"
_TASK_KEY = "run_task"
_ENVIRONMENT_KEY = "default"
# Must stay in sync with ``timeout_seconds`` on the DAB job and
# ``_IN_FLIGHT_MAX_AGE`` in ``scheduler_service``.
_TASK_RUNNER_TIMEOUT_SECONDS = 4 * 60 * 60
# Most service principals offered in the setup picker; a longer list falls back to pasting an ID.
MAX_LISTED_PRINCIPALS = 500
_PRINCIPAL_ATTRIBUTES = "id,displayName,applicationId"
# Either role lets the app run a job as the service principal (the Permissions tab's CAN USE / CAN MANAGE).
_USE_ROLES = frozenset({"roles/servicePrincipal.user", "roles/servicePrincipal.manager"})
_INVALID_APPLICATION_ID = "A service principal Application ID is a value like 1a2b3c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d."
_RUN_AS_SUMMARIES = {
    "task_runner_run_as_missing": (
        "Studio's background job needs its own service principal, separate from the app's. "
        "Follow the three steps below."
    ),
    "task_runner_run_as_matches_app": (
        "Studio's background job is currently using the app's own service principal instead of a dedicated "
        "task runner service principal. Follow the steps below to create one."
    ),
    "task_runner_run_as_not_service_principal": (
        "Studio's background job runs as a person, so it would stop working if they left. "
        "It needs a service principal. Follow the three steps below."
    ),
}
_DEPLOYMENT_RUN_AS_SUMMARY = (
    "Studio was installed with a deployment bundle, so this job's service principal is chosen at install time "
    "and can't be changed here. Ask whoever installed Studio to set dqx_service_principal_application_id in the "
    "bundle's settings to the Application ID of a service principal (not the app's own), then deploy Studio "
    "again. After that, click Verify again."
)


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
            if all(tags.get(key) == value for key, value in _DISCOVERY_TAGS.items()):
                job_id = getattr(job, "job_id", None)
                if isinstance(job_id, int):
                    return ResolvedJob(job_id=job_id, created=False)

        created = self.workspace.jobs.create(
            name=_JOB_NAME,
            max_concurrent_runs=10,
            timeout_seconds=_TASK_RUNNER_TIMEOUT_SECONDS,
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

    def exists(self, job_id: int) -> bool:
        """Return whether task-runner job *job_id* still exists; other lookup errors propagate."""
        try:
            self.workspace.jobs.get(job_id)
        except NotFound:
            return False
        return True

    def validate_run_as(self, job_id: int, app_sp_id: str, *, managed_by_deployment: bool = False) -> SetupStep:
        """Report whether a task-runner job uses a distinct service principal.

        Args:
            job_id: Task-runner job to inspect.
            app_sp_id: Application ID of the app's own service principal.
            managed_by_deployment: Whether the job comes from the deployment bundle, which sets
                its run_as; Studio then points to the deployment variable instead of offering
                to choose a service principal, because a redeploy would undo the choice.
        """
        job = self.workspace.jobs.get(job_id)
        run_as = getattr(getattr(job, "settings", None), "run_as", None)
        service_principal = getattr(run_as, "service_principal_name", None)
        if service_principal and not _same_application_id(service_principal, app_sp_id):
            return SetupStep(
                id=SetupStepId.TASK_RUNNER,
                state=StepState.PASSED,
                code="task_runner_run_as_ready",
                summary="The task-runner job runs as its own service principal.",
            )
        code = _run_as_action_code(run_as, app_sp_id)
        if managed_by_deployment:
            return SetupStep(
                id=SetupStepId.TASK_RUNNER,
                state=StepState.ACTION_REQUIRED,
                code=code,
                summary=_DEPLOYMENT_RUN_AS_SUMMARY,
                actions=(SetupActionId.VERIFY_AGAIN,),
            )
        return SetupStep(
            id=SetupStepId.TASK_RUNNER,
            state=StepState.ACTION_REQUIRED,
            code=code,
            summary=_RUN_AS_SUMMARIES[code],
            actions=(SetupActionId.CHOOSE_TASK_RUNNER, SetupActionId.VERIFY_AGAIN),
        )

    def app_permission_on(self, application_id: str, app_application_id: str) -> TaskRunnerSpPermission:
        """Whether the app may use service principal *application_id*, as far as the app can tell.

        Reads the service principal's account rule set as the app. The answer is *confirmed*
        when the app is granted a role that lets it use the service principal, *missing* when
        the rule set shows no such grant to the app or to any group, and *unverified* when the
        rule set can't be read (or a group grant might include the app). Setting the job's
        run_as is what finally proves it, so no answer blocks: *missing* is only a warning,
        since the app may hold the role in a way the rule set doesn't show.

        Args:
            application_id: Application ID of the chosen service principal.
            app_application_id: Application ID of the app's own service principal.
        """
        account_id = (getattr(self.workspace.config, "account_id", None) or "").strip()
        if not account_id:
            return "unverified"
        name = f"accounts/{account_id}/servicePrincipals/{application_id}/ruleSets/default"
        try:
            rule_set = self.workspace.account_access_control_proxy.get_rule_set(name=name, etag="")
        except Exception as error:
            logger.info(f"Could not read the chosen service principal's permissions ({type(error).__name__})")
            return "unverified"
        app_principal = f"servicePrincipals/{app_application_id.strip()}".casefold()
        group_may_grant = False
        for rule in rule_set.grant_rules or []:
            if rule.role not in _USE_ROLES:
                continue
            principals = [principal.casefold() for principal in rule.principals or []]
            if app_principal in principals:
                return "confirmed"
            group_may_grant = group_may_grant or any(principal.startswith("groups/") for principal in principals)
        return "unverified" if group_may_grant else "missing"

    def set_run_as(self, job_id: int, application_id: str) -> None:
        """Make task-runner job *job_id* run as the service principal *application_id*.

        Only run_as is sent, so every other job setting is left alone, and a job already
        running as that service principal is not updated. Errors propagate: Databricks
        refuses with PermissionDenied until the app may use the service principal.

        Args:
            job_id: Task-runner job to update.
            application_id: Normalized Application ID of the chosen service principal.
        """
        job = self.workspace.jobs.get(job_id)
        run_as = getattr(getattr(job, "settings", None), "run_as", None)
        current = getattr(run_as, "service_principal_name", None)
        if current and _same_application_id(current, application_id):
            return
        self.workspace.jobs.update(
            job_id=job_id,
            new_settings=JobSettings(run_as=JobRunAs(service_principal_name=application_id)),
        )

    def configure(self, job_id: int, wheel_paths: list[str]) -> None:
        """Update only Studio-managed task, parameter, tag, and environment settings."""
        job = self.workspace.jobs.get(job_id)
        existing_tags = getattr(getattr(job, "settings", None), "tags", None) or {}
        self.workspace.jobs.update(
            job_id=job_id,
            new_settings=JobSettings(
                tags={**existing_tags, **_MANAGED_TAGS},
                timeout_seconds=_TASK_RUNNER_TIMEOUT_SECONDS,
                tasks=[_task_runner_task()],
                parameters=_task_runner_parameters(),
                environments=[_task_runner_environment(wheel_paths)],
            ),
        )


@dataclass(frozen=True)
class TaskRunnerPrincipalListing:
    """Service principals offered for the task-runner job, and the app's own display name.

    *truncated* is true when the workspace has more service principals than were listed.
    """

    principals: tuple[TaskRunnerPrincipal, ...]
    app_display_name: str | None
    truncated: bool = False


def normalize_application_id(value: str) -> str:
    """Return *value* as a lower-case canonical UUID, the form of a service principal Application ID.

    Raises:
        InvalidParameterError: If *value* isn't a UUID.
    """
    candidate = (value or "").strip()
    try:
        parsed = uuid.UUID(candidate)
    except ValueError:
        raise InvalidParameterError(_INVALID_APPLICATION_ID) from None
    if str(parsed) != candidate.lower():
        # Rejects the brace, URN and undashed forms uuid.UUID also accepts.
        raise InvalidParameterError(_INVALID_APPLICATION_ID)
    return str(parsed)


def list_task_runner_principals(
    workspace: WorkspaceClient, app_application_id: str, *, limit: int = MAX_LISTED_PRINCIPALS
) -> TaskRunnerPrincipalListing:
    """List workspace service principals the task-runner job could run as, sorted by name.

    The app's own service principal is left out (its display name is returned separately so
    the administrator can find the app), as are entries without an Application ID. At most
    *limit* service principals are listed: reading stops at the first one past the limit,
    which sets *truncated*. Listing errors propagate.

    Args:
        workspace: App service principal workspace client.
        app_application_id: Application ID of the app's own service principal.
        limit: Most service principals to list.
    """
    principals: list[TaskRunnerPrincipal] = []
    app_display_name: str | None = None
    truncated = False
    for principal in workspace.service_principals.list(attributes=_PRINCIPAL_ATTRIBUTES):
        application_id = (principal.application_id or "").strip()
        display_name = _display(principal.display_name or "") or application_id
        if not application_id:
            continue
        if _same_application_id(application_id, app_application_id):
            app_display_name = display_name
            continue
        if len(principals) == limit:
            truncated = True
            break
        principals.append(
            TaskRunnerPrincipal(
                id=_display(principal.id or ""),
                display_name=display_name,
                application_id=_display(application_id),
            )
        )
    principals.sort(key=lambda item: (item.display_name.casefold(), item.application_id))
    return TaskRunnerPrincipalListing(
        principals=tuple(principals), app_display_name=app_display_name, truncated=truncated
    )


def _same_application_id(left: str, right: str) -> bool:
    return left.strip().casefold() == right.strip().casefold()


def _display(value: str) -> str:
    return replace_control_characters(value).strip()


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
    service_principal = getattr(run_as, "service_principal_name", None)
    if service_principal and _same_application_id(service_principal, app_sp_id):
        return "task_runner_run_as_matches_app"
    return "task_runner_run_as_not_service_principal"
