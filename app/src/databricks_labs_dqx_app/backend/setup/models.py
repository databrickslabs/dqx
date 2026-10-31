"""API-safe models for DQX Studio setup readiness."""

from enum import Enum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class _StrEnum(str, Enum):
    def __str__(self) -> str:
        return self.value


class SetupState(_StrEnum):
    """Process-wide setup lifecycle states."""

    CHECKING = "checking"
    SETUP_REQUIRED = "setup_required"
    INITIALIZING = "initializing"
    READY = "ready"


class StepState(_StrEnum):
    """State of one ordered setup capability or action."""

    PENDING = "pending"
    RUNNING = "running"
    PASSED = "passed"
    ACTION_REQUIRED = "action_required"
    FAILED = "failed"
    WARNING = "warning"
    OVERRIDDEN = "overridden"


class SetupStepId(_StrEnum):
    """Stable identifiers for the ordered setup workflow."""

    IDENTITY = "identity"
    LAKEBASE = "lakebase"
    CONFIGURATION = "configuration"
    UNITY_CATALOG = "unity_catalog"
    STORAGE = "storage"
    WAREHOUSE = "warehouse"
    TASK_RUNNER = "task_runner"
    WHEELS = "wheels"
    MIGRATIONS = "migrations"
    ACTIVATION = "activation"
    ACCESS = "access"
    AI = "ai"
    APP_SHARING = "app_sharing"


class SetupActionId(_StrEnum):
    """Retry-safe actions advertised by setup reports."""

    RECONCILE = "reconcile"
    VERIFY_AGAIN = "verify_again"
    CONFIGURE = "configure"
    OVERRIDE = "override"
    CHOOSE_TASK_RUNNER = "choose_task_runner"


class _ImmutableModel(BaseModel):
    model_config = ConfigDict(frozen=True)


class SetupStep(_ImmutableModel):
    """Sanitized status for one setup step.

    *instructions* is prose for the administrator; *grant_statements* holds the GRANT
    statements the step still needs, kept apart so they can be copied or run as SQL.
    """

    id: SetupStepId
    state: StepState
    code: str = ""
    summary: str = ""
    instructions: tuple[str, ...] = ()
    grant_statements: tuple[str, ...] = ()
    actions: tuple[SetupActionId, ...] = ()


class SetupReport(_ImmutableModel):
    """Current ordered setup state published to request handlers."""

    state: SetupState
    steps: tuple[SetupStep, ...]
    current_step: SetupStepId | None = None

    def step(self, step_id: SetupStepId) -> SetupStep:
        """Return a reported step by its stable identifier.

        Args:
            step_id: Step identifier to locate.

        Returns:
            The matching setup step.

        Raises:
            LookupError: If the report does not contain the requested step.
        """
        for item in self.steps:
            if item.id == step_id:
                return item
        raise LookupError(f"Setup report does not contain step {step_id.value!r}")


GrantErrorCode = Literal["permission_denied", "warehouse_unavailable", "not_found", "rate_limited", "unknown"]


class SetupGrantFailure(_ImmutableModel):
    """A setup GRANT statement that could not be run, with a sanitized error code."""

    statement: str
    error_code: GrantErrorCode


class SetupApplyGrantsResponse(_ImmutableModel):
    """Outcome of running a step's GRANT statements as the signed-in administrator."""

    report: SetupReport
    applied: int
    total: int
    failed: tuple[SetupGrantFailure, ...] = ()


class SetupConfigurationView(_ImmutableModel):
    """Sanitized view of the active Studio storage and audience configuration."""

    source: Literal["deployment", "saved", "none"]
    catalog: str = ""
    prefix: str = ""
    audience_group: str = ""
    schemas: tuple[str, ...] = ()
    broad_audience: bool = False
    locked: bool = False


class SetupConfigurationRequest(BaseModel):
    """Administrator-submitted catalog, prefix and audience group (Marketplace path)."""

    model_config = ConfigDict(extra="forbid")

    catalog: str = Field(max_length=255)
    prefix: str = Field(default="dqx_studio", max_length=64)
    audience_group: str = Field(max_length=255)


class SetupGroupsResponse(_ImmutableModel):
    """Display names of the workspace groups an administrator can choose as the default user group.

    *truncated* is true when the workspace has more groups than are listed, so the
    administrator may need to type the name.
    """

    groups: tuple[str, ...] = ()
    truncated: bool = False


class SetupOverrideRequest(BaseModel):
    """Administrator confirmation that a step's requirement was met in a way Studio cannot see."""

    model_config = ConfigDict(extra="forbid")

    step_id: SetupStepId


class TaskRunnerPrincipal(_ImmutableModel):
    """A workspace service principal the task-runner job could run as."""

    id: str
    display_name: str
    application_id: str


class TaskRunnerPrincipalsResponse(_ImmutableModel):
    """Service principals an administrator can choose for the task-runner job.

    *available* is false when Studio couldn't list them, so the administrator pastes an
    Application ID instead. *app_name* names the DQX Studio app so the administrator can
    find it when giving it access to the chosen service principal. *truncated* is true
    when the workspace has more service principals than are listed, so the administrator
    may need to paste the Application ID.
    """

    available: bool
    principals: tuple[TaskRunnerPrincipal, ...] = ()
    app_name: str = ""
    truncated: bool = False


class TaskRunnerPrincipalRequest(BaseModel):
    """The service principal an administrator picked for the task-runner job, by Application ID."""

    model_config = ConfigDict(extra="forbid")

    application_id: str = Field(max_length=64)


TaskRunnerSpPermission = Literal["confirmed", "missing", "unverified"]


class TaskRunnerAccessResponse(_ImmutableModel):
    """Whether a picked service principal is ready to run Studio's task-runner job.

    *sp_permission* says whether the app may use the service principal: *confirmed*,
    *missing*, or *unverified* when Studio can't read that permission. *grant_statements*
    are the GRANT statements only a person can run for it; *automatic_grants* are the ones
    setup runs itself on Studio's own objects once the task-runner job runs as it.
    *grants_unverified* is true when some of its access couldn't be read.
    """

    sp_permission: TaskRunnerSpPermission
    grant_statements: tuple[str, ...] = ()
    automatic_grants: tuple[str, ...] = ()
    grants_unverified: bool = False


class TaskRunnerGrantsResponse(_ImmutableModel):
    """Outcome of running a picked service principal's GRANT statements as the administrator.

    *grant_statements* are the statements the service principal still needs afterwards.
    """

    applied: int
    total: int
    failed: tuple[SetupGrantFailure, ...] = ()
    grant_statements: tuple[str, ...] = ()
    grants_unverified: bool = False


class SetupStatusResponse(_ImmutableModel):
    """Setup report projected with caller-specific management access."""

    report: SetupReport
    can_manage: bool
    admin_group: str
    user_name: str = ""
    task_runner_job_id: int | None = None
    configuration: SetupConfigurationView | None = None
