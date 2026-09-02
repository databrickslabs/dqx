"""API-safe models for DQX Studio setup readiness."""

from enum import StrEnum

from pydantic import BaseModel, ConfigDict


class SetupState(StrEnum):
    """Process-wide setup lifecycle states."""

    CHECKING = "checking"
    SETUP_REQUIRED = "setup_required"
    INITIALIZING = "initializing"
    READY = "ready"


class StepState(StrEnum):
    """State of one ordered setup capability or action."""

    PENDING = "pending"
    RUNNING = "running"
    PASSED = "passed"
    ACTION_REQUIRED = "action_required"
    FAILED = "failed"


class SetupStepId(StrEnum):
    """Stable identifiers for the ordered setup workflow."""

    IDENTITY = "identity"
    VOLUME = "volume"
    UNITY_CATALOG = "unity_catalog"
    SCHEMAS = "schemas"
    LAKEBASE = "lakebase"
    WAREHOUSE = "warehouse"
    TASK_RUNNER = "task_runner"
    WHEELS = "wheels"
    MIGRATIONS = "migrations"
    ACTIVATION = "activation"


class SetupActionId(StrEnum):
    """Retry-safe actions advertised by setup reports."""

    RECONCILE = "reconcile"
    VERIFY_AGAIN = "verify_again"


class _ImmutableModel(BaseModel):
    model_config = ConfigDict(frozen=True)


class SetupStep(_ImmutableModel):
    """Sanitized status for one setup step."""

    id: SetupStepId
    state: StepState
    code: str = ""
    summary: str = ""
    instructions: tuple[str, ...] = ()
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


class SetupStatusResponse(_ImmutableModel):
    """Setup report projected with caller-specific management access."""

    report: SetupReport
    can_manage: bool
    admin_group: str
