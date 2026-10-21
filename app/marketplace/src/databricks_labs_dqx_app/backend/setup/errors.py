"""Sanitized failures for required Studio setup actions."""

from databricks.labs.dqx.errors import InvalidParameterError


class RequiredViewSetupError(RuntimeError):
    """Required score or entitlement objects could not be created."""

    def __init__(self) -> None:
        super().__init__("Could not create required Studio views.")


class TaskRunnerManagedByDeploymentError(RuntimeError):
    """The task-runner job comes from the deployment bundle, which sets its run_as."""

    def __init__(self) -> None:
        super().__init__("The task-runner job's service principal is set by the deployment.")


class TaskRunnerJobUnavailableError(RuntimeError):
    """Setup hasn't resolved a task-runner job yet."""

    def __init__(self) -> None:
        super().__init__("Studio's task-runner job isn't set up yet.")


class AppServicePrincipalChosenError(InvalidParameterError):
    """The app's own service principal was chosen to run the task-runner job."""

    def __init__(self) -> None:
        super().__init__("The task-runner job needs a service principal other than the app's own.")
