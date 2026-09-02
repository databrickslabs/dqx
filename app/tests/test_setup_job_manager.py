"""Behavioural tests for task-runner job reconciliation."""

from unittest.mock import MagicMock

import pytest
from databricks.sdk.service.jobs import Job, JobRunAs, JobSettings

from databricks_labs_dqx_app.backend.setup.job_manager import ResolvedJob, TaskRunnerJobManager
from databricks_labs_dqx_app.backend.setup.models import SetupStepId, StepState


@pytest.fixture
def manager() -> TaskRunnerJobManager:
    """Return a job manager against an isolated Jobs API boundary."""
    workspace = MagicMock(name="WorkspaceClient")
    workspace.jobs.list.return_value = []
    return TaskRunnerJobManager(workspace=workspace, setup_user="admin@example.com")


def managed_job(job_id: int) -> Job:
    """Build the minimal Jobs API response for a Studio-managed job."""
    return Job(
        job_id=job_id,
        settings=JobSettings(tags={"dqx_studio_managed": "true", "dqx_component": "task_runner"}),
    )


def job_with_run_as(
    service_principal_name: str | None = None, *, user_name: str | None = None, group_name: str | None = None
) -> Job:
    """Build a job response with an explicit run-as assignment."""
    return Job(
        settings=JobSettings(
            run_as=JobRunAs(
                service_principal_name=service_principal_name,
                user_name=user_name,
                group_name=group_name,
            )
        )
    )


def test_resolve_uses_configured_job(manager: TaskRunnerJobManager) -> None:
    """A configured job ID prevents discovery and duplicate creation."""
    result = manager.resolve("42")

    assert result == ResolvedJob(job_id=42, created=False)
    manager.workspace.jobs.create.assert_not_called()


def test_resolve_rediscovers_tagged_job_before_create(manager: TaskRunnerJobManager) -> None:
    """A retry reuses a tagged job instead of creating a second runner."""
    manager.workspace.jobs.list.return_value = [managed_job(job_id=43)]

    assert manager.resolve(None) == ResolvedJob(job_id=43, created=False)
    manager.workspace.jobs.create.assert_not_called()


def test_resolve_creates_tagged_job_with_setup_admin_access(manager: TaskRunnerJobManager) -> None:
    """A first reconciliation creates a discoverable job the setup admin can manage."""
    manager.workspace.jobs.create.return_value.job_id = 44

    assert manager.resolve(None) == ResolvedJob(job_id=44, created=True)

    created = manager.workspace.jobs.create.call_args.kwargs
    assert created["tags"] == {"dqx_studio_managed": "true", "dqx_component": "task_runner"}
    assert created["access_control_list"][0].user_name == "admin@example.com"


def test_configure_preserves_external_run_as(manager: TaskRunnerJobManager) -> None:
    """Reconciliation updates Studio-owned settings without overwriting the runner identity."""
    manager.workspace.jobs.get.return_value = job_with_run_as("runner-client-id")

    manager.configure(42, ["/Volumes/c/s/v/runner.whl", "/Volumes/c/s/v/dqx.whl"])

    settings = manager.workspace.jobs.update.call_args.kwargs["new_settings"]
    assert settings.run_as is None
    assert settings.environments[0].spec.dependencies == ["/Volumes/c/s/v/runner.whl", "/Volumes/c/s/v/dqx.whl"]


def test_configure_preserves_external_job_tags(manager: TaskRunnerJobManager) -> None:
    """Reconciliation retains ownership, cost, and governance tags set outside Studio."""
    manager.workspace.jobs.get.return_value = Job(
        settings=JobSettings(
            tags={
                "owner": "data-platform",
                "cost_center": "finance",
                "governance": "regulated",
                "dqx_studio_managed": "false",
            }
        )
    )

    manager.configure(42, ["/Volumes/c/s/v/runner.whl"])

    settings = manager.workspace.jobs.update.call_args.kwargs["new_settings"]
    assert settings.tags == {
        "owner": "data-platform",
        "cost_center": "finance",
        "governance": "regulated",
        "dqx_studio_managed": "true",
        "dqx_component": "task_runner",
    }


@pytest.mark.parametrize(
    ("run_as", "expected_state"),
    [
        (None, StepState.ACTION_REQUIRED),
        (JobRunAs(user_name="runner@example.com"), StepState.ACTION_REQUIRED),
        (JobRunAs(group_name="runner-group"), StepState.ACTION_REQUIRED),
        (JobRunAs(service_principal_name="app-client-id"), StepState.ACTION_REQUIRED),
        (JobRunAs(service_principal_name="runner-client-id"), StepState.PASSED),
    ],
)
def test_validate_run_as_requires_a_distinct_service_principal(
    manager: TaskRunnerJobManager,
    run_as: JobRunAs | None,
    expected_state: StepState,
) -> None:
    """Only an external service principal satisfies task-runner readiness."""
    manager.workspace.jobs.get.return_value = Job(settings=JobSettings(run_as=run_as))

    step = manager.validate_run_as(42, "app-client-id")

    assert step.id == SetupStepId.TASK_RUNNER
    assert step.state == expected_state
