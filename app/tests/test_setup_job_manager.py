"""Behavioural tests for task-runner job reconciliation."""

import uuid
from unittest.mock import MagicMock, create_autospec

import pytest
from databricks.labs.dqx.errors import InvalidParameterError
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, PermissionDenied
from databricks.sdk.service.iam import GrantRule, RuleSetResponse, ServicePrincipal
from databricks.sdk.service.jobs import (
    Job,
    JobAccessControlResponse,
    JobPermission,
    JobPermissionLevel,
    JobPermissions,
    JobRunAs,
    JobSettings,
)

from databricks_labs_dqx_app.backend.setup.job_manager import (
    ResolvedJob,
    TaskRunnerJobManager,
    list_task_runner_principals,
    normalize_application_id,
)
from databricks_labs_dqx_app.backend.setup.models import SetupActionId, SetupStepId, StepState


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
        settings=JobSettings(tags={"app": "dqx-studio", "dqx_studio_managed": "true", "dqx_component": "task_runner"}),
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


def test_exists_reports_a_deleted_job_as_missing(manager: TaskRunnerJobManager) -> None:
    manager.workspace.jobs.get.return_value = managed_job(42)
    assert manager.exists(42) is True

    manager.workspace.jobs.get.side_effect = NotFound("job deleted")
    assert manager.exists(42) is False


def test_exists_does_not_hide_other_lookup_errors(manager: TaskRunnerJobManager) -> None:
    manager.workspace.jobs.get.side_effect = PermissionDenied("no access")

    with pytest.raises(PermissionDenied):
        manager.exists(42)


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


def test_resolve_rediscovers_and_upgrades_legacy_managed_job(manager: TaskRunnerJobManager) -> None:
    """An upgraded installation reuses its legacy job and adds the ownership tag."""
    legacy_job = Job(
        job_id=45,
        settings=JobSettings(
            tags={
                "owner": "data-platform",
                "dqx_studio_managed": "true",
                "dqx_component": "task_runner",
            }
        ),
    )
    manager.workspace.jobs.list.return_value = [legacy_job]
    manager.workspace.jobs.get.return_value = legacy_job

    resolved = manager.resolve(None)
    manager.configure(resolved.job_id, ["/Volumes/c/s/v/runner.whl"])

    assert resolved == ResolvedJob(job_id=45, created=False)
    manager.workspace.jobs.create.assert_not_called()
    settings = manager.workspace.jobs.update.call_args.kwargs["new_settings"]
    assert settings.tags == {
        "owner": "data-platform",
        "app": "dqx-studio",
        "dqx_studio_managed": "true",
        "dqx_component": "task_runner",
    }


def test_resolve_creates_tagged_job_with_setup_admin_access(manager: TaskRunnerJobManager) -> None:
    """A first reconciliation creates a discoverable job the setup admin can manage."""
    manager.workspace.jobs.create.return_value.job_id = 44

    assert manager.resolve(None) == ResolvedJob(job_id=44, created=True)

    created = manager.workspace.jobs.create.call_args.kwargs
    assert created["timeout_seconds"] == 4 * 60 * 60
    assert created["tags"] == {
        "app": "dqx-studio",
        "dqx_studio_managed": "true",
        "dqx_component": "task_runner",
    }
    assert created["access_control_list"][0].user_name == "admin@example.com"


@pytest.mark.parametrize("permission_level", [JobPermissionLevel.IS_OWNER, JobPermissionLevel.CAN_MANAGE])
def test_grant_setup_admin_preserves_existing_management_access(
    manager: TaskRunnerJobManager,
    permission_level: JobPermissionLevel,
) -> None:
    """Existing management access is not changed by an idempotent setup retry."""
    manager.workspace.jobs.get_permissions.return_value = JobPermissions(
        object_id="42",
        object_type="job",
        access_control_list=[
            JobAccessControlResponse(
                user_name="admin@example.com",
                display_name="Setup Admin",
                all_permissions=[
                    JobPermission(inherited=False, permission_level=permission_level),
                ],
            )
        ],
    )

    manager.grant_setup_admin(42, "admin@example.com")

    manager.workspace.jobs.update_permissions.assert_not_called()


def test_configure_preserves_external_run_as(manager: TaskRunnerJobManager) -> None:
    """Reconciliation updates Studio-owned settings without overwriting the runner identity."""
    manager.workspace.jobs.get.return_value = job_with_run_as("runner-client-id")

    manager.configure(42, ["/Volumes/c/s/v/runner.whl", "/Volumes/c/s/v/dqx.whl"])

    settings = manager.workspace.jobs.update.call_args.kwargs["new_settings"]
    assert settings.run_as is None
    assert settings.environments[0].spec.dependencies == ["/Volumes/c/s/v/runner.whl", "/Volumes/c/s/v/dqx.whl"]


def test_configure_forwards_every_declared_parameter_to_the_runner(manager: TaskRunnerJobManager) -> None:
    """Each declared job parameter reaches the runner as a *--name* argument."""
    manager.workspace.jobs.get.return_value = managed_job(42)

    manager.configure(42, ["/Volumes/c/s/v/runner.whl"])

    settings = manager.workspace.jobs.update.call_args.kwargs["new_settings"]
    task_args = settings.tasks[0].python_wheel_task.parameters
    for name in (p.name for p in settings.parameters):
        assert f"--{name}" in task_args
        assert f"{{{{job.parameters.{name}}}}}" in task_args


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
    assert settings.timeout_seconds == 4 * 60 * 60
    assert settings.tags == {
        "owner": "data-platform",
        "cost_center": "finance",
        "governance": "regulated",
        "app": "dqx-studio",
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
    if expected_state == StepState.ACTION_REQUIRED:
        assert SetupActionId.VERIFY_AGAIN in step.actions


RUNNER_APPLICATION_ID = "6f1c2d3e-4a5b-4c6d-8e7f-90a1b2c3d4e5"
APP_APPLICATION_ID = "11111111-2222-4333-8444-555555555555"
_RUN_AS_CODES = (
    "task_runner_run_as_missing",
    "task_runner_run_as_matches_app",
    "task_runner_run_as_not_service_principal",
)


@pytest.mark.parametrize(
    ("run_as", "code", "summary"),
    [
        (
            None,
            "task_runner_run_as_missing",
            "Studio's background job needs its own service principal, separate from the app's. "
            "Follow the three steps below.",
        ),
        (
            JobRunAs(service_principal_name=APP_APPLICATION_ID),
            "task_runner_run_as_matches_app",
            "Studio's background job is currently using the app's own service principal instead of a dedicated "
            "task runner service principal. Follow the steps below to create one.",
        ),
        (
            JobRunAs(user_name="runner@example.com"),
            "task_runner_run_as_not_service_principal",
            "Studio's background job runs as a person, so it would stop working if they left. "
            "It needs a service principal. Follow the three steps below.",
        ),
    ],
)
def test_validate_run_as_offers_choosing_a_service_principal_for_an_app_created_job(
    manager: TaskRunnerJobManager, run_as: JobRunAs | None, code: str, summary: str
) -> None:
    """A job Studio created gets plain guidance and the in-Studio service principal picker."""
    manager.workspace.jobs.get.return_value = Job(settings=JobSettings(run_as=run_as))

    step = manager.validate_run_as(42, APP_APPLICATION_ID)

    assert step.code == code
    assert step.summary == summary
    assert step.actions == (SetupActionId.CHOOSE_TASK_RUNNER, SetupActionId.VERIFY_AGAIN)


def test_validate_run_as_ready_offers_no_action(manager: TaskRunnerJobManager) -> None:
    manager.workspace.jobs.get.return_value = job_with_run_as(RUNNER_APPLICATION_ID)

    step = manager.validate_run_as(42, APP_APPLICATION_ID)

    assert step.code == "task_runner_run_as_ready"
    assert step.actions == ()


@pytest.mark.parametrize(
    "run_as", [None, JobRunAs(service_principal_name=APP_APPLICATION_ID), JobRunAs(user_name="runner@example.com")]
)
def test_validate_run_as_points_bundle_managed_jobs_to_the_deployment_variable(
    manager: TaskRunnerJobManager, run_as: JobRunAs | None
) -> None:
    """A bundle-managed job's run_as is set at deploy time, so Studio never offers to change it."""
    manager.workspace.jobs.get.return_value = Job(settings=JobSettings(run_as=run_as))

    step = manager.validate_run_as(42, APP_APPLICATION_ID, managed_by_deployment=True)

    assert step.code in _RUN_AS_CODES
    assert step.summary == (
        "Studio was installed with a deployment bundle, so this job's service principal is chosen at install "
        "time and can't be changed here. Ask whoever installed Studio to set dqx_service_principal_application_id "
        "in the bundle's settings to the Application ID of a service principal (not the app's own), then deploy "
        "Studio again. After that, click Verify again."
    )
    assert step.actions == (SetupActionId.VERIFY_AGAIN,)


def test_validate_run_as_passes_a_bundle_managed_job_with_its_own_service_principal(
    manager: TaskRunnerJobManager,
) -> None:
    manager.workspace.jobs.get.return_value = job_with_run_as(RUNNER_APPLICATION_ID)

    step = manager.validate_run_as(42, APP_APPLICATION_ID, managed_by_deployment=True)

    assert step.state == StepState.PASSED
    assert step.actions == ()


def test_set_run_as_updates_only_the_job_run_as(manager: TaskRunnerJobManager) -> None:
    manager.workspace.jobs.get.return_value = Job(settings=JobSettings(run_as=None))

    manager.set_run_as(42, RUNNER_APPLICATION_ID)

    manager.workspace.jobs.update.assert_called_once()
    call = manager.workspace.jobs.update.call_args.kwargs
    assert call["job_id"] == 42
    settings = call["new_settings"]
    assert settings.run_as.service_principal_name == RUNNER_APPLICATION_ID
    assert settings.as_dict() == {"run_as": {"service_principal_name": RUNNER_APPLICATION_ID}}


def test_set_run_as_with_the_same_service_principal_is_a_no_op(manager: TaskRunnerJobManager) -> None:
    manager.workspace.jobs.get.return_value = job_with_run_as(RUNNER_APPLICATION_ID)

    manager.set_run_as(42, RUNNER_APPLICATION_ID)

    manager.workspace.jobs.update.assert_not_called()


def test_set_run_as_lets_permission_errors_propagate(manager: TaskRunnerJobManager) -> None:
    manager.workspace.jobs.get.return_value = Job(settings=JobSettings(run_as=None))
    manager.workspace.jobs.update.side_effect = PermissionDenied("missing servicePrincipal.user")

    with pytest.raises(PermissionDenied):
        manager.set_run_as(42, RUNNER_APPLICATION_ID)


def test_configure_after_set_run_as_never_sends_run_as(manager: TaskRunnerJobManager) -> None:
    """Studio's routine job update leaves the chosen (or bundle-set) run_as alone."""
    manager.workspace.jobs.get.return_value = Job(settings=JobSettings(run_as=None))
    manager.set_run_as(42, RUNNER_APPLICATION_ID)
    manager.workspace.jobs.get.return_value = job_with_run_as(RUNNER_APPLICATION_ID)

    manager.configure(42, ["/Volumes/c/s/v/runner.whl"])

    settings = manager.workspace.jobs.update.call_args.kwargs["new_settings"]
    assert settings.run_as is None
    assert "run_as" not in settings.as_dict()


@pytest.mark.parametrize("value", ["", "not-a-uuid", "6f1c2d3e4a5b", "admin@example.com", "6f1c2d3e-4a5b-4c6d-8e7f"])
def test_application_id_must_be_a_uuid(value: str) -> None:
    with pytest.raises(InvalidParameterError):
        normalize_application_id(value)


def test_application_id_is_normalized() -> None:
    assert normalize_application_id(f"  {RUNNER_APPLICATION_ID.upper()} ") == RUNNER_APPLICATION_ID


def _principal(display_name: str | None, application_id: str | None, principal_id: str = "1") -> ServicePrincipal:
    return ServicePrincipal(id=principal_id, display_name=display_name, application_id=application_id)


def test_list_task_runner_principals_excludes_the_app_and_sorts_by_name() -> None:
    workspace = create_autospec(WorkspaceClient, instance=True)
    workspace.service_principals.list.return_value = iter(
        [
            _principal("zeta-runner", "22222222-2222-4222-8222-222222222222", "3"),
            _principal("app-xyz dqx-studio", APP_APPLICATION_ID, "2"),
            _principal("Alpha runner", RUNNER_APPLICATION_ID, "1"),
            _principal("no application id", None, "4"),
        ]
    )

    listing = list_task_runner_principals(workspace, APP_APPLICATION_ID.upper())

    workspace.service_principals.list.assert_called_once_with(attributes="id,displayName,applicationId")
    assert [principal.display_name for principal in listing.principals] == ["Alpha runner", "zeta-runner"]
    assert APP_APPLICATION_ID not in {principal.application_id for principal in listing.principals}
    assert listing.app_display_name == "app-xyz dqx-studio"


def test_list_task_runner_principals_is_capped_and_sanitized() -> None:
    workspace = create_autospec(WorkspaceClient, instance=True)
    workspace.service_principals.list.return_value = iter(
        [_principal(f"runner\n{index:04d}", str(uuid.UUID(int=index + 1)), str(index)) for index in range(700)]
    )

    listing = list_task_runner_principals(workspace, APP_APPLICATION_ID, limit=500)

    assert len(listing.principals) == 500
    assert all("\n" not in principal.display_name for principal in listing.principals)


def test_list_task_runner_principals_reports_a_cut_list_and_stops_reading() -> None:
    read: list[int] = []

    def principals():
        for index in range(700):
            read.append(index)
            yield _principal(f"runner {index:04d}", str(uuid.UUID(int=index + 1)), str(index))

    workspace = create_autospec(WorkspaceClient, instance=True)
    workspace.service_principals.list.return_value = principals()

    listing = list_task_runner_principals(workspace, APP_APPLICATION_ID, limit=500)

    assert listing.truncated is True
    assert len(listing.principals) == 500
    assert len(read) == 501


def test_list_task_runner_principals_is_not_cut_when_everything_fits() -> None:
    # The app's own service principal doesn't count towards the limit.
    workspace = create_autospec(WorkspaceClient, instance=True)
    workspace.service_principals.list.return_value = iter(
        [
            _principal("app", APP_APPLICATION_ID, "0"),
            *[_principal(f"runner {index}", str(uuid.UUID(int=index + 1)), str(index)) for index in range(1, 3)],
        ]
    )

    listing = list_task_runner_principals(workspace, APP_APPLICATION_ID, limit=2)

    assert listing.truncated is False
    assert len(listing.principals) == 2


_RULE_SET_NAME = f"accounts/acct-1/servicePrincipals/{RUNNER_APPLICATION_ID}/ruleSets/default"


def _rule_set(*rules: GrantRule) -> RuleSetResponse:
    return RuleSetResponse(name=_RULE_SET_NAME, etag="etag", grant_rules=list(rules))


@pytest.fixture
def rule_set_manager() -> TaskRunnerJobManager:
    """A job manager whose app client knows its Databricks account."""
    workspace = create_autospec(WorkspaceClient, instance=True)
    workspace.config.account_id = "acct-1"
    return TaskRunnerJobManager(workspace=workspace)


@pytest.mark.parametrize("role", ["roles/servicePrincipal.user", "roles/servicePrincipal.manager"])
def test_app_permission_is_confirmed_when_the_app_holds_a_role_that_lets_it_use_the_service_principal(
    rule_set_manager: TaskRunnerJobManager, role: str
) -> None:
    rule_set_manager.workspace.account_access_control_proxy.get_rule_set.return_value = _rule_set(
        GrantRule(role=role, principals=["users/admin@example.com", f"servicePrincipals/{APP_APPLICATION_ID}"])
    )

    outcome = rule_set_manager.app_permission_on(RUNNER_APPLICATION_ID, APP_APPLICATION_ID.upper())

    assert outcome == "confirmed"
    rule_set_manager.workspace.account_access_control_proxy.get_rule_set.assert_called_once_with(
        name=_RULE_SET_NAME, etag=""
    )


def test_app_permission_is_missing_when_the_readable_rule_set_has_no_role_for_the_app(
    rule_set_manager: TaskRunnerJobManager,
) -> None:
    rule_set_manager.workspace.account_access_control_proxy.get_rule_set.return_value = _rule_set(
        GrantRule(role="roles/servicePrincipal.manager", principals=["users/admin@example.com"]),
        GrantRule(role="roles/servicePrincipal.user", principals=["servicePrincipals/someone-else"]),
    )

    assert rule_set_manager.app_permission_on(RUNNER_APPLICATION_ID, APP_APPLICATION_ID) == "missing"


def test_app_permission_is_unverified_when_a_group_could_give_the_app_the_role(
    rule_set_manager: TaskRunnerJobManager,
) -> None:
    """Studio can't see the app's group memberships here, so a group grant can't be ruled out."""
    rule_set_manager.workspace.account_access_control_proxy.get_rule_set.return_value = _rule_set(
        GrantRule(role="roles/servicePrincipal.user", principals=["groups/automation"]),
    )

    assert rule_set_manager.app_permission_on(RUNNER_APPLICATION_ID, APP_APPLICATION_ID) == "unverified"


@pytest.mark.parametrize("error", [PermissionDenied("no access"), NotFound("no rule set"), ValueError("unsupported")])
def test_app_permission_is_unverified_when_the_rule_set_cannot_be_read(
    rule_set_manager: TaskRunnerJobManager, error: Exception
) -> None:
    rule_set_manager.workspace.account_access_control_proxy.get_rule_set.side_effect = error

    assert rule_set_manager.app_permission_on(RUNNER_APPLICATION_ID, APP_APPLICATION_ID) == "unverified"


def test_app_permission_is_unverified_without_a_known_account(rule_set_manager: TaskRunnerJobManager) -> None:
    rule_set_manager.workspace.config.account_id = None

    assert rule_set_manager.app_permission_on(RUNNER_APPLICATION_ID, APP_APPLICATION_ID) == "unverified"
    rule_set_manager.workspace.account_access_control_proxy.get_rule_set.assert_not_called()
