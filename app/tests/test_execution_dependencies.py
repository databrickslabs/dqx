"""Execution dependencies fail closed with sanitized, actionable API errors."""

import traceback
from typing import Annotated
from unittest.mock import MagicMock, create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import PermissionDenied
from databricks.sdk.service.iam import User
from databricks.sdk.service.jobs import Job, JobRunAs, JobSettings
from fastapi import Depends, FastAPI, HTTPException
from fastapi.testclient import TestClient

from databricks_labs_dqx_app.backend import dependencies
from databricks_labs_dqx_app.backend.config import AppConfig
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.job_service import JobService
from databricks_labs_dqx_app.backend.services.view_service import ViewService
from databricks_labs_dqx_app.backend.setup.runtime import setup_runtime


@pytest.fixture
def execution_workspace(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    workspace = create_autospec(WorkspaceClient, instance=True)
    workspace.jobs.get.return_value = Job(settings=JobSettings(run_as=JobRunAs(service_principal_name="runner-sp")))
    workspace.current_user.me.return_value = User(user_name="app-sp")
    monkeypatch.setattr(setup_runtime, "job_id", 42)
    monkeypatch.setattr(dependencies, "conf", AppConfig(_env_file=None, task_runner_postgres_role=""))
    return workspace


@pytest.fixture(params=[dependencies.get_view_service, dependencies.get_job_service], ids=["view", "job"])
def execution_client(
    request: pytest.FixtureRequest, execution_workspace: MagicMock, sql_executor_mock: MagicMock
) -> TestClient:
    app = FastAPI()
    settings = create_autospec(AppSettingsService, instance=True)
    settings.get_sql_warehouse_id.return_value = "test-warehouse"
    app.dependency_overrides[dependencies.get_sp_ws] = lambda: execution_workspace
    app.dependency_overrides[dependencies.get_obo_sql_executor] = lambda: sql_executor_mock
    app.dependency_overrides[dependencies.get_sp_sql_executor] = lambda: sql_executor_mock
    app.dependency_overrides[dependencies.get_sp_oltp_executor] = lambda: sql_executor_mock
    app.dependency_overrides[dependencies.get_app_settings_service] = lambda: settings

    @app.get("/execution")
    async def execution(
        service: Annotated[ViewService | JobService, Depends(request.param)],
    ) -> dict[str, bool]:
        return {"ready": isinstance(service, (ViewService, JobService))}

    return TestClient(app, raise_server_exceptions=False)


def test_execution_dependencies_accept_distinct_principals(execution_client: TestClient) -> None:
    response = execution_client.get("/execution")

    assert response.status_code == 200
    assert response.json() == {"ready": True}


@pytest.mark.parametrize(
    "job",
    [
        Job(),
        Job(settings=JobSettings()),
        Job(settings=JobSettings(run_as=JobRunAs(user_name="user@example.com"))),
        Job(settings=JobSettings(run_as=JobRunAs(service_principal_name=""))),
        Job(settings=JobSettings(run_as=JobRunAs(service_principal_name="app-sp"))),
        Job(settings=JobSettings(run_as=JobRunAs(service_principal_name="APP-SP"))),
        Job(settings=JobSettings(run_as=JobRunAs(service_principal_name="runner\nsp"))),
    ],
)
def test_execution_dependencies_reject_invalid_runner_with_setup_action(
    execution_client: TestClient, execution_workspace: MagicMock, job: Job
) -> None:
    execution_workspace.jobs.get.return_value = job

    response = execution_client.get("/execution")

    assert response.status_code == 503
    detail = response.json()["detail"]
    assert "distinct" in detail
    assert "Jobs UI" in detail
    assert "setup" in detail
    execution_workspace.jobs.run_now.assert_not_called()


@pytest.mark.parametrize("identity", [User(), User(user_name=" "), User(user_name="app\nsp")])
def test_execution_dependencies_reject_unresolved_app_identity(
    execution_client: TestClient, execution_workspace: MagicMock, identity: User
) -> None:
    execution_workspace.current_user.me.return_value = identity

    response = execution_client.get("/execution")

    assert response.status_code == 503
    assert "app service principal" in response.json()["detail"]
    execution_workspace.jobs.run_now.assert_not_called()


def test_execution_dependencies_reject_legacy_role_mismatch(
    execution_client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(dependencies, "conf", AppConfig(_env_file=None, task_runner_postgres_role="other-sp"))

    response = execution_client.get("/execution")

    assert response.status_code == 503
    assert "DQX_TASK_RUNNER_POSTGRES_ROLE" in response.json()["detail"]
    assert "match" in response.json()["detail"]


@pytest.mark.parametrize("boundary", ["job", "identity"])
@pytest.mark.parametrize("error_type", [PermissionDenied, OSError])
def test_sdk_resolution_errors_are_sanitized_and_actionable(
    execution_client: TestClient,
    execution_workspace: MagicMock,
    caplog: pytest.LogCaptureFixture,
    boundary: str,
    error_type: type[Exception],
) -> None:
    sensitive = "sensitive-source token=do-not-log\nforged-entry"
    if boundary == "job":
        execution_workspace.jobs.get.side_effect = error_type(sensitive)
    else:
        execution_workspace.current_user.me.side_effect = error_type(sensitive)

    response = execution_client.get("/execution")

    assert response.status_code == 503
    detail = response.json()["detail"]
    assert "app service principal" in detail
    assert "setup" in detail
    assert sensitive not in response.text
    assert sensitive not in caplog.text
    execution_workspace.jobs.run_now.assert_not_called()
    with pytest.raises(HTTPException) as raised:
        dependencies.resolve_execution_principals(execution_workspace)
    assert sensitive not in "".join(traceback.format_exception(raised.value))


def test_execution_dependencies_reject_unresolved_job(
    execution_client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(setup_runtime, "job_id", None)

    response = execution_client.get("/execution")

    assert response.status_code == 503
    assert "setup" in response.json()["detail"]


def test_principal_resolution_uses_app_id_when_username_is_absent(execution_workspace: MagicMock) -> None:
    execution_workspace.current_user.me.return_value = User(id="app-sp")

    assert dependencies.resolve_execution_principals(execution_workspace) == ("runner-sp", "app-sp")


@pytest.mark.parametrize("boundary", ["job", "identity"])
def test_principal_resolution_does_not_reuse_previously_valid_identity(
    execution_workspace: MagicMock, boundary: str
) -> None:
    assert dependencies.resolve_execution_principals(execution_workspace) == ("runner-sp", "app-sp")
    if boundary == "job":
        execution_workspace.jobs.get.return_value = Job(
            settings=JobSettings(run_as=JobRunAs(service_principal_name="app-sp"))
        )
    else:
        execution_workspace.current_user.me.return_value = User(user_name="runner-sp")

    with pytest.raises(HTTPException) as raised:
        dependencies.resolve_execution_principals(execution_workspace)

    assert raised.value.status_code == 503
