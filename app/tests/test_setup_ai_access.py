"""Tests for the non-blocking AI model access setup check."""

from dataclasses import dataclass, field
from types import SimpleNamespace
from unittest.mock import MagicMock, create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

from databricks_labs_dqx_app.backend.services.app_settings_service import AiEnabledSource
from databricks_labs_dqx_app.backend.setup.ai_access import AiAccess
from databricks_labs_dqx_app.backend.setup.audience import resolve_audience
from databricks_labs_dqx_app.backend.setup.models import SetupActionId, SetupStepId, StepState
from databricks_labs_dqx_app.backend.setup.resources import ActiveResources, LakebaseConnection, VolumeLocation


@dataclass
class FakeAiSettings:
    enabled: bool = True
    source: AiEnabledSource = "default"
    endpoint: str = "chat-endpoint"
    embedding: str = "embed-endpoint"
    saves: list[tuple[bool, AiEnabledSource, str | None]] = field(default_factory=list)

    def get_ai_enabled(self) -> bool:
        return self.enabled

    def get_ai_enabled_source(self) -> AiEnabledSource:
        return self.source

    def save_ai_enabled(
        self, enabled: bool, *, user_email: str | None = None, source: AiEnabledSource = "admin"
    ) -> bool:
        self.enabled, self.source = enabled, source
        self.saves.append((enabled, source, user_email))
        return enabled

    def get_ai_endpoint_name(self) -> str:
        return self.endpoint

    def get_embedding_endpoint_name(self) -> str:
        return self.embedding


@pytest.fixture
def resources() -> ActiveResources:
    return ActiveResources(
        volume=VolumeLocation("main", "dqx_studio", "wheels", "/Volumes/main/dqx_studio/wheels"),
        lakebase=LakebaseConnection(
            endpoint="projects/p/branches/b/endpoints/e",
            host=None,
            port=5432,
            database="databricks_postgres",
            username=None,
            password=None,
            schema="dqx_studio",
        ),
        warehouse_id="warehouse-id",
        job_id=None,
        tmp_schema="dqx_studio_tmp",
        genie_schema="dqx_studio_genie",
        demo_schema="dqx_studio_demo",
        audience=resolve_audience(["data-team"], "admins", allow_broad=False),
    )


def _acl(*entries: tuple[str, str]) -> SimpleNamespace:
    return SimpleNamespace(
        access_control_list=[
            SimpleNamespace(
                group_name=group,
                service_principal_name=None,
                user_name=None,
                all_permissions=[SimpleNamespace(permission_level=level)],
            )
            for group, level in entries
        ]
    )


@pytest.fixture
def workspace() -> MagicMock:
    client = create_autospec(WorkspaceClient, instance=True)
    client.serving_endpoints.get.side_effect = lambda name: SimpleNamespace(id=f"id-{name}")
    client.serving_endpoints.get_permissions.return_value = _acl(("data-team", "CAN_QUERY"))
    return client


def _check(resources: ActiveResources, workspace: MagicMock, settings: FakeAiSettings, reader: MagicMock | None = None):
    return AiAccess(resources=resources, workspace=workspace, settings=settings).check_ai_access(reader)


def test_passes_when_the_audience_can_query_every_endpoint(resources, workspace) -> None:
    settings = FakeAiSettings()

    step = _check(resources, workspace, settings)

    assert step.id == SetupStepId.AI
    assert step.state == StepState.PASSED
    assert settings.saves == []
    workspace.serving_endpoints.update_permissions.assert_not_called()


def test_all_workspace_users_with_query_access_covers_the_audience(resources, workspace) -> None:
    workspace.serving_endpoints.get_permissions.return_value = _acl(("users", "CAN_QUERY"))

    assert _check(resources, workspace, FakeAiSettings()).state == StepState.PASSED


def test_missing_access_is_granted_then_reverified(resources, workspace) -> None:
    workspace.serving_endpoints.get_permissions.side_effect = [
        _acl(),
        _acl(("data-team", "CAN_QUERY")),
        _acl(("data-team", "CAN_MANAGE")),
    ]

    step = _check(resources, workspace, FakeAiSettings(embedding="chat-endpoint"))

    assert step.state == StepState.PASSED
    request = workspace.serving_endpoints.update_permissions.call_args.kwargs["access_control_list"][0]
    assert request.group_name == "data-team"
    assert request.permission_level.value == "CAN_QUERY"


def test_missing_endpoint_warns_and_turns_default_ai_off(resources, workspace) -> None:
    workspace.serving_endpoints.get.side_effect = NotFound("no such endpoint")
    settings = FakeAiSettings()

    step = _check(resources, workspace, settings)

    assert step.state == StepState.WARNING
    assert step.code == "ai_endpoint_missing"
    assert step.actions == (SetupActionId.VERIFY_AGAIN, SetupActionId.OVERRIDE)
    assert settings.enabled is False
    assert settings.source == "setup"
    assert "turned off" in step.summary


def test_unreadable_permissions_are_unverified(resources, workspace) -> None:
    workspace.serving_endpoints.get_permissions.side_effect = PermissionError("denied")
    reader = create_autospec(WorkspaceClient, instance=True)
    reader.serving_endpoints.get_permissions.side_effect = PermissionError("denied")

    step = _check(resources, workspace, FakeAiSettings(), reader)

    assert step.state == StepState.WARNING
    assert step.code == "ai_access_unverified"
    assert "denied" not in str(step)
    assert any("data-team" in instruction for instruction in step.instructions)


def test_permissions_fall_back_to_the_administrator_client(resources, workspace) -> None:
    workspace.serving_endpoints.get_permissions.side_effect = PermissionError("denied")
    reader = create_autospec(WorkspaceClient, instance=True)
    reader.serving_endpoints.get_permissions.return_value = _acl(("data-team", "CAN_QUERY"))

    assert _check(resources, workspace, FakeAiSettings(), reader).state == StepState.PASSED


def test_access_still_missing_after_grant_names_the_group(resources, workspace) -> None:
    workspace.serving_endpoints.get_permissions.return_value = _acl(("other-team", "CAN_QUERY"))

    step = _check(resources, workspace, FakeAiSettings())

    assert step.code == "ai_access_missing"
    assert any("`data-team`" in instruction and "Can query" in instruction for instruction in step.instructions)


def test_admin_enabled_ai_stays_on_when_access_fails(resources, workspace) -> None:
    workspace.serving_endpoints.get_permissions.return_value = _acl()
    settings = FakeAiSettings(source="admin")

    step = _check(resources, workspace, settings)

    assert step.state == StepState.WARNING
    assert settings.enabled is True
    assert settings.saves == []
    assert "AI features are on" in step.summary


def test_setup_turned_off_ai_comes_back_once_access_is_confirmed(resources, workspace) -> None:
    settings = FakeAiSettings(enabled=False, source="setup")

    step = _check(resources, workspace, settings)

    assert step.state == StepState.PASSED
    assert settings.enabled is True
    assert settings.saves == [(True, "setup", None)]


def test_setup_turned_off_ai_stays_off_while_access_fails(resources, workspace) -> None:
    workspace.serving_endpoints.get_permissions.return_value = _acl()
    settings = FakeAiSettings(enabled=False, source="setup")

    step = _check(resources, workspace, settings)

    assert step.state == StepState.WARNING
    assert settings.enabled is False
    assert settings.saves == []


def test_admin_turned_off_ai_is_not_checked(resources, workspace) -> None:
    settings = FakeAiSettings(enabled=False, source="admin")

    step = _check(resources, workspace, settings)

    assert step.state == StepState.PASSED
    assert "turned off in Settings" in step.summary
    workspace.serving_endpoints.get.assert_not_called()
    assert settings.saves == []


def test_unreadable_settings_warn_without_blocking(resources, workspace) -> None:
    settings = FakeAiSettings()
    settings.get_ai_enabled = MagicMock(side_effect=RuntimeError("lakebase down"))

    step = _check(resources, workspace, settings)

    assert step.state == StepState.WARNING
    assert step.code == "ai_settings_unreadable"


def test_keep_enabled_is_an_administrator_choice(resources, workspace) -> None:
    settings = FakeAiSettings(enabled=False, source="setup")

    AiAccess(resources=resources, workspace=workspace, settings=settings).keep_enabled(user_email="admin@example.com")

    assert settings.saves == [(True, "admin", "admin@example.com")]


def test_endpoint_names_are_sorted_and_deduplicated(resources, workspace) -> None:
    access = AiAccess(resources=resources, workspace=workspace, settings=FakeAiSettings(endpoint="b", embedding="a"))

    assert access.endpoint_names() == ("a", "b")
    same = AiAccess(resources=resources, workspace=workspace, settings=FakeAiSettings(endpoint="x", embedding="x"))
    assert same.endpoint_names() == ("x",)
