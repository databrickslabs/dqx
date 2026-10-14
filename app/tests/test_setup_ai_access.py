"""Tests for the non-blocking AI model endpoint setup check."""

from dataclasses import dataclass, field
from types import SimpleNamespace
from unittest.mock import MagicMock, create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

from databricks_labs_dqx_app.backend.services.app_settings_service import AiEnabledSource
from databricks_labs_dqx_app.backend.setup.ai_access import AiAccess
from databricks_labs_dqx_app.backend.setup.models import SetupActionId, SetupStepId, StepState


@dataclass
class FakeAiSettings:
    enabled: bool = True
    source: AiEnabledSource = "default"
    endpoint: str = "chat-endpoint"
    embedding: str = "embed-endpoint"
    saves: list[tuple[bool, AiEnabledSource, str | None]] = field(default_factory=list)
    save_failure: Exception | None = None

    def get_ai_enabled(self) -> bool:
        return self.enabled

    def get_ai_enabled_source(self) -> AiEnabledSource:
        return self.source

    def save_ai_enabled(
        self, enabled: bool, *, user_email: str | None = None, source: AiEnabledSource = "admin"
    ) -> bool:
        if self.save_failure is not None:
            raise self.save_failure
        self.enabled, self.source = enabled, source
        self.saves.append((enabled, source, user_email))
        return enabled

    def get_ai_endpoint_name(self) -> str:
        return self.endpoint

    def get_embedding_endpoint_name(self) -> str:
        return self.embedding


@pytest.fixture
def workspace() -> MagicMock:
    client = create_autospec(WorkspaceClient, instance=True)
    # Databricks-hosted models are listed without an endpoint ID.
    client.serving_endpoints.get.side_effect = lambda name: SimpleNamespace(id=None, name=name)
    return client


@pytest.fixture
def admin() -> MagicMock:
    """Setup administrator's client; its presence makes the check attended."""
    client = create_autospec(WorkspaceClient, instance=True)
    client.serving_endpoints.get.side_effect = NotFound("not visible to the admin either")
    return client


def _check(workspace: MagicMock, settings: FakeAiSettings, reader: MagicMock | None = None):
    return AiAccess(workspace=workspace, settings=settings).check_ai_access(reader)


def test_passes_when_every_endpoint_is_listed(workspace) -> None:
    settings = FakeAiSettings()

    step = _check(workspace, settings)

    assert step.id == SetupStepId.AI
    assert step.state == StepState.PASSED
    assert step.summary == "The AI models are available."
    assert settings.saves == []
    assert {call.args[0] for call in workspace.serving_endpoints.get.call_args_list} == {
        "chat-endpoint",
        "embed-endpoint",
    }


def test_never_reads_permissions_grants_or_queries(workspace) -> None:
    _check(workspace, FakeAiSettings())

    workspace.serving_endpoints.get_permissions.assert_not_called()
    workspace.serving_endpoints.update_permissions.assert_not_called()
    workspace.serving_endpoints.query.assert_not_called()


def test_missing_endpoint_warns_and_turns_default_ai_off(workspace, admin) -> None:
    workspace.serving_endpoints.get.side_effect = NotFound("no such endpoint")
    settings = FakeAiSettings()

    step = _check(workspace, settings, admin)

    assert step.state == StepState.WARNING
    assert step.code == "ai_endpoint_missing"
    assert step.actions == (SetupActionId.VERIFY_AGAIN, SetupActionId.OVERRIDE)
    assert settings.enabled is False
    assert settings.source == "setup"
    assert "turned off" in step.summary
    assert any("`chat-endpoint`" in instruction for instruction in step.instructions)


def test_missing_endpoint_reports_ai_still_on_when_turning_it_off_fails(workspace, admin) -> None:
    workspace.serving_endpoints.get.side_effect = NotFound("no such endpoint")
    settings = FakeAiSettings(save_failure=RuntimeError("lakebase unavailable"))

    step = _check(workspace, settings, admin)

    assert step.state == StepState.WARNING
    assert settings.enabled is True
    assert "couldn't turn AI features off" in step.summary
    assert "lakebase unavailable" not in str(step)


def test_endpoint_lookup_failure_is_unverified_without_leaking_details(workspace) -> None:
    workspace.serving_endpoints.get.side_effect = RuntimeError("internal timeout detail")

    step = _check(workspace, FakeAiSettings())

    assert step.state == StepState.WARNING
    assert step.code == "ai_endpoint_unverified"
    assert "internal timeout detail" not in str(step)


def test_missing_endpoint_outranks_a_failed_lookup(workspace) -> None:
    def get(name: str) -> SimpleNamespace:
        if name == "chat-endpoint":
            raise NotFound("missing")
        raise RuntimeError("timeout")

    workspace.serving_endpoints.get.side_effect = get

    assert _check(workspace, FakeAiSettings()).code == "ai_endpoint_missing"


def test_admin_enabled_ai_stays_on_when_an_endpoint_is_missing(workspace, admin) -> None:
    workspace.serving_endpoints.get.side_effect = NotFound("missing")
    settings = FakeAiSettings(source="admin")

    step = _check(workspace, settings, admin)

    assert step.state == StepState.WARNING
    assert settings.enabled is True
    assert settings.saves == []
    assert "AI features are on" in step.summary


def test_setup_turned_off_ai_comes_back_once_endpoints_are_listed(workspace, admin) -> None:
    settings = FakeAiSettings(enabled=False, source="setup")

    step = _check(workspace, settings, admin)

    assert step.state == StepState.PASSED
    assert settings.enabled is True
    assert settings.saves == [(True, "setup", None)]


def test_setup_turned_off_ai_stays_off_while_an_endpoint_is_missing(workspace, admin) -> None:
    workspace.serving_endpoints.get.side_effect = NotFound("missing")
    settings = FakeAiSettings(enabled=False, source="setup")

    step = _check(workspace, settings, admin)

    assert step.state == StepState.WARNING
    assert settings.enabled is False
    assert settings.saves == []


def test_admin_turned_off_ai_is_not_checked(workspace) -> None:
    settings = FakeAiSettings(enabled=False, source="admin")

    step = _check(workspace, settings)

    assert step.state == StepState.PASSED
    assert "turned off in Settings" in step.summary
    workspace.serving_endpoints.get.assert_not_called()
    assert settings.saves == []


def test_unreadable_settings_warn_without_blocking(workspace) -> None:
    settings = FakeAiSettings()
    settings.get_ai_enabled = MagicMock(side_effect=RuntimeError("lakebase down"))

    step = _check(workspace, settings)

    assert step.state == StepState.WARNING
    assert step.code == "ai_settings_unreadable"


def test_keep_enabled_is_an_administrator_choice(workspace) -> None:
    settings = FakeAiSettings(enabled=False, source="setup")

    AiAccess(workspace=workspace, settings=settings).keep_enabled(user_email="admin@example.com")

    assert settings.saves == [(True, "admin", "admin@example.com")]


def test_endpoint_names_are_sorted_and_deduplicated(workspace) -> None:
    access = AiAccess(workspace=workspace, settings=FakeAiSettings(endpoint="b", embedding="a"))

    assert access.endpoint_names() == ("a", "b")
    same = AiAccess(workspace=workspace, settings=FakeAiSettings(endpoint="x", embedding="x"))
    assert same.endpoint_names() == ("x",)


def test_unattended_check_reports_but_never_changes_the_ai_setting(workspace) -> None:
    """A restart may not see an endpoint the administrator can, so it must not flip AI off."""
    workspace.serving_endpoints.get.side_effect = NotFound("not visible to the app")
    settings = FakeAiSettings()

    step = _check(workspace, settings)

    assert step.state == StepState.WARNING
    assert step.code == "ai_endpoint_missing"
    assert settings.saves == []
    assert "until an administrator" in step.summary


def test_unattended_check_does_not_turn_setup_disabled_ai_back_on(workspace) -> None:
    settings = FakeAiSettings(enabled=False, source="setup")

    step = _check(workspace, settings)

    assert step.state == StepState.PASSED
    assert settings.enabled is False
    assert settings.saves == []


def test_endpoint_the_app_cannot_see_is_found_as_the_administrator(workspace, admin) -> None:
    workspace.serving_endpoints.get.side_effect = NotFound("no CAN_VIEW for the app")
    admin.serving_endpoints.get.side_effect = lambda name: SimpleNamespace(id="custom-id", name=name)
    settings = FakeAiSettings()

    step = _check(workspace, settings, admin)

    assert step.state == StepState.PASSED
    assert settings.saves == []
