"""Tests for the non-blocking AI model access setup check."""

import dataclasses
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
    endpoint: str = "system.ai.chat-model"
    embedding: str = "system.ai.embed-model"
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


_DEFAULT_GRANTS = {"USE_CATALOG", "USE_SCHEMA", "EXECUTE"}


class FakeGrants:
    """Effective Unity Catalog privileges per (principal, securable kind, full name)."""

    def __init__(self) -> None:
        self.privileges: dict[tuple[str, str, str], set[str]] = {}
        self.default: set[str] | None = set(_DEFAULT_GRANTS)
        self.unreadable = False

    def get_effective(self, kind: str, full_name: str, *, principal: str) -> SimpleNamespace:
        if self.unreadable:
            raise PermissionError("denied")
        held = self.privileges.get((principal, kind, full_name), self.default or set())
        return SimpleNamespace(
            privilege_assignments=[
                SimpleNamespace(principal=principal, privileges=[SimpleNamespace(privilege=p) for p in sorted(held)])
            ]
        )


def _client(grants: FakeGrants, existing: set[str] | None = None) -> MagicMock:
    client = create_autospec(WorkspaceClient, instance=True)
    known = existing if existing is not None else {"system.ai.chat-model", "system.ai.embed-model"}

    def _do(method: str, path: str, **_: object) -> dict:
        name = path.rsplit("/", 1)[-1]
        if name not in known:
            raise NotFound("Resource not found")
        return {"name": f"model-services/{name}"}

    client.api_client.do.side_effect = _do
    client.grants.get_effective.side_effect = grants.get_effective
    return client


@pytest.fixture
def grants() -> FakeGrants:
    return FakeGrants()


@pytest.fixture
def workspace(grants: FakeGrants) -> MagicMock:
    return _client(grants)


def _check(
    resources: ActiveResources,
    workspace: MagicMock,
    settings: FakeAiSettings,
    reader: MagicMock | None = None,
    *,
    app_sp_id: str = "",
):
    access = AiAccess(resources=resources, workspace=workspace, settings=settings, app_sp_id=app_sp_id)
    return access.check_ai_access(reader)


def test_passes_when_the_audience_can_use_every_model(resources, workspace) -> None:
    settings = FakeAiSettings()

    step = _check(resources, workspace, settings)

    assert step.id == SetupStepId.AI
    assert step.state == StepState.PASSED
    assert settings.saves == []


def test_checks_catalog_schema_and_model_service_grants(resources, workspace) -> None:
    _check(resources, workspace, FakeAiSettings(embedding="system.ai.chat-model"))

    inspected = {(call.args[0], call.args[1]) for call in workspace.grants.get_effective.call_args_list}
    assert inspected == {("CATALOG", "system"), ("SCHEMA", "system.ai"), ("MODEL_SERVICE", "system.ai.chat-model")}


def test_shared_catalog_and_schema_grants_are_read_once_per_run(resources, workspace) -> None:
    _check(resources, workspace, FakeAiSettings())

    calls = [
        (call.args[0], call.args[1], call.kwargs["principal"]) for call in workspace.grants.get_effective.call_args_list
    ]
    assert len(calls) == len(set(calls)) == 4


def test_never_grants_anything(resources, workspace) -> None:
    _check(resources, workspace, FakeAiSettings())

    workspace.grants.update.assert_not_called()


def test_missing_grants_list_the_statements_to_run(resources, workspace, grants) -> None:
    grants.privileges[("data-team", "MODEL_SERVICE", "system.ai.chat-model")] = set()
    grants.privileges[("data-team", "SCHEMA", "system.ai")] = {"EXECUTE"}

    step = _check(resources, workspace, FakeAiSettings())

    assert step.state == StepState.WARNING
    assert step.code == "ai_access_missing"
    # The statements are kept apart from the prose, so they show as SQL that can be copied or run.
    assert step.grant_statements == (
        "GRANT USE_SCHEMA ON SCHEMA `system`.`ai` TO `data-team`;",
        "GRANT EXECUTE ON MODEL SERVICE `system`.`ai`.`chat-model` TO `data-team`;",
    )
    assert len(step.instructions) == 1
    assert "metastore admin" in step.instructions[0]
    assert "GRANT" not in step.instructions[0]


def test_a_name_that_cant_be_quoted_safely_is_never_a_statement(resources, workspace, grants) -> None:
    unsafe = "data`team\nGRANT ALL"
    resources = dataclasses.replace(
        resources, audience=dataclasses.replace(resources.audience, uc_principals=(unsafe, "data-team"))
    )
    grants.default = set()

    step = _check(resources, workspace, FakeAiSettings())

    assert step.grant_statements
    assert all("data-team" in statement for statement in step.grant_statements)
    assert not any("ALL" in statement or "data`" in statement for statement in step.grant_statements)
    assert any("can't write a safe" in instruction for instruction in step.instructions)


def test_inspect_only_never_changes_the_ai_setting(resources, grants) -> None:
    workspace = _client(grants, existing={"system.ai.embed-model"})
    reader = _client(grants, existing={"system.ai.embed-model"})
    grants.default = set()
    settings = FakeAiSettings()

    access = AiAccess(resources=resources, workspace=workspace, settings=settings)
    step = access.check_ai_access(reader, inspect_only=True)

    assert step.state == StepState.WARNING
    assert settings.saves == []
    assert settings.enabled is True


def test_inspect_only_never_turns_ai_back_on(resources, workspace) -> None:
    settings = FakeAiSettings(enabled=False, source="setup")

    access = AiAccess(resources=resources, workspace=workspace, settings=settings)
    access.check_ai_access(workspace, inspect_only=True)

    assert settings.saves == []


def test_all_privileges_satisfies_every_requirement(resources, workspace, grants) -> None:
    grants.default = {"ALL_PRIVILEGES"}

    assert _check(resources, workspace, FakeAiSettings()).state == StepState.PASSED


def test_app_service_principal_needs_the_embedding_model(resources, workspace, grants) -> None:
    sp = "11111111-2222-3333-4444-555555555555"
    grants.privileges[(sp, "MODEL_SERVICE", "system.ai.embed-model")] = set()

    step = _check(resources, workspace, FakeAiSettings(), app_sp_id=sp)

    assert step.code == "ai_access_missing"
    assert f"GRANT EXECUTE ON MODEL SERVICE `system`.`ai`.`embed-model` TO `{sp}`;" in step.grant_statements
    inspected_for_chat = {
        call.kwargs["principal"]
        for call in workspace.grants.get_effective.call_args_list
        if call.args[1] == "system.ai.chat-model"
    }
    assert sp not in inspected_for_chat


def test_missing_model_warns_and_turns_default_ai_off(resources, grants) -> None:
    workspace = _client(grants, existing={"system.ai.embed-model"})
    reader = _client(grants, existing={"system.ai.embed-model"})
    settings = FakeAiSettings()

    step = _check(resources, workspace, settings, reader)

    assert step.state == StepState.WARNING
    assert step.code == "ai_endpoint_missing"
    assert step.actions == (SetupActionId.VERIFY_AGAIN, SetupActionId.OVERRIDE)
    assert settings.enabled is False
    assert settings.source == "setup"
    assert "turned off" in step.summary
    assert any("Settings > AI" in instruction for instruction in step.instructions)


def test_model_hidden_from_the_app_is_looked_up_as_the_administrator(resources, grants) -> None:
    workspace = _client(grants, existing=set())
    reader = _client(grants)

    assert _check(resources, workspace, FakeAiSettings(), reader).state == StepState.PASSED


def test_model_hidden_from_the_app_without_an_administrator_is_unverified(resources, grants) -> None:
    """At startup there is no administrator client, so the app SP's miss can't prove the model is gone."""
    sp = "11111111-2222-3333-4444-555555555555"
    workspace = _client(grants, existing={"system.ai.embed-model"})

    step = _check(resources, workspace, FakeAiSettings(), app_sp_id=sp)

    assert step.code == "ai_access_unverified"
    assert any(
        f"GRANT READ_METADATA ON MODEL SERVICE `system`.`ai`.`chat-model` TO `{sp}`;" in instruction
        for instruction in step.instructions
    )
    assert not any("doesn't exist" in instruction for instruction in step.instructions)


def test_failed_administrator_lookup_is_unverified_not_missing(resources, grants) -> None:
    workspace = _client(grants, existing={"system.ai.embed-model"})
    reader = _client(grants)
    reader.api_client.do.side_effect = PermissionError("denied")

    step = _check(resources, workspace, FakeAiSettings(), reader)

    assert step.code == "ai_access_unverified"


def test_non_model_service_name_asks_for_a_model_choice(resources, workspace) -> None:
    step = _check(resources, workspace, FakeAiSettings(endpoint="my-provisioned-endpoint"))

    assert step.code == "ai_endpoint_missing"
    assert any("isn't a Unity Gateway model service" in instruction for instruction in step.instructions)


def test_no_chat_model_selected_warns(resources, workspace) -> None:
    step = _check(resources, workspace, FakeAiSettings(endpoint=""))

    assert step.code == "ai_endpoint_missing"
    assert any("No AI model is selected" in instruction for instruction in step.instructions)


def test_unreadable_grants_are_confirmed_by_the_app_seeing_the_model(resources, workspace, grants) -> None:
    """Only owners and MANAGE holders can read grants on system.ai, so most installs can't read them."""
    grants.unreadable = True

    step = _check(resources, workspace, FakeAiSettings())

    assert step.state == StepState.PASSED


def test_unreadable_grants_on_a_model_the_app_cant_see_are_unverified(resources, grants) -> None:
    grants.unreadable = True
    workspace = _client(grants, existing={"system.ai.embed-model"})
    reader = _client(grants)

    step = _check(resources, workspace, FakeAiSettings(), reader)

    assert step.state == StepState.WARNING
    assert step.code == "ai_access_unverified"
    assert "denied" not in str(step)
    assert any("`data-team`" in instruction for instruction in step.instructions)


def test_grants_fall_back_to_the_administrator_client(resources, workspace) -> None:
    workspace.grants.get_effective.side_effect = PermissionError("denied")
    reader = _client(FakeGrants())

    assert _check(resources, workspace, FakeAiSettings(), reader).state == StepState.PASSED


def test_admin_enabled_ai_stays_on_when_access_fails(resources, workspace, grants) -> None:
    grants.default = set()
    settings = FakeAiSettings(source="admin")

    step = _check(resources, workspace, settings)

    assert step.state == StepState.WARNING
    assert settings.enabled is True
    assert settings.saves == []
    assert "AI features are on" in step.summary


def test_setup_turned_off_ai_comes_back_once_access_is_confirmed(resources, workspace) -> None:
    settings = FakeAiSettings(enabled=False, source="setup")

    step = _check(resources, workspace, settings, workspace)

    assert step.state == StepState.PASSED
    assert settings.enabled is True
    assert settings.saves == [(True, "setup", None)]


def test_unattended_check_asks_an_administrator_to_turn_ai_back_on(resources, workspace) -> None:
    """A restart never flips the setting, but it must not hide that AI is still off."""
    settings = FakeAiSettings(enabled=False, source="setup")

    step = _check(resources, workspace, settings)

    assert step.state == StepState.WARNING
    assert step.code == "ai_reenable_pending"
    assert "Verify again" in step.summary
    assert settings.saves == []


def test_unattended_check_never_turns_ai_off(resources, grants) -> None:
    workspace = _client(grants, existing={"system.ai.embed-model"})
    grants.default = set()
    settings = FakeAiSettings()

    step = _check(resources, workspace, settings)

    assert step.state == StepState.WARNING
    assert settings.enabled is True
    assert settings.saves == []
    assert "until an administrator clicks Verify again" in step.summary


def test_unverified_access_never_turns_ai_off(resources, grants) -> None:
    workspace = _client(grants, existing={"system.ai.embed-model"})
    reader = _client(grants)
    reader.api_client.do.side_effect = PermissionError("denied")
    settings = FakeAiSettings()

    step = _check(resources, workspace, settings, reader)

    assert step.code == "ai_access_unverified"
    assert settings.enabled is True
    assert settings.saves == []


def test_reports_ai_still_on_when_turning_it_off_fails(resources, grants) -> None:
    workspace = _client(grants, existing={"system.ai.embed-model"})
    reader = _client(grants, existing={"system.ai.embed-model"})
    settings = FakeAiSettings(save_failure=RuntimeError("lakebase unavailable"))

    step = _check(resources, workspace, settings, reader)

    assert step.code == "ai_endpoint_missing"
    assert settings.enabled is True
    assert "couldn't turn AI features off" in step.summary
    assert "lakebase unavailable" not in str(step)


def test_setup_turned_off_ai_stays_off_while_access_fails(resources, workspace, grants) -> None:
    grants.default = set()
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
    workspace.api_client.do.assert_not_called()
    workspace.grants.get_effective.assert_not_called()
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


def test_model_names_are_sorted_and_deduplicated(resources, workspace) -> None:
    access = AiAccess(resources=resources, workspace=workspace, settings=FakeAiSettings(endpoint="b", embedding="a"))

    assert access.model_names() == ("a", "b")
    same = AiAccess(resources=resources, workspace=workspace, settings=FakeAiSettings(endpoint="x", embedding="x"))
    assert same.model_names() == ("x",)
