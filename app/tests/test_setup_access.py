"""Audience and administrator access reconciliation."""

from types import SimpleNamespace
from unittest.mock import MagicMock, create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    EffectivePermissionsList,
    EffectivePrivilege,
    EffectivePrivilegeAssignment,
    Privilege,
)
from databricks.sdk.service.iam import PermissionLevel

from databricks_labs_dqx_app.backend.setup.access import GENIE_ALLOWLIST, AudienceAccess
from databricks_labs_dqx_app.backend.setup.audience import resolve_audience
from databricks_labs_dqx_app.backend.setup.models import SetupStepId, StepState
from databricks_labs_dqx_app.backend.setup.resources import (
    BootstrapResources,
    LakebaseConnection,
    build_active_resources,
)
from databricks_labs_dqx_app.backend.setup.storage import derive_storage
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor


class MemorySettings:
    def __init__(self, values: dict[str, str] | None = None) -> None:
        self.values = values or {}

    def get_setting(self, key: str) -> str | None:
        return self.values.get(key)

    def save_setting(self, key: str, value: str, *, user_email: str | None = None) -> None:
        self.values[key] = value


def _grants(*privileges: Privilege) -> EffectivePermissionsList:
    return EffectivePermissionsList(
        privilege_assignments=[
            EffectivePrivilegeAssignment(
                principal="p", privileges=[EffectivePrivilege(privilege=p) for p in privileges]
            )
        ]
    )


def _acl(*entries: tuple[str, PermissionLevel]) -> SimpleNamespace:
    return SimpleNamespace(
        access_control_list=[
            SimpleNamespace(group_name=group, all_permissions=[SimpleNamespace(permission_level=level)])
            for group, level in entries
        ]
    )


@pytest.fixture
def workspace() -> MagicMock:
    return create_autospec(WorkspaceClient, instance=True)


@pytest.fixture
def sql() -> MagicMock:
    executor = create_autospec(SqlExecutor, instance=True)
    executor.q.side_effect = lambda identifier: "`" + identifier.replace("`", "``") + "`"
    return executor


def _access(
    workspace: MagicMock,
    sql: MagicMock,
    *,
    groups: tuple[str, ...] = ("data-team",),
    admin_group: str = "admins",
    allow_broad: bool = False,
    settings: MemorySettings | None = None,
    dashboard_id: str = "",
) -> AudienceAccess:
    lakebase = LakebaseConnection(
        "projects/p/branches/b/endpoints/e", None, 5432, "databricks_postgres", None, None, "dqx_studio"
    )
    resources = build_active_resources(
        BootstrapResources(lakebase, "wh", None),
        derive_storage("main", "dqx_studio"),
        resolve_audience(list(groups), admin_group, allow_broad=allow_broad),
    )
    return AudienceAccess(
        resources=resources,
        workspace=workspace,
        sql=sql,
        settings=settings or MemorySettings(),
        app_name="dqx-studio",
        dashboard_id=dashboard_id,
    )


def _statements(sql: MagicMock) -> list[str]:
    return [call.args[0] for call in sql.execute_no_schema.call_args_list]


def test_genie_select_is_limited_to_the_allowlist(workspace, sql) -> None:
    triples = _access(workspace, sql).required_uc_grants()

    selects = {name for kind, name, privilege in triples if privilege == "SELECT" and kind == "TABLE"}
    assert selects == {f"main.dqx_studio_genie.{name}" for name in GENIE_ALLOWLIST}
    assert ("SCHEMA", "main.dqx_studio_genie", "SELECT") not in triples
    assert all("dq_user_table_entitlements" not in name for _, name, _ in triples)


def test_required_grants_cover_tmp_genie_and_demo_schemas(workspace, sql) -> None:
    triples = set(_access(workspace, sql).required_uc_grants())

    assert {
        ("SCHEMA", "main.dqx_studio_tmp", "USE_SCHEMA"),
        ("SCHEMA", "main.dqx_studio_tmp", "CREATE_TABLE"),
        ("SCHEMA", "main.dqx_studio_genie", "USE_SCHEMA"),
        ("SCHEMA", "main.dqx_studio_demo", "USE_SCHEMA"),
        ("SCHEMA", "main.dqx_studio_demo", "SELECT"),
    } <= triples
    assert not any(name == "main.dqx_studio" for _, name, _ in triples)


def test_access_passes_when_every_grant_is_verified(workspace, sql) -> None:
    workspace.grants.get_effective.return_value = _grants(Privilege.ALL_PRIVILEGES)

    step = _access(workspace, sql).reconcile_access()

    assert step.id == SetupStepId.ACCESS
    assert step.state == StepState.PASSED


def test_access_applies_quoted_grants_to_every_uc_principal(workspace, sql) -> None:
    workspace.grants.get_effective.return_value = _grants(Privilege.ALL_PRIVILEGES)

    _access(workspace, sql).reconcile_access()

    statements = _statements(sql)
    assert "GRANT USE SCHEMA ON SCHEMA `main`.`dqx_studio_tmp` TO `data-team`" in statements
    assert "GRANT CREATE TABLE ON SCHEMA `main`.`dqx_studio_tmp` TO `data-team`" in statements
    assert "GRANT SELECT ON TABLE `main`.`dqx_studio_genie`.`mv_dq_scores` TO `data-team`" in statements
    assert "GRANT SELECT ON SCHEMA `main`.`dqx_studio_demo` TO `data-team`" in statements
    assert not any("ON SCHEMA `main`.`dqx_studio_genie` TO" in s and "SELECT" in s for s in statements)


def test_access_reports_missing_grants_with_statements(workspace, sql) -> None:
    workspace.grants.get_effective.return_value = _grants()

    step = _access(workspace, sql).reconcile_access()

    assert step.state == StepState.ACTION_REQUIRED
    assert step.code == "audience_grants_missing"
    assert "GRANT USE SCHEMA ON SCHEMA `main`.`dqx_studio_tmp` TO `data-team`;" in step.instructions


def test_access_reports_uninspectable_grants_separately(workspace, sql) -> None:
    workspace.grants.get_effective.side_effect = PermissionError("denied")

    step = _access(workspace, sql).reconcile_access()

    assert step.code == "audience_grant_check_failed"
    assert "denied" not in " ".join(step.instructions)
    assert "READ METADATA" in step.instructions[0]


def test_unknown_grants_take_precedence_over_missing_grants(workspace, sql) -> None:
    def effective(kind: str, name: str, *, principal: str) -> EffectivePermissionsList:
        if name == "main.dqx_studio_tmp":
            return _grants()
        raise PermissionError("denied")

    workspace.grants.get_effective.side_effect = effective

    step = _access(workspace, sql).reconcile_access()

    assert step.code == "audience_grant_check_failed"
    assert "GRANT USE SCHEMA ON SCHEMA `main`.`dqx_studio_tmp` TO `data-team`;" in step.instructions


def test_grant_failures_are_ignored_and_verification_decides(workspace, sql) -> None:
    sql.execute_no_schema.side_effect = RuntimeError("denied")
    workspace.grants.get_effective.return_value = _grants(Privilege.ALL_PRIVILEGES)

    assert _access(workspace, sql).reconcile_access().state == StepState.PASSED


def test_custom_admin_group_is_granted_and_verified(workspace, sql) -> None:
    workspace.grants.get_effective.return_value = _grants(Privilege.ALL_PRIVILEGES)

    _access(workspace, sql, admin_group="dqx-admins").reconcile_access()

    statements = _statements(sql)
    assert any(statement.endswith("TO `dqx-admins`") for statement in statements)
    assert not any(statement.endswith("TO `admins`") for statement in statements)
    verified = {call.kwargs["principal"] for call in workspace.grants.get_effective.call_args_list}
    assert verified == {"data-team", "dqx-admins"}


def test_workspace_admins_never_receive_uc_grants(workspace, sql) -> None:
    workspace.grants.get_effective.return_value = _grants(Privilege.ALL_PRIVILEGES)

    _access(workspace, sql, admin_group="ADMINS").reconcile_access()

    assert not any("admins" in statement.casefold() for statement in _statements(sql))


def test_broad_audience_grants_account_users(workspace, sql) -> None:
    workspace.grants.get_effective.return_value = _grants(Privilege.ALL_PRIVILEGES)

    _access(workspace, sql, groups=("users",), allow_broad=True).reconcile_access()

    statements = _statements(sql)
    assert statements
    assert all(statement.endswith("TO `account users`") for statement in statements)


def test_access_reapplies_missing_admin_grant_on_recheck(workspace, sql) -> None:
    workspace.grants.get_effective.return_value = _grants(Privilege.ALL_PRIVILEGES)
    access = _access(workspace, sql, admin_group="dqx-admins")
    access.reconcile_access()
    sql.execute_no_schema.reset_mock()

    access.reconcile_access()

    assert any(statement.endswith("TO `dqx-admins`") for statement in _statements(sql))


def test_configured_genie_space_requires_can_run(workspace, sql) -> None:
    workspace.grants.get_effective.return_value = _grants(Privilege.ALL_PRIVILEGES)
    workspace.permissions.get.return_value = SimpleNamespace(access_control_list=[])

    step = _access(workspace, sql, settings=MemorySettings({"dq_genie_space_id": "space-1"})).reconcile_access()

    assert step.code == "genie_space_sharing_missing"
    workspace.permissions.update.assert_called_once()
    workspace.permissions.set.assert_not_called()
    object_type, object_id = workspace.permissions.update.call_args.args
    assert (object_type, object_id) == ("genie", "space-1")
    request = workspace.permissions.update.call_args.kwargs["access_control_list"]
    assert [(entry.group_name, entry.permission_level) for entry in request] == [("data-team", PermissionLevel.CAN_RUN)]


def test_genie_space_sharing_passes_after_additive_update(workspace, sql) -> None:
    workspace.grants.get_effective.return_value = _grants(Privilege.ALL_PRIVILEGES)
    workspace.permissions.get.side_effect = [_acl(), _acl(("data-team", PermissionLevel.CAN_RUN))]

    step = _access(workspace, sql, settings=MemorySettings({"dq_genie_space_id": "space-1"})).reconcile_access()

    assert step.state == StepState.PASSED
    workspace.permissions.set.assert_not_called()


def test_genie_space_with_stronger_level_is_not_updated(workspace, sql) -> None:
    workspace.grants.get_effective.return_value = _grants(Privilege.ALL_PRIVILEGES)
    workspace.permissions.get.return_value = _acl(("data-team", PermissionLevel.CAN_MANAGE))

    step = _access(workspace, sql, settings=MemorySettings({"dq_genie_space_id": "space-1"})).reconcile_access()

    assert step.state == StepState.PASSED
    workspace.permissions.update.assert_not_called()


def test_configured_dashboard_requires_can_read(workspace, sql) -> None:
    workspace.grants.get_effective.return_value = _grants(Privilege.ALL_PRIVILEGES)
    workspace.permissions.get.return_value = _acl()

    step = _access(workspace, sql, dashboard_id="dash-1").reconcile_access()

    assert step.code == "dashboard_sharing_missing"
    object_type, object_id = workspace.permissions.update.call_args.args
    assert (object_type, object_id) == ("dashboards", "dash-1")
    request = workspace.permissions.update.call_args.kwargs["access_control_list"]
    assert [(entry.group_name, entry.permission_level) for entry in request] == [
        ("data-team", PermissionLevel.CAN_READ)
    ]
    workspace.permissions.set.assert_not_called()


def test_unreadable_shared_resource_acl_fails_closed(workspace, sql) -> None:
    workspace.grants.get_effective.return_value = _grants(Privilege.ALL_PRIVILEGES)
    workspace.permissions.get.side_effect = PermissionError("denied")

    step = _access(workspace, sql, dashboard_id="dash-1").reconcile_access()

    assert step.code == "shared_resource_check_failed"
    assert "denied" not in step.summary + " ".join(step.instructions)


def test_unconfigured_shared_resources_are_not_applicable(workspace, sql) -> None:
    workspace.grants.get_effective.return_value = _grants(Privilege.ALL_PRIVILEGES)

    step = _access(workspace, sql).reconcile_access()

    assert step.state == StepState.PASSED
    assert "not applicable" in step.summary
    workspace.permissions.get.assert_not_called()


def test_app_sharing_is_deferred(workspace, sql) -> None:
    step = _access(workspace, sql).check_app_sharing()

    assert step.id == SetupStepId.APP_SHARING
    assert step.state == StepState.PASSED
