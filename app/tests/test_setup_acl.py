"""Workspace ACL parsing and additive grants used by setup sharing checks."""

from types import SimpleNamespace
from unittest.mock import create_autospec

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import AccessControlRequest, PermissionLevel

from databricks_labs_dqx_app.backend.setup.acl import (
    GENIE_RUN_LEVELS,
    group_levels,
    missing_principals,
    reconcile_group_acl,
)


def _entry(group: str, *levels: object) -> SimpleNamespace:
    return SimpleNamespace(
        group_name=group,
        service_principal_name=None,
        user_name=None,
        all_permissions=[SimpleNamespace(permission_level=level) for level in levels],
    )


def test_group_levels_normalize_enum_and_string_levels() -> None:
    levels = group_levels([_entry("Data-Team", PermissionLevel.CAN_USE), _entry("ops", "CAN_MANAGE")])

    assert levels == {"data-team": frozenset({"CAN_USE"}), "ops": frozenset({"CAN_MANAGE"})}


def test_missing_principals_accept_stronger_levels_and_preserve_order() -> None:
    levels = {"ops": frozenset({"CAN_MANAGE"})}
    sufficient = frozenset({"CAN_USE", "CAN_MANAGE", "IS_OWNER"})

    assert missing_principals(levels, ["data-team", "Ops", "users"], sufficient) == ("data-team", "users")


def _acl(*entries: SimpleNamespace) -> SimpleNamespace:
    return SimpleNamespace(access_control_list=list(entries))


def test_missing_principals_treat_the_users_group_as_everyone() -> None:
    levels = {"users": frozenset({"CAN_USE"})}

    assert missing_principals(levels, ["data-team", "admins-custom"], frozenset({"CAN_USE", "CAN_MANAGE"})) == ()


def test_missing_principals_ignore_an_insufficient_users_level() -> None:
    levels = {"users": frozenset({"CAN_VIEW"})}

    assert missing_principals(levels, ["data-team"], frozenset({"CAN_RUN"})) == ("data-team",)


def test_reconcile_group_acl_grants_only_missing_groups_then_verifies() -> None:
    ws = create_autospec(WorkspaceClient, instance=True)
    ws.permissions.get.side_effect = [
        _acl(_entry("editors", "CAN_EDIT")),
        _acl(_entry("editors", "CAN_EDIT"), _entry("readers", "CAN_RUN")),
    ]

    status, missing = reconcile_group_acl(
        ws, "genie", "space-1", ("editors", "readers"), PermissionLevel.CAN_RUN, GENIE_RUN_LEVELS
    )

    assert (status, missing) == ("granted", ())
    ws.permissions.update.assert_called_once_with(
        "genie",
        "space-1",
        access_control_list=[AccessControlRequest(group_name="readers", permission_level=PermissionLevel.CAN_RUN)],
    )


def test_reconcile_group_acl_reports_groups_still_missing_after_a_failed_update() -> None:
    ws = create_autospec(WorkspaceClient, instance=True)
    ws.permissions.get.return_value = _acl()
    ws.permissions.update.side_effect = RuntimeError("denied")

    assert reconcile_group_acl(ws, "genie", "space-1", ("readers",), PermissionLevel.CAN_RUN, GENIE_RUN_LEVELS) == (
        "missing",
        ("readers",),
    )


def test_reconcile_group_acl_falls_back_to_later_readers() -> None:
    app_ws = create_autospec(WorkspaceClient, instance=True)
    admin_ws = create_autospec(WorkspaceClient, instance=True)
    app_ws.permissions.get.side_effect = RuntimeError("no access")
    admin_ws.permissions.get.return_value = _acl(_entry("readers", "CAN_MANAGE"))

    status, _ = reconcile_group_acl(
        app_ws,
        "dashboards",
        "dash-1",
        ("readers",),
        PermissionLevel.CAN_READ,
        frozenset({"CAN_READ", "CAN_MANAGE"}),
        readers=(app_ws, admin_ws),
    )

    assert status == "granted"
    app_ws.permissions.update.assert_not_called()


def test_reconcile_group_acl_is_unknown_when_no_reader_can_read() -> None:
    ws = create_autospec(WorkspaceClient, instance=True)
    ws.permissions.get.side_effect = RuntimeError("no access")

    assert reconcile_group_acl(ws, "genie", "space-1", ("readers",), PermissionLevel.CAN_RUN, GENIE_RUN_LEVELS) == (
        "unknown",
        (),
    )
    ws.permissions.update.assert_not_called()
