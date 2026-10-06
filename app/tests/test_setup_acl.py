"""Pure workspace ACL parsing used by setup sharing checks."""

from types import SimpleNamespace

from databricks.sdk.service.iam import PermissionLevel

from databricks_labs_dqx_app.backend.setup.acl import group_levels, missing_principals


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
