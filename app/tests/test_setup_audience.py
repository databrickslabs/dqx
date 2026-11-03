"""Audience principal mapping for workspace ACLs and Unity Catalog grants."""

import pytest

from databricks.labs.dqx.errors import InvalidParameterError
from databricks_labs_dqx_app.backend.setup.audience import (
    AudienceGroupRequiredError,
    BuiltInAudienceGroupError,
    resolve_audience,
)


def test_dedicated_group_uses_same_principal_everywhere() -> None:
    audience = resolve_audience(["data-team"], "admins", allow_broad=False)

    assert audience.workspace_principals == ("data-team",)
    assert audience.uc_principals == ("data-team",)
    assert audience.broad is False
    assert audience.admin_uc_group is None


@pytest.mark.parametrize("value", ["users", " USERS "])
def test_broad_mode_maps_to_users_and_account_users(value: str) -> None:
    audience = resolve_audience([value], "admins", allow_broad=True)

    assert audience.workspace_principals == ("users",)
    assert audience.uc_principals == ("account users",)
    assert audience.broad is True


def test_broad_mode_is_rejected_when_not_allowed() -> None:
    with pytest.raises(BuiltInAudienceGroupError):
        resolve_audience(["users"], "admins", allow_broad=False)


@pytest.mark.parametrize("value", ["account users", "Admins"])
def test_builtin_groups_raise_a_dedicated_error(value: str) -> None:
    with pytest.raises(BuiltInAudienceGroupError):
        resolve_audience([value], "admins", allow_broad=True)


@pytest.mark.parametrize("value", ["", "  "])
def test_missing_group_raises_a_dedicated_error(value: str) -> None:
    with pytest.raises(AudienceGroupRequiredError):
        resolve_audience([value], "admins", allow_broad=True)


def test_no_groups_raises_a_dedicated_error() -> None:
    with pytest.raises(AudienceGroupRequiredError):
        resolve_audience([], "admins", allow_broad=True)


@pytest.mark.parametrize("value", ["a`b", "x\ny"])
def test_unsafe_groups_are_not_reported_as_builtin_or_missing(value: str) -> None:
    with pytest.raises(InvalidParameterError) as error:
        resolve_audience([value], "admins", allow_broad=True)

    assert not isinstance(error.value, (BuiltInAudienceGroupError, AudienceGroupRequiredError))


@pytest.mark.parametrize("value", ["account users", "admins", "", "a`b", "x\ny"])
def test_unsafe_or_builtin_groups_are_rejected(value: str) -> None:
    with pytest.raises(InvalidParameterError):
        resolve_audience([value], "admins", allow_broad=True)


def test_custom_admin_group_receives_uc_and_acl_access() -> None:
    audience = resolve_audience(["data-team"], "dqx-admins", allow_broad=False)

    assert audience.workspace_principals == ("data-team", "dqx-admins")
    assert audience.uc_principals == ("data-team", "dqx-admins")
    assert audience.admin_uc_group == "dqx-admins"


@pytest.mark.parametrize("admin_group", ["admins", "Admins"])
def test_workspace_admins_never_receive_uc_grants(admin_group: str) -> None:
    audience = resolve_audience(["data-team"], admin_group, allow_broad=False)

    assert audience.uc_principals == ("data-team",)
    assert audience.admin_uc_group is None


def test_groups_are_trimmed_and_deduplicated_case_insensitively() -> None:
    audience = resolve_audience([" data-team ", "DATA-TEAM", "ops"], "admins", allow_broad=False)

    assert audience.groups == ("data-team", "ops")


def test_audience_requires_a_group() -> None:
    with pytest.raises(InvalidParameterError):
        resolve_audience([], "admins", allow_broad=True)
