"""Unit tests for the pure privilege model in ``common.permissions``."""

from __future__ import annotations

from databricks_labs_dqx_app.backend.common.permissions import (
    DEFAULT_USERS_GROUP_PRIVILEGES,
    USERS_GROUP_PRINCIPAL_ID,
    Privilege,
    expand_privileges,
    is_reserved_principal_id,
    is_users_group,
    normalize_privileges,
    parse_privileges,
    serialize_privileges,
)


def test_default_users_group_is_select_and_apply_not_modify():
    assert DEFAULT_USERS_GROUP_PRIVILEGES == {Privilege.SELECT, Privilege.APPLY}
    assert Privilege.MODIFY not in DEFAULT_USERS_GROUP_PRIVILEGES


def test_users_group_principal_helpers():
    assert is_users_group(USERS_GROUP_PRINCIPAL_ID)
    assert not is_users_group("someone")


def test_reserved_principal_rejects_legacy_sentinel():
    assert is_reserved_principal_id("__all__")
    assert not is_reserved_principal_id(USERS_GROUP_PRINCIPAL_ID)
    assert not is_reserved_principal_id("u1")


def test_expand_all_privileges_to_concrete_set():
    assert expand_privileges({Privilege.ALL_PRIVILEGES}) == {
        Privilege.SELECT,
        Privilege.MODIFY,
        Privilege.APPLY,
    }


def test_expand_passthrough_concrete():
    assert expand_privileges({Privilege.MODIFY}) == {Privilege.MODIFY}


def test_parse_privileges_roundtrip():
    assert parse_privileges("SELECT,MODIFY") == {Privilege.SELECT, Privilege.MODIFY}
    assert parse_privileges("ALL_PRIVILEGES") == {Privilege.ALL_PRIVILEGES}
    assert parse_privileges("") == set()
    assert parse_privileges(None) == set()


def test_parse_privileges_ignores_unknown_tokens():
    assert parse_privileges("SELECT, BOGUS ,MODIFY") == {Privilege.SELECT, Privilege.MODIFY}


def test_serialize_all_privileges_is_superset_token():
    assert serialize_privileges({Privilege.ALL_PRIVILEGES}) == "ALL_PRIVILEGES"


def test_serialize_concrete_is_stable_order():
    assert serialize_privileges({Privilege.APPLY, Privilege.SELECT}) == "SELECT,APPLY"


def test_normalize_collapses_full_set_to_all_privileges():
    assert normalize_privileges({Privilege.SELECT, Privilege.MODIFY, Privilege.APPLY}) == {
        Privilege.ALL_PRIVILEGES
    }


def test_normalize_keeps_partial_set():
    assert normalize_privileges({Privilege.SELECT, Privilege.MODIFY}) == {
        Privilege.SELECT,
        Privilege.MODIFY,
    }
