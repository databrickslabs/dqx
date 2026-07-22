"""Unit tests for the pure privilege model in ``common.permissions``."""

from __future__ import annotations

from databricks_labs_dqx_app.backend.common.permissions import (
    DEFAULT_USERS_GROUP_PRIVILEGES,
    USERS_GROUP_PRINCIPAL_ID,
    ObjectType,
    Privilege,
    default_users_group_privileges_for,
    expand_privileges,
    is_reserved_principal_id,
    is_users_group,
    normalize_privileges,
    parse_privileges,
    serialize_privileges,
)


def test_default_users_group_is_select_and_apply_not_modify():
    assert DEFAULT_USERS_GROUP_PRIVILEGES == {Privilege.SELECT, Privilege.APPLY, Privilege.EXECUTE}
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
        Privilege.EXECUTE,
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
    assert normalize_privileges({Privilege.SELECT, Privilege.MODIFY, Privilege.APPLY, Privilege.EXECUTE}) == {
        Privilege.ALL_PRIVILEGES
    }


def test_normalize_keeps_partial_set():
    assert normalize_privileges({Privilege.SELECT, Privilege.MODIFY}) == {
        Privilege.SELECT,
        Privilege.MODIFY,
    }


def test_execute_is_concrete():
    from databricks_labs_dqx_app.backend.common.permissions import _CONCRETE_PRIVILEGES

    assert Privilege.EXECUTE in _CONCRETE_PRIVILEGES


def test_all_privileges_expands_to_include_execute():
    assert Privilege.EXECUTE in expand_privileges({Privilege.ALL_PRIVILEGES})


def test_execute_in_users_group_default():
    assert Privilege.EXECUTE in DEFAULT_USERS_GROUP_PRIVILEGES


def test_serialize_order_places_execute_last():
    s = serialize_privileges({Privilege.EXECUTE, Privilege.SELECT})
    assert s == "SELECT,EXECUTE"


def test_normalize_collapses_full_set_to_all():
    assert normalize_privileges(
        {Privilege.SELECT, Privilege.MODIFY, Privilege.APPLY, Privilege.EXECUTE}
    ) == {Privilege.ALL_PRIVILEGES}


def test_parse_roundtrips_execute():
    assert Privilege.EXECUTE in parse_privileges("SELECT,EXECUTE")


# ---------------------------------------------------------------------------
# default_users_group_privileges_for — per-type seeding helper
# ---------------------------------------------------------------------------


def test_registry_rule_default_has_no_execute():
    """EXECUTE must NOT be in the default privilege set for registry_rule.

    EXECUTE means 'run profiling/validation on a table or collection'; it is
    meaningless on a rule template and the grant dialog already hides it for
    rules. Seeding EXECUTE onto a rule would confuse users and pollute the
    stored grants with a meaningless privilege.
    """
    privs = default_users_group_privileges_for(ObjectType.REGISTRY_RULE.value)
    assert Privilege.EXECUTE not in privs
    assert Privilege.SELECT in privs
    assert Privilege.APPLY in privs


def test_monitored_table_default_includes_execute():
    privs = default_users_group_privileges_for(ObjectType.MONITORED_TABLE.value)
    assert Privilege.EXECUTE in privs
    assert Privilege.SELECT in privs
    assert Privilege.APPLY in privs


def test_data_product_default_includes_execute():
    privs = default_users_group_privileges_for(ObjectType.DATA_PRODUCT.value)
    assert Privilege.EXECUTE in privs
    assert Privilege.SELECT in privs
    assert Privilege.APPLY in privs


def test_registry_rule_default_equals_select_apply():
    assert default_users_group_privileges_for(ObjectType.REGISTRY_RULE.value) == frozenset(
        {Privilege.SELECT, Privilege.APPLY}
    )


def test_non_rule_defaults_equal_default_users_group_privileges():
    for obj_type in (ObjectType.MONITORED_TABLE.value, ObjectType.DATA_PRODUCT.value):
        assert default_users_group_privileges_for(obj_type) == DEFAULT_USERS_GROUP_PRIVILEGES
