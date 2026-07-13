"""Tests for ``PermissionsService`` — grant CRUD, inheritance resolution, and
the enforcement matrix (baseline defaults, ownership, role bypass).

Resolution/enforcement tests run against a small in-memory fake executor
(:class:`_FakeOltp`) that answers the two SELECT shapes the service emits
(grants-for-object, members-for-binding). CRUD tests use a spec-bound mock
and assert on the emitted SQL.
"""

from __future__ import annotations

import re
from unittest.mock import MagicMock, create_autospec

import pytest
from fastapi import HTTPException

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.common.permissions import (
    USERS_GROUP_PRINCIPAL_ID,
    ObjectType,
    Privilege,
)
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.permissions_service import PermissionsService
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor


class _FakeOltp:
    """Minimal OLTP executor supporting the service's SELECT shapes.

    Grants are preloaded as raw rows keyed by (object_type, object_id) in the
    exact column order ``list_grants`` selects. Members map binding_id ->
    [product_id]. Only ``query`` is interpreted; ``execute`` is a no-op.
    """

    def __init__(self) -> None:
        self.grants: dict[tuple[str, str], list[list[object]]] = {}
        self.members: dict[str, list[str]] = {}

    def fqn(self, table: str) -> str:
        return table

    def ts_text(self, col: str) -> str:
        return col

    def execute(self, sql: str, *, timeout_seconds: int = 120) -> None:
        return None

    def query(self, sql: str, *, timeout_seconds: int = 120) -> list[list[object]]:
        if "dq_data_product_members" in sql:
            m = re.search(r"binding_id = '([^']*)'", sql)
            binding_id = m.group(1) if m else ""
            return [[pid] for pid in self.members.get(binding_id, [])]
        ot = re.search(r"object_type = '([^']*)'", sql)
        oid = re.search(r"object_id = '([^']*)'", sql)
        key = (ot.group(1) if ot else "", oid.group(1) if oid else "")
        return list(self.grants.get(key, []))

    def add_grant(
        self,
        object_type: str,
        object_id: str,
        principal_id: str,
        privileges: str,
        *,
        inherit: bool = False,
        principal_type: str = "user",
        principal_name: str = "someone",
    ) -> None:
        row = [
            object_type,
            object_id,
            principal_id,
            principal_type,
            principal_name,
            privileges,
            "true" if inherit else "false",
            "grantor@x.com",
            None,
        ]
        self.grants.setdefault((object_type, object_id), []).append(row)


@pytest.fixture
def app_settings_mock() -> MagicMock:
    return create_autospec(AppSettingsService, instance=True)


@pytest.fixture
def fake() -> _FakeOltp:
    return _FakeOltp()


@pytest.fixture
def svc(fake, app_settings_mock) -> PermissionsService:
    return PermissionsService(sql=fake, app_settings=app_settings_mock)


# ---------------------------------------------------------------------------
# Users-group default (implicit-unless-overridden)
# ---------------------------------------------------------------------------


def test_default_grants_select_and_apply_to_everyone(svc):
    eff = svc.effective_privileges("registry_rule", "r1", principal_ids=set())
    assert Privilege.SELECT in eff
    assert Privilege.APPLY in eff
    assert Privilege.MODIFY not in eff


def test_list_effective_grants_surfaces_synthetic_default(svc):
    eff = svc.list_effective_grants("registry_rule", "r1")
    assert len(eff) == 1
    default = eff[0]
    assert default.is_default is True
    assert default.principal_id == USERS_GROUP_PRINCIPAL_ID
    assert default.principal_type == "group"
    assert default.privileges == {Privilege.SELECT, Privilege.APPLY}


def test_explicit_users_group_grant_replaces_synthetic_default(svc, fake):
    # An explicit users-group row narrowed to SELECT overrides the default.
    fake.add_grant("registry_rule", "r1", USERS_GROUP_PRINCIPAL_ID, "SELECT", principal_type="group")
    eff = svc.list_effective_grants("registry_rule", "r1")
    assert len(eff) == 1
    assert eff[0].is_default is False
    assert eff[0].privileges == {Privilege.SELECT}


def test_narrowing_users_group_to_select_makes_apply_bite(svc, fake):
    # Revoking APPLY on the users group means a non-owner author loses APPLY.
    fake.add_grant("monitored_table", "b1", USERS_GROUP_PRINCIPAL_ID, "SELECT", principal_type="group")
    eff = svc.effective_privileges("monitored_table", "b1", principal_ids={"u1"})
    assert Privilege.SELECT in eff
    assert Privilege.APPLY not in eff


def test_revoked_users_group_grant_confers_nothing(svc, fake):
    # An explicit empty users-group row is the per-object "revoked" marker.
    fake.add_grant("registry_rule", "r1", USERS_GROUP_PRINCIPAL_ID, "", principal_type="group")
    eff = svc.effective_privileges("registry_rule", "r1", principal_ids={"u1"})
    assert eff == set()


def test_author_cannot_modify_without_grant(svc):
    # RULE_AUTHOR, no grant, not owner -> MODIFY denied (feature gates MODIFY).
    with pytest.raises(HTTPException) as exc:
        svc.require(
            "registry_rule",
            "r1",
            Privilege.MODIFY,
            role=UserRole.RULE_AUTHOR,
            principal_ids={"u1"},
            owner_email="other@x.com",
            principal_email="me@x.com",
        )
    assert exc.value.status_code == 403


def test_apply_allowed_by_baseline_for_author(svc):
    # No exception: APPLY is in the day-one baseline.
    svc.require(
        "monitored_table",
        "b1",
        Privilege.APPLY,
        role=UserRole.RULE_AUTHOR,
        principal_ids={"u1"},
        owner_email="other@x.com",
        principal_email="me@x.com",
    )


# ---------------------------------------------------------------------------
# Direct grants
# ---------------------------------------------------------------------------


def test_direct_grant_confers_modify(svc, fake):
    fake.add_grant("registry_rule", "r1", "u1", "MODIFY")
    assert svc.has_privilege(
        "registry_rule",
        "r1",
        Privilege.MODIFY,
        role=UserRole.RULE_AUTHOR,
        principal_ids={"u1"},
        owner_email="other@x.com",
        principal_email="me@x.com",
    )


def test_grant_to_group_matches_via_principal_ids(svc, fake):
    fake.add_grant("registry_rule", "r1", "grp1", "MODIFY", principal_type="group")
    assert svc.has_privilege(
        "registry_rule",
        "r1",
        Privilege.MODIFY,
        role=UserRole.RULE_AUTHOR,
        principal_ids={"u1", "grp1"},
        owner_email="other@x.com",
        principal_email="me@x.com",
    )


def test_users_group_grant_applies_to_everyone(svc, fake):
    fake.add_grant("data_product", "p1", USERS_GROUP_PRINCIPAL_ID, "MODIFY", principal_type="group")
    assert svc.has_privilege(
        "data_product",
        "p1",
        Privilege.MODIFY,
        role=UserRole.RULE_AUTHOR,
        principal_ids={"whoever"},
        owner_email="other@x.com",
        principal_email="me@x.com",
    )


def test_all_privileges_grant_expands(svc, fake):
    fake.add_grant("registry_rule", "r1", "u1", "ALL_PRIVILEGES")
    eff = svc.effective_privileges("registry_rule", "r1", principal_ids={"u1"})
    assert eff == {Privilege.SELECT, Privilege.MODIFY, Privilege.APPLY}


# ---------------------------------------------------------------------------
# Role bypass + ownership
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("role", [UserRole.ADMIN, UserRole.RULE_APPROVER])
def test_admin_and_approver_bypass_object_grants(svc, role):
    assert svc.has_privilege(
        "registry_rule",
        "r1",
        Privilege.MODIFY,
        role=role,
        principal_ids=set(),
        owner_email="other@x.com",
        principal_email="me@x.com",
    )


def test_owner_has_all_privileges(svc):
    eff = svc.effective_privileges(
        "registry_rule",
        "r1",
        principal_ids=set(),
        owner_email="me@x.com",
        principal_email="ME@x.com",  # case-insensitive
    )
    assert Privilege.MODIFY in eff


# ---------------------------------------------------------------------------
# Inheritance
# ---------------------------------------------------------------------------


def test_inherited_grant_flows_from_product_to_member_table(svc, fake):
    fake.members["b1"] = ["p1"]
    fake.add_grant("data_product", "p1", "u1", "MODIFY", inherit=True)
    assert svc.has_privilege(
        "monitored_table",
        "b1",
        Privilege.MODIFY,
        role=UserRole.RULE_AUTHOR,
        principal_ids={"u1"},
        owner_email="other@x.com",
        principal_email="me@x.com",
    )


def test_non_inheriting_product_grant_does_not_flow(svc, fake):
    fake.members["b1"] = ["p1"]
    fake.add_grant("data_product", "p1", "u1", "MODIFY", inherit=False)
    assert not svc.has_privilege(
        "monitored_table",
        "b1",
        Privilege.MODIFY,
        role=UserRole.RULE_AUTHOR,
        principal_ids={"u1"},
        owner_email="other@x.com",
        principal_email="me@x.com",
    )


def test_list_effective_grants_flags_inherited(svc, fake):
    fake.members["b1"] = ["p1"]
    fake.add_grant("data_product", "p1", "u1", "MODIFY", inherit=True)
    eff = [g for g in svc.list_effective_grants("monitored_table", "b1") if not g.is_default]
    assert len(eff) == 1
    assert eff[0].inherited_from_type == "data_product"
    assert eff[0].inherited_from_id == "p1"


def test_direct_grant_shadows_inherited_for_same_principal(svc, fake):
    fake.members["b1"] = ["p1"]
    fake.add_grant("data_product", "p1", "u1", "SELECT", inherit=True)
    fake.add_grant("monitored_table", "b1", "u1", "MODIFY")
    eff = [g for g in svc.list_effective_grants("monitored_table", "b1") if not g.is_default]
    # Only the direct grant surfaces for u1 (inherited one is shadowed).
    assert len(eff) == 1
    assert eff[0].inherited_from_type is None
    assert eff[0].privileges == {Privilege.MODIFY}


# ---------------------------------------------------------------------------
# can_manage_grants
# ---------------------------------------------------------------------------


def test_manage_grants_requires_owner_or_admin(svc, fake):
    fake.add_grant("registry_rule", "r1", "u1", "ALL_PRIVILEGES")
    # ALL PRIVILEGES alone does NOT confer manage (UC: MANAGE is separate).
    assert not svc.can_manage_grants(
        "registry_rule",
        "r1",
        role=UserRole.RULE_AUTHOR,
        principal_ids={"u1"},
        owner_email="other@x.com",
        principal_email="me@x.com",
    )
    # Owner can manage.
    assert svc.can_manage_grants(
        "registry_rule",
        "r1",
        role=UserRole.RULE_AUTHOR,
        principal_ids={"u1"},
        owner_email="me@x.com",
        principal_email="me@x.com",
    )
    # Admin can manage.
    assert svc.can_manage_grants(
        "registry_rule", "r1", role=UserRole.ADMIN, principal_ids=set(), owner_email=None, principal_email=None
    )


# ---------------------------------------------------------------------------
# CRUD (emitted SQL)
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_sql() -> MagicMock:
    m = create_autospec(SqlExecutor, instance=True)
    m.fqn.side_effect = lambda t: t
    m.ts_text.side_effect = lambda c: c
    m.query.return_value = []
    return m


def test_set_grant_deletes_then_inserts(mock_sql, app_settings_mock):
    svc = PermissionsService(sql=mock_sql, app_settings=app_settings_mock)
    svc.set_grant(
        "registry_rule",
        "r1",
        "u1",
        principal_type="user",
        principal_name="Alice",
        privileges={Privilege.SELECT, Privilege.MODIFY},
        inherit=True,
        grantor="admin@x.com",
    )
    executed = " ".join(str(c.args[0]) for c in mock_sql.execute.call_args_list)
    assert "DELETE FROM dq_object_grants" in executed
    assert "INSERT INTO dq_object_grants" in executed
    assert "SELECT,MODIFY" in executed
    assert "TRUE" in executed
    # History row written.
    assert "dq_object_grants_history" in executed


def test_set_grant_full_set_stored_as_all_privileges(mock_sql, app_settings_mock):
    svc = PermissionsService(sql=mock_sql, app_settings=app_settings_mock)
    svc.set_grant(
        "data_product",
        "p1",
        "u1",
        principal_type="user",
        principal_name="Alice",
        privileges={Privilege.SELECT, Privilege.MODIFY, Privilege.APPLY},
        inherit=False,
        grantor="admin@x.com",
    )
    executed = " ".join(str(c.args[0]) for c in mock_sql.execute.call_args_list)
    assert "ALL_PRIVILEGES" in executed


def test_set_grant_empty_privileges_removes(mock_sql, app_settings_mock):
    svc = PermissionsService(sql=mock_sql, app_settings=app_settings_mock)
    svc.set_grant(
        "data_product",
        "p1",
        "u1",
        principal_type="user",
        principal_name="Alice",
        privileges=set(),
        inherit=False,
        grantor="admin@x.com",
    )
    executed = " ".join(str(c.args[0]) for c in mock_sql.execute.call_args_list)
    assert "DELETE FROM dq_object_grants" in executed
    assert "INSERT INTO dq_object_grants " not in executed


def test_set_grant_users_group_empty_materializes_revoked_row(mock_sql, app_settings_mock):
    # For the users group, empty privileges = explicit "revoked" marker row
    # (INSERT with empty privileges), NOT a delete-to-default.
    svc = PermissionsService(sql=mock_sql, app_settings=app_settings_mock)
    svc.set_grant(
        "registry_rule",
        "r1",
        USERS_GROUP_PRINCIPAL_ID,
        principal_type="group",
        principal_name="users",
        privileges=set(),
        inherit=False,
        grantor="admin@x.com",
    )
    executed = " ".join(str(c.args[0]) for c in mock_sql.execute.call_args_list)
    assert "INSERT INTO dq_object_grants " in executed


def test_set_grant_rejects_legacy_sentinel(mock_sql, app_settings_mock):
    svc = PermissionsService(sql=mock_sql, app_settings=app_settings_mock)
    with pytest.raises(HTTPException) as exc:
        svc.set_grant(
            "registry_rule",
            "r1",
            "__all__",
            principal_type="group",
            principal_name="everyone",
            privileges={Privilege.MODIFY},
            inherit=False,
            grantor="admin@x.com",
        )
    assert exc.value.status_code == 400


def test_set_grant_rejects_bad_object_type(mock_sql, app_settings_mock):
    svc = PermissionsService(sql=mock_sql, app_settings=app_settings_mock)
    with pytest.raises(HTTPException) as exc:
        svc.set_grant(
            "bogus",
            "x",
            "u1",
            principal_type="user",
            principal_name="Alice",
            privileges={Privilege.MODIFY},
            inherit=False,
            grantor="admin@x.com",
        )
    assert exc.value.status_code == 400


def test_default_inherit_delegates_to_app_settings(mock_sql, app_settings_mock):
    app_settings_mock.get_permissions_default_inherit.return_value = True
    svc = PermissionsService(sql=mock_sql, app_settings=app_settings_mock)
    assert svc.get_default_inherit() is True
    svc.set_default_inherit(False, user_email="admin@x.com")
    app_settings_mock.save_permissions_default_inherit.assert_called_once()


def test_object_type_enum_values():
    assert ObjectType.REGISTRY_RULE.value == "registry_rule"
    assert ObjectType.MONITORED_TABLE.value == "monitored_table"
    assert ObjectType.DATA_PRODUCT.value == "data_product"


# ---------------------------------------------------------------------------
# object_id validation at every SQL entry boundary
#
# object_id is a raw path parameter reachable by any authenticated user
# (GET/PUT/DELETE /permissions/{type}/{id}/...); it is interpolated into SQL
# string literals via escape_sql_string, which deliberately does not escape
# backslashes. These assert the 400-mapped rejection at each boundary method
# rather than only in the shared sql_utils helper.
# ---------------------------------------------------------------------------


_MALICIOUS_OBJECT_IDS = ["r1\\", "r1' OR '1'='1", "r1\n", "", "r1 space"]


@pytest.mark.parametrize("bad_id", _MALICIOUS_OBJECT_IDS)
def test_list_grants_rejects_bad_object_id(svc, bad_id):
    with pytest.raises(HTTPException) as exc:
        svc.list_grants("registry_rule", bad_id)
    assert exc.value.status_code == 400


@pytest.mark.parametrize("bad_id", _MALICIOUS_OBJECT_IDS)
def test_list_effective_grants_rejects_bad_object_id(svc, bad_id):
    with pytest.raises(HTTPException) as exc:
        svc.list_effective_grants("registry_rule", bad_id)
    assert exc.value.status_code == 400


@pytest.mark.parametrize("bad_id", _MALICIOUS_OBJECT_IDS)
def test_effective_privileges_rejects_bad_object_id(svc, bad_id):
    with pytest.raises(HTTPException) as exc:
        svc.effective_privileges("registry_rule", bad_id, principal_ids=set())
    assert exc.value.status_code == 400


@pytest.mark.parametrize("bad_id", _MALICIOUS_OBJECT_IDS)
def test_get_object_owner_rejects_bad_object_id(svc, bad_id):
    with pytest.raises(HTTPException) as exc:
        svc.get_object_owner("registry_rule", bad_id)
    assert exc.value.status_code == 400


@pytest.mark.parametrize("bad_id", _MALICIOUS_OBJECT_IDS)
def test_require_object_rejects_bad_object_id(svc, bad_id):
    with pytest.raises(HTTPException) as exc:
        svc.require_object(
            "registry_rule",
            bad_id,
            Privilege.MODIFY,
            role=UserRole.RULE_AUTHOR,
            principal_ids=set(),
            principal_email="me@x.com",
        )
    assert exc.value.status_code == 400


@pytest.mark.parametrize("bad_id", _MALICIOUS_OBJECT_IDS)
def test_set_grant_rejects_bad_object_id(mock_sql, app_settings_mock, bad_id):
    svc = PermissionsService(sql=mock_sql, app_settings=app_settings_mock)
    with pytest.raises(HTTPException) as exc:
        svc.set_grant(
            "registry_rule",
            bad_id,
            "u1",
            principal_type="user",
            principal_name="Alice",
            privileges={Privilege.MODIFY},
            inherit=False,
            grantor="admin@x.com",
        )
    assert exc.value.status_code == 400
    mock_sql.execute.assert_not_called()


@pytest.mark.parametrize("bad_id", _MALICIOUS_OBJECT_IDS)
def test_remove_grant_rejects_bad_object_id(mock_sql, app_settings_mock, bad_id):
    svc = PermissionsService(sql=mock_sql, app_settings=app_settings_mock)
    with pytest.raises(HTTPException) as exc:
        svc.remove_grant("registry_rule", bad_id, "u1", actor="admin@x.com")
    assert exc.value.status_code == 400
    mock_sql.execute.assert_not_called()


@pytest.mark.parametrize(
    "object_id",
    ["r1", "a" * 32, "a" * 16, "binding-abc_123"],
)
def test_valid_object_id_formats_pass_through(svc, object_id):
    # Covers the three real id shapes: registry_rule (uuid4().hex[:16]),
    # monitored_table binding_id (uuid4().hex[:16]), data_product product_id
    # (uuid4().hex) — plus a dash/underscore id. No exception raised.
    svc.list_grants("registry_rule", object_id)
    svc.get_object_owner("monitored_table", object_id)
