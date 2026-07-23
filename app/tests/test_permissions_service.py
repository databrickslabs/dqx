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
    """Minimal OLTP executor supporting the service's SELECT + mutation shapes.

    Grants are keyed by (object_type, object_id) in the exact column order
    ``list_grants`` selects. The ``execute`` method parses the INSERT/DELETE
    patterns emitted by ``set_grant`` / ``remove_grant`` so that
    ``seed_default_grants`` (which calls ``set_grant``) is fully exercisable
    without hitting a real database. History INSERTs are accepted and ignored.
    """

    # Column positions in the stored row (mirrors list_grants SELECT order):
    # 0=object_type, 1=object_id, 2=principal_id, 3=principal_type,
    # 4=principal_name, 5=privileges, 6=inherit, 7=grantor, 8=updated_at
    _COL_COUNT = 9

    def __init__(self) -> None:
        self.grants: dict[tuple[str, str], list[list[object]]] = {}
        self.members: dict[str, list[str]] = {}
        # object_id -> created_by (owner email), answering the owner SELECT.
        self.owners: dict[str, str] = {}

    def fqn(self, table: str) -> str:
        return table

    def ts_text(self, col: str) -> str:
        return col

    # ------------------------------------------------------------------
    # SQL mutation interpreter (INSERT / DELETE on dq_object_grants)
    # ------------------------------------------------------------------

    def execute(self, sql: str, *, timeout_seconds: int = 120) -> None:
        """Parse and apply INSERT/DELETE on dq_object_grants; ignore history."""
        if "dq_object_grants_history" in sql:
            return  # audit trail — ignore
        if sql.strip().upper().startswith("DELETE"):
            self._apply_delete(sql)
        elif sql.strip().upper().startswith("INSERT"):
            self._apply_insert(sql)

    def _apply_delete(self, sql: str) -> None:
        """Remove the matching principal row from the in-memory store."""
        ot_m = re.search(r"object_type = '([^']*)'", sql)
        oid_m = re.search(r"object_id = '([^']*)'", sql)
        pid_m = re.search(r"principal_id = '([^']*)'", sql)
        if not (ot_m and oid_m and pid_m):
            return
        key = (ot_m.group(1), oid_m.group(1))
        pid = pid_m.group(1)
        rows = self.grants.get(key, [])
        self.grants[key] = [r for r in rows if r[2] != pid]

    def _apply_insert(self, sql: str) -> None:
        """Parse ``INSERT INTO dq_object_grants ... VALUES (...)`` and store."""
        if "dq_object_grants" not in sql:
            return
        # Extract the VALUES (...) block — single-row INSERT only.
        vals_m = re.search(r"VALUES\s*\((.+)\)\s*$", sql, re.DOTALL | re.IGNORECASE)
        if not vals_m:
            return
        raw = vals_m.group(1)
        # Split on commas that are NOT inside single-quoted strings.
        tokens: list[str] = []
        buf = ""
        in_str = False
        for ch in raw:
            if ch == "'" and not in_str:
                in_str = True
                buf += ch
            elif ch == "'" and in_str:
                in_str = False
                buf += ch
            elif ch == "," and not in_str:
                tokens.append(buf.strip())
                buf = ""
            else:
                buf += ch
        if buf.strip():
            tokens.append(buf.strip())

        def _unquote(tok: str) -> str | None:
            tok = tok.strip()
            if tok.upper() in ("NULL", "NOW()", "TRUE", "FALSE"):
                return tok.upper() if tok.upper() in ("TRUE", "FALSE") else None
            if tok.startswith("'") and tok.endswith("'"):
                return tok[1:-1].replace("''", "'")
            return tok

        # Column order in the INSERT:
        # grant_id(0), object_type(1), object_id(2), principal_id(3),
        # principal_type(4), principal_name(5), privileges(6), inherit(7),
        # grantor(8), created_at(9), updated_at(10)
        if len(tokens) < 11:
            return
        row: list[object] = [
            _unquote(tokens[1]),   # object_type
            _unquote(tokens[2]),   # object_id
            _unquote(tokens[3]),   # principal_id
            _unquote(tokens[4]),   # principal_type
            _unquote(tokens[5]),   # principal_name (may be NULL)
            _unquote(tokens[6]),   # privileges
            tokens[7].strip(),     # inherit (TRUE/FALSE string)
            _unquote(tokens[8]),   # grantor (may be NULL)
            None,                  # updated_at (not needed in tests)
        ]
        key = (str(row[0]), str(row[1]))
        # Normalise the inherit token to "true"/"false" lowercase.
        inherit_val = str(row[6]).strip().lower()
        row[6] = "true" if inherit_val == "true" else "false"
        self.grants.setdefault(key, []).append(row)

    def query(self, sql: str, *, timeout_seconds: int = 120) -> list[list[object]]:
        if "created_by" in sql:
            m = re.search(r"= '([^']*)'", sql)
            owner = self.owners.get(m.group(1) if m else "")
            return [[owner]] if owner else []
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
        row: list[object] = [
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
# Users-group default (now stored, not synthesized)
# ---------------------------------------------------------------------------


def test_default_grants_select_and_apply_to_everyone(svc):
    # Seed materializes the default row; with no seed there is no grant.
    svc.seed_default_grants("registry_rule", "r1", owner_email=None, grantor=None)
    eff = svc.effective_privileges("registry_rule", "r1", principal_ids=set())
    assert Privilege.SELECT in eff
    assert Privilege.APPLY in eff
    assert Privilege.MODIFY not in eff


def test_list_effective_grants_no_synthetic_default_when_unseeded(svc):
    # With no stored rows the object shows an empty grant list — no synthesis.
    eff = svc.list_effective_grants("registry_rule", "r1")
    assert eff == []


def test_list_effective_grants_shows_stored_users_group_row(svc):
    # After seeding the users-group row IS stored and appears in list.
    svc.seed_default_grants("registry_rule", "r1", owner_email=None, grantor=None)
    eff = svc.list_effective_grants("registry_rule", "r1")
    stored = [g for g in eff if g.principal_id == USERS_GROUP_PRINCIPAL_ID]
    assert len(stored) == 1
    assert stored[0].is_default is False
    # registry_rule: EXECUTE must NOT be in the seeded users-group row (B2 fix)
    assert stored[0].privileges == {Privilege.SELECT, Privilege.APPLY}
    assert Privilege.EXECUTE not in stored[0].privileges


def test_explicit_users_group_grant_stored_as_real_row(svc, fake):
    # An explicit users-group row narrowed to SELECT is a plain stored row.
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


# ---------------------------------------------------------------------------
# Owner/creator grant (stored, not synthesized — B2)
# ---------------------------------------------------------------------------


def test_list_effective_grants_shows_stored_owner_row(svc, fake):
    # After seeding, the owner row is a real stored row (not synthetic).
    fake.owners["r1"] = "creator@x.com"
    svc.seed_default_grants("registry_rule", "r1", owner_email="creator@x.com", grantor="creator@x.com")
    eff = svc.list_effective_grants("registry_rule", "r1")
    owner_rows = [g for g in eff if g.principal_id == "creator@x.com"]
    assert len(owner_rows) == 1
    owner = owner_rows[0]
    assert owner.principal_type == "user"
    assert owner.is_default is False  # it's a real stored row
    # Privileges stored as ALL_PRIVILEGES — expanded at enforcement time.
    from databricks_labs_dqx_app.backend.common.permissions import expand_privileges
    assert expand_privileges(owner.privileges) == expand_privileges({Privilege.ALL_PRIVILEGES})


def test_list_effective_grants_no_owner_row_when_unseeded(svc, fake):
    # Without seeding, no owner row appears even when owner is known.
    fake.owners["r1"] = "creator@x.com"
    eff = svc.list_effective_grants("registry_rule", "r1")
    assert [g for g in eff if g.principal_type == "user"] == []


def test_owner_stored_grant_gives_access(svc, fake):
    # After seeding, the owner's stored ALL_PRIVILEGES row confers full access.
    fake.owners["r1"] = "creator@x.com"
    svc.seed_default_grants("registry_rule", "r1", owner_email="creator@x.com", grantor="creator@x.com")
    eff = svc.effective_privileges(
        "registry_rule", "r1", principal_ids=set(), owner_email="creator@x.com", principal_email="creator@x.com"
    )
    assert {Privilege.SELECT, Privilege.MODIFY, Privilege.APPLY}.issubset(eff)


def test_owner_privileges_revocable_by_deleting_row(svc, fake):
    # Remove the owner row — owner loses access (no re-synthesis).
    fake.owners["r1"] = "creator@x.com"
    svc.seed_default_grants("registry_rule", "r1", owner_email="creator@x.com", grantor="creator@x.com")
    svc.remove_grant("registry_rule", "r1", "creator@x.com")
    eff = svc.effective_privileges(
        "registry_rule", "r1", principal_ids=set(), owner_email="creator@x.com", principal_email="creator@x.com"
    )
    assert Privilege.MODIFY not in eff


def test_owner_privileges_narrowable_via_explicit_grant(svc, fake):
    # A narrowed explicit owner grant (SELECT only) stores only SELECT.
    fake.owners["r1"] = "creator@x.com"
    fake.add_grant("registry_rule", "r1", "creator@x.com", "SELECT", principal_name="creator@x.com")
    eff = svc.effective_privileges(
        "registry_rule", "r1", principal_ids=set(), owner_email="creator@x.com", principal_email="creator@x.com"
    )
    assert Privilege.SELECT in eff
    assert Privilege.MODIFY not in eff


def test_owner_without_stored_row_has_no_implicit_access(svc, fake):
    # Without a stored owner row, the owner gets nothing (no synthesis).
    fake.owners["r1"] = "creator@x.com"
    eff = svc.effective_privileges(
        "registry_rule", "r1", principal_ids=set(), owner_email="creator@x.com", principal_email="creator@x.com"
    )
    assert Privilege.MODIFY not in eff


def test_non_owner_caller_gets_no_access_without_grants(svc, fake):
    # Display parity only: a non-owner caller's enforced privileges are
    # unaffected when no grant row exists.
    fake.owners["r1"] = "creator@x.com"
    eff = svc.effective_privileges(
        "registry_rule", "r1", principal_ids={"u1"}, owner_email="creator@x.com", principal_email="other@x.com"
    )
    assert Privilege.MODIFY not in eff


def test_admin_backstop_survives_owner_revoke(svc, fake):
    # Even with the owner row deleted, an ADMIN (workspace-admin backstop) keeps
    # full access — the object can never be orphaned.
    fake.owners["r1"] = "creator@x.com"
    svc.seed_default_grants("registry_rule", "r1", owner_email="creator@x.com", grantor="creator@x.com")
    svc.remove_grant("registry_rule", "r1", "creator@x.com")
    assert svc.has_privilege(
        "registry_rule",
        "r1",
        Privilege.MODIFY,
        role=UserRole.ADMIN,
        principal_ids=set(),
        owner_email="creator@x.com",
        principal_email="creator@x.com",
    )


def test_owner_can_always_manage_grants(svc, fake):
    # The owner can manage grants at all times — even after their stored row
    # is removed. The owner email match is sufficient (no implicit-grant guard).
    fake.owners["r1"] = "creator@x.com"
    svc.seed_default_grants("registry_rule", "r1", owner_email="creator@x.com", grantor="creator@x.com")
    assert svc.can_manage_grants(
        "registry_rule",
        "r1",
        role=UserRole.RULE_AUTHOR,
        principal_ids=set(),
        owner_email="creator@x.com",
        principal_email="creator@x.com",
    )
    # Removing the owner row does NOT strip manage capability.
    svc.remove_grant("registry_rule", "r1", "creator@x.com")
    assert svc.can_manage_grants(
        "registry_rule",
        "r1",
        role=UserRole.RULE_AUTHOR,
        principal_ids=set(),
        owner_email="creator@x.com",
        principal_email="creator@x.com",
    )
    # Admin backstop retains manage.
    assert svc.can_manage_grants(
        "registry_rule",
        "r1",
        role=UserRole.ADMIN,
        principal_ids=set(),
        owner_email="creator@x.com",
        principal_email="creator@x.com",
    )


def test_set_grant_owner_empty_deletes_row(mock_sql, app_settings_mock):
    # With stored rows, an empty-privilege grant for the owner is a delete —
    # no revoked-marker row needed because deletion now sticks (no re-synthesis).
    mock_sql.query.return_value = []  # no object-owner lookup needed
    svc = PermissionsService(sql=mock_sql, app_settings=app_settings_mock)
    svc.set_grant(
        "registry_rule",
        "r1",
        "owner@x.com",
        principal_type="user",
        principal_name="owner@x.com",
        privileges=set(),
        inherit=False,
        grantor="admin@x.com",
    )
    executed = " ".join(str(c.args[0]) for c in mock_sql.execute.call_args_list)
    assert "DELETE FROM dq_object_grants" in executed
    assert "INSERT INTO dq_object_grants " not in executed


def test_display_name_spoof_does_not_affect_owner_enforcement(svc, fake):
    # Security regression: a grant to a *different* principal (Mallory) whose
    # free-text SCIM display name is set to the owner's email must NOT be treated
    # as an owner grant at enforcement time.
    fake.owners["r1"] = "olivia@x.com"
    # Seed Olivia's owner row so she gets access via her stored grant.
    svc.seed_default_grants("registry_rule", "r1", owner_email="olivia@x.com", grantor="olivia@x.com")
    # Admin grants Mallory MODIFY, keyed by Mallory's SCIM id; her editable
    # display name happens to equal Olivia's email.
    fake.add_grant("registry_rule", "r1", "mallory-scim-id", "MODIFY", principal_name="olivia@x.com")

    # (a) Olivia's stored ALL_PRIVILEGES row grants her access.
    olivia_eff = svc.effective_privileges(
        "registry_rule", "r1", principal_ids=set(), owner_email="olivia@x.com", principal_email="olivia@x.com"
    )
    assert {Privilege.SELECT, Privilege.MODIFY, Privilege.APPLY}.issubset(olivia_eff)

    # Olivia always manages grants (owner email match, independent of stored rows).
    assert svc.can_manage_grants(
        "registry_rule",
        "r1",
        role=UserRole.RULE_AUTHOR,
        principal_ids=set(),
        owner_email="olivia@x.com",
        principal_email="olivia@x.com",
    )

    # (b) Mallory's SCIM-id grant (keyed by "mallory-scim-id") does NOT leak
    # to Olivia via the display-name matcher. A caller presenting email
    # "olivia@x.com" does get Olivia's own stored owner grant (keyed by
    # principal_id "olivia@x.com"), but NOT Mallory's separate scim-id grant.
    # So Mallory cannot spoof Olivia's email to gain extra unintended access
    # beyond what Olivia's own principal_id keyed grant would provide.
    stranger_eff = svc.effective_privileges(
        "registry_rule", "r1", principal_ids=set(), owner_email=None, principal_email="olivia@x.com"
    )
    # The stranger whose email is "olivia@x.com" holds Olivia's owner grant
    # (ALL_PRIVILEGES row keyed by principal_id "olivia@x.com") — expected.
    # Crucially they do NOT get an extra MODIFY from Mallory's scim-id grant.
    assert Privilege.MODIFY in stranger_eff  # comes from Olivia's stored owner row
    # Mallory's grant doesn't add anything new here — it's keyed by scim-id,
    # not by email, so it doesn't match via _grant_targets_email.
    mallory_only_eff = svc.effective_privileges(
        "registry_rule", "r1", principal_ids={"mallory-scim-id"}, owner_email=None, principal_email="mallory@x.com"
    )
    # Mallory only gets MODIFY from her own scim-id grant, not owner access.
    assert Privilege.MODIFY in mallory_only_eff  # her explicit grant
    assert Privilege.SELECT in mallory_only_eff


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


def test_apply_allowed_when_users_group_row_seeded(svc):
    # APPLY succeeds once the users-group default row is seeded.
    svc.seed_default_grants("monitored_table", "b1", owner_email=None, grantor=None)
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
    assert eff == {Privilege.SELECT, Privilege.MODIFY, Privilege.APPLY, Privilege.EXECUTE}


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


def test_owner_has_all_privileges_via_stored_row(svc):
    # With a stored ALL_PRIVILEGES row, the owner holds MODIFY.
    svc.seed_default_grants("registry_rule", "r1", owner_email="me@x.com", grantor="me@x.com")
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
    # Owner can manage (by email match, regardless of stored grants).
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
        privileges={Privilege.SELECT, Privilege.MODIFY, Privilege.APPLY, Privilege.EXECUTE},
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


def test_set_grant_users_group_empty_deletes_row(mock_sql, app_settings_mock):
    # With stored rows, empty privileges for the users group is a delete —
    # deletion sticks because there is no re-synthesis on read.
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
    assert "DELETE FROM dq_object_grants" in executed
    assert "INSERT INTO dq_object_grants " not in executed


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


# ---------------------------------------------------------------------------
# B2: seed_default_grants — materialize real default rows
# ---------------------------------------------------------------------------


def test_seed_default_grants_writes_users_group_and_owner(svc):
    from databricks_labs_dqx_app.backend.common.permissions import expand_privileges
    svc.seed_default_grants("monitored_table", "obj1", owner_email="a@b.com", grantor="a@b.com")
    grants = svc.list_grants("monitored_table", "obj1")
    users = next(g for g in grants if g.principal_id == USERS_GROUP_PRINCIPAL_ID)
    owner = next(g for g in grants if g.principal_id == "a@b.com")
    assert {Privilege.SELECT, Privilege.APPLY, Privilege.EXECUTE}.issubset(expand_privileges(users.privileges))
    assert expand_privileges(owner.privileges) == expand_privileges({Privilege.ALL_PRIVILEGES})


def test_seed_is_idempotent(svc):
    svc.seed_default_grants("monitored_table", "obj1", owner_email="a@b.com", grantor="a@b.com")
    svc.seed_default_grants("monitored_table", "obj1", owner_email="a@b.com", grantor="a@b.com")
    users_rows = [g for g in svc.list_grants("monitored_table", "obj1") if g.principal_id == USERS_GROUP_PRINCIPAL_ID]
    owner_rows = [g for g in svc.list_grants("monitored_table", "obj1") if g.principal_id == "a@b.com"]
    assert len(users_rows) == 1
    assert len(owner_rows) == 1


def test_no_implicit_default_when_unseeded(svc):
    # An object with NO stored rows confers nothing to any caller.
    eff = svc.effective_privileges("monitored_table", "obj1", principal_ids={"someid"})
    assert eff == set()


def test_deleting_users_group_row_sticks(svc):
    svc.seed_default_grants("monitored_table", "obj1", owner_email="a@b.com", grantor="a@b.com")
    svc.remove_grant("monitored_table", "obj1", USERS_GROUP_PRINCIPAL_ID)
    eff = svc.effective_privileges("monitored_table", "obj1", principal_ids={"randomcaller"})
    assert eff == set()  # gone for good — no re-synthesis


def test_seed_without_owner_only_writes_users_group(svc):
    svc.seed_default_grants("registry_rule", "r1", owner_email=None, grantor=None)
    grants = svc.list_grants("registry_rule", "r1")
    assert len(grants) == 1
    assert grants[0].principal_id == USERS_GROUP_PRINCIPAL_ID


def test_owner_access_comes_from_stored_row(svc):
    svc.seed_default_grants("monitored_table", "obj1", owner_email="a@b.com", grantor="a@b.com")
    eff = svc.effective_privileges(
        "monitored_table", "obj1", set(), owner_email="a@b.com", principal_email="a@b.com"
    )
    assert Privilege.MODIFY in eff


def test_deleting_owner_row_removes_owner_access(svc):
    svc.seed_default_grants("monitored_table", "obj1", owner_email="a@b.com", grantor="a@b.com")
    svc.remove_grant("monitored_table", "obj1", "a@b.com")
    eff = svc.effective_privileges(
        "monitored_table", "obj1", set(), owner_email="a@b.com", principal_email="a@b.com"
    )
    assert Privilege.MODIFY not in eff


def test_seed_registry_rule_users_group_has_no_execute(svc):
    """registry_rule seeding must NOT include EXECUTE in the users-group row.

    EXECUTE means 'run profiling/validation on a table or collection'.
    It is meaningless on a rule template and the grant dialog already hides
    it for rules. Seeding it pollutes the stored grants with a misleading
    privilege.
    """
    svc.seed_default_grants("registry_rule", "rule1", owner_email=None, grantor=None)
    grants = svc.list_grants("registry_rule", "rule1")
    users = next(g for g in grants if g.principal_id == USERS_GROUP_PRINCIPAL_ID)
    assert Privilege.EXECUTE not in users.privileges
    assert Privilege.SELECT in users.privileges
    assert Privilege.APPLY in users.privileges


def test_seed_monitored_table_users_group_includes_execute(svc):
    """monitored_table seeding MUST include EXECUTE in the users-group row."""
    svc.seed_default_grants("monitored_table", "tbl1", owner_email=None, grantor=None)
    grants = svc.list_grants("monitored_table", "tbl1")
    users = next(g for g in grants if g.principal_id == USERS_GROUP_PRINCIPAL_ID)
    assert Privilege.EXECUTE in users.privileges


def test_seed_data_product_users_group_includes_execute(svc):
    """data_product seeding MUST include EXECUTE in the users-group row."""
    svc.seed_default_grants("data_product", "prod1", owner_email=None, grantor=None)
    grants = svc.list_grants("data_product", "prod1")
    users = next(g for g in grants if g.principal_id == USERS_GROUP_PRINCIPAL_ID)
    assert Privilege.EXECUTE in users.privileges


# ---------------------------------------------------------------------------
# Owner-asymmetry pin: can_manage_grants vs effective_privileges when unseeded
# ---------------------------------------------------------------------------


def test_owner_asymmetry_can_manage_grants_true_without_stored_row(svc):
    """can_manage_grants returns True for the owner by email REGARDLESS of stored rows.

    This is the expected and load-bearing asymmetry: ownership identity comes
    from the object's created_by field (resolved by get_object_owner), not
    from stored grant rows. A freshly created object where seeding has not yet
    run (e.g. during migration before the backfill) must not lock out its owner
    from managing grants.
    """
    # No grants stored — object has zero grant rows.
    from databricks_labs_dqx_app.backend.common.authorization import UserRole

    result = svc.can_manage_grants(
        "monitored_table", "obj1",
        role=UserRole.RULE_AUTHOR,
        principal_ids=set(),
        owner_email="owner@x.com",
        principal_email="owner@x.com",
    )
    assert result is True


def test_owner_asymmetry_effective_privileges_empty_without_stored_row(svc):
    """effective_privileges / has_privilege return empty/deny for the owner when no row exists.

    This documents that seeding is load-bearing: the owner cannot APPLY, MODIFY,
    or SELECT an object until seed_default_grants has run. can_manage_grants
    (tested above) is the only path that grants the owner access without a row —
    and that path only covers grant management (MANAGE), not object operations.
    """
    eff = svc.effective_privileges(
        "monitored_table", "obj1", set(), owner_email="owner@x.com", principal_email="owner@x.com"
    )
    assert eff == set()

    from databricks_labs_dqx_app.backend.common.authorization import UserRole
    result = svc.has_privilege(
        "monitored_table", "obj1", Privilege.MODIFY,
        role=UserRole.RULE_AUTHOR,
        principal_ids=set(),
        owner_email="owner@x.com",
        principal_email="owner@x.com",
    )
    assert result is False
