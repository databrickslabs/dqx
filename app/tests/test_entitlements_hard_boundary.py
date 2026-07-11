"""Entitlements hard-boundary invariant (item #43).

The app's authorization is two-layer:

* **Roles** (``require_role``) are the coarse, hard OUTER gate on every route.
* **Object grants** (``dq_object_grants`` via :class:`PermissionsService`) refine
  access *within* a role — they are purely additive.

These tests pin the invariant that **object grants can never exceed what a
user's role permits** — i.e. ``require_role`` is the ceiling. A user with a low
role and a broad object grant still cannot perform a role-gated action.

We deliberately keep the ``ADMIN``/``RULE_APPROVER`` object-grant bypass: that
bypass is the *upper* boundary of the ceiling (a governance-role widening),
consistent with intent — it is asserted here, not removed.

The proofs are structural + behavioural, no workspace required:

1. The object-grant privilege vocabulary is disjoint from the role-permission
   vocabulary, so a grant cannot even *express* a role capability.
2. ``require_role`` rejects a low-role caller and its only input is the resolved
   role — there is no object-grant seam that can satisfy it.
3. A broad grant (even ``ALL_PRIVILEGES``) tops out at the concrete object
   privileges and never confers a role capability; the caller's role is
   unchanged, so the role-gated route still 403s.
4. Only ``ADMIN``/``RULE_APPROVER`` bypass object grants; ``RULE_AUTHOR`` /
   ``VIEWER`` / ``RUNNER`` do not.
"""

from __future__ import annotations

import re

import pytest
from fastapi import HTTPException

from databricks_labs_dqx_app.backend.common.authorization import PERMISSIONS, UserRole
from databricks_labs_dqx_app.backend.common.permissions import ObjectType, Privilege
from databricks_labs_dqx_app.backend.dependencies import require_role
from databricks_labs_dqx_app.backend.services.permissions_service import PermissionsService


class _FakeOltp:
    """Minimal OLTP executor answering ``list_grants`` / members SELECT shapes."""

    def __init__(self) -> None:
        self.grants: dict[tuple[str, str], list[list[object]]] = {}

    def fqn(self, table: str) -> str:
        return table

    def ts_text(self, col: str) -> str:
        return col

    def execute(self, sql: str, *, timeout_seconds: int = 120) -> None:
        return None

    def query(self, sql: str, *, timeout_seconds: int = 120) -> list[list[object]]:
        if "dq_data_product_members" in sql:
            return []
        ot = re.search(r"object_type = '([^']*)'", sql)
        oid = re.search(r"object_id = '([^']*)'", sql)
        key = (ot.group(1) if ot else "", oid.group(1) if oid else "")
        return list(self.grants.get(key, []))

    def add_grant(self, object_type: str, object_id: str, principal_id: str, privileges: str) -> None:
        self.grants.setdefault((object_type, object_id), []).append(
            [object_type, object_id, principal_id, "user", "someone", privileges, "false", "grantor@x.com", None]
        )


@pytest.fixture
def svc() -> PermissionsService:
    class _AppSettings:
        def get_permissions_default_inherit(self) -> bool:
            return False

    return PermissionsService(sql=_FakeOltp(), app_settings=_AppSettings())  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# 1. Vocabularies are disjoint — a grant cannot express a role capability.
# ---------------------------------------------------------------------------


def test_object_privilege_vocabulary_disjoint_from_role_permissions():
    grant_vocab = {p.value.lower() for p in Privilege}
    role_vocab: set[str] = set()
    for perms in PERMISSIONS.values():
        role_vocab.update(p.lower() for p in perms)

    # No overlap even case-insensitively: an object grant can never name a
    # role capability such as ``approve_rules`` / ``manage_roles``.
    assert grant_vocab.isdisjoint(role_vocab)
    # Sanity: the role vocabulary actually contains the sensitive capabilities
    # we are asserting a grant cannot reach.
    assert {"approve_rules", "manage_roles"}.issubset(role_vocab)


# ---------------------------------------------------------------------------
# 2. require_role is the ceiling and only sees the resolved role.
# ---------------------------------------------------------------------------


async def test_require_role_rejects_low_role_regardless_of_grants():
    # A role-gated route (ADMIN / RULE_APPROVER only). The dependency's only
    # input is the resolved role — no object-grant parameter exists that could
    # flip the decision, so a broad grant elsewhere is irrelevant here.
    check = require_role(UserRole.ADMIN, UserRole.RULE_APPROVER).dependency
    for low_role in (UserRole.VIEWER, UserRole.RULE_AUTHOR, UserRole.RUNNER):
        with pytest.raises(HTTPException) as exc:
            await check(role=low_role)
        assert exc.value.status_code == 403


async def test_require_role_admits_listed_roles():
    check = require_role(UserRole.ADMIN, UserRole.RULE_APPROVER).dependency
    assert await check(role=UserRole.ADMIN) == UserRole.ADMIN
    assert await check(role=UserRole.RULE_APPROVER) == UserRole.RULE_APPROVER


# ---------------------------------------------------------------------------
# 3. A broad grant is bounded and never escalates the role.
# ---------------------------------------------------------------------------


def test_all_privileges_grant_tops_out_at_object_privileges(svc):
    fake: _FakeOltp = svc._sql  # type: ignore[assignment]
    fake.add_grant("registry_rule", "r1", "u1", "ALL_PRIVILEGES")
    eff = svc.effective_privileges("registry_rule", "r1", principal_ids={"u1"})
    # The widest possible grant confers exactly the three concrete object
    # privileges — nothing role-shaped, and never MANAGE.
    assert eff == {Privilege.SELECT, Privilege.MODIFY, Privilege.APPLY}


async def test_broad_grant_does_not_let_low_role_pass_role_gate(svc):
    # Author holds ALL_PRIVILEGES on the object (object layer grants MODIFY)...
    fake: _FakeOltp = svc._sql  # type: ignore[assignment]
    fake.add_grant("registry_rule", "r1", "u1", "ALL_PRIVILEGES")
    assert svc.has_privilege(
        "registry_rule",
        "r1",
        Privilege.MODIFY,
        role=UserRole.RULE_AUTHOR,
        principal_ids={"u1"},
        owner_email="other@x.com",
        principal_email="me@x.com",
    )
    # ...yet the caller is still a RULE_AUTHOR at the role layer, so a route
    # gated at ADMIN/RULE_APPROVER (e.g. role management, rule approval) rejects
    # them. The grant did not raise the ceiling.
    check = require_role(UserRole.ADMIN, UserRole.RULE_APPROVER).dependency
    with pytest.raises(HTTPException) as exc:
        await check(role=UserRole.RULE_AUTHOR)
    assert exc.value.status_code == 403


def test_grant_cannot_confer_manage_on_object(svc):
    # Even ALL_PRIVILEGES does not grant MANAGE (re-granting) — that stays with
    # owners / bypass roles, mirroring UC. A grant is not a role.
    fake: _FakeOltp = svc._sql  # type: ignore[assignment]
    fake.add_grant("registry_rule", "r1", "u1", "ALL_PRIVILEGES")
    assert not svc.can_manage_grants(
        "registry_rule",
        "r1",
        role=UserRole.RULE_AUTHOR,
        principal_ids={"u1"},
        owner_email="other@x.com",
        principal_email="me@x.com",
    )


# ---------------------------------------------------------------------------
# 4. Bypass is scoped to governance roles only (the deliberate upper boundary).
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("role", [UserRole.ADMIN, UserRole.RULE_APPROVER])
def test_governance_roles_bypass_object_grants(svc, role):
    # The intentional upper boundary of the ceiling: admin/approver may modify
    # even with no grant. This is preserved, not removed (item #43).
    assert svc.has_privilege(
        "registry_rule",
        "r1",
        Privilege.MODIFY,
        role=role,
        principal_ids=set(),
        owner_email="other@x.com",
        principal_email="me@x.com",
    )


@pytest.mark.parametrize("role", [UserRole.RULE_AUTHOR, UserRole.VIEWER, UserRole.RUNNER])
def test_non_governance_roles_do_not_bypass_object_grants(svc, role):
    # Without a grant and without ownership, a non-governance role is denied a
    # gated object privilege — the object layer bites for everyone below the
    # bypass line.
    assert not svc.has_privilege(
        "registry_rule",
        "r1",
        Privilege.MODIFY,
        role=role,
        principal_ids={"u1"},
        owner_email="other@x.com",
        principal_email="me@x.com",
    )


def test_object_type_values_unchanged(svc):
    # Guard the object-grant surface: the grantable object types are exactly the
    # three securables — a grant can never target a role or a route.
    assert {ot.value for ot in ObjectType} == {"registry_rule", "monitored_table", "data_product"}
