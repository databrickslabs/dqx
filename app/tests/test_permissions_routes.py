"""Tests for the ``/permissions`` and ``/principals`` route handlers.

Handlers are called directly with a mocked ``PermissionsService`` (same
convention as ``test_data_products_routes.py``) — no FastAPI TestClient.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.common.permissions import Privilege
from databricks_labs_dqx_app.backend.models import (
    SetObjectGrantIn,
    SetPermissionsDefaultInheritIn,
)
from databricks_labs_dqx_app.backend.routes.v1.permissions import (
    get_default_inherit,
    get_effective_permissions,
    list_object_grants,
    remove_object_grant,
    set_default_inherit,
    set_object_grant,
)
from databricks_labs_dqx_app.backend.services.permissions_service import ObjectGrant


def _perms_mock() -> MagicMock:
    m = MagicMock()
    m.get_object_owner.return_value = "owner@x.com"
    m.list_effective_grants.return_value = []
    m.can_manage_grants.return_value = True
    m.get_default_inherit.return_value = False
    return m


def test_list_object_grants_returns_grants():
    perms = _perms_mock()
    perms.list_effective_grants.return_value = [
        ObjectGrant(
            object_type="registry_rule",
            object_id="r1",
            principal_id="u1",
            principal_type="user",
            principal_name="Alice",
            privileges={Privilege.MODIFY},
            inherit=False,
        )
    ]
    out = list_object_grants("registry_rule", "r1", "me@x.com", UserRole.ADMIN, frozenset(), perms)
    assert out.object_type == "registry_rule"
    assert len(out.grants) == 1
    assert out.grants[0].principal_name == "Alice"


def test_list_object_grants_surfaces_default_flag():
    perms = _perms_mock()
    perms.list_effective_grants.return_value = [
        ObjectGrant(
            object_type="registry_rule",
            object_id="r1",
            principal_id="users",
            principal_type="group",
            principal_name="users",
            privileges={Privilege.SELECT, Privilege.APPLY},
            inherit=False,
            is_default=True,
        )
    ]
    out = list_object_grants("registry_rule", "r1", "me@x.com", UserRole.ADMIN, frozenset(), perms)
    assert out.grants[0].is_default is True
    assert out.grants[0].principal_id == "users"


def test_set_object_grant_rejects_legacy_sentinel():
    perms = _perms_mock()
    body = SetObjectGrantIn(principal_id="__all__", principal_type="group", privileges=["MODIFY"], inherit=False)
    with pytest.raises(HTTPException) as exc:
        set_object_grant("registry_rule", "r1", body, "me@x.com", UserRole.ADMIN, frozenset(), perms)
    assert exc.value.status_code == 400
    perms.set_grant.assert_not_called()


def test_list_object_grants_rejects_bad_type():
    with pytest.raises(HTTPException) as exc:
        list_object_grants("bogus", "x", "me@x.com", UserRole.ADMIN, frozenset(), _perms_mock())
    assert exc.value.status_code == 404


def test_set_object_grant_requires_manage():
    perms = _perms_mock()
    perms.can_manage_grants.return_value = False
    body = SetObjectGrantIn(principal_id="u1", principal_type="user", privileges=["MODIFY"], inherit=False)
    with pytest.raises(HTTPException) as exc:
        set_object_grant("registry_rule", "r1", body, "me@x.com", UserRole.RULE_AUTHOR, frozenset(), perms)
    assert exc.value.status_code == 403


def test_set_object_grant_calls_service_when_allowed():
    perms = _perms_mock()
    body = SetObjectGrantIn(principal_id="u1", principal_type="user", principal_name="Alice", privileges=["MODIFY"], inherit=True)
    set_object_grant("registry_rule", "r1", body, "me@x.com", UserRole.ADMIN, frozenset(), perms)
    perms.set_grant.assert_called_once()
    _, kwargs = perms.set_grant.call_args
    assert kwargs["privileges"] == {Privilege.MODIFY}
    assert kwargs["inherit"] is True


def test_set_object_grant_rejects_bad_privilege():
    perms = _perms_mock()
    body = SetObjectGrantIn(principal_id="u1", principal_type="user", privileges=["BOGUS"], inherit=False)
    with pytest.raises(HTTPException) as exc:
        set_object_grant("registry_rule", "r1", body, "me@x.com", UserRole.ADMIN, frozenset(), perms)
    assert exc.value.status_code == 400


def test_remove_object_grant_requires_manage():
    perms = _perms_mock()
    perms.can_manage_grants.return_value = False
    with pytest.raises(HTTPException) as exc:
        remove_object_grant("data_product", "p1", "u1", "me@x.com", UserRole.RULE_AUTHOR, frozenset(), perms)
    assert exc.value.status_code == 403


def test_remove_object_grant_calls_service_when_allowed():
    perms = _perms_mock()
    remove_object_grant("data_product", "p1", "u1", "me@x.com", UserRole.ADMIN, frozenset(), perms)
    perms.remove_grant.assert_called_once()


def test_effective_permissions_shape():
    perms = _perms_mock()
    perms.has_privilege.side_effect = lambda ot, oid, priv, **kw: priv == Privilege.APPLY
    perms.can_manage_grants.return_value = False
    perms.get_object_owner.return_value = "other@x.com"
    out = get_effective_permissions("monitored_table", "b1", "me@x.com", UserRole.RULE_AUTHOR, frozenset(), perms)
    assert out.can_apply is True
    assert out.can_modify is False
    assert out.is_owner is False
    assert "APPLY" in out.privileges


def test_effective_permissions_owner_flag():
    perms = _perms_mock()
    perms.has_privilege.return_value = True
    perms.get_object_owner.return_value = "Me@x.com"
    out = get_effective_permissions("registry_rule", "r1", "me@x.com", UserRole.RULE_AUTHOR, frozenset(), perms)
    assert out.is_owner is True


def test_default_inherit_get_and_set():
    perms = _perms_mock()
    perms.get_default_inherit.return_value = True
    assert get_default_inherit(perms).enabled is True
    perms.set_default_inherit.return_value = False
    out = set_default_inherit(SetPermissionsDefaultInheritIn(enabled=False), "admin@x.com", perms)
    assert out.enabled is False
    perms.set_default_inherit.assert_called_once()
