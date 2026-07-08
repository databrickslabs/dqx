"""Object-permissions routes — UC-style grants on rules / monitored tables /
table spaces, plus the principal-search-backed Permissions tab (P22-D item 10).

Endpoints:
* ``GET  /permissions/{object_type}/{object_id}/grants``     — list grants (direct + inherited) + baseline + capability
* ``PUT  /permissions/{object_type}/{object_id}/grants``     — create/replace one principal's grant
* ``DELETE /permissions/{object_type}/{object_id}/grants/{principal_id}`` — remove a grant
* ``GET  /permissions/{object_type}/{object_id}/effective``  — the caller's effective privileges (drives UI gating)
* ``GET  /permissions/default-inherit`` / ``PUT`` (admin)    — default per-grant inheritance setting

Roles remain the coarse gate; grant *mutations* additionally require the
caller to own the object or hold an admin/approver role (see
``PermissionsService.can_manage_grants``).
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status

from databricks_labs_dqx_app.backend.common.authorization import CurrentUser, UserRole
from databricks_labs_dqx_app.backend.common.permissions import (
    BASELINE_PRIVILEGES,
    ObjectType,
    Privilege,
    serialize_privileges,
)
from databricks_labs_dqx_app.backend.dependencies import (
    CurrentPrincipalIds,
    CurrentUserRole,
    get_permissions_service,
    require_role,
)
from databricks_labs_dqx_app.backend.models import (
    EffectivePermissionsOut,
    ObjectGrantOut,
    ObjectGrantsOut,
    PermissionsDefaultInheritOut,
    SetObjectGrantIn,
    SetPermissionsDefaultInheritIn,
)
from databricks_labs_dqx_app.backend.services.permissions_service import ObjectGrant, PermissionsService

router = APIRouter()

_ALL_ROLES = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR, UserRole.VIEWER]
_AUTHORS_AND_ABOVE = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR]


def _validate_object_type(object_type: str) -> str:
    try:
        return ObjectType(object_type).value
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Unknown object type.") from exc


def _parse_privileges(raw: list[str]) -> set[Privilege]:
    out: set[Privilege] = set()
    for token in raw:
        try:
            out.add(Privilege(token))
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Unknown privilege.") from exc
    return out


def _to_out(grant: ObjectGrant) -> ObjectGrantOut:
    return ObjectGrantOut(
        principal_id=grant.principal_id,
        principal_type=grant.principal_type,
        principal_name=grant.principal_name,
        privileges=sorted(p.value for p in grant.privileges),
        inherit=grant.inherit,
        grantor=grant.grantor,
        updated_at=grant.updated_at.isoformat() if grant.updated_at else None,
        inherited=grant.inherited_from_type is not None,
        inherited_from_type=grant.inherited_from_type,
        inherited_from_id=grant.inherited_from_id,
    )


@router.get(
    "/default-inherit",
    response_model=PermissionsDefaultInheritOut,
    operation_id="getPermissionsDefaultInherit",
    dependencies=[require_role(*_ALL_ROLES)],
)
def get_default_inherit(
    perms: Annotated[PermissionsService, Depends(get_permissions_service)],
) -> PermissionsDefaultInheritOut:
    """Return the admin default for the per-grant inheritance toggle."""
    return PermissionsDefaultInheritOut(enabled=perms.get_default_inherit())


@router.put(
    "/default-inherit",
    response_model=PermissionsDefaultInheritOut,
    operation_id="setPermissionsDefaultInherit",
    dependencies=[require_role(UserRole.ADMIN)],
)
def set_default_inherit(
    body: SetPermissionsDefaultInheritIn,
    user: CurrentUser,
    perms: Annotated[PermissionsService, Depends(get_permissions_service)],
) -> PermissionsDefaultInheritOut:
    """Set the admin default for the per-grant inheritance toggle (admin only)."""
    saved = perms.set_default_inherit(body.enabled, user_email=user)
    return PermissionsDefaultInheritOut(enabled=saved)


@router.get(
    "/{object_type}/{object_id}/grants",
    response_model=ObjectGrantsOut,
    operation_id="listObjectGrants",
    dependencies=[require_role(*_ALL_ROLES)],
)
def list_object_grants(
    object_type: str,
    object_id: str,
    user: CurrentUser,
    role: CurrentUserRole,
    principal_ids: CurrentPrincipalIds,
    perms: Annotated[PermissionsService, Depends(get_permissions_service)],
) -> ObjectGrantsOut:
    """List the grants on an object (direct + inherited) with baseline + capability."""
    ot = _validate_object_type(object_type)
    owner = perms.get_object_owner(ot, object_id)
    grants = [_to_out(g) for g in perms.list_effective_grants(ot, object_id)]
    can_manage = perms.can_manage_grants(
        ot, object_id, role=role, principal_ids=set(principal_ids), owner_email=owner, principal_email=user
    )
    return ObjectGrantsOut(
        object_type=ot,
        object_id=object_id,
        grants=grants,
        baseline_privileges=serialize_privileges(set(BASELINE_PRIVILEGES)).split(","),
        can_manage=can_manage,
        default_inherit=perms.get_default_inherit(),
    )


@router.put(
    "/{object_type}/{object_id}/grants",
    response_model=ObjectGrantsOut,
    operation_id="setObjectGrant",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def set_object_grant(
    object_type: str,
    object_id: str,
    body: SetObjectGrantIn,
    user: CurrentUser,
    role: CurrentUserRole,
    principal_ids: CurrentPrincipalIds,
    perms: Annotated[PermissionsService, Depends(get_permissions_service)],
) -> ObjectGrantsOut:
    """Create or replace one principal's grant on an object.

    Requires the caller to own the object or hold an admin/approver role.
    """
    ot = _validate_object_type(object_type)
    owner = perms.get_object_owner(ot, object_id)
    if not perms.can_manage_grants(
        ot, object_id, role=role, principal_ids=set(principal_ids), owner_email=owner, principal_email=user
    ):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You must own this object (or be an admin/approver) to change its permissions.",
        )
    perms.set_grant(
        ot,
        object_id,
        body.principal_id,
        principal_type=body.principal_type,
        principal_name=body.principal_name,
        privileges=_parse_privileges(body.privileges),
        inherit=body.inherit,
        grantor=user,
    )
    return list_object_grants(object_type, object_id, user, role, principal_ids, perms)


@router.delete(
    "/{object_type}/{object_id}/grants/{principal_id}",
    response_model=ObjectGrantsOut,
    operation_id="removeObjectGrant",
    dependencies=[require_role(*_AUTHORS_AND_ABOVE)],
)
def remove_object_grant(
    object_type: str,
    object_id: str,
    principal_id: str,
    user: CurrentUser,
    role: CurrentUserRole,
    principal_ids: CurrentPrincipalIds,
    perms: Annotated[PermissionsService, Depends(get_permissions_service)],
) -> ObjectGrantsOut:
    """Remove a principal's grant from an object (owner/admin/approver only)."""
    ot = _validate_object_type(object_type)
    owner = perms.get_object_owner(ot, object_id)
    if not perms.can_manage_grants(
        ot, object_id, role=role, principal_ids=set(principal_ids), owner_email=owner, principal_email=user
    ):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You must own this object (or be an admin/approver) to change its permissions.",
        )
    perms.remove_grant(ot, object_id, principal_id, actor=user)
    return list_object_grants(object_type, object_id, user, role, principal_ids, perms)


@router.get(
    "/{object_type}/{object_id}/effective",
    response_model=EffectivePermissionsOut,
    operation_id="getEffectivePermissions",
    dependencies=[require_role(*_ALL_ROLES)],
)
def get_effective_permissions(
    object_type: str,
    object_id: str,
    user: CurrentUser,
    role: CurrentUserRole,
    principal_ids: CurrentPrincipalIds,
    perms: Annotated[PermissionsService, Depends(get_permissions_service)],
) -> EffectivePermissionsOut:
    """Return the caller's effective privileges on an object (drives UI gating)."""
    ot = _validate_object_type(object_type)
    owner = perms.get_object_owner(ot, object_id)
    pid_set = set(principal_ids)
    can_modify = perms.has_privilege(
        ot, object_id, Privilege.MODIFY, role=role, principal_ids=pid_set, owner_email=owner, principal_email=user
    )
    can_apply = perms.has_privilege(
        ot, object_id, Privilege.APPLY, role=role, principal_ids=pid_set, owner_email=owner, principal_email=user
    )
    can_manage = perms.can_manage_grants(
        ot, object_id, role=role, principal_ids=pid_set, owner_email=owner, principal_email=user
    )
    is_owner = bool(owner and owner.strip().lower() == user.strip().lower())
    privileges: list[str] = []
    if can_modify:
        privileges.append(Privilege.MODIFY.value)
    if can_apply:
        privileges.append(Privilege.APPLY.value)
    return EffectivePermissionsOut(
        object_type=ot,
        object_id=object_id,
        privileges=privileges,
        can_modify=can_modify,
        can_apply=can_apply,
        can_manage_grants=can_manage,
        is_owner=is_owner,
    )
