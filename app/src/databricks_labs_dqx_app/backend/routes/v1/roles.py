"""Role management endpoints (Admin only).

Allows admins to manage role-to-group mappings for RBAC.
"""

from typing import Annotated

from databricks.sdk import WorkspaceClient
from fastapi import APIRouter, Depends, HTTPException

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.dependencies import (
    get_obo_ws,
    get_sp_ws,
    get_role_service,
    require_role,
)
from databricks_labs_dqx_app.backend.logger import logger
from databricks_labs_dqx_app.backend.models import (
    CreateRoleMappingIn,
    GroupOut,
    RoleMappingOut,
)
from databricks_labs_dqx_app.backend.services.role_service import RoleService

router = APIRouter(dependencies=[require_role(UserRole.ADMIN)])


def _mapping_to_out(mapping) -> RoleMappingOut:
    return RoleMappingOut(
        role=mapping.role,
        group_name=mapping.group_name,
        created_by=mapping.created_by,
        created_at=mapping.created_at.isoformat() if mapping.created_at else None,
        updated_by=mapping.updated_by,
        updated_at=mapping.updated_at.isoformat() if mapping.updated_at else None,
    )


@router.get("", response_model=list[RoleMappingOut], operation_id="listRoleMappings")
def list_role_mappings(
    svc: Annotated[RoleService, Depends(get_role_service)],
) -> list[RoleMappingOut]:
    """List all role-to-group mappings (Admin only)."""
    try:
        mappings = svc.list_mappings()
        return [_mapping_to_out(m) for m in mappings]
    except Exception as e:
        logger.error(f"Failed to list role mappings: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to list role mappings: {e}")


@router.post("", response_model=RoleMappingOut, operation_id="createRoleMapping")
def create_role_mapping(
    body: CreateRoleMappingIn,
    svc: Annotated[RoleService, Depends(get_role_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> RoleMappingOut:
    """Create or update a role-to-group mapping (Admin only)."""
    try:
        user = obo_ws.current_user.me()
        user_email = user.user_name or "unknown"
        mapping = svc.create_mapping(body.role, body.group_name, user_email)
        return _mapping_to_out(mapping)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to create role mapping: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to create role mapping: {e}")


@router.delete("/{role}/{group_name}", operation_id="deleteRoleMapping")
def delete_role_mapping(
    role: str,
    group_name: str,
    svc: Annotated[RoleService, Depends(get_role_service)],
) -> dict[str, str]:
    """Delete a role-to-group mapping (Admin only)."""
    valid_roles = {r.value for r in UserRole}
    if role not in valid_roles:
        raise HTTPException(status_code=400, detail=f"Invalid role: {role}. Must be one of {sorted(valid_roles)}")

    try:
        svc.delete_mapping(role, group_name)
        return {"status": "deleted", "role": role, "group_name": group_name}
    except Exception as e:
        logger.error(f"Failed to delete role mapping: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to delete role mapping: {e}")


@router.get("/groups", response_model=list[GroupOut], operation_id="listWorkspaceGroups")
def list_workspace_groups(
    sp_ws: Annotated[WorkspaceClient, Depends(get_sp_ws)],
) -> list[GroupOut]:
    """List available Databricks workspace groups (Admin only).

    Uses the SP client which has full SCIM access without user-scope restrictions.
    """
    try:
        groups = sp_ws.groups.list()
        return [GroupOut(display_name=g.display_name or "", id=g.id) for g in groups if g.display_name]
    except Exception as e:
        logger.error(f"Failed to list workspace groups: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to list workspace groups: {e}")


@router.get("/available-roles", response_model=list[str], operation_id="listAvailableRoles")
def list_available_roles() -> list[str]:
    """List all available role names that can be assigned (Admin only)."""
    return [r.value for r in UserRole]
