import logging
from enum import StrEnum
from typing import Annotated

from fastapi import Depends, Header, HTTPException, status

logger = logging.getLogger(__name__)


class UserRole(StrEnum):
    ADMIN = "admin"
    RULE_APPROVER = "rule_approver"
    RULE_AUTHOR = "rule_author"
    VIEWER = "viewer"


# Role hierarchy for resolution (higher index = higher priority)
ROLE_PRIORITY: list[UserRole] = [
    UserRole.VIEWER,
    UserRole.RULE_AUTHOR,
    UserRole.RULE_APPROVER,
    UserRole.ADMIN,
]

# Permissions granted to each role
PERMISSIONS: dict[UserRole, list[str]] = {
    UserRole.ADMIN: [
        "view_rules",
        "create_rules",
        "edit_rules",
        "generate_rules",
        "submit_rules",
        "approve_rules",
        "export_rules",
        "configure_storage",
        "manage_roles",
    ],
    UserRole.RULE_APPROVER: [
        "view_rules",
        "create_rules",
        "edit_rules",
        "generate_rules",
        "submit_rules",
        "approve_rules",
        "export_rules",
        "configure_storage",
    ],
    UserRole.RULE_AUTHOR: [
        "view_rules",
        "create_rules",
        "edit_rules",
        "generate_rules",
        "submit_rules",
    ],
    UserRole.VIEWER: [
        "view_rules",
    ],
}


def get_permissions_for_role(role: UserRole) -> list[str]:
    """Return the list of permissions granted to a role."""
    return PERMISSIONS.get(role, [])


def get_user_email(
    x_forwarded_email: Annotated[str | None, Header()] = None,
    x_forwarded_user: Annotated[str | None, Header()] = None,
) -> str:
    email = x_forwarded_email or x_forwarded_user
    if not email:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User identity not found. Missing X-Forwarded-Email or X-Forwarded-User header.",
        )
    return email


CurrentUser = Annotated[str, Depends(get_user_email)]

# Note: get_user_role, require_role, and CurrentUserRole are defined in
# dependencies.py to avoid circular imports (they need RoleService).
