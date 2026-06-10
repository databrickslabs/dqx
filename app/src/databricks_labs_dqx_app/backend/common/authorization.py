import logging
from enum import Enum
from typing import Annotated

from fastapi import Depends, Header, HTTPException, status

logger = logging.getLogger(__name__)


class UserRole(str, Enum):
    ADMIN = "admin"
    RULE_APPROVER = "rule_approver"
    RULE_AUTHOR = "rule_author"
    VIEWER = "viewer"
    # ``RUNNER`` is an *orthogonal* (additive) role rather than a hierarchy
    # rank. A user's primary role (the one that gates rule authoring,
    # approving, etc.) is still resolved from the priority list below; the
    # runner role is resolved independently and only governs the "Run
    # Rules" page (manual execution + schedule list view). Admins are
    # implicit runners without needing an explicit mapping.
    RUNNER = "runner"


# Role hierarchy for primary-role resolution (higher index = higher
# priority). RUNNER is intentionally *not* on this list — assigning RUNNER
# alone leaves the user's primary role at VIEWER, but unlocks the Run
# Rules page via the separate ``run_rules`` permission below.
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
        # Admins implicitly inherit the runner permission so they never
        # need an explicit RUNNER group assignment.
        "run_rules",
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
    # RUNNER carries only ``run_rules`` — nothing else. The orthogonal
    # nature is what the user requested: assigning RUNNER must not silently
    # promote a viewer/author into an approver or vice-versa.
    UserRole.RUNNER: [
        "run_rules",
    ],
}


def get_permissions_for_role(role: UserRole) -> list[str]:
    """Return the list of permissions granted to a role."""
    return PERMISSIONS.get(role, [])


def get_user_email(
    x_forwarded_email: Annotated[str | None, Header()] = None,
    x_forwarded_access_token: Annotated[str | None, Header()] = None,
) -> str:
    """Resolve the authenticated user's email.

    Primary source is the platform-verified X-Forwarded-Email header.
    Fallback derives the email from the OBO access token via the SCIM API,
    which is needed for local dev where the proxy only injects the token.
    """
    if x_forwarded_email:
        return x_forwarded_email

    if x_forwarded_access_token:
        try:
            from databricks.sdk import WorkspaceClient

            ws = WorkspaceClient(token=x_forwarded_access_token, auth_type="pat")
            me = ws.current_user.me()
            if me.user_name:
                logger.debug("Resolved user email from OBO token: %s", me.user_name)
                return me.user_name
        except Exception:
            logger.warning("Failed to resolve user email from OBO token", exc_info=True)

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="User identity not found. Missing X-Forwarded-Email header.",
    )


CurrentUser = Annotated[str, Depends(get_user_email)]

# Note: get_user_role, require_role, and CurrentUserRole are defined in
# dependencies.py to avoid circular imports (they need RoleService).
