import logging
from enum import StrEnum
from typing import Annotated

from fastapi import Depends, Header, HTTPException, status

logger = logging.getLogger(__name__)


class UserRole(StrEnum):
    ADMIN = "admin"
    DATA_STEWARD = "data_steward"
    VIEWER = "viewer"


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


def get_user_role(
    email: Annotated[str, Depends(get_user_email)],
) -> UserRole:
    # Stubbed — always returns ADMIN for now.
    # Future: look up role from a permission table keyed by email.
    logger.debug(f"Resolving role for {email} — stubbed as ADMIN")
    return UserRole.ADMIN


def require_role(*roles: UserRole):
    """Dependency factory that rejects requests from users without one of the listed roles."""

    def _check(role: Annotated[UserRole, Depends(get_user_role)]) -> UserRole:
        if role not in roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Requires one of {[r.value for r in roles]}, but user has '{role.value}'.",
            )
        return role

    return Depends(_check)


CurrentUser = Annotated[str, Depends(get_user_email)]
CurrentUserRole = Annotated[UserRole, Depends(get_user_role)]
