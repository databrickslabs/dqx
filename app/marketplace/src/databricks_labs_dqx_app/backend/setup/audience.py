"""Map Studio audience and administrator groups to workspace and UC principals."""

from collections.abc import Sequence
from dataclasses import dataclass

from databricks.labs.dqx.errors import InvalidParameterError
from databricks_labs_dqx_app.backend.sanitization import replace_control_characters

BROAD_AUDIENCE = "users"
ACCOUNT_USERS = "account users"
WORKSPACE_ADMINS = "admins"


@dataclass(frozen=True)
class StudioAudience:
    """Principals that receive Studio OBO prerequisites.

    *workspace_principals* receive app, warehouse, Genie space and dashboard ACLs.
    *uc_principals* receive catalog, temporary, Genie and demo grants. The workspace
    admins group never appears in *uc_principals*: Unity Catalog cannot grant to
    workspace-local groups.
    """

    groups: tuple[str, ...]
    workspace_principals: tuple[str, ...]
    uc_principals: tuple[str, ...]
    broad: bool
    admin_uc_group: str | None


def resolve_audience(groups: Sequence[str], admin_group: str, *, allow_broad: bool) -> StudioAudience:
    """Validate audience groups and derive ACL and UC principals.

    Args:
        groups: Audience group names; *users* selects broad mode.
        admin_group: Configured Studio administrator group.
        allow_broad: Whether broad mode is permitted (deployment configuration only).

    Returns:
        The resolved audience.

    Raises:
        InvalidParameterError: If a group is unsafe, built-in, or broad mode is not allowed.
    """
    cleaned: list[str] = []
    for value in groups:
        group = _clean_group(value)
        if group.casefold() in {ACCOUNT_USERS, WORKSPACE_ADMINS}:
            raise InvalidParameterError("The Studio audience must be a dedicated group or users.")
        if group.casefold() == BROAD_AUDIENCE:
            if not allow_broad:
                raise InvalidParameterError("Broad audience mode is only available for bundle deployments.")
            group = BROAD_AUDIENCE
        if group.casefold() not in {existing.casefold() for existing in cleaned}:
            cleaned.append(group)
    if not cleaned:
        raise InvalidParameterError("A Studio audience group is required.")
    broad = BROAD_AUDIENCE in cleaned
    if broad and len(cleaned) > 1:
        raise InvalidParameterError("Broad audience mode cannot be combined with other groups.")

    admin = _clean_group(admin_group)
    admin_uc_group = None if admin.casefold() == WORKSPACE_ADMINS else admin
    workspace = [*cleaned]
    uc = [ACCOUNT_USERS] if broad else [*cleaned]
    if admin_uc_group is not None:
        if admin_uc_group.casefold() not in {item.casefold() for item in workspace}:
            workspace.append(admin_uc_group)
        if admin_uc_group.casefold() not in {item.casefold() for item in uc}:
            uc.append(admin_uc_group)
    return StudioAudience(
        groups=tuple(cleaned),
        workspace_principals=tuple(workspace),
        uc_principals=tuple(uc),
        broad=broad,
        admin_uc_group=admin_uc_group,
    )


def _clean_group(value: str) -> str:
    group = value.strip()
    if not group or "`" in group or replace_control_characters(value) != value:
        raise InvalidParameterError("Group names must be non-empty and contain no backticks or control characters.")
    return group
