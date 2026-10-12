"""Helpers for additive workspace ACL checks and grants."""

import logging
from collections.abc import Iterable, Mapping, Sequence
from typing import Literal

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import AccessControlRequest, PermissionLevel

AccessStatus = Literal["granted", "missing", "unknown"]

GENIE_RUN_LEVELS = frozenset({"CAN_RUN", "CAN_EDIT", "CAN_MANAGE", "IS_OWNER"})
logger = logging.getLogger(__name__)


def group_levels(entries: Iterable[object]) -> dict[str, frozenset[str]]:
    """Map each ACL principal (casefolded) to its permission level names.

    Args:
        entries: Access control entries from any Databricks permissions API.

    Returns:
        Principal name to upper-case permission levels.
    """
    levels: dict[str, set[str]] = {}
    for entry in entries:
        principal = (
            getattr(entry, "group_name", None)
            or getattr(entry, "service_principal_name", None)
            or getattr(entry, "user_name", None)
        )
        if not isinstance(principal, str) or not principal.strip():
            continue
        names = levels.setdefault(principal.strip().casefold(), set())
        for permission in getattr(entry, "all_permissions", None) or []:
            level = getattr(permission, "permission_level", None)
            value = getattr(level, "value", level)
            if isinstance(value, str):
                names.add(value.upper())
    return {principal: frozenset(names) for principal, names in levels.items()}


def missing_principals(
    levels: Mapping[str, frozenset[str]],
    principals: Sequence[str],
    sufficient: frozenset[str],
) -> tuple[str, ...]:
    """Return the principals that hold none of the sufficient levels, in input order.

    Args:
        levels: Casefolded principal to permission levels, from *group_levels*.
        principals: Principals to verify.
        sufficient: Levels that satisfy the requirement.

    Returns:
        The principals lacking any sufficient level.
    """
    return tuple(
        principal for principal in principals if not levels.get(principal.casefold(), frozenset()) & sufficient
    )


def reconcile_group_acl(
    writer: WorkspaceClient,
    object_type: str,
    object_id: str,
    principals: Sequence[str],
    level: PermissionLevel,
    sufficient: frozenset[str],
    *,
    readers: Sequence[WorkspaceClient] = (),
) -> tuple[AccessStatus, tuple[str, ...]]:
    """Grant *level* to the groups lacking a sufficient level, then re-read the ACL.

    The update is additive (never replaces the ACL) and best effort; the re-read
    state decides the result.

    Args:
        writer: Client that applies the update, normally the app service principal.
        object_type: Permissions API object type, for example *genie* or *dashboards*.
        object_id: Object to update.
        principals: Workspace groups that need access.
        level: Level to grant.
        sufficient: Levels that already satisfy the requirement.
        readers: Clients tried in order to read the ACL; defaults to *writer*.

    Returns:
        *granted* with no groups, *missing* with the groups still lacking access, or
        *unknown* when the ACL could not be read.
    """
    clients = tuple(readers) or (writer,)
    absent = _missing_groups(clients, object_type, object_id, principals, sufficient)
    if absent is None:
        return "unknown", ()
    if not absent:
        return "granted", ()
    try:
        writer.permissions.update(
            object_type,
            object_id,
            access_control_list=[AccessControlRequest(group_name=group, permission_level=level) for group in absent],
        )
    except Exception:
        logger.warning("Could not share a Studio resource with its users; verifying access instead.")
    absent = _missing_groups(clients, object_type, object_id, principals, sufficient)
    if absent is None:
        return "unknown", ()
    return ("missing", absent) if absent else ("granted", ())


def _missing_groups(
    clients: Sequence[WorkspaceClient],
    object_type: str,
    object_id: str,
    principals: Sequence[str],
    sufficient: frozenset[str],
) -> tuple[str, ...] | None:
    for client in clients:
        try:
            permissions = client.permissions.get(object_type, object_id)
        except Exception:
            continue
        return missing_principals(group_levels(permissions.access_control_list or []), principals, sufficient)
    return None
