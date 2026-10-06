"""Pure helpers for additive workspace ACL checks."""

from collections.abc import Iterable, Mapping, Sequence
from typing import Literal

AccessStatus = Literal["granted", "missing", "unknown"]


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
