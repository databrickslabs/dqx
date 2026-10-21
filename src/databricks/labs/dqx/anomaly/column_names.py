"""Allocate transient anomaly columns without replacing source fields."""

import re
import uuid


def names_resolve_same(left: str, right: str, *, case_sensitive: bool = False) -> bool:
    """Compare literal identifiers without expanding Unicode letters into multiple characters."""
    if case_sensitive:
        return left == right
    return re.fullmatch(re.escape(left), right, flags=re.IGNORECASE) is not None


def reserve_temporary_column(preferred: str, occupied_names: set[str]) -> str:
    """Reserve a name against case-insensitive input and generated names.

    The supplied set is updated so successive allocations cannot collide. On a
    conflict, use DQX's existing UUID-suffix convention for temporary columns.
    Names are transient and never persisted.
    """
    candidate = preferred
    while any(names_resolve_same(candidate, name) for name in occupied_names):
        candidate = f"{preferred}_{uuid.uuid4().hex}"
    occupied_names.add(candidate)
    return candidate
