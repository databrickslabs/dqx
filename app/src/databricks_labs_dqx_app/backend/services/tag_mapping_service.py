"""Pure tag→column mapping resolver for the apply-on-tag feature.

Given a rule's declared slots (each with a family) and its slot→tags map, plus a
table's columns (each with a UC type and its governed tags), compute the valid
slot→column mapping GROUPS. A column matches a slot when it is family-compatible
AND carries at least one of the slot's declared governed tags (any-overlap; bare
key matches any value, ``key=value`` requires an exact value). Any governed tag
key is eligible — there is no namespace restriction. A group is valid only when
every slot is filled; the full result is the Cartesian product across slots (no
column reused within one group).

No I/O — unit-testable in isolation. The orchestrating reads/writes live in
``tag_reconcile_service``.
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass, field

from databricks_labs_dqx_app.backend.registry_models import ColumnMappingGroup, RuleSlot

_TYPE_FAMILY: dict[str, str] = {
    "TINYINT": "numeric", "SMALLINT": "numeric", "INT": "numeric", "INTEGER": "numeric",
    "BIGINT": "numeric", "LONG": "numeric", "FLOAT": "numeric", "DOUBLE": "numeric", "DECIMAL": "numeric",
    "STRING": "text", "VARCHAR": "text", "CHAR": "text",
    "DATE": "temporal", "TIMESTAMP": "temporal", "TIMESTAMP_NTZ": "temporal",
    "BOOLEAN": "boolean", "ARRAY": "array",
}


def family_for_type(type_name: str) -> str:
    """Classify a UC column *type_name* into a registry slot family."""
    head = (type_name or "").upper().split("(")[0].split("<")[0].strip()
    return _TYPE_FAMILY.get(head, "any")


def parse_tag(tag: str) -> tuple[str, str | None]:
    """Split a tag string into ``(key, value|None)``.

    Example: ``"class.x=v"`` → ``("class.x", "v")``;
    ``"class.x"`` → ``("class.x", None)``.
    """
    key, sep, value = tag.partition("=")
    return (key, value if sep else None)


@dataclass
class ColumnInfo:
    """One candidate column with its UC type and raw tag strings."""

    name: str
    type_name: str
    tags: list[str] = field(default_factory=list)


def _parse_tags(tags: list[str]) -> list[tuple[str, str | None]]:
    return [parse_tag(t) for t in tags]


def _column_matches(col: ColumnInfo, slot: RuleSlot, slot_tags: list[str]) -> bool:
    if slot.family != "any" and family_for_type(col.type_name) != slot.family:
        return False
    col_tags = _parse_tags(col.tags)
    for want in slot_tags:
        want_key, want_val = parse_tag(want)
        for have_key, have_val in col_tags:
            if have_key != want_key:
                continue
            if want_val is None or want_val == have_val:
                return True
    return False


def resolve(
    slots: list[RuleSlot],
    slot_tags: dict[str, list[str]],
    columns: list[ColumnInfo],
    *,
    single: bool = False,
) -> list[ColumnMappingGroup]:
    """Return valid slot→column mapping groups (see module docstring).

    Args:
        slots: ordered rule slots (position determines group ordering).
        slot_tags: mapping from slot name to its declared governed tags.
        columns: candidate columns with type and tags.
        single: when *True*, return at most one representative group.

    Returns:
        List of mapping groups; each group maps slot name → column name.
        Empty list when no valid assignment exists.
    """
    if not slots:
        return []
    ordered = sorted(slots, key=lambda s: s.position)
    sorted_cols = sorted(columns, key=lambda c: c.name)
    per_slot: list[list[str]] = []
    for slot in ordered:
        tags = slot_tags.get(slot.name, [])
        if not tags:
            return []  # a slot with no declared tags can never be filled by this feature
        matches = [c.name for c in sorted_cols if _column_matches(c, slot, tags)]
        if not matches:
            return []
        per_slot.append(matches)

    groups: list[ColumnMappingGroup] = []
    for combo in itertools.product(*per_slot):
        if len(set(combo)) != len(combo):
            continue  # a column can't fill two slots of one group
        groups.append({slot.name: col for slot, col in zip(ordered, combo)})
        if single:
            break
    return groups
