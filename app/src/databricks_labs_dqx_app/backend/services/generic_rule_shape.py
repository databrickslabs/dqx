"""Structural identity for reusable registry rules, independent of slot names.

Two rules that differ only in what their column placeholders are called
(``is_not_null`` on ``{{order_id}}`` vs ``{{value}}``) are the same generic
rule. The exact fingerprint (``registry_fingerprint``) hashes slot names, so
it treats them as distinct; the *shape key* here renames slots positionally
before hashing so imports can reuse the existing generic rule instead of
minting one copy per column.
"""

import hashlib
import json
import re
from typing import Any

from databricks_labs_dqx_app.backend.registry_models import RuleDefinition, RuleSlot

# Modes whose body references slots only through ``{{slot}}`` placeholders, so
# renaming a slot is a safe textual rewrite. Low-code bodies carry an AST that
# may name slots structurally and are left alone.
_RENAMEABLE_MODES = frozenset({"dqx_native", "sql"})
_PLACEHOLDER = re.compile(r"\{\{\s*([^{}\s]+)\s*\}\}")


def _ordered_slots(slots: list[RuleSlot]) -> list[RuleSlot]:
    return [s for _, s in sorted(enumerate(slots), key=lambda pair: (pair[1].position, pair[0]))]


def _rename_placeholders(value: Any, renames: dict[str, str]) -> Any:
    if isinstance(value, str):
        # Single pass so swaps (a -> b, b -> a) don't clobber each other.
        return _PLACEHOLDER.sub(lambda m: "{{" + renames.get(m.group(1), m.group(1)) + "}}", value)
    if isinstance(value, dict):
        return {k: _rename_placeholders(v, renames) for k, v in value.items()}
    if isinstance(value, list):
        return [_rename_placeholders(v, renames) for v in value]
    return value


def rename_slots(definition: RuleDefinition, renames: dict[str, str]) -> RuleDefinition:
    """Copy of *definition* with slots renamed per *renames* (old -> new), body included."""
    renamed = definition.model_copy(deep=True)
    renamed.body = _rename_placeholders(renamed.body, renames)
    for slot in renamed.slots:
        slot.name = renames.get(slot.name, slot.name)
    return renamed


def shape_key(mode: str, definition: RuleDefinition, polarity: str | None) -> str:
    """Hash of the rule's structure with slots renamed to their position."""
    ordered = _ordered_slots(definition.slots)
    positional = {s.name: f"__slot_{i}" for i, s in enumerate(ordered)}
    body = _rename_placeholders(definition.body, positional) if mode in _RENAMEABLE_MODES else definition.body
    payload = {
        "mode": mode,
        "polarity": polarity,
        "body": body,
        "slots": (
            [positional[s.name] + ":" + s.cardinality for s in ordered]
            if mode in _RENAMEABLE_MODES
            else [s.name + ":" + s.cardinality for s in ordered]
        ),
        "parameters": sorted(
            ({"name": p.name, "type": p.type, "value": p.value} for p in definition.parameters),
            key=lambda p: p["name"],
        ),
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()


def slot_renames_between(source: RuleDefinition, target: RuleDefinition) -> dict[str, str]:
    """Map each *source* slot name to the *target* slot in the same position."""
    return {s.name: t.name for s, t in zip(_ordered_slots(source.slots), _ordered_slots(target.slots))}


def canonical_slot_names(count: int) -> list[str]:
    """The position-neutral slot names *generalize* assigns to a rule with *count* slots.

    A single slot is simply ``column``; multiple slots are ``column_1`` …
    ``column_N``. This is the single source of truth for the generalized naming
    scheme: both *generalize* (which assigns them) and *has_generalized_slots*
    (which recognizes them) derive from it, so the producer and recognizer can
    never drift — a mismatch previously excluded generalized rules with 3+ slots
    from cross-import reuse.
    """
    if count == 1:
        return ["column"]
    return [f"column_{i + 1}" for i in range(count)]


def generalize(mode: str, definition: RuleDefinition) -> tuple[RuleDefinition, dict[str, str]]:
    """Rename column-specific slots to neutral ones (``column`` / ``column_1``…).

    Returns the new definition and the old -> new slot renames. Modes whose
    slots can't be safely renamed are returned unchanged.
    """
    if mode not in _RENAMEABLE_MODES or not definition.slots:
        return definition, {}
    ordered = _ordered_slots(definition.slots)
    renames = {slot.name: name for slot, name in zip(ordered, canonical_slot_names(len(ordered)))}
    return rename_slots(definition, renames), renames


def has_generalized_slots(definition: RuleDefinition) -> bool:
    """True if *definition*'s slots are already in the exact form *generalize* would produce.

    A generalized import rule is reusable across re-imports — any column maps
    onto its ``column`` / ``column_N`` slots — whereas a rule still carrying
    column-specific slot names is not. Compared positionally against
    *canonical_slot_names*, so the recognizer can never fall out of step with the
    names *generalize* assigns.
    """
    ordered = _ordered_slots(definition.slots)
    return [slot.name for slot in ordered] == canonical_slot_names(len(ordered))


def generic_name(mode: str, definition: RuleDefinition) -> str | None:
    """Readable name for a generalized native rule, e.g. ``Is not less than (limit: 0)``."""
    if mode != "dqx_native":
        return None
    function = str(definition.body.get("function") or "").strip()
    if not function:
        return None
    name = function.replace("_", " ").capitalize()
    params = [f"{p.name}: {p.value}" for p in definition.parameters if p.value not in (None, "", [])]
    return f"{name} ({', '.join(params)})" if params else name
