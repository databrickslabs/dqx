"""Rules Registry fingerprint (Phase 2A — task 2.2).

Mirrors the core DQX fingerprint approach
(:func:`databricks.labs.dqx.rule.compute_rule_fingerprint`): normalize the
canonical shape, dump it as sorted-key JSON, and SHA-256 it. Registry rules
have a different canonical shape than a materialized check dict though — a
registry rule is *table-agnostic*, so the fingerprint is computed over
``(mode, definition body, slots, parameters, polarity)`` only.

Descriptive tags (name/description/dimension/severity/free-text), lifecycle
fields (status, version, owner, is_builtin, audit timestamps) are
deliberately excluded: two rules that do the exact same thing but carry
different tags or are at different lifecycle stages must fingerprint
identically, so ``RegistryService`` (a later phase) can warn on true
duplicates regardless of who authored them or what they're called.
"""

import hashlib
import json
from typing import Any

from databricks.labs.dqx.utils import normalize_bound_args

from .registry_canonical import canonical_parameters, canonical_polarity
from .registry_models import RegistryRule, RuleSlot

__all__ = ["compute_registry_rule_fingerprint"]


def compute_registry_rule_fingerprint(rule: RegistryRule) -> str:
    """Compute a deterministic SHA-256 dedup fingerprint for *rule*.

    Order-independent: slots and parameters are sorted by name before
    hashing, so declaring the same slots/params in a different order
    produces the same fingerprint. A slot's ``position`` (display-only
    ordering) is excluded for the same reason. Polarity and parameters are
    compared in their behaviour-equivalent form (``registry_canonical``).
    ``definition.filter`` is a *default* population that applied rules can
    override, so it is excluded — two rules that differ only by filter
    fingerprint identically.

    Args:
        rule: The registry rule to fingerprint.

    Returns:
        A hex-encoded SHA-256 hash string.
    """
    fingerprint_data = {
        "mode": rule.mode,
        "polarity": canonical_polarity(rule.polarity),
        "body": _normalize(rule.definition.body),
        "slots": sorted(
            (_normalize_slot(slot) for slot in rule.definition.slots),
            key=lambda s: s["name"],
        ),
        "parameters": [
            {**param, "value": _normalize(param["value"])} for param in canonical_parameters(rule.mode, rule.definition)
        ],
    }
    combined = json.dumps(fingerprint_data, sort_keys=True)
    return hashlib.sha256(combined.encode()).hexdigest()


def _normalize(value: Any) -> Any:
    """Recursively normalize a value using the core DQX normalizer.

    ``allow_simple_expressions_only=False`` because this is used for
    fingerprinting/dedup only, not round-trip storage — mirrors
    ``compute_rule_fingerprint``'s own use of the normalizer.
    """
    return normalize_bound_args(value, allow_simple_expressions_only=False)


def _normalize_slot(slot: RuleSlot) -> dict[str, Any]:
    return {
        "name": slot.name,
        "family": slot.family,
        "cardinality": slot.cardinality,
    }


