"""Rule fingerprint functions for deterministic hashing of DQ rules and rule sets."""

import hashlib
import json
from collections.abc import Callable

from databricks.labs.dqx.checks_serializer import deserialize_checks
from databricks.labs.dqx.rule import DQRule, compute_rule_fingerprint

__all__ = [
    "compute_rule_fingerprint",
    "compute_rule_set_fingerprint",
    "compute_rule_set_fingerprint_by_metadata",
]


def compute_rule_set_fingerprint(checks: list[DQRule]) -> str:
    """Compute a deterministic SHA-256 hash of the complete rule set.

    The hash is order-independent: individual rule fingerprints are sorted before combining.
    Expects expanded rules (for_each_column already expanded via deserialization).

    Args:
        checks: List of DQRule objects (expanded form).

    Returns:
        A hex-encoded SHA-256 hash string representing the entire rule set.
    """
    fingerprints = sorted(r.rule_fingerprint for r in checks)
    combined = json.dumps(fingerprints, sort_keys=True)
    return hashlib.sha256(combined.encode()).hexdigest()


def compute_rule_set_fingerprint_by_metadata(
    checks: list[dict], custom_checks: dict[str, Callable] | None = None
) -> str:
    """Compute rule set fingerprint from metadata. Thin wrapper: deserialize then fingerprint.

    Ensures for_each_column is expanded via deserialization so compact and expanded
    metadata produce the same fingerprint.

    Args:
        checks: List of check dictionaries (may contain for_each_column).
        custom_checks: Optional mapping of custom function names to callables.

    Returns:
        A hex-encoded SHA-256 hash string representing the entire rule set.
    """
    rules = deserialize_checks(checks, custom_checks)
    return compute_rule_set_fingerprint(rules)
