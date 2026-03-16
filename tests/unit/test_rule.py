"""Unit tests for rule fingerprinting, expansion, and serialization alignment."""

import re

from databricks.labs.dqx.check_funcs import is_not_null
from databricks.labs.dqx.checks_serializer import compute_rule_set_fingerprint
from databricks.labs.dqx.rule import DQRowRule, compute_rule_fingerprint


def _hex_sha256_pattern() -> re.Pattern[str]:
    return re.compile(r"^[a-f0-9]{64}$")


def test_compute_rule_fingerprint_same_rule_same_fingerprint():
    """Same rule definition produces the same fingerprint (determinism)."""
    check = {
        "name": "id_not_null",
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "id"}},
    }
    fp1 = compute_rule_fingerprint(check)
    fp2 = compute_rule_fingerprint(check)
    assert fp1 == fp2
    assert _hex_sha256_pattern().match(fp1)


def test_compute_rule_set_fingerprint_same_set_same_fingerprint():
    """Same rule set produces the same rule_set_fingerprint (determinism)."""
    checks = [
        {"name": "a", "criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "id"}}},
        {"name": "b", "criticality": "warn", "check": {"function": "is_not_null", "arguments": {"column": "name"}}},
    ]
    assert compute_rule_set_fingerprint(checks) == compute_rule_set_fingerprint(checks)


def test_compute_rule_set_fingerprint_for_each_column_deterministic():
    """for_each_column is included in fingerprint; same compact check yields same rule_set_fingerprint."""
    checks = [
        {
            "name": "rule_fec_test",
            "criticality": "warn",
            "check": {
                "function": "is_not_null",
                "arguments": {},
                "for_each_column": ["x", "y"],
            },
        },
    ]
    assert compute_rule_set_fingerprint(checks) == compute_rule_set_fingerprint(checks)


def test_compute_rule_set_fingerprint_order_independent():
    """Rule set fingerprint is order-independent: same rules in different order give same hash."""
    rule_check_first = {
        "name": "rule_first",
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "pk"}},
    }
    rule_check_second = {
        "name": "rule_second",
        "criticality": "warn",
        "check": {"function": "is_not_null_and_not_empty", "arguments": {"column": "label"}},
    }
    fp_ab = compute_rule_set_fingerprint([rule_check_first, rule_check_second])
    fp_ba = compute_rule_set_fingerprint([rule_check_second, rule_check_first])
    assert fp_ab == fp_ba


def test_dq_rule_rule_fingerprint_equals_compute_rule_fingerprint_of_to_dict():
    """DQRule.rule_fingerprint returns the same hash as compute_rule_fingerprint(rule.to_dict())."""
    rule = DQRowRule(name="id_not_null", criticality="error", check_func=is_not_null, column="id")
    assert rule.rule_fingerprint == compute_rule_fingerprint(rule.to_dict())


def test_compute_rule_fingerprint_includes_for_each_column():
    """for_each_column is included in the fingerprint; different column sets yield different fingerprints."""
    compact_ab = [
        {"criticality": "error", "check": {"function": "is_not_null", "for_each_column": ["a", "b"], "arguments": {}}},
    ]
    compact_ac = [
        {"criticality": "error", "check": {"function": "is_not_null", "for_each_column": ["a", "c"], "arguments": {}}},
    ]
    assert compute_rule_set_fingerprint(compact_ab) != compute_rule_set_fingerprint(compact_ac)
