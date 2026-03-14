"""Unit tests for rule.py: compute_rule_fingerprint, compute_rule_set_fingerprint, expand_checks_for_each_column."""

import re

from databricks.labs.dqx.check_funcs import is_not_null
from databricks.labs.dqx.checks_serializer import deserialize_checks
from databricks.labs.dqx.rule import (
    DQRowRule,
    compute_rule_fingerprint,
    compute_rule_set_fingerprint,
    expand_checks_for_each_column,
)


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


def test_compute_rule_set_fingerprint_for_each_column_same_as_expanded():
    """for_each_column [a, b] yields same rule_set_fingerprint as two expanded rules for a and b."""
    unexpanded = [
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
    expanded = [
        {
            "name": "rule_fec_test",
            "criticality": "warn",
            "check": {"function": "is_not_null", "arguments": {"column": "x"}},
        },
        {
            "name": "rule_fec_test",
            "criticality": "warn",
            "check": {"function": "is_not_null", "arguments": {"column": "y"}},
        },
    ]
    assert compute_rule_set_fingerprint(unexpanded) == compute_rule_set_fingerprint(expanded)


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


def test_expand_checks_for_each_column_expands_one_per_column():
    """expand_checks_for_each_column turns one check with for_each_column into one dict per column."""
    checks = [
        {
            "name": "r",
            "criticality": "error",
            "check": {
                "function": "is_not_null",
                "arguments": {},
                "for_each_column": ["a", "b"],
            },
        },
    ]
    expanded = expand_checks_for_each_column(checks)
    assert len(expanded) == 2
    assert expanded[0]["check"]["arguments"] == {"column": "a"}
    assert expanded[1]["check"]["arguments"] == {"column": "b"}
    assert "for_each_column" not in expanded[0]["check"]
    assert "for_each_column" not in expanded[1]["check"]


def test_expand_checks_for_each_column_includes_user_metadata():
    """expand_checks_for_each_column copies user_metadata from source into each expanded dict."""
    checks = [
        {
            "name": "r",
            "criticality": "error",
            "user_metadata": {"key": "value"},
            "check": {
                "function": "is_not_null",
                "arguments": {},
                "for_each_column": ["a"],
            },
        },
    ]
    expanded = expand_checks_for_each_column(checks)
    assert len(expanded) == 1
    assert expanded[0].get("user_metadata") == {"key": "value"}


def test_expand_and_fingerprint_matches_deserialize_checks_for_for_each_column():
    """expand_checks_for_each_column + compute_rule_fingerprint aligns with deserialize_checks (Delta/Lakebase)."""
    checks = [
        {
            "name": "cols_not_null",
            "criticality": "error",
            "check": {
                "function": "is_not_null",
                "arguments": {},
                "for_each_column": ["col_a", "col_b"],
            },
        },
    ]
    delta_rules = deserialize_checks(checks)
    delta_fingerprints = [r.rule_fingerprint for r in delta_rules]
    expanded = expand_checks_for_each_column(checks)
    expanded_fingerprints = [compute_rule_fingerprint(e) for e in expanded]
    assert delta_fingerprints == expanded_fingerprints


def test_expand_checks_for_each_column_passes_through_without_for_each_column():
    """Checks without for_each_column are passed through unchanged."""
    checks = [
        {"name": "r", "criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "id"}}},
    ]
    expanded = expand_checks_for_each_column(checks)
    assert len(expanded) == 1
    assert expanded[0] == checks[0]
