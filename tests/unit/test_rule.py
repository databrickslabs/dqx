"""Unit tests for rule fingerprinting, expansion, and serialization alignment."""

import re

from databricks.labs.dqx.check_funcs import is_not_null
from databricks.labs.dqx.rule import DQRowRule
from databricks.labs.dqx.rule_fingerprint import compute_rule_fingerprint, compute_rule_set_fingerprint_by_metadata


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


def test_compute_rule_set_fingerprint_by_metadata_same_set_same_fingerprint():
    """Same rule set produces the same rule_set_fingerprint (determinism)."""
    checks = [
        {"name": "a", "criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "id"}}},
        {"name": "b", "criticality": "warn", "check": {"function": "is_not_null", "arguments": {"column": "name"}}},
    ]
    assert compute_rule_set_fingerprint_by_metadata(checks) == compute_rule_set_fingerprint_by_metadata(checks)


def test_compute_rule_set_fingerprint_by_metadata_for_each_column_deterministic():
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
    assert compute_rule_set_fingerprint_by_metadata(checks) == compute_rule_set_fingerprint_by_metadata(checks)


def test_compute_rule_set_fingerprint_by_metadata_order_independent():
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
    fp_ab = compute_rule_set_fingerprint_by_metadata([rule_check_first, rule_check_second])
    fp_ba = compute_rule_set_fingerprint_by_metadata([rule_check_second, rule_check_first])
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
    assert compute_rule_set_fingerprint_by_metadata(compact_ab) != compute_rule_set_fingerprint_by_metadata(compact_ac)


def test_compute_rule_fingerprint_empty_for_each_column_same_as_none():
    """Empty for_each_column list and None both produce the same fingerprint.

    The normalization `sorted(fec) if fec else None` treats [] and None identically
    (both are falsy), so they are canonicalized to None before hashing.
    """
    check_empty_list = {
        "criticality": "error",
        "check": {"function": "is_not_null", "for_each_column": [], "arguments": {"column": "id"}},
    }
    check_none = {
        "criticality": "error",
        "check": {"function": "is_not_null", "for_each_column": None, "arguments": {"column": "id"}},
    }
    check_absent = {
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "id"}},
    }
    fp_empty = compute_rule_fingerprint(check_empty_list)
    fp_none = compute_rule_fingerprint(check_none)
    fp_absent = compute_rule_fingerprint(check_absent)
    assert fp_empty == fp_none == fp_absent


def test_compute_rule_fingerprint_missing_criticality_same_as_explicit_error():
    """A rule with no criticality key hashes the same as one with criticality='error'.

    check_dict.get('criticality', 'error') returns the default only when the key
    is absent, so missing and explicit 'error' are indistinguishable in the hash.
    """
    check_missing = {"check": {"function": "is_not_null", "arguments": {"column": "id"}}}
    check_explicit = {"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "id"}}}
    assert compute_rule_fingerprint(check_missing) == compute_rule_fingerprint(check_explicit)


def test_compute_rule_fingerprint_for_each_column_order_independent():
    """for_each_column column order does not affect the fingerprint (columns are sorted)."""
    check_ab = {
        "criticality": "error",
        "check": {"function": "is_not_null", "for_each_column": ["a", "b"], "arguments": {}},
    }
    check_ba = {
        "criticality": "error",
        "check": {"function": "is_not_null", "for_each_column": ["b", "a"], "arguments": {}},
    }
    assert compute_rule_fingerprint(check_ab) == compute_rule_fingerprint(check_ba)


def test_compute_rule_fingerprint_user_metadata_excluded():
    """user_metadata is not part of the fingerprint.

    Two rules that differ only in user_metadata must produce identical fingerprints so
    that a metadata-only update does not create a new version in storage.
    """
    check_with_metadata = {
        "name": "id_not_null",
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "id"}},
        "user_metadata": {"owner": "alice", "team": "data-eng"},
    }
    check_different_metadata = {
        "name": "id_not_null",
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "id"}},
        "user_metadata": {"owner": "bob"},
    }
    check_no_metadata = {
        "name": "id_not_null",
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "id"}},
    }
    fp_with = compute_rule_fingerprint(check_with_metadata)
    fp_different = compute_rule_fingerprint(check_different_metadata)
    fp_without = compute_rule_fingerprint(check_no_metadata)
    assert fp_with == fp_different == fp_without


def test_compute_rule_set_fingerprint_by_metadata_user_metadata_only_change_same_fingerprint():
    """Changing user_metadata across the entire rule set does not change the set fingerprint."""
    checks_v1 = [
        {
            "name": "a_not_null",
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": "a"}},
            "user_metadata": {"version": "1"},
        },
        {
            "name": "b_not_empty",
            "criticality": "warn",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"column": "b"}},
            "user_metadata": {"owner": "alice"},
        },
    ]
    checks_v2 = [
        {
            "name": "a_not_null",
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": "a"}},
            "user_metadata": {"version": "2", "owner": "bob"},
        },
        {
            "name": "b_not_empty",
            "criticality": "warn",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"column": "b"}},
        },
    ]
    assert compute_rule_set_fingerprint_by_metadata(checks_v1) == compute_rule_set_fingerprint_by_metadata(checks_v2)


def test_compute_rule_fingerprint_different_functions_differ():
    """Two rules identical except for their check function produce different fingerprints."""
    check_not_null = {
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "id"}},
    }
    check_not_empty = {
        "criticality": "error",
        "check": {"function": "is_not_null_and_not_empty", "arguments": {"column": "id"}},
    }
    assert compute_rule_fingerprint(check_not_null) != compute_rule_fingerprint(check_not_empty)


def test_compute_rule_fingerprint_different_filters_differ():
    """Two rules identical except for their filter produce different fingerprints."""
    check_no_filter = {
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "id"}},
    }
    check_with_filter = {
        "criticality": "error",
        "filter": "country = 'PL'",
        "check": {"function": "is_not_null", "arguments": {"column": "id"}},
    }
    assert compute_rule_fingerprint(check_no_filter) != compute_rule_fingerprint(check_with_filter)
