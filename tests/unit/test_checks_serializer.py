"""Unit tests for checks_serializer fingerprinting and conversion."""

import re
from datetime import datetime, timezone
from unittest.mock import create_autospec

from pyspark.sql import SparkSession

from databricks.labs.dqx.check_funcs import is_not_null
from databricks.labs.dqx.checks_serializer import DataFrameConverter, compute_rule_set_fingerprint
from databricks.labs.dqx.rule import DQRowRule, compute_rule_fingerprint


def _hex_sha256_pattern() -> re.Pattern[str]:
    return re.compile(r"^[a-f0-9]{64}$")


def test_compute_rule_fingerprint_returns_64_char_hex():
    """Rule fingerprint is a 64-character lowercase hex string (SHA-256)."""
    check = {
        "name": "id_not_null",
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "id"}},
    }
    fingerprint = compute_rule_fingerprint(check)
    assert _hex_sha256_pattern().match(fingerprint), f"Expected 64-char hex, got {fingerprint!r}"


def test_compute_rule_fingerprint_deterministic():
    """Same check dict produces the same fingerprint every time."""
    check = {
        "name": "id_not_null",
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "id"}},
    }
    assert compute_rule_fingerprint(check) == compute_rule_fingerprint(check)


def test_compute_rule_fingerprint_different_inputs_different_output():
    """Different rule content produces different fingerprints."""
    check_a = {
        "name": "a",
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "id"}},
    }
    check_b = {
        "name": "b",
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "id"}},
    }
    check_c = {
        "name": "a",
        "criticality": "warn",
        "check": {"function": "is_not_null", "arguments": {"column": "id"}},
    }
    fp_a = compute_rule_fingerprint(check_a)
    fp_b = compute_rule_fingerprint(check_b)
    fp_c = compute_rule_fingerprint(check_c)
    assert len({fp_a, fp_b, fp_c}) == 3


def test_compute_rule_fingerprint_uses_canonical_representation():
    """Fingerprint is based on canonical key order; input dict order does not matter."""
    check1 = {
        "name": "n",
        "criticality": "error",
        "check": {"function": "f", "arguments": {"column": "c"}},
    }
    check2 = {
        "check": {"arguments": {"column": "c"}, "function": "f"},
        "criticality": "error",
        "name": "n",
    }
    assert compute_rule_fingerprint(check1) == compute_rule_fingerprint(check2)


def test_compute_rule_fingerprint_handles_missing_optional_fields():
    """Missing optional fields use defaults (e.g. criticality 'error') and do not raise."""
    minimal = {"check": {"function": "is_not_null", "arguments": {"column": "id"}}}
    fingerprint = compute_rule_fingerprint(minimal)
    assert _hex_sha256_pattern().match(fingerprint)
    # Same as explicit default criticality
    with_default = {
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "id"}},
    }
    assert compute_rule_fingerprint(minimal) == compute_rule_fingerprint(with_default)


def test_compute_rule_fingerprint_includes_filter():
    """Filter is part of the fingerprint input; for_each_column is excluded (rules are always expanded first)."""
    base = {
        "name": "r",
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "id"}},
    }
    with_filter = {**base, "filter": "id > 0"}
    assert compute_rule_fingerprint(base) != compute_rule_fingerprint(with_filter)


def test_compute_rule_fingerprint_includes_for_each_column():
    """for_each_column is included in the fingerprint; adding it changes the fingerprint."""
    base = {
        "name": "r",
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "id"}},
    }
    with_for_each_column = {
        **base,
        "check": {**base["check"], "for_each_column": ["a", "b"]},
    }
    assert compute_rule_fingerprint(base) != compute_rule_fingerprint(with_for_each_column)


def test_compute_rule_fingerprint_excludes_user_metadata():
    """user_metadata is excluded from the fingerprint; rules with/without it hash identically."""
    base = {
        "name": "r",
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "id"}},
    }
    with_meta = {**base, "user_metadata": {"team": "eng"}}
    assert compute_rule_fingerprint(base) == compute_rule_fingerprint(with_meta)


def test_compute_rule_set_fingerprint_returns_64_char_hex():
    """Rule set fingerprint is a 64-character lowercase hex string (SHA-256)."""
    checks = [
        {
            "name": "a",
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": "id"}},
        },
    ]
    fingerprint = compute_rule_set_fingerprint(checks)
    assert _hex_sha256_pattern().match(fingerprint), f"Expected 64-char hex, got {fingerprint!r}"


def test_compute_rule_set_fingerprint_deterministic():
    """Same list of checks produces the same rule set fingerprint every time."""
    checks = [
        {
            "name": "a",
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": "id"}},
        },
        {
            "name": "b",
            "criticality": "warn",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"column": "name"}},
        },
    ]
    assert compute_rule_set_fingerprint(checks) == compute_rule_set_fingerprint(checks)


def test_compute_rule_set_fingerprint_order_independent():
    """Same rules in different order produce the same rule set fingerprint."""
    check_a = {
        "name": "a",
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "id"}},
    }
    check_b = {
        "name": "b",
        "criticality": "warn",
        "check": {"function": "is_not_null_and_not_empty", "arguments": {"column": "name"}},
    }
    fp_ab = compute_rule_set_fingerprint([check_a, check_b])
    fp_ba = compute_rule_set_fingerprint([check_b, check_a])
    assert fp_ab == fp_ba


def test_compute_rule_set_fingerprint_empty_list():
    """Empty list produces a deterministic fingerprint (hash of empty/sorted list)."""
    fingerprint = compute_rule_set_fingerprint([])
    assert _hex_sha256_pattern().match(fingerprint)
    assert fingerprint == compute_rule_set_fingerprint([])


def test_compute_rule_set_fingerprint_different_sets_different_output():
    """Different rule sets produce different fingerprints."""
    set1 = [
        {
            "name": "a",
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": "id"}},
        },
    ]
    set2 = [
        {
            "name": "b",
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": "id"}},
        },
    ]
    set3 = [
        {
            "name": "a",
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": "id"}},
        },
        {
            "name": "b",
            "criticality": "warn",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"column": "name"}},
        },
    ]
    fingerprint_1 = compute_rule_set_fingerprint(set1)
    fingerprint_2 = compute_rule_set_fingerprint(set2)
    fingerprint_3 = compute_rule_set_fingerprint(set3)
    assert len({fingerprint_1, fingerprint_2, fingerprint_3}) == 3


def test_compute_rule_fingerprint_matches_dq_rule_rule_fingerprint():
    """DQRule.rule_fingerprint and compute_rule_fingerprint(rule.to_dict()) produce the same hash."""
    rule = DQRowRule(name="id_not_null", criticality="error", check_func=is_not_null, column="id")
    from_dict = compute_rule_fingerprint(rule.to_dict())
    from_rule = rule.rule_fingerprint
    assert from_dict == from_rule


_SIMPLE_CHECKS = [{"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "id"}}}]
_FIXED_TS = datetime(2024, 6, 15, 10, 30, 0, tzinfo=timezone.utc)


def test_to_dataframe_uses_injected_created_at():
    """When created_at is provided, every row uses that exact timestamp."""
    spark = create_autospec(SparkSession)

    DataFrameConverter.to_dataframe(spark, _SIMPLE_CHECKS, created_at=_FIXED_TS)

    rows = spark.createDataFrame.call_args.args[0]
    # created_at is the 7th element (index 6) in each row tuple
    assert all(row[6] == _FIXED_TS for row in rows)


def test_to_dataframe_uses_current_utc_time_when_created_at_is_none():
    """When created_at is not supplied, the timestamp defaults to datetime.now(utc)."""
    spark = create_autospec(SparkSession)
    before = datetime.now(timezone.utc)

    DataFrameConverter.to_dataframe(spark, _SIMPLE_CHECKS, created_at=None)

    after = datetime.now(timezone.utc)
    rows = spark.createDataFrame.call_args.args[0]
    created_at_value = rows[0][6]
    assert before <= created_at_value <= after


def test_to_dataframe_all_rows_share_same_created_at():
    """All rows in a single call carry the same created_at so they form one logical batch."""
    spark = create_autospec(SparkSession)
    multi_checks = [
        {"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "id"}}},
        {"criticality": "warn", "check": {"function": "is_not_null", "arguments": {"column": "name"}}},
    ]

    DataFrameConverter.to_dataframe(spark, multi_checks, created_at=_FIXED_TS)

    rows = spark.createDataFrame.call_args.args[0]
    assert len(rows) == 2
    assert rows[0][6] == rows[1][6] == _FIXED_TS
