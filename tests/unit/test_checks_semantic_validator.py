"""Unit tests for ChecksSemanticValidator."""

import logging
import pytest

from databricks.labs.dqx.checks_semantic_validator import ChecksSemanticValidator, ChecksSemanticValidationMode


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_check(function: str, column: str, criticality: str = "error", filter_expr=None, **arguments) -> dict:
    """Build a DQX-style check dict."""
    check: dict = {
        "check": {
            "function": function,
            "arguments": {"column": column, **arguments},
        },
        "criticality": criticality,
    }
    if filter_expr is not None:
        check["filter"] = filter_expr
    return check


# ---------------------------------------------------------------------------
# detect_duplicates
# ---------------------------------------------------------------------------


def test_no_duplicates_returns_empty():
    checks = [
        _make_check("is_not_null", "age"),
        _make_check("is_not_null", "name"),
    ]
    assert not ChecksSemanticValidator.detect_duplicates(checks)


def test_identical_rules_flagged_as_duplicate():
    checks = [
        _make_check("is_not_null", "age"),
        _make_check("is_not_null", "age"),
    ]
    issues = ChecksSemanticValidator.detect_duplicates(checks)
    assert len(issues) == 1
    assert "Duplicate rule detected" in issues[0]
    assert "index 1" in issues[0]
    assert "index 0" in issues[0]


def test_duplicate_with_same_criticality_and_filter():
    checks = [
        _make_check("is_not_null", "age", criticality="warn", filter_expr="status = 'ACTIVE'"),
        _make_check("is_not_null", "age", criticality="warn", filter_expr="status = 'ACTIVE'"),
    ]
    issues = ChecksSemanticValidator.detect_duplicates(checks)
    assert len(issues) == 1


def test_different_criticality_not_duplicate():
    checks = [
        _make_check("is_not_null", "age", criticality="error"),
        _make_check("is_not_null", "age", criticality="warn"),
    ]
    assert not ChecksSemanticValidator.detect_duplicates(checks)


def test_different_filter_not_duplicate():
    checks = [
        _make_check("is_not_null", "age", filter_expr="status = 'ACTIVE'"),
        _make_check("is_not_null", "age", filter_expr="status = 'INACTIVE'"),
    ]
    assert not ChecksSemanticValidator.detect_duplicates(checks)


def test_multiple_duplicates_all_flagged():
    checks = [
        _make_check("is_not_null", "age"),
        _make_check("is_not_null", "age"),
        _make_check("is_not_null", "age"),
    ]
    issues = ChecksSemanticValidator.detect_duplicates(checks)
    assert len(issues) == 2


def test_list_valued_arguments_do_not_crash_duplicate_detection():
    """List-valued arguments must not raise 'unhashable type: list' (regression)."""
    checks = [
        _make_check("is_in_list", "status", allowed=["A", "B", "C"]),
        _make_check("is_in_list", "status", allowed=["A", "B", "C"]),
    ]
    issues = ChecksSemanticValidator.detect_duplicates(checks)
    assert len(issues) == 1
    assert "Duplicate rule detected" in issues[0]


def test_list_valued_arguments_with_different_lists_not_duplicate():
    checks = [
        _make_check("is_in_list", "status", allowed=["A", "B"]),
        _make_check("is_in_list", "status", allowed=["A", "C"]),
    ]
    assert not ChecksSemanticValidator.detect_duplicates(checks)


def test_for_each_column_same_columns_is_duplicate():
    checks = [
        {"criticality": "error", "check": {"function": "is_not_null", "for_each_column": ["a", "b"], "arguments": {}}},
        {"criticality": "error", "check": {"function": "is_not_null", "for_each_column": ["a", "b"], "arguments": {}}},
    ]
    issues = ChecksSemanticValidator.detect_duplicates(checks)
    assert len(issues) == 1
    assert "Duplicate rule detected" in issues[0]


def test_for_each_column_different_columns_not_duplicate():
    """for_each_column is part of a rule's identity; different column groups are distinct rules."""
    checks = [
        {"criticality": "error", "check": {"function": "is_not_null", "for_each_column": ["a", "b"], "arguments": {}}},
        {"criticality": "error", "check": {"function": "is_not_null", "for_each_column": ["c", "d"], "arguments": {}}},
    ]
    assert not ChecksSemanticValidator.detect_duplicates(checks)


def test_for_each_column_reordered_is_duplicate():
    """Column order in for_each_column is not significant, so reordered lists are duplicates."""
    checks = [
        {"criticality": "error", "check": {"function": "is_not_null", "for_each_column": ["a", "b"], "arguments": {}}},
        {"criticality": "error", "check": {"function": "is_not_null", "for_each_column": ["b", "a"], "arguments": {}}},
    ]
    assert len(ChecksSemanticValidator.detect_duplicates(checks)) == 1


def test_reordered_columns_argument_is_duplicate():
    """Column order in the plural 'columns' argument is not significant."""
    checks = [
        {"criticality": "error", "check": {"function": "is_unique", "arguments": {"columns": ["a", "b"]}}},
        {"criticality": "error", "check": {"function": "is_unique", "arguments": {"columns": ["b", "a"]}}},
    ]
    assert len(ChecksSemanticValidator.detect_duplicates(checks)) == 1


def test_nested_filter_same_is_duplicate():
    checks = [
        {"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "x"}, "filter": "a > 0"}},
        {"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "x"}, "filter": "a > 0"}},
    ]
    issues = ChecksSemanticValidator.detect_duplicates(checks)
    assert len(issues) == 1


def test_nested_filter_different_not_duplicate():
    """A filter nested inside the check block is part of a rule's identity."""
    checks = [
        {"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "x"}, "filter": "a > 0"}},
        {"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "x"}, "filter": "a < 0"}},
    ]
    assert not ChecksSemanticValidator.detect_duplicates(checks)


def test_flat_form_checks_detected_as_duplicate():
    """Checks in the flat form (no nested 'check' block) are also compared."""
    checks = [
        {"function": "is_not_null", "arguments": {"column": "x"}},
        {"function": "is_not_null", "arguments": {"column": "x"}},
    ]
    issues = ChecksSemanticValidator.detect_duplicates(checks)
    assert len(issues) == 1


# ---------------------------------------------------------------------------
# detect_conflicts
# ---------------------------------------------------------------------------


def test_no_conflicts_returns_empty():
    checks = [
        _make_check("is_in_range", "age", min=0, max=120),
        _make_check("is_in_range", "score", min=0, max=100),
    ]
    assert not ChecksSemanticValidator.detect_conflicts(checks)


def test_same_function_same_column_different_args_flagged():
    checks = [
        _make_check("is_in_range", "age", min=0, max=120),
        _make_check("is_in_range", "age", min=18, max=65),
    ]
    issues = ChecksSemanticValidator.detect_conflicts(checks)
    assert len(issues) == 1
    assert "Conflicting rules detected" in issues[0]
    assert "is_in_range" in issues[0]
    assert "age" in issues[0]


def test_same_function_same_column_same_args_no_conflict():
    """Identical args on same column/function is a duplicate, not a conflict."""
    checks = [
        _make_check("is_in_range", "age", min=0, max=120),
        _make_check("is_in_range", "age", min=0, max=120),
    ]
    assert not ChecksSemanticValidator.detect_conflicts(checks)


def test_check_without_column_skipped_in_conflict_detection():
    checks = [
        {"check": {"function": "sql_expression", "arguments": {"expression": "age > 0"}}, "criticality": "error"},
        {"check": {"function": "sql_expression", "arguments": {"expression": "age > 18"}}, "criticality": "error"},
    ]
    assert not ChecksSemanticValidator.detect_conflicts(checks)


def test_plural_columns_same_columns_different_args_flagged():
    """Conflict detection handles the plural 'columns' argument (e.g. is_unique)."""
    checks = [
        {
            "criticality": "error",
            "check": {"function": "is_unique", "arguments": {"columns": ["a", "b"], "nulls_distinct": True}},
        },
        {
            "criticality": "error",
            "check": {"function": "is_unique", "arguments": {"columns": ["a", "b"], "nulls_distinct": False}},
        },
    ]
    issues = ChecksSemanticValidator.detect_conflicts(checks)
    assert len(issues) == 1
    assert "is_unique" in issues[0]


def test_plural_columns_different_column_sets_not_conflict():
    checks = [
        {"criticality": "error", "check": {"function": "is_unique", "arguments": {"columns": ["a", "b"]}}},
        {"criticality": "error", "check": {"function": "is_unique", "arguments": {"columns": ["c", "d"]}}},
    ]
    assert not ChecksSemanticValidator.detect_conflicts(checks)


def test_plural_columns_reordered_same_set_flagged_as_conflict():
    """Reordered 'columns' lists target the same set, so differing args still conflict."""
    checks = [
        {
            "criticality": "error",
            "check": {"function": "is_unique", "arguments": {"columns": ["a", "b"], "nulls_distinct": True}},
        },
        {
            "criticality": "error",
            "check": {"function": "is_unique", "arguments": {"columns": ["b", "a"], "nulls_distinct": False}},
        },
    ]
    assert len(ChecksSemanticValidator.detect_conflicts(checks)) == 1


def test_reordered_columns_identical_args_not_conflict():
    """Reordered columns with otherwise identical args are a duplicate, not a conflict."""
    checks = [
        {
            "criticality": "error",
            "check": {"function": "is_unique", "arguments": {"columns": ["a", "b"], "nulls_distinct": True}},
        },
        {
            "criticality": "error",
            "check": {"function": "is_unique", "arguments": {"columns": ["b", "a"], "nulls_distinct": True}},
        },
    ]
    assert not ChecksSemanticValidator.detect_conflicts(checks)
    assert len(ChecksSemanticValidator.detect_duplicates(checks)) == 1


def test_malformed_checks_do_not_crash_validation():
    """Malformed checks (non-dict check block or arguments) must not raise (regression).

    Structural validation reports these separately; semantic validation should skip them.
    """
    checks = [
        {"criticality": "warn", "check": "not_a_dict"},
        {"criticality": "warn", "check": {"function": "dummy_func", "arguments": "not_a_dict"}},
    ]
    assert not ChecksSemanticValidator.validate_ruleset(checks)


@pytest.mark.parametrize(
    "checks",
    [
        # list-valued filter
        [
            {
                "criticality": "error",
                "check": {"function": "is_not_null", "arguments": {"column": "x"}},
                "filter": ["a"],
            },
            {
                "criticality": "error",
                "check": {"function": "is_not_null", "arguments": {"column": "x"}},
                "filter": ["a"],
            },
        ],
        # list-valued criticality
        [
            {"criticality": ["error"], "check": {"function": "is_not_null", "arguments": {"column": "x"}}},
            {"criticality": ["error"], "check": {"function": "is_not_null", "arguments": {"column": "x"}}},
        ],
        # nested list inside the 'columns' argument (conflict path)
        [
            {"criticality": "error", "check": {"function": "f", "arguments": {"columns": [["a", "b"]], "x": 1}}},
            {"criticality": "error", "check": {"function": "f", "arguments": {"columns": [["a", "b"]], "x": 2}}},
        ],
        # dict-valued argument
        [
            {"criticality": "error", "check": {"function": "f", "arguments": {"column": "x", "opts": {"k": [1, 2]}}}},
            {"criticality": "error", "check": {"function": "f", "arguments": {"column": "x", "opts": {"k": [1, 2]}}}},
        ],
    ],
)
def test_unhashable_values_do_not_crash_validation(checks):
    """List/dict-valued fields must not raise 'unhashable type' when building rule keys."""
    # Must not raise, regardless of mode.
    ChecksSemanticValidator.validate_ruleset(checks)
    ChecksSemanticValidator.apply(checks, mode=ChecksSemanticValidationMode.WARN)
    ChecksSemanticValidator.detect_duplicates(checks)
    ChecksSemanticValidator.detect_conflicts(checks)


def test_unhashable_value_beyond_normalization_is_skipped(caplog):
    """A value that stays unhashable (e.g. a set) is skipped and logged, not raised."""
    checks = [
        {"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "x"}}, "filter": {"a"}},
        {"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "x"}}, "filter": {"a"}},
    ]
    with caplog.at_level(logging.WARNING, logger="databricks.labs.dqx.checks_semantic_validator"):
        # Must not raise; the malformed checks are skipped.
        assert ChecksSemanticValidator.detect_duplicates(checks) == []
        assert ChecksSemanticValidator.detect_conflicts(checks) == []
    assert any("Skipping" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# validate_ruleset
# ---------------------------------------------------------------------------


def test_validate_ruleset_combines_both():
    checks = [
        _make_check("is_not_null", "age"),
        _make_check("is_not_null", "age"),  # duplicate
        _make_check("is_in_range", "score", min=0, max=100),
        _make_check("is_in_range", "score", min=0, max=50),  # conflict
    ]
    issues = ChecksSemanticValidator.validate_ruleset(checks)
    assert len(issues) == 2
    assert any("Duplicate" in i for i in issues)
    assert any("Conflicting" in i for i in issues)


def test_validate_ruleset_clean_returns_empty():
    checks = [
        _make_check("is_not_null", "age"),
        _make_check("is_not_null", "name"),
        _make_check("is_in_range", "score", min=0, max=100),
    ]
    assert not ChecksSemanticValidator.validate_ruleset(checks)


# ---------------------------------------------------------------------------
# apply — WARN mode
# ---------------------------------------------------------------------------


def test_apply_warn_mode_logs_and_does_not_raise(caplog):
    checks = [
        _make_check("is_not_null", "age"),
        _make_check("is_not_null", "age"),
    ]
    with caplog.at_level(logging.WARNING, logger="databricks.labs.dqx.checks_semantic_validator"):
        ChecksSemanticValidator.apply(checks, mode=ChecksSemanticValidationMode.WARN)

    assert any("Duplicate" in r.message for r in caplog.records)


def test_apply_warn_mode_clean_ruleset_no_logs(caplog):
    checks = [_make_check("is_not_null", "age")]
    with caplog.at_level(logging.WARNING, logger="databricks.labs.dqx.checks_semantic_validator"):
        ChecksSemanticValidator.apply(checks, mode=ChecksSemanticValidationMode.WARN)
    assert caplog.records == []


# ---------------------------------------------------------------------------
# apply — FAIL mode
# ---------------------------------------------------------------------------


def test_apply_fail_mode_raises_on_duplicate():
    checks = [
        _make_check("is_not_null", "age"),
        _make_check("is_not_null", "age"),
    ]
    with pytest.raises(ValueError, match="Semantic validation failed"):
        ChecksSemanticValidator.apply(checks, mode=ChecksSemanticValidationMode.FAIL)


def test_apply_fail_mode_raises_on_conflict():
    checks = [
        _make_check("is_in_range", "age", min=0, max=120),
        _make_check("is_in_range", "age", min=18, max=65),
    ]
    with pytest.raises(ValueError, match="Semantic validation failed"):
        ChecksSemanticValidator.apply(checks, mode=ChecksSemanticValidationMode.FAIL)


def test_apply_fail_mode_clean_ruleset_does_not_raise():
    checks = [_make_check("is_not_null", "age")]
    ChecksSemanticValidator.apply(checks, mode=ChecksSemanticValidationMode.FAIL)  # should not raise


def test_apply_invalid_mode_raises():
    with pytest.raises(ValueError, match="Unsupported semantic validation mode"):
        ChecksSemanticValidator.apply([], mode="invalid")


# ---------------------------------------------------------------------------
# apply — None mode (skip validation)
# ---------------------------------------------------------------------------


def test_apply_none_mode_skips_validation():
    """Passing mode=None should skip all semantic checks entirely."""
    checks = [
        _make_check("is_not_null", "age"),
        _make_check("is_not_null", "age"),  # would normally be a duplicate
    ]
    # Should not raise and should not log
    ChecksSemanticValidator.apply(checks, mode=None)
