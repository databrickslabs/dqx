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


def test_malformed_checks_do_not_crash_validation():
    """Malformed checks (non-dict check block or arguments) must not raise (regression).

    Structural validation reports these separately; semantic validation should skip them.
    """
    checks = [
        {"criticality": "warn", "check": "not_a_dict"},
        {"criticality": "warn", "check": {"function": "dummy_func", "arguments": "not_a_dict"}},
    ]
    assert not ChecksSemanticValidator.validate_ruleset(checks)


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
