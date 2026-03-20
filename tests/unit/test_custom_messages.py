"""Tests for custom message and name parameters on check functions."""

from databricks.labs.dqx.check_funcs import (
    is_not_null,
    is_not_empty,
    regex_match,
    is_in_range,
    is_not_null_and_is_in_list,
    is_valid_date,
)


def test_is_not_null_accepts_msg_and_name():
    result = is_not_null("col_a", msg="ID required", name="custom_null")
    # Just verify it returns a Column without error
    assert result is not None


def test_is_not_empty_accepts_msg():
    result = is_not_empty("col_a", msg="Must not be empty")
    assert result is not None


def test_regex_match_accepts_msg():
    result = regex_match("col_a", r"^\d+$", msg="Must be numeric")
    assert result is not None


def test_is_in_range_accepts_msg():
    result = is_in_range("col_a", 0, 100, msg="Out of range")
    assert result is not None


def test_default_msg_when_none():
    # Verify existing behavior is preserved when msg=None
    result = is_not_null("col_a")
    assert result is not None


def test_is_not_null_and_is_in_list_accepts_msg():
    result = is_not_null_and_is_in_list("col_a", ["a", "b"], msg="Not in allowed list")
    assert result is not None


def test_is_valid_date_accepts_msg():
    result = is_valid_date("col_a", msg="Invalid date format")
    assert result is not None
