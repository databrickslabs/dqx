"""Tests for custom message SQL expressions on DQRule."""

from databricks.labs.dqx.check_funcs import is_not_null, is_not_null_and_not_empty
from databricks.labs.dqx.rule import DQRowRule, DQForEachColRule


def test_dq_row_rule_accepts_message_string():
    """DQRowRule with message parameter should accept a SQL expression string."""
    rule = DQRowRule(
        check_func=is_not_null,
        column="id",
        message="concat('Failed: ', '{rule_name}')",
    )
    assert rule.message == "concat('Failed: ', '{rule_name}')"


def test_dq_row_rule_message_defaults_to_none():
    """DQRowRule without message should default to None."""
    rule = DQRowRule(check_func=is_not_null, column="id")
    assert rule.message is None


def test_dq_dataset_rule_message_defaults_to_none():
    """DQDatasetRule without message should default to None."""
    rule = DQRowRule(check_func=is_not_null_and_not_empty, column="id")
    assert rule.message is None


def test_dq_for_each_col_rule_propagates_message():
    """DQForEachColRule should propagate message to generated DQRowRules."""
    msg = "concat('Check failed for ', '{rule_name}')"
    for_each_rule = DQForEachColRule(
        columns=["a", "b"],
        check_func=is_not_null,
        message=msg,
    )
    rules = for_each_rule.get_rules()
    assert len(rules) == 2
    for rule in rules:
        assert rule.message == msg


def test_dq_for_each_col_rule_message_defaults_to_none():
    """DQForEachColRule without message should produce rules with message=None."""
    for_each_rule = DQForEachColRule(
        columns=["a", "b"],
        check_func=is_not_null,
    )
    rules = for_each_rule.get_rules()
    for rule in rules:
        assert rule.message is None


def test_dq_rule_to_dict_includes_message():
    """to_dict() should include the message string since it is serializable."""
    msg = "'Custom error for ' || '{rule_name}'"
    rule = DQRowRule(
        check_func=is_not_null,
        column="id",
        name="id_not_null",
        message=msg,
    )
    rule_dict = rule.to_dict()
    assert rule_dict["message"] == msg


def test_dq_rule_to_dict_omits_message_when_none():
    """to_dict() should not include message key when it is None."""
    rule = DQRowRule(
        check_func=is_not_null,
        column="id",
        name="id_not_null",
    )
    rule_dict = rule.to_dict()
    assert "message" not in rule_dict
