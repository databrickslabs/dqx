"""Tests for custom message callable on DQRule."""

import inspect
from typing import Any

import pyspark.sql.functions as F
from pyspark.sql import Column

from databricks.labs.dqx.check_funcs import is_not_null, is_not_null_and_not_empty
from databricks.labs.dqx.rule import DQRowRule, DQForEachColRule


def _sample_message_callable(rule_name: str) -> Column:
    """Custom message function that returns a string-valued column."""
    return F.lit(f"Custom: {rule_name}")


def test_dq_row_rule_accepts_message_callable():
    """DQRowRule with message parameter should be creatable."""
    rule = DQRowRule(
        check_func=is_not_null,
        column="id",
        message=_sample_message_callable,
    )
    assert rule.message is _sample_message_callable


def test_dq_row_rule_message_defaults_to_none():
    """DQRowRule without message should default to None."""
    rule = DQRowRule(check_func=is_not_null, column="id")
    assert rule.message is None


def test_dq_dataset_rule_accepts_message_callable():
    """DQDatasetRule without message should default to None."""
    rule = DQRowRule(check_func=is_not_null_and_not_empty, column="id")
    assert rule.message is None


def test_dq_for_each_col_rule_propagates_message():
    """DQForEachColRule should propagate message to generated DQRowRules."""
    for_each_rule = DQForEachColRule(
        columns=["a", "b"],
        check_func=is_not_null,
        message=_sample_message_callable,
    )
    rules = for_each_rule.get_rules()
    assert len(rules) == 2
    for rule in rules:
        assert rule.message is _sample_message_callable


def test_dq_for_each_col_rule_message_defaults_to_none():
    """DQForEachColRule without message should produce rules with message=None."""
    for_each_rule = DQForEachColRule(
        columns=["a", "b"],
        check_func=is_not_null,
    )
    rules = for_each_rule.get_rules()
    for rule in rules:
        assert rule.message is None


def test_message_callable_receives_correct_args():
    """The message callable signature should accept rule_name, check_func_name, check_func_args, column_value."""
    sig = inspect.signature(_sample_message_callable)
    params = list(sig.parameters.keys())
    assert params == ["rule_name", "check_func_name", "check_func_args", "column_value"]


def test_dq_rule_to_dict_does_not_include_message():
    """to_dict() should not include the message callable since it is not serializable."""
    rule = DQRowRule(
        check_func=is_not_null,
        column="id",
        name="id_not_null",
        message=_sample_message_callable,
    )
    rule_dict = rule.to_dict()
    assert "message" not in rule_dict
