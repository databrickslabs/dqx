"""Tests for custom message expressions on DQRule."""

import logging

import pyspark.sql.functions as F

from databricks.labs.dqx.check_funcs import is_not_null, is_not_null_and_not_empty
from databricks.labs.dqx.rule import DQRowRule, DQForEachColRule


def test_dq_row_rule_accepts_message_expr_string():
    """DQRowRule with message_expr should accept a SQL expression string."""
    rule = DQRowRule(
        check_func=is_not_null,
        column="id",
        message_expr="concat('Failed: ', 'id_not_null')",
    )
    assert rule.message_expr == "concat('Failed: ', 'id_not_null')"


def test_dq_row_rule_accepts_message_expr_column():
    """DQRowRule with message_expr should accept a Spark Column object."""
    column_expr = F.concat(F.lit("Failed: "), F.lit("id_not_null"))
    rule = DQRowRule(
        check_func=is_not_null,
        column="id",
        message_expr=column_expr,
    )
    assert rule.message_expr is not None


def test_dq_row_rule_message_expr_defaults_to_none():
    """DQRowRule without message_expr should default to None."""
    rule = DQRowRule(check_func=is_not_null, column="id")
    assert rule.message_expr is None


def test_dq_dataset_rule_message_expr_defaults_to_none():
    """DQDatasetRule without message_expr should default to None."""
    rule = DQRowRule(check_func=is_not_null_and_not_empty, column="id")
    assert rule.message_expr is None


def test_dq_for_each_col_rule_propagates_message_expr():
    """DQForEachColRule should propagate message_expr to generated DQRowRules."""
    msg = "concat('Check failed for ', 'rule_name')"
    for_each_rule = DQForEachColRule(
        columns=["a", "b"],
        check_func=is_not_null,
        message_expr=msg,
    )
    rules = for_each_rule.get_rules()
    assert len(rules) == 2
    for rule in rules:
        assert rule.message_expr == msg


def test_dq_for_each_col_rule_message_expr_defaults_to_none():
    """DQForEachColRule without message_expr should produce rules with message_expr=None."""
    for_each_rule = DQForEachColRule(
        columns=["a", "b"],
        check_func=is_not_null,
    )
    rules = for_each_rule.get_rules()
    for rule in rules:
        assert rule.message_expr is None


def test_dq_rule_to_dict_includes_message_expr_when_string():
    """to_dict() should include the message_expr key when set to a SQL string."""
    msg = "'Custom error for id_not_null'"
    rule = DQRowRule(
        check_func=is_not_null,
        column="id",
        name="id_not_null",
        message_expr=msg,
    )
    rule_dict = rule.to_dict()
    assert rule_dict["message_expr"] == msg


def test_dq_rule_to_dict_omits_message_expr_when_none():
    """to_dict() should not include message_expr key when it is None."""
    rule = DQRowRule(
        check_func=is_not_null,
        column="id",
        name="id_not_null",
    )
    rule_dict = rule.to_dict()
    assert "message_expr" not in rule_dict


def test_dq_rule_to_dict_omits_message_expr_when_column():
    """Column message_expr is in-process only and is excluded from serialised metadata."""
    rule = DQRowRule(
        check_func=is_not_null,
        column="id",
        name="id_not_null",
        message_expr=F.lit("Custom error"),
    )
    rule_dict = rule.to_dict()
    assert "message_expr" not in rule_dict


def test_dq_rule_to_dict_warns_when_message_expr_is_column(caplog):
    """Serialising a Column message_expr should warn that it cannot be round-tripped."""
    rule = DQRowRule(
        check_func=is_not_null,
        column="id",
        name="id_not_null",
        message_expr=F.lit("Custom error"),
    )
    with caplog.at_level(logging.WARNING, logger="databricks.labs.dqx.rule"):
        rule.to_dict()
    assert "cannot be serialized" in caplog.text


def test_dq_rule_to_dict_does_not_warn_when_message_expr_is_string(caplog):
    """A string message_expr is serializable, so no serialization warning should be emitted."""
    rule = DQRowRule(
        check_func=is_not_null,
        column="id",
        name="id_not_null",
        message_expr="'Custom error for id_not_null'",
    )
    with caplog.at_level(logging.WARNING, logger="databricks.labs.dqx.rule"):
        rule.to_dict()
    assert "cannot be serialized" not in caplog.text


def test_dq_for_each_col_rule_to_dict_warns_for_each_column_when_message_expr_is_column(caplog):
    """Column message_expr propagated by DQForEachColRule should warn once per generated rule."""
    for_each_rule = DQForEachColRule(
        columns=["a", "b"],
        check_func=is_not_null,
        message_expr=F.lit("Custom error"),
    )
    rules = for_each_rule.get_rules()
    with caplog.at_level(logging.WARNING, logger="databricks.labs.dqx.rule"):
        for rule in rules:
            rule_dict = rule.to_dict()
            assert "message_expr" not in rule_dict
    assert caplog.text.count("cannot be serialized") == len(rules)
