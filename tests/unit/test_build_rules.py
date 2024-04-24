import pprint
from typing import List

import pytest
from pyspark.sql import SparkSession

from databricks.labs.dqx.dq_engine import (
    DQRule,
    DQRuleColSet,
    build_checks,
    build_checks_by_metadata,
)
from databricks.labs.dqx.dq_functions import (
    col_is_not_null_and_not_empty,
    col_sql_expression,
    col_value_is_in_list,
)

schema = "a: int, b: int, c: int"


def test_build_rules_empty(spark_session: SparkSession):
    actual_rules = build_checks()

    expected_rules: List[DQRule] = []

    assert actual_rules == expected_rules


def test_get_rules(spark_session: SparkSession):
    actual_rules = (
        # set of columns for the same check
        DQRuleColSet(columns=["a", "b"], check_func=col_is_not_null_and_not_empty).get_rules()
        # with check function params provided as positional arguments
        + DQRuleColSet(
            columns=["c", "d"], criticality="error", check_func=col_value_is_in_list, check_func_args=[[1, 2]]
        ).get_rules()
        # with check function params provided as named arguments
        + DQRuleColSet(
            columns=["e"], criticality="warn", check_func=col_value_is_in_list, check_func_kwargs={"allowed": [3]}
        ).get_rules()
        # should be skipped
        + DQRuleColSet(columns=[], criticality="error", check_func=col_is_not_null_and_not_empty).get_rules()
    )

    expected_rules = [
        DQRule(name="col_a_is_null_or_empty", criticality="error", check=col_is_not_null_and_not_empty("a")),
        DQRule(name="col_b_is_null_or_empty", criticality="error", check=col_is_not_null_and_not_empty("b")),
        DQRule(
            name="col_c_value_is_not_in_the_list", criticality="error", check=col_value_is_in_list("c", allowed=[1, 2])
        ),
        DQRule(
            name="col_d_value_is_not_in_the_list", criticality="error", check=col_value_is_in_list("d", allowed=[1, 2])
        ),
        DQRule(name="col_e_value_is_not_in_the_list", criticality="warn", check=col_value_is_in_list("e", allowed=[3])),
    ]

    assert pprint.pformat(actual_rules) == pprint.pformat(expected_rules)


def test_build_rules(spark_session: SparkSession):
    actual_rules = build_checks(
        # set of columns for the same check
        DQRuleColSet(columns=["a", "b"], criticality="error", check_func=col_is_not_null_and_not_empty),
        DQRuleColSet(columns=["c"], criticality="warn", check_func=col_is_not_null_and_not_empty),
        # with check function params provided as positional arguments
        DQRuleColSet(
            columns=["d", "e"], criticality="error", check_func=col_value_is_in_list, check_func_args=[[1, 2]]
        ),
        # with check function params provided as named arguments
        DQRuleColSet(
            columns=["f"], criticality="warn", check_func=col_value_is_in_list, check_func_kwargs={"allowed": [3]}
        ),
        # should be skipped
        DQRuleColSet(columns=[], criticality="error", check_func=col_is_not_null_and_not_empty),
    ) + [
        DQRule(name="col_g_is_null_or_empty", criticality="warn", check=col_is_not_null_and_not_empty("g")),
        DQRule(criticality="warn", check=col_value_is_in_list("h", allowed=[1, 2])),
    ]

    expected_rules = [
        DQRule(name="col_a_is_null_or_empty", criticality="error", check=col_is_not_null_and_not_empty("a")),
        DQRule(name="col_b_is_null_or_empty", criticality="error", check=col_is_not_null_and_not_empty("b")),
        DQRule(name="col_c_is_null_or_empty", criticality="warn", check=col_is_not_null_and_not_empty("c")),
        DQRule(
            name="col_d_value_is_not_in_the_list", criticality="error", check=col_value_is_in_list("d", allowed=[1, 2])
        ),
        DQRule(
            name="col_e_value_is_not_in_the_list", criticality="error", check=col_value_is_in_list("e", allowed=[1, 2])
        ),
        DQRule(name="col_f_value_is_not_in_the_list", criticality="warn", check=col_value_is_in_list("f", allowed=[3])),
        DQRule(name="col_g_is_null_or_empty", criticality="warn", check=col_is_not_null_and_not_empty("g")),
        DQRule(
            name="col_h_value_is_not_in_the_list", criticality="warn", check=col_value_is_in_list("h", allowed=[1, 2])
        ),
    ]

    assert pprint.pformat(actual_rules) == pprint.pformat(expected_rules)


def test_build_rules_by_metadata(spark_session: SparkSession):
    checks = [
        {
            "criticality": "error",
            "check": {"function": "col_is_not_null_and_not_empty", "arguments": {"col_names": ["a", "b"]}},
        },
        {
            "criticality": "warn",
            "check": {"function": "col_is_not_null_and_not_empty", "arguments": {"col_names": ["c"]}},
        },
        {
            "criticality": "error",
            "check": {"function": "col_value_is_in_list", "arguments": {"col_names": ["d", "e"], "allowed": [1, 2]}},
        },
        {
            "criticality": "warn",
            "check": {"function": "col_value_is_in_list", "arguments": {"col_names": ["f"], "allowed": [3]}},
        },
        {
            "criticality": "error",
            "check": {"function": "col_is_not_null_and_not_empty", "arguments": {"col_names": []}},
        },
        {
            "name": "col_g_is_null_or_empty",
            "criticality": "warn",
            "check": {"function": "col_is_not_null_and_not_empty", "arguments": {"col_name": "g"}},
        },
        {
            "criticality": "warn",
            "check": {"function": "col_value_is_in_list", "arguments": {"col_name": "h", "allowed": [1, 2]}},
        },
        {
            "name": "d_not_in_a",
            "criticality": "error",
            "check": {
                "function": "col_sql_expression",
                "arguments": {"expression": "a != substring(b, 8, 1)", "msg": "a not found in b"},
            },
        },
    ]

    actual_rules = build_checks_by_metadata(checks)

    expected_rules = [
        DQRule(name="col_a_is_null_or_empty", criticality="error", check=col_is_not_null_and_not_empty("a")),
        DQRule(name="col_b_is_null_or_empty", criticality="error", check=col_is_not_null_and_not_empty("b")),
        DQRule(name="col_c_is_null_or_empty", criticality="warn", check=col_is_not_null_and_not_empty("c")),
        DQRule(
            name="col_d_value_is_not_in_the_list", criticality="error", check=col_value_is_in_list("d", allowed=[1, 2])
        ),
        DQRule(
            name="col_e_value_is_not_in_the_list", criticality="error", check=col_value_is_in_list("e", allowed=[1, 2])
        ),
        DQRule(name="col_f_value_is_not_in_the_list", criticality="warn", check=col_value_is_in_list("f", allowed=[3])),
        DQRule(name="col_g_is_null_or_empty", criticality="warn", check=col_is_not_null_and_not_empty("g")),
        DQRule(
            name="col_h_value_is_not_in_the_list", criticality="warn", check=col_value_is_in_list("h", allowed=[1, 2])
        ),
        DQRule(
            name="d_not_in_a",
            criticality="error",
            check=col_sql_expression(expression="a != substring(b, 8, 1)", msg="a not found in b"),
        ),
    ]

    assert pprint.pformat(actual_rules) == pprint.pformat(expected_rules)


def test_build_checks_by_metadata_when_check_spec_is_missing(spark_session: SparkSession):
    checks: List[dict] = [{}]  # missing check spec

    with pytest.raises(Exception):
        build_checks_by_metadata(checks)


def test_build_checks_by_metadata_when_function_spec_is_missing(spark_session: SparkSession):
    checks: List[dict] = [{"check": {}}]  # missing func spec

    with pytest.raises(Exception):
        build_checks_by_metadata(checks)


def test_build_checks_by_metadata_when_arguments_are_missing(spark_session: SparkSession):
    checks = [
        {
            "check": {
                "function": "col_is_not_null_and_not_empty"
                # missing arguments spec
            }
        }
    ]

    with pytest.raises(Exception):
        build_checks_by_metadata(checks)


def test_build_checks_by_metadata_when_function_does_not_exist(spark_session: SparkSession):
    checks = [{"check": {"function": "function_does_not_exists", "arguments": {"col_name": "a"}}}]

    with pytest.raises(Exception):
        build_checks_by_metadata(checks)
