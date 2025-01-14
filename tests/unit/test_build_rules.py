import pprint
import logging
import pytest

from databricks.labs.dqx.col_functions import (
    is_not_null_and_not_empty,
    sql_expression,
    value_is_in_list,
    is_not_null_and_not_empty_array,
)
from databricks.labs.dqx.engine import (
    DQRule,
    DQRuleColSet,
    DQEngine,
)

SCHEMA = "a: int, b: int, c: int"


def test_build_rules_empty() -> None:
    actual_rules = DQEngine.build_checks()

    expected_rules: list[DQRule] = []

    assert actual_rules == expected_rules


def test_get_rules():
    actual_rules = (
        # set of columns for the same check
        DQRuleColSet(columns=["a", "b"], check_func=is_not_null_and_not_empty).get_rules()
        # with check function params provided as positional arguments
        + DQRuleColSet(
            columns=["c", "d"], criticality="error", check_func=value_is_in_list, check_func_args=[[1, 2]]
        ).get_rules()
        # with check function params provided as named arguments
        + DQRuleColSet(
            columns=["e"], criticality="warn", check_func=value_is_in_list, check_func_kwargs={"allowed": [3]}
        ).get_rules()
        # should be skipped
        + DQRuleColSet(columns=[], criticality="error", check_func=is_not_null_and_not_empty).get_rules()
        # set of columns for the same check
        + DQRuleColSet(columns=["a", "b"], check_func=is_not_null_and_not_empty_array).get_rules()
    )

    expected_rules = [
        DQRule(name="col_a_is_null_or_empty", criticality="error", check=is_not_null_and_not_empty("a")),
        DQRule(name="col_b_is_null_or_empty", criticality="error", check=is_not_null_and_not_empty("b")),
        DQRule(name="col_c_value_is_not_in_the_list", criticality="error", check=value_is_in_list("c", allowed=[1, 2])),
        DQRule(name="col_d_value_is_not_in_the_list", criticality="error", check=value_is_in_list("d", allowed=[1, 2])),
        DQRule(name="col_e_value_is_not_in_the_list", criticality="warn", check=value_is_in_list("e", allowed=[3])),
        DQRule(name="col_a_is_null_or_empty_array", criticality="error", check=is_not_null_and_not_empty_array("a")),
        DQRule(name="col_b_is_null_or_empty_array", criticality="error", check=is_not_null_and_not_empty_array("b")),
    ]

    assert pprint.pformat(actual_rules) == pprint.pformat(expected_rules)


def test_build_rules():
    actual_rules = DQEngine.build_checks(
        # set of columns for the same check
        DQRuleColSet(columns=["a", "b"], criticality="error", check_func=is_not_null_and_not_empty),
        DQRuleColSet(columns=["c"], criticality="warn", check_func=is_not_null_and_not_empty),
        # with check function params provided as positional arguments
        DQRuleColSet(columns=["d", "e"], criticality="error", check_func=value_is_in_list, check_func_args=[[1, 2]]),
        # with check function params provided as named arguments
        DQRuleColSet(
            columns=["f"], criticality="warn", check_func=value_is_in_list, check_func_kwargs={"allowed": [3]}
        ),
        # should be skipped
        DQRuleColSet(columns=[], criticality="error", check_func=is_not_null_and_not_empty),
        # set of columns for the same check
        DQRuleColSet(columns=["a", "b"], criticality="error", check_func=is_not_null_and_not_empty_array),
        DQRuleColSet(columns=["c"], criticality="warn", check_func=is_not_null_and_not_empty_array),
    ) + [
        DQRule(name="col_g_is_null_or_empty", criticality="warn", check=is_not_null_and_not_empty("g")),
        DQRule(criticality="warn", check=value_is_in_list("h", allowed=[1, 2])),
    ]

    expected_rules = [
        DQRule(name="col_a_is_null_or_empty", criticality="error", check=is_not_null_and_not_empty("a")),
        DQRule(name="col_b_is_null_or_empty", criticality="error", check=is_not_null_and_not_empty("b")),
        DQRule(name="col_c_is_null_or_empty", criticality="warn", check=is_not_null_and_not_empty("c")),
        DQRule(name="col_d_value_is_not_in_the_list", criticality="error", check=value_is_in_list("d", allowed=[1, 2])),
        DQRule(name="col_e_value_is_not_in_the_list", criticality="error", check=value_is_in_list("e", allowed=[1, 2])),
        DQRule(name="col_f_value_is_not_in_the_list", criticality="warn", check=value_is_in_list("f", allowed=[3])),
        DQRule(name="col_a_is_null_or_empty_array", criticality="error", check=is_not_null_and_not_empty_array("a")),
        DQRule(name="col_b_is_null_or_empty_array", criticality="error", check=is_not_null_and_not_empty_array("b")),
        DQRule(name="col_c_is_null_or_empty_array", criticality="warn", check=is_not_null_and_not_empty_array("c")),
        DQRule(name="col_g_is_null_or_empty", criticality="warn", check=is_not_null_and_not_empty("g")),
        DQRule(name="col_h_value_is_not_in_the_list", criticality="warn", check=value_is_in_list("h", allowed=[1, 2])),
    ]

    assert pprint.pformat(actual_rules) == pprint.pformat(expected_rules)


def test_build_rules_by_metadata():
    checks = [
        {
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"col_names": ["a", "b"]}},
        },
        {
            "criticality": "warn",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"col_names": ["c"]}},
        },
        {
            "criticality": "error",
            "check": {"function": "value_is_in_list", "arguments": {"col_names": ["d", "e"], "allowed": [1, 2]}},
        },
        {
            "criticality": "warn",
            "check": {"function": "value_is_in_list", "arguments": {"col_names": ["f"], "allowed": [3]}},
        },
        {
            "name": "col_g_is_null_or_empty",
            "criticality": "warn",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"col_name": "g"}},
        },
        {
            "criticality": "warn",
            "check": {"function": "value_is_in_list", "arguments": {"col_name": "h", "allowed": [1, 2]}},
        },
        {
            "name": "d_not_in_a",
            "criticality": "error",
            "check": {
                "function": "sql_expression",
                "arguments": {"expression": "a != substring(b, 8, 1)", "msg": "a not found in b"},
            },
        },
        {
            "check": {"function": "is_not_null_and_not_empty_array", "arguments": {"col_names": ["a", "b"]}},
        },
        {
            "criticality": "warn",
            "check": {"function": "is_not_null_and_not_empty_array", "arguments": {"col_names": ["c"]}},
        },
    ]

    actual_rules = DQEngine.build_checks_by_metadata(checks)

    expected_rules = [
        DQRule(name="col_a_is_null_or_empty", criticality="error", check=is_not_null_and_not_empty("a")),
        DQRule(name="col_b_is_null_or_empty", criticality="error", check=is_not_null_and_not_empty("b")),
        DQRule(name="col_c_is_null_or_empty", criticality="warn", check=is_not_null_and_not_empty("c")),
        DQRule(name="col_d_value_is_not_in_the_list", criticality="error", check=value_is_in_list("d", allowed=[1, 2])),
        DQRule(name="col_e_value_is_not_in_the_list", criticality="error", check=value_is_in_list("e", allowed=[1, 2])),
        DQRule(name="col_f_value_is_not_in_the_list", criticality="warn", check=value_is_in_list("f", allowed=[3])),
        DQRule(name="col_g_is_null_or_empty", criticality="warn", check=is_not_null_and_not_empty("g")),
        DQRule(name="col_h_value_is_not_in_the_list", criticality="warn", check=value_is_in_list("h", allowed=[1, 2])),
        DQRule(
            name="d_not_in_a",
            criticality="error",
            check=sql_expression(expression="a != substring(b, 8, 1)", msg="a not found in b"),
        ),
        DQRule(name="col_a_is_null_or_empty_array", criticality="error", check=is_not_null_and_not_empty_array("a")),
        DQRule(name="col_b_is_null_or_empty_array", criticality="error", check=is_not_null_and_not_empty_array("b")),
        DQRule(name="col_c_is_null_or_empty_array", criticality="warn", check=is_not_null_and_not_empty_array("c")),
    ]

    assert pprint.pformat(actual_rules) == pprint.pformat(expected_rules)


def test_build_checks_by_metadata_when_check_spec_is_missing() -> None:
    checks: list[dict] = [{}]  # missing check spec

    with pytest.raises(ValueError, match="'check' field is missing"):
        DQEngine.build_checks_by_metadata(checks)


def test_build_checks_by_metadata_when_function_spec_is_missing() -> None:
    checks: list[dict] = [{"check": {}}]  # missing func spec

    with pytest.raises(ValueError, match="'function' field is missing in the 'check' block"):
        DQEngine.build_checks_by_metadata(checks)


def test_build_checks_by_metadata_when_arguments_are_missing():
    checks = [
        {
            "check": {
                "function": "is_not_null_and_not_empty"
                # missing arguments spec
            }
        }
    ]

    with pytest.raises(
        ValueError, match="No arguments provided for function 'is_not_null_and_not_empty' in the 'arguments' block"
    ):
        DQEngine.build_checks_by_metadata(checks)


def test_build_checks_by_metadata_when_function_does_not_exist():
    checks = [{"check": {"function": "function_does_not_exists", "arguments": {"col_name": "a"}}}]

    with pytest.raises(ValueError, match="function 'function_does_not_exists' is not defined"):
        DQEngine.build_checks_by_metadata(checks)


def test_build_checks_by_metadata_logging_debug_calls(caplog):
    checks = [
        {
            "criticality": "error",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"col_names": ["a", "b"]}},
        }
    ]
    logger = logging.getLogger("databricks.labs.dqx.engine")
    logger.setLevel(logging.DEBUG)
    with caplog.at_level("DEBUG"):
        DQEngine.build_checks_by_metadata(checks)
        assert "Resolving function: is_not_null_and_not_empty" in caplog.text
