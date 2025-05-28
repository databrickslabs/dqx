import pprint
import logging
import pytest

from databricks.labs.dqx.row_checks import (
    is_not_null,
    is_not_null_and_not_empty,
    sql_expression,
    is_in_list,
    is_not_null_and_not_empty_array,
    is_unique,
)
from databricks.labs.dqx.rule import (
    DQRule,
    DQRuleColSet,
    DQColRule,
    DQColSetRule,
)
from databricks.labs.dqx.engine import DQEngineCore

SCHEMA = "a: int, b: int, c: int"


def test_build_rules_empty() -> None:
    actual_rules = DQEngineCore.build_checks()

    expected_rules: list[DQColRule] = []

    assert actual_rules == expected_rules


def test_get_rules():
    actual_rules = (
        # set of columns for the same check
        DQColSetRule(columns=["a", "b"], check_func=is_not_null_and_not_empty).get_rules()
        # with check function params provided as positional arguments
        + DQColSetRule(
            columns=["c", "d"], criticality="error", check_func=is_in_list, check_func_args=[[1, 2]]
        ).get_rules()
        # with check function params provided as named arguments
        + DQColSetRule(
            columns=["e"], criticality="warn", check_func=is_in_list, check_func_kwargs={"allowed": [3]}
        ).get_rules()
        # should be skipped
        + DQColSetRule(columns=[], criticality="error", check_func=is_not_null_and_not_empty).get_rules()
        # set of columns for the same check
        + DQColSetRule(columns=["a", "b"], check_func=is_not_null_and_not_empty_array).get_rules()
        # set of columns for the same check with the same custom name
        + DQColSetRule(columns=["a", "b"], check_func=is_not_null, name="custom_common_name").get_rules()
        # set of columns for check taking as input multiple columns
        + DQColSetRule(
            columns=[["a", "b"], ["c"]], check_func=is_unique, check_func_kwargs={"nulls_distinct": False}
        ).get_rules()
    )

    expected_rules = [
        DQColRule(name="col_a_is_null_or_empty", criticality="error", check_func=is_not_null_and_not_empty, column="a"),
        DQColRule(name="col_b_is_null_or_empty", criticality="error", check_func=is_not_null_and_not_empty, column="b"),
        DQColRule(
            name="col_c_is_not_in_the_list",
            criticality="error",
            check_func=is_in_list,
            column="c",
            check_func_args=[[1, 2]],
        ),
        DQColRule(
            name="col_d_is_not_in_the_list",
            criticality="error",
            check_func=is_in_list,
            column="d",
            check_func_args=[[1, 2]],
        ),
        DQColRule(
            name="col_e_is_not_in_the_list",
            criticality="warn",
            check_func=is_in_list,
            column="e",
            check_func_kwargs={"allowed": [3]},
        ),
        DQColRule(
            name="col_a_is_null_or_empty_array",
            criticality="error",
            check_func=is_not_null_and_not_empty_array,
            column="a",
        ),
        DQColRule(
            name="col_b_is_null_or_empty_array",
            criticality="error",
            check_func=is_not_null_and_not_empty_array,
            column="b",
        ),
        DQColRule(
            name="custom_common_name",
            criticality="error",
            check_func=is_not_null,
            column="a",
        ),
        DQColRule(
            name="custom_common_name",
            criticality="error",
            check_func=is_not_null,
            column="b",
        ),
        DQColRule(
            name="col_struct_a_b_is_not_unique",
            criticality="error",
            check_func=is_unique,
            columns=["a", "b"],
            check_func_kwargs={"nulls_distinct": False},
        ),
        DQColRule(
            name="col_struct_c_is_not_unique",
            criticality="error",
            check_func=is_unique,
            columns=["c"],
            check_func_kwargs={"nulls_distinct": False},
        ),
    ]

    assert pprint.pformat(actual_rules) == pprint.pformat(expected_rules)


def test_build_rules():
    actual_rules = DQEngineCore.build_checks(
        # set of columns for the same check
        DQColSetRule(columns=["a", "b"], criticality="error", filter="c>0", check_func=is_not_null_and_not_empty),
        DQColSetRule(columns=["c"], criticality="warn", check_func=is_not_null_and_not_empty),
        # with check function params provided as positional arguments
        DQColSetRule(columns=["d", "e"], criticality="error", check_func=is_in_list, check_func_args=[[1, 2]]),
        # with check function params provided as named arguments
        DQColSetRule(columns=["f"], criticality="warn", check_func=is_in_list, check_func_kwargs={"allowed": [3]}),
        # should be skipped
        DQColSetRule(columns=[], criticality="error", check_func=is_not_null_and_not_empty),
        # set of columns for the same check
        DQColSetRule(columns=["a", "b"], criticality="error", check_func=is_not_null_and_not_empty_array),
        DQColSetRule(columns=["c"], criticality="warn", check_func=is_not_null_and_not_empty_array),
        # set of columns for the same check with the same custom name
        DQColSetRule(columns=["a", "b"], check_func=is_not_null, name="custom_common_name"),
        # set of columns for check taking as input multiple columns
        DQColSetRule(columns=[["a", "b"], ["c"]], check_func=is_unique, check_func_kwargs={"nulls_distinct": False}),
    ) + [
        DQColRule(
            name="col_g_is_null_or_empty",
            criticality="warn",
            filter="a=0",
            check_func=is_not_null_and_not_empty,
            column="g",
        ),
        DQColRule(criticality="warn", check_func=is_in_list, column="h", check_func_args=[[1, 2]]),
    ]

    expected_rules = [
        DQColRule(
            name="col_a_is_null_or_empty",
            criticality="error",
            filter="c>0",
            check_func=is_not_null_and_not_empty,
            column="a",
        ),
        DQColRule(
            name="col_b_is_null_or_empty",
            criticality="error",
            filter="c>0",
            check_func=is_not_null_and_not_empty,
            column="b",
        ),
        DQColRule(name="col_c_is_null_or_empty", criticality="warn", check_func=is_not_null_and_not_empty, column="c"),
        DQColRule(
            name="col_d_is_not_in_the_list",
            criticality="error",
            check_func=is_in_list,
            column="d",
            check_func_args=[[1, 2]],
        ),
        DQColRule(
            name="col_e_is_not_in_the_list",
            criticality="error",
            check_func=is_in_list,
            column="e",
            check_func_args=[[1, 2]],
        ),
        DQColRule(
            name="col_f_is_not_in_the_list",
            criticality="warn",
            check_func=is_in_list,
            column="f",
            check_func_kwargs={"allowed": [3]},
        ),
        DQColRule(
            name="col_a_is_null_or_empty_array",
            criticality="error",
            check_func=is_not_null_and_not_empty_array,
            column="a",
        ),
        DQColRule(
            name="col_b_is_null_or_empty_array",
            criticality="error",
            check_func=is_not_null_and_not_empty_array,
            column="b",
        ),
        DQColRule(
            name="col_c_is_null_or_empty_array",
            criticality="warn",
            check_func=is_not_null_and_not_empty_array,
            column="c",
        ),
        DQColRule(
            name="custom_common_name",
            criticality="error",
            check_func=is_not_null,
            column="a",
        ),
        DQColRule(
            name="custom_common_name",
            criticality="error",
            check_func=is_not_null,
            column="b",
        ),
        DQColRule(
            name="col_struct_a_b_is_not_unique",
            criticality="error",
            check_func=is_unique,
            columns=["a", "b"],
            check_func_kwargs={"nulls_distinct": False},
        ),
        DQColRule(
            name="col_struct_c_is_not_unique",
            criticality="error",
            check_func=is_unique,
            columns=["c"],
            check_func_kwargs={"nulls_distinct": False},
        ),
        DQColRule(
            name="col_g_is_null_or_empty",
            criticality="warn",
            filter="a=0",
            check_func=is_not_null_and_not_empty,
            column="g",
        ),
        DQColRule(
            name="col_h_is_not_in_the_list",
            criticality="warn",
            check_func=is_in_list,
            column="h",
            check_func_args=[[1, 2]],
        ),
    ]

    assert pprint.pformat(actual_rules) == pprint.pformat(expected_rules)


def test_build_rules_by_metadata():
    checks = [
        {
            "check": {"function": "is_not_null_and_not_empty", "for_each_column": ["a", "b"], "arguments": {}},
        },
        {
            "criticality": "warn",
            "filter": "a>0",
            "check": {
                "function": "is_not_null_and_not_empty",
                "for_each_column": ["c"],
                "arguments": {"column": "another"},
            },
        },
        {
            "criticality": "error",
            "filter": "c=0",
            "check": {"function": "is_in_list", "for_each_column": ["d", "e"], "arguments": {"allowed": [1, 2]}},
        },
        {
            "criticality": "warn",
            "check": {"function": "is_in_list", "for_each_column": ["f"], "arguments": {"allowed": [3]}},
        },
        {
            "name": "col_g_is_null_or_empty",
            "criticality": "warn",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"column": "g"}},
        },
        {
            "criticality": "warn",
            "check": {"function": "is_in_list", "arguments": {"column": "h", "allowed": [1, 2]}},
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
            "check": {"function": "is_not_null_and_not_empty_array", "for_each_column": ["a", "b"]},
        },
        {
            "criticality": "warn",
            "check": {"function": "is_not_null_and_not_empty_array", "for_each_column": ["c"], "arguments": {}},
        },
        {
            "name": "custom_common_name",
            "criticality": "error",
            "check": {"function": "is_not_null", "for_each_column": ["a", "b"], "arguments": {}},
        },
        {
            "criticality": "error",
            "check": {
                "function": "is_unique",
                "for_each_column": [["a", "b"], ["c"]],
                "arguments": {"nulls_distinct": True},
            },
        },
    ]

    actual_rules = DQEngineCore.build_checks_by_metadata(checks)

    expected_rules = [
        DQColRule(name="col_a_is_null_or_empty", criticality="error", check_func=is_not_null_and_not_empty, column="a"),
        DQColRule(name="col_b_is_null_or_empty", criticality="error", check_func=is_not_null_and_not_empty, column="b"),
        DQColRule(
            name="col_c_is_null_or_empty",
            criticality="warn",
            filter="a>0",
            check_func=is_not_null_and_not_empty,
            column="c",
        ),
        DQColRule(
            name="col_d_is_not_in_the_list",
            criticality="error",
            filter="c=0",
            check_func=is_in_list,
            column="d",
            check_func_kwargs={"allowed": [1, 2]},
        ),
        DQColRule(
            name="col_e_is_not_in_the_list",
            criticality="error",
            filter="c=0",
            check_func=is_in_list,
            column="e",
            check_func_kwargs={"allowed": [1, 2]},
        ),
        DQColRule(
            name="col_f_is_not_in_the_list",
            criticality="warn",
            check_func=is_in_list,
            column="f",
            check_func_kwargs={"allowed": [3]},
        ),
        DQColRule(name="col_g_is_null_or_empty", criticality="warn", check_func=is_not_null_and_not_empty, column="g"),
        DQColRule(
            name="col_h_is_not_in_the_list",
            criticality="warn",
            check_func=is_in_list,
            column="h",
            check_func_kwargs={"allowed": [1, 2]},
        ),
        DQColRule(
            name="d_not_in_a",
            criticality="error",
            check_func=sql_expression,
            check_func_kwargs={"expression": "a != substring(b, 8, 1)", "msg": "a not found in b"},
        ),
        DQColRule(
            name="col_a_is_null_or_empty_array",
            criticality="error",
            check_func=is_not_null_and_not_empty_array,
            column="a",
        ),
        DQColRule(
            name="col_b_is_null_or_empty_array",
            criticality="error",
            check_func=is_not_null_and_not_empty_array,
            column="b",
        ),
        DQColRule(
            name="col_c_is_null_or_empty_array",
            criticality="warn",
            check_func=is_not_null_and_not_empty_array,
            column="c",
        ),
        DQColRule(
            name="custom_common_name",
            criticality="error",
            check_func=is_not_null,
            column="a",
        ),
        DQColRule(
            name="custom_common_name",
            criticality="error",
            check_func=is_not_null,
            column="b",
        ),
        DQColRule(
            name="col_struct_a_b_is_not_unique",
            criticality="error",
            check_func=is_unique,
            columns=["a", "b"],
            check_func_kwargs={"nulls_distinct": True},
        ),
        DQColRule(
            name="col_struct_c_is_not_unique",
            criticality="error",
            check_func=is_unique,
            columns=["c"],
            check_func_kwargs={"nulls_distinct": True},
        ),
    ]

    assert pprint.pformat(actual_rules) == pprint.pformat(expected_rules)


def test_build_checks_by_metadata_when_check_spec_is_missing() -> None:
    checks: list[dict] = [{}]  # missing check spec

    with pytest.raises(ValueError, match="'check' field is missing"):
        DQEngineCore.build_checks_by_metadata(checks)


def test_build_checks_by_metadata_when_function_spec_is_missing() -> None:
    checks: list[dict] = [{"check": {}}]  # missing func spec

    with pytest.raises(ValueError, match="'function' field is missing in the 'check' block"):
        DQEngineCore.build_checks_by_metadata(checks)


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
        DQEngineCore.build_checks_by_metadata(checks)


def test_build_checks_by_metadata_when_function_does_not_exist():
    checks = [{"check": {"function": "function_does_not_exists", "arguments": {"column": "a"}}}]

    with pytest.raises(ValueError, match="function 'function_does_not_exists' is not defined"):
        DQEngineCore.build_checks_by_metadata(checks)


def test_build_checks_by_metadata_logging_debug_calls(caplog):
    checks = [
        {
            "criticality": "error",
            "check": {"function": "is_not_null_and_not_empty", "for_each_column": ["a", "b"], "arguments": {}},
        }
    ]
    logger = logging.getLogger("databricks.labs.dqx.engine")
    logger.setLevel(logging.DEBUG)
    with caplog.at_level("DEBUG"):
        DQEngineCore.build_checks_by_metadata(checks)
        assert "Resolving function: is_not_null_and_not_empty" in caplog.text


def test_validate_check_func_arguments_too_many_positional():
    with pytest.raises(TypeError, match="takes 2 positional arguments but 3 were given"):
        DQColRule(
            name="col_col1_is_not_in_the_list",
            criticality="error",
            check_func=is_in_list,
            column="col1",
            check_func_args=[[1, 2], "extra_arg"],
        )


def test_validate_check_func_arguments_invalid_keyword():
    with pytest.raises(TypeError, match="got an unexpected keyword argument 'invalid_kwarg'"):
        DQColRule(
            name="col_col1_is_not_in_the_list",
            criticality="error",
            check_func=is_in_list,
            column="col1",
            check_func_kwargs={"allowed": [3], "invalid_kwarg": "invalid_kwarg", "invalid_kwarg2": "invalid_kwarg2"},
        )


def test_deprecated_warning_dqrule_class():
    with pytest.warns(DeprecationWarning, match="DQRule is deprecated and will be removed in a future version"):
        DQRule(criticality="error", check_func=is_not_null, column="col1")


def test_deprecated_warning_dqrulecolset_class():
    with pytest.warns(DeprecationWarning, match="DQRuleColSet is deprecated and will be removed in a future version"):
        DQRuleColSet(criticality="error", check_func=is_not_null, columns=["col1"])


def test_validate_check_fail_when_column_and_columns_provided():
    with pytest.raises(ValueError, match="Invalid initialization: Only one of `column` or `columns` must be set."):
        DQColRule(
            criticality="error",
            check_func=is_not_null,
            column="a",
            columns=["b"],
        )
