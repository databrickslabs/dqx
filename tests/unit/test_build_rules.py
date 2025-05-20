import pprint
import logging
import pytest

from databricks.labs.dqx.row_checks import (
    is_not_null,
    is_not_null_and_not_empty,
    sql_expression,
    is_in_list,
    is_not_null_and_not_empty_array,
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
    )

    expected_rules = [
        DQColRule(
            name="col_a_is_null_or_empty", criticality="error", check_func=is_not_null_and_not_empty, col_name="a"
        ),
        DQColRule(
            name="col_b_is_null_or_empty", criticality="error", check_func=is_not_null_and_not_empty, col_name="b"
        ),
        DQColRule(
            name="col_c_is_not_in_the_list",
            criticality="error",
            check_func=is_in_list,
            col_name="c",
            check_func_args=[[1, 2]],
        ),
        DQColRule(
            name="col_d_is_not_in_the_list",
            criticality="error",
            check_func=is_in_list,
            col_name="d",
            check_func_args=[[1, 2]],
        ),
        DQColRule(
            name="col_e_is_not_in_the_list",
            criticality="warn",
            check_func=is_in_list,
            col_name="e",
            check_func_kwargs={"allowed": [3]},
        ),
        DQColRule(
            name="col_a_is_null_or_empty_array",
            criticality="error",
            check_func=is_not_null_and_not_empty_array,
            col_name="a",
        ),
        DQColRule(
            name="col_b_is_null_or_empty_array",
            criticality="error",
            check_func=is_not_null_and_not_empty_array,
            col_name="b",
        ),
        DQColRule(
            name="custom_common_name",
            criticality="error",
            check_func=is_not_null,
            col_name="a",
        ),
        DQColRule(
            name="custom_common_name",
            criticality="error",
            check_func=is_not_null,
            col_name="b",
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
    ) + [
        DQColRule(
            name="col_g_is_null_or_empty",
            criticality="warn",
            filter="a=0",
            check_func=is_not_null_and_not_empty,
            col_name="g",
        ),
        DQColRule(criticality="warn", check_func=is_in_list, col_name="h", check_func_args=[[1, 2]]),
    ]

    expected_rules = [
        DQColRule(
            name="col_a_is_null_or_empty",
            criticality="error",
            filter="c>0",
            check_func=is_not_null_and_not_empty,
            col_name="a",
        ),
        DQColRule(
            name="col_b_is_null_or_empty",
            criticality="error",
            filter="c>0",
            check_func=is_not_null_and_not_empty,
            col_name="b",
        ),
        DQColRule(
            name="col_c_is_null_or_empty", criticality="warn", check_func=is_not_null_and_not_empty, col_name="c"
        ),
        DQColRule(
            name="col_d_is_not_in_the_list",
            criticality="error",
            check_func=is_in_list,
            col_name="d",
            check_func_args=[[1, 2]],
        ),
        DQColRule(
            name="col_e_is_not_in_the_list",
            criticality="error",
            check_func=is_in_list,
            col_name="e",
            check_func_args=[[1, 2]],
        ),
        DQColRule(
            name="col_f_is_not_in_the_list",
            criticality="warn",
            check_func=is_in_list,
            col_name="f",
            check_func_kwargs={"allowed": [3]},
        ),
        DQColRule(
            name="col_a_is_null_or_empty_array",
            criticality="error",
            check_func=is_not_null_and_not_empty_array,
            col_name="a",
        ),
        DQColRule(
            name="col_b_is_null_or_empty_array",
            criticality="error",
            check_func=is_not_null_and_not_empty_array,
            col_name="b",
        ),
        DQColRule(
            name="col_c_is_null_or_empty_array",
            criticality="warn",
            check_func=is_not_null_and_not_empty_array,
            col_name="c",
        ),
        DQColRule(
            name="custom_common_name",
            criticality="error",
            check_func=is_not_null,
            col_name="a",
        ),
        DQColRule(
            name="custom_common_name",
            criticality="error",
            check_func=is_not_null,
            col_name="b",
        ),
        DQColRule(
            name="col_g_is_null_or_empty",
            criticality="warn",
            filter="a=0",
            check_func=is_not_null_and_not_empty,
            col_name="g",
        ),
        DQColRule(
            name="col_h_is_not_in_the_list",
            criticality="warn",
            check_func=is_in_list,
            col_name="h",
            check_func_args=[[1, 2]],
        ),
    ]

    assert pprint.pformat(actual_rules) == pprint.pformat(expected_rules)


def test_build_rules_by_metadata():
    checks = [
        {
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"col_names": ["a", "b"]}},
        },
        {
            "criticality": "warn",
            "filter": "a>0",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"col_names": ["c"]}},
        },
        {
            "criticality": "error",
            "filter": "c=0",
            "check": {"function": "is_in_list", "arguments": {"col_names": ["d", "e"], "allowed": [1, 2]}},
        },
        {
            "criticality": "warn",
            "check": {"function": "is_in_list", "arguments": {"col_names": ["f"], "allowed": [3]}},
        },
        {
            "name": "col_g_is_null_or_empty",
            "criticality": "warn",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"col_name": "g"}},
        },
        {
            "criticality": "warn",
            "check": {"function": "is_in_list", "arguments": {"col_name": "h", "allowed": [1, 2]}},
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
        {
            "name": "custom_common_name",
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"col_names": ["a", "b"]}},
        },
    ]

    actual_rules = DQEngineCore.build_checks_by_metadata(checks)

    expected_rules = [
        DQColRule(
            name="col_a_is_null_or_empty", criticality="error", check_func=is_not_null_and_not_empty, col_name="a"
        ),
        DQColRule(
            name="col_b_is_null_or_empty", criticality="error", check_func=is_not_null_and_not_empty, col_name="b"
        ),
        DQColRule(
            name="col_c_is_null_or_empty",
            criticality="warn",
            filter="a>0",
            check_func=is_not_null_and_not_empty,
            col_name="c",
        ),
        DQColRule(
            name="col_d_is_not_in_the_list",
            criticality="error",
            filter="c=0",
            check_func=is_in_list,
            col_name="d",
            check_func_kwargs={"allowed": [1, 2]},
        ),
        DQColRule(
            name="col_e_is_not_in_the_list",
            criticality="error",
            filter="c=0",
            check_func=is_in_list,
            col_name="e",
            check_func_kwargs={"allowed": [1, 2]},
        ),
        DQColRule(
            name="col_f_is_not_in_the_list",
            criticality="warn",
            check_func=is_in_list,
            col_name="f",
            check_func_kwargs={"allowed": [3]},
        ),
        DQColRule(
            name="col_g_is_null_or_empty", criticality="warn", check_func=is_not_null_and_not_empty, col_name="g"
        ),
        DQColRule(
            name="col_h_is_not_in_the_list",
            criticality="warn",
            check_func=is_in_list,
            col_name="h",
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
            col_name="a",
        ),
        DQColRule(
            name="col_b_is_null_or_empty_array",
            criticality="error",
            check_func=is_not_null_and_not_empty_array,
            col_name="b",
        ),
        DQColRule(
            name="col_c_is_null_or_empty_array",
            criticality="warn",
            check_func=is_not_null_and_not_empty_array,
            col_name="c",
        ),
        DQColRule(
            name="custom_common_name",
            criticality="error",
            check_func=is_not_null,
            col_name="a",
        ),
        DQColRule(
            name="custom_common_name",
            criticality="error",
            check_func=is_not_null,
            col_name="b",
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
    checks = [{"check": {"function": "function_does_not_exists", "arguments": {"col_name": "a"}}}]

    with pytest.raises(ValueError, match="function 'function_does_not_exists' is not defined"):
        DQEngineCore.build_checks_by_metadata(checks)


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
        DQEngineCore.build_checks_by_metadata(checks)
        assert "Resolving function: is_not_null_and_not_empty" in caplog.text


def test_validate_check_func_arguments_too_many_positional():
    with pytest.raises(TypeError, match="takes 2 positional arguments but 3 were given"):
        DQColRule(
            name="col_col1_is_not_in_the_list",
            criticality="error",
            check_func=is_in_list,
            col_name="col1",
            check_func_args=[[1, 2], "extra_arg"],
        )


def test_validate_check_func_arguments_invalid_keyword():
    with pytest.raises(TypeError, match="got an unexpected keyword argument 'invalid_kwarg'"):
        DQColRule(
            name="col_col1_is_not_in_the_list",
            criticality="error",
            check_func=is_in_list,
            col_name="col1",
            check_func_kwargs={"allowed": [3], "invalid_kwarg": "invalid_kwarg", "invalid_kwarg2": "invalid_kwarg2"},
        )


def test_deprecated_warning_dqrule_class():
    with pytest.warns(DeprecationWarning, match="DQRule is deprecated and will be removed in a future version"):
        DQRule(criticality="error", check_func=is_not_null, col_name="col1")


def test_deprecated_warning_dqrulecolset_class():
    with pytest.warns(DeprecationWarning, match="DQRuleColSet is deprecated and will be removed in a future version"):
        DQRuleColSet(criticality="error", check_func=is_not_null, columns=["col1"])


def test_build_quality_rules_from_dataframe(spark_local):
    test_checks = [
        {
            "name": "column_is_not_null",
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"col_name": "test_col"}},
        },
        {
            "name": "column_is_not_null_or_empty",
            "criticality": "warn",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"col_name": "test_col"}},
        },
        {
            "name": "column_not_less_than",
            "criticality": "warn",
            "check": {"function": "is_not_less_than", "arguments": {"col_name": "test_col", "limit": "5"}},
        },
    ]
    df = DQEngineCore.build_dataframe_from_quality_rules(test_checks, spark=spark_local)
    checks = DQEngineCore.build_quality_rules_from_dataframe(df)
    assert checks == test_checks, "The loaded checks do not match the expected checks."
