import pprint
import logging
import pytest

from databricks.labs.dqx.check_funcs import (
    is_not_null,
    is_not_null_and_not_empty,
    sql_expression,
    is_in_list,
    is_not_null_and_not_empty_array,
    is_unique,
    is_aggr_not_greater_than,
    is_aggr_not_less_than,
    foreign_key,
)
from databricks.labs.dqx.rule import (
    DQForEachColRule,
    DQRowRule,
    DQRule,
    CHECK_FUNC_REGISTRY,
    register_rule,
    DQDatasetRule,
)
from databricks.labs.dqx.engine import DQEngineCore

SCHEMA = "a: int, b: int, c: int"


def test_build_rules_empty() -> None:
    actual_rules = DQEngineCore.build_quality_rules_foreach_col()

    expected_rules: list[DQRule] = []

    assert actual_rules == expected_rules


def test_get_rules():
    actual_rules = (
        # set of columns for the same check
        DQForEachColRule(
            columns=["a", "b"],
            check_func=is_not_null_and_not_empty,
            user_metadata={"check_type": "completeness", "check_owner": "someone@email.com"},
        ).get_rules()
        # with check function params provided as positional arguments
        + DQForEachColRule(
            columns=["c", "d"], criticality="error", check_func=is_in_list, check_func_args=[[1, 2]]
        ).get_rules()
        # with check function params provided as named arguments
        + DQForEachColRule(
            columns=["e"], criticality="warn", check_func=is_in_list, check_func_kwargs={"allowed": [3]}
        ).get_rules()
        # should be skipped
        + DQForEachColRule(columns=[], criticality="error", check_func=is_not_null_and_not_empty).get_rules()
        # set of columns for the same check
        + DQForEachColRule(columns=["a", "b"], check_func=is_not_null_and_not_empty_array).get_rules()
        # set of columns for the same check with the same custom name
        + DQForEachColRule(columns=["a", "b"], check_func=is_not_null, name="custom_common_name").get_rules()
        # set of columns for check taking as input multiple columns
        + DQForEachColRule(
            columns=[["a", "b"], ["c"]], check_func=is_unique, check_func_kwargs={"nulls_distinct": False}
        ).get_rules()
    )

    expected_rules = [
        DQRowRule(
            name="a_is_null_or_empty",
            criticality="error",
            check_func=is_not_null_and_not_empty,
            column="a",
            user_metadata={"check_type": "completeness", "check_owner": "someone@email.com"},
        ),
        DQRowRule(
            name="b_is_null_or_empty",
            criticality="error",
            check_func=is_not_null_and_not_empty,
            column="b",
            user_metadata={"check_type": "completeness", "check_owner": "someone@email.com"},
        ),
        DQRowRule(
            name="c_is_not_in_the_list",
            criticality="error",
            check_func=is_in_list,
            column="c",
            check_func_args=[[1, 2]],
        ),
        DQRowRule(
            name="d_is_not_in_the_list",
            criticality="error",
            check_func=is_in_list,
            column="d",
            check_func_args=[[1, 2]],
        ),
        DQRowRule(
            name="e_is_not_in_the_list",
            criticality="warn",
            check_func=is_in_list,
            column="e",
            check_func_kwargs={"allowed": [3]},
        ),
        DQRowRule(
            name="a_is_null_or_empty_array",
            criticality="error",
            check_func=is_not_null_and_not_empty_array,
            column="a",
        ),
        DQRowRule(
            name="b_is_null_or_empty_array",
            criticality="error",
            check_func=is_not_null_and_not_empty_array,
            column="b",
        ),
        DQRowRule(
            name="custom_common_name",
            criticality="error",
            check_func=is_not_null,
            column="a",
        ),
        DQRowRule(
            name="custom_common_name",
            criticality="error",
            check_func=is_not_null,
            column="b",
        ),
        DQDatasetRule(
            name="struct_a_b_is_not_unique",
            criticality="error",
            check_func=is_unique,
            columns=["a", "b"],
            check_func_kwargs={"nulls_distinct": False},
        ),
        DQDatasetRule(
            name="struct_c_is_not_unique",
            criticality="error",
            check_func=is_unique,
            columns=["c"],
            check_func_kwargs={"nulls_distinct": False},
        ),
    ]

    assert pprint.pformat(actual_rules) == pprint.pformat(expected_rules)


def test_build_rules():
    actual_rules = DQEngineCore.build_quality_rules_foreach_col(
        # set of columns for the same check
        DQForEachColRule(
            columns=["a", "b"],
            criticality="error",
            filter="c>0",
            check_func=is_not_null_and_not_empty,
            user_metadata={"check_type": "completeness", "check_owner": "someone@email.com"},
        ),
        DQForEachColRule(columns=["c"], criticality="warn", check_func=is_not_null_and_not_empty),
        # with check function params provided as positional arguments
        DQForEachColRule(columns=["d", "e"], criticality="error", check_func=is_in_list, check_func_args=[[1, 2]]),
        # with check function params provided as named arguments
        DQForEachColRule(columns=["f"], criticality="warn", check_func=is_in_list, check_func_kwargs={"allowed": [3]}),
        # should be skipped
        DQForEachColRule(columns=[], criticality="error", check_func=is_not_null_and_not_empty),
        # set of columns for the same check
        DQForEachColRule(columns=["a", "b"], criticality="error", check_func=is_not_null_and_not_empty_array),
        DQForEachColRule(columns=["c"], criticality="warn", check_func=is_not_null_and_not_empty_array),
        # set of columns for the same check with the same custom name
        DQForEachColRule(columns=["a", "b"], check_func=is_not_null, name="custom_common_name"),
        # set of columns for check taking as input multiple columns
        DQForEachColRule(
            columns=[["a", "b"], ["c"]], check_func=is_unique, check_func_kwargs={"nulls_distinct": False}
        ),
        DQForEachColRule(
            name="is_unique_with_filter",
            columns=[["a", "b"], ["c"]],
            filter="a > b",
            check_func=is_unique,
            check_func_kwargs={"nulls_distinct": False},
        ),
        DQForEachColRule(
            name="count_aggr_greater_than",
            columns=["a", "*"],
            filter="a > b",
            check_func=is_aggr_not_greater_than,
            check_func_kwargs={"limit": 1, "group_by": ["c"], "aggr_type": "count"},
        ),
        DQForEachColRule(
            name="count_aggr_less_than",
            columns=["a", "*"],
            filter="a > b",
            check_func=is_aggr_not_less_than,
            check_func_kwargs={"limit": 1, "group_by": ["c"], "aggr_type": "count"},
        ),
        DQForEachColRule(
            name="foreign_key",
            criticality="warn",
            columns=[["a"], ["c"]],
            filter="a > b",
            check_func=foreign_key,
            check_func_kwargs={"ref_columns": ["ref_a"], "ref_df_name": "ref_df_key"},
        ),
    ) + [
        DQRowRule(
            name="g_is_null_or_empty",
            criticality="warn",
            filter="a=0",
            check_func=is_not_null_and_not_empty,
            column="g",
        ),
        DQRowRule(criticality="warn", check_func=is_in_list, column="h", check_func_args=[[1, 2]]),
    ]

    expected_rules = [
        DQRowRule(
            name="a_is_null_or_empty",
            criticality="error",
            filter="c>0",
            check_func=is_not_null_and_not_empty,
            column="a",
            user_metadata={"check_type": "completeness", "check_owner": "someone@email.com"},
        ),
        DQRowRule(
            name="b_is_null_or_empty",
            criticality="error",
            filter="c>0",
            check_func=is_not_null_and_not_empty,
            column="b",
            user_metadata={"check_type": "completeness", "check_owner": "someone@email.com"},
        ),
        DQRowRule(name="c_is_null_or_empty", criticality="warn", check_func=is_not_null_and_not_empty, column="c"),
        DQRowRule(
            name="d_is_not_in_the_list",
            criticality="error",
            check_func=is_in_list,
            column="d",
            check_func_args=[[1, 2]],
        ),
        DQRowRule(
            name="e_is_not_in_the_list",
            criticality="error",
            check_func=is_in_list,
            column="e",
            check_func_args=[[1, 2]],
        ),
        DQRowRule(
            name="f_is_not_in_the_list",
            criticality="warn",
            check_func=is_in_list,
            column="f",
            check_func_kwargs={"allowed": [3]},
        ),
        DQRowRule(
            name="a_is_null_or_empty_array",
            criticality="error",
            check_func=is_not_null_and_not_empty_array,
            column="a",
        ),
        DQRowRule(
            name="b_is_null_or_empty_array",
            criticality="error",
            check_func=is_not_null_and_not_empty_array,
            column="b",
        ),
        DQRowRule(
            name="c_is_null_or_empty_array",
            criticality="warn",
            check_func=is_not_null_and_not_empty_array,
            column="c",
        ),
        DQRowRule(
            name="custom_common_name",
            criticality="error",
            check_func=is_not_null,
            column="a",
        ),
        DQRowRule(
            name="custom_common_name",
            criticality="error",
            check_func=is_not_null,
            column="b",
        ),
        DQDatasetRule(
            name="struct_a_b_is_not_unique",
            criticality="error",
            check_func=is_unique,
            columns=["a", "b"],
            check_func_kwargs={"nulls_distinct": False},
        ),
        DQDatasetRule(
            name="struct_c_is_not_unique",
            criticality="error",
            check_func=is_unique,
            columns=["c"],
            check_func_kwargs={"nulls_distinct": False},
        ),
        DQDatasetRule(
            name="is_unique_with_filter",
            criticality="error",
            check_func=is_unique,
            columns=["a", "b"],
            filter="a > b",
            check_func_kwargs={"nulls_distinct": False},
        ),
        DQDatasetRule(
            name="is_unique_with_filter",
            criticality="error",
            check_func=is_unique,
            columns=["c"],
            filter="a > b",
            check_func_kwargs={"nulls_distinct": False},
        ),
        DQDatasetRule(
            name="count_aggr_greater_than",
            criticality="error",
            check_func=is_aggr_not_greater_than,
            column="a",
            filter="a > b",
            check_func_kwargs={"limit": 1, "group_by": ["c"], "aggr_type": "count"},
        ),
        DQDatasetRule(
            name="count_aggr_greater_than",
            criticality="error",
            check_func=is_aggr_not_greater_than,
            column="*",
            filter="a > b",
            check_func_kwargs={"limit": 1, "group_by": ["c"], "aggr_type": "count"},
        ),
        DQDatasetRule(
            name="count_aggr_less_than",
            criticality="error",
            check_func=is_aggr_not_less_than,
            column="a",
            filter="a > b",
            check_func_kwargs={"limit": 1, "group_by": ["c"], "aggr_type": "count"},
        ),
        DQDatasetRule(
            name="count_aggr_less_than",
            criticality="error",
            check_func=is_aggr_not_less_than,
            column="*",
            filter="a > b",
            check_func_kwargs={"limit": 1, "group_by": ["c"], "aggr_type": "count"},
        ),
        DQDatasetRule(
            name="foreign_key",
            criticality="warn",
            check_func=foreign_key,
            columns=["a"],
            filter="a > b",
            check_func_kwargs={
                "ref_columns": ["ref_a"],
                "ref_df_name": "ref_df_key",
            },
        ),
        DQDatasetRule(
            name="foreign_key",
            criticality="warn",
            check_func=foreign_key,
            columns=["c"],
            filter="a > b",
            check_func_kwargs={
                "ref_columns": ["ref_a"],
                "ref_df_name": "ref_df_key",
            },
        ),
        DQRowRule(
            name="g_is_null_or_empty",
            criticality="warn",
            filter="a=0",
            check_func=is_not_null_and_not_empty,
            column="g",
        ),
        DQRowRule(
            name="h_is_not_in_the_list",
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
            "check": {
                "function": "is_not_null_and_not_empty",
                "for_each_column": ["a", "b"],
                "arguments": {},
            },
            "user_metadata": {"check_type": "completeness", "check_owner": "someone@email.com"},
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
            "name": "g_is_null_or_empty",
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
        {
            "name": "is_not_unique_with_filter",
            "criticality": "error",
            "filter": "a > b",
            "check": {
                "function": "is_unique",
                "for_each_column": [["a", "b"], ["c"]],
                "arguments": {"nulls_distinct": True},
            },
        },
        {
            "criticality": "error",
            "check": {
                "function": "is_aggr_not_greater_than",
                "for_each_column": ["a", "*"],
                "arguments": {"limit": 1, "aggr_type": "count", "group_by": ["c"]},
            },
        },
        {
            "name": "count_group_by_c_greater_than_limit_with_filter",
            "criticality": "error",
            "filter": "a > b",
            "check": {
                "function": "is_aggr_not_greater_than",
                "for_each_column": ["a", "*"],
                "arguments": {"limit": 1, "aggr_type": "count", "group_by": ["c"]},
            },
        },
        {
            "criticality": "error",
            "check": {
                "function": "is_aggr_not_less_than",
                "for_each_column": ["a", "*"],
                "arguments": {"limit": 1, "aggr_type": "count", "group_by": ["c"]},
            },
        },
        {
            "name": "foreign_key",
            "criticality": "warn",
            "filter": "a > b",
            "check": {
                "function": "foreign_key",
                "for_each_column": [["a"], ["c"]],
                "arguments": {"ref_columns": ["ref_a"], "ref_df_name": "ref_df_key"},
            },
        },
    ]

    actual_rules = DQEngineCore.build_quality_rules_by_metadata(checks)

    expected_rules = [
        DQRowRule(
            name="a_is_null_or_empty",
            criticality="error",
            check_func=is_not_null_and_not_empty,
            column="a",
            user_metadata={"check_type": "completeness", "check_owner": "someone@email.com"},
        ),
        DQRowRule(
            name="b_is_null_or_empty",
            criticality="error",
            check_func=is_not_null_and_not_empty,
            column="b",
            user_metadata={"check_type": "completeness", "check_owner": "someone@email.com"},
        ),
        DQRowRule(
            name="c_is_null_or_empty",
            criticality="warn",
            filter="a>0",
            check_func=is_not_null_and_not_empty,
            column="c",
        ),
        DQRowRule(
            name="d_is_not_in_the_list",
            criticality="error",
            filter="c=0",
            check_func=is_in_list,
            column="d",
            check_func_kwargs={"allowed": [1, 2]},
        ),
        DQRowRule(
            name="e_is_not_in_the_list",
            criticality="error",
            filter="c=0",
            check_func=is_in_list,
            column="e",
            check_func_kwargs={"allowed": [1, 2]},
        ),
        DQRowRule(
            name="f_is_not_in_the_list",
            criticality="warn",
            check_func=is_in_list,
            column="f",
            check_func_kwargs={"allowed": [3]},
        ),
        DQRowRule(name="g_is_null_or_empty", criticality="warn", check_func=is_not_null_and_not_empty, column="g"),
        DQRowRule(
            name="h_is_not_in_the_list",
            criticality="warn",
            check_func=is_in_list,
            column="h",
            check_func_kwargs={"allowed": [1, 2]},
        ),
        DQRowRule(
            name="d_not_in_a",
            criticality="error",
            check_func=sql_expression,
            check_func_kwargs={"expression": "a != substring(b, 8, 1)", "msg": "a not found in b"},
        ),
        DQRowRule(
            name="a_is_null_or_empty_array",
            criticality="error",
            check_func=is_not_null_and_not_empty_array,
            column="a",
        ),
        DQRowRule(
            name="b_is_null_or_empty_array",
            criticality="error",
            check_func=is_not_null_and_not_empty_array,
            column="b",
        ),
        DQRowRule(
            name="c_is_null_or_empty_array",
            criticality="warn",
            check_func=is_not_null_and_not_empty_array,
            column="c",
        ),
        DQRowRule(
            name="custom_common_name",
            criticality="error",
            check_func=is_not_null,
            column="a",
        ),
        DQRowRule(
            name="custom_common_name",
            criticality="error",
            check_func=is_not_null,
            column="b",
        ),
        DQDatasetRule(
            name="struct_a_b_is_not_unique",
            criticality="error",
            check_func=is_unique,
            columns=["a", "b"],
            check_func_kwargs={"nulls_distinct": True},
        ),
        DQDatasetRule(
            name="struct_c_is_not_unique",
            criticality="error",
            check_func=is_unique,
            columns=["c"],
            check_func_kwargs={"nulls_distinct": True},
        ),
        DQDatasetRule(
            name="is_not_unique_with_filter",
            criticality="error",
            check_func=is_unique,
            columns=["a", "b"],
            filter="a > b",
            check_func_kwargs={"nulls_distinct": True},
        ),
        DQDatasetRule(
            name="is_not_unique_with_filter",
            criticality="error",
            check_func=is_unique,
            columns=["c"],
            filter="a > b",
            check_func_kwargs={"nulls_distinct": True},
        ),
        DQDatasetRule(
            name="a_count_group_by_c_greater_than_limit",
            criticality="error",
            check_func=is_aggr_not_greater_than,
            column="a",
            check_func_kwargs={"limit": 1, "aggr_type": "count", "group_by": ["c"]},
        ),
        DQDatasetRule(
            name="count_group_by_c_greater_than_limit",
            criticality="error",
            check_func=is_aggr_not_greater_than,
            column="*",
            check_func_kwargs={"limit": 1, "aggr_type": "count", "group_by": ["c"]},
        ),
        DQDatasetRule(
            name="count_group_by_c_greater_than_limit_with_filter",
            criticality="error",
            check_func=is_aggr_not_greater_than,
            column="a",
            filter="a > b",
            check_func_kwargs={"limit": 1, "aggr_type": "count", "group_by": ["c"]},
        ),
        DQDatasetRule(
            name="count_group_by_c_greater_than_limit_with_filter",
            criticality="error",
            check_func=is_aggr_not_greater_than,
            column="*",
            filter="a > b",
            check_func_kwargs={"limit": 1, "aggr_type": "count", "group_by": ["c"]},
        ),
        DQDatasetRule(
            name="a_count_group_by_c_less_than_limit",
            criticality="error",
            check_func=is_aggr_not_less_than,
            column="a",
            check_func_kwargs={"limit": 1, "aggr_type": "count", "group_by": ["c"]},
        ),
        DQDatasetRule(
            name="count_group_by_c_less_than_limit",
            criticality="error",
            check_func=is_aggr_not_less_than,
            column="*",
            check_func_kwargs={"limit": 1, "aggr_type": "count", "group_by": ["c"]},
        ),
        DQDatasetRule(
            name="foreign_key",
            criticality="warn",
            check_func=foreign_key,
            columns=["a"],
            filter="a > b",
            check_func_kwargs={
                "ref_columns": ["ref_a"],
                "ref_df_name": "ref_df_key",
            },
        ),
        DQDatasetRule(
            name="foreign_key",
            criticality="warn",
            check_func=foreign_key,
            columns=["c"],
            filter="a > b",
            check_func_kwargs={
                "ref_columns": ["ref_a"],
                "ref_df_name": "ref_df_key",
            },
        ),
    ]

    assert pprint.pformat(actual_rules) == pprint.pformat(expected_rules)


def test_build_checks_by_metadata_when_check_spec_is_missing() -> None:
    checks: list[dict] = [{}]  # missing check spec

    with pytest.raises(ValueError, match="'check' field is missing"):
        DQEngineCore.build_quality_rules_by_metadata(checks)


def test_build_checks_by_metadata_when_function_spec_is_missing() -> None:
    checks: list[dict] = [{"check": {}}]  # missing func spec

    with pytest.raises(ValueError, match="'function' field is missing in the 'check' block"):
        DQEngineCore.build_quality_rules_by_metadata(checks)


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
        DQEngineCore.build_quality_rules_by_metadata(checks)


def test_build_checks_by_metadata_when_function_does_not_exist():
    checks = [{"check": {"function": "function_does_not_exists", "arguments": {"column": "a"}}}]

    with pytest.raises(ValueError, match="function 'function_does_not_exists' is not defined"):
        DQEngineCore.build_quality_rules_by_metadata(checks)


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
        DQEngineCore.build_quality_rules_by_metadata(checks)
        assert "Resolving function: is_not_null_and_not_empty" in caplog.text


def test_validate_check_func_arguments_too_many_positional():
    with pytest.raises(TypeError, match="takes 2 positional arguments but 3 were given"):
        DQRowRule(
            name="col1_is_not_in_the_list",
            criticality="error",
            check_func=is_in_list,
            column="col1",
            check_func_args=[[1, 2], "extra_arg"],
        )


def test_validate_check_func_arguments_invalid_keyword():
    with pytest.raises(TypeError, match="got an unexpected keyword argument 'invalid_kwarg'"):
        DQRowRule(
            name="col1_is_not_in_the_list",
            criticality="error",
            check_func=is_in_list,
            column="col1",
            check_func_kwargs={"allowed": [3], "invalid_kwarg": "invalid_kwarg", "invalid_kwarg2": "invalid_kwarg2"},
        )


def test_validate_correct_dataset_rule_used():
    with pytest.raises(ValueError, match="Function 'is_not_null' is not a dataset-level rule"):
        DQDatasetRule(criticality="error", check_func=is_not_null, columns=["a"])


def test_validate_correct_row_rule_used():
    with pytest.raises(ValueError, match="Function 'is_unique' is not a row-level rule"):
        DQRowRule(criticality="error", check_func=is_unique, column="a")


def test_validate_column_and_columns_provided():
    with pytest.raises(ValueError, match="Both 'column' and 'columns' cannot be provided at the same time"):
        DQDatasetRule(check_func=is_unique, column="a", columns=["b"])


def test_register_rule():

    @register_rule("single_column")
    def mock_check_func():
        pass

    # Assert that the function is registered correctly
    assert "mock_check_func" in CHECK_FUNC_REGISTRY
    assert CHECK_FUNC_REGISTRY["mock_check_func"] == "single_column"


def test_row_rule_null_column():
    with pytest.raises(TypeError, match="missing 1 required positional argument: 'column'"):
        DQRowRule(
            criticality="warn",
            check_func=is_not_null,
            column=None,
        )


def test_dataset_rule_null_column():
    with pytest.raises(TypeError, match="missing 1 required positional argument: 'column'"):
        DQDatasetRule(
            criticality="warn",
            check_func=is_aggr_not_greater_than,
            column=None,
            check_func_kwargs={
                "limit": 1,
            },
        )


def test_dataset_rule_null_columns_items():
    with pytest.raises(ValueError, match="'columns' list contains a None element"):
        DQDatasetRule(
            criticality="warn",
            check_func=is_unique,
            columns=[None],
            check_func_kwargs={
                "limit": 1,
            },
        )


def test_dataset_rule_empty_columns():
    with pytest.raises(ValueError, match="'columns' cannot be empty"):
        DQDatasetRule(
            criticality="warn",
            check_func=is_unique,
            columns=[],
            check_func_kwargs={
                "limit": 1,
            },
        )
