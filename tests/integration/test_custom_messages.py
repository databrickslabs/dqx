"""Integration tests for custom message callable on DQRule."""

from typing import Any

import pyspark.sql.functions as F
from pyspark.sql import Column

from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQRowRule, DQForEachColRule
from databricks.labs.dqx import check_funcs
from tests.integration.conftest import (
    EXTRA_PARAMS,
    assert_df_equality_ignore_fingerprints as assert_df_equality,
    REPORTING_COLUMNS,
    build_quality_violation,
)


SCHEMA = "a: int, b: int, c: int"
EXPECTED_SCHEMA = SCHEMA + REPORTING_COLUMNS


def test_apply_checks_with_custom_message(ws, spark):
    """Custom message callable should appear in the result DataFrame."""
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[1, 3, None]], SCHEMA)

    rules = [
        DQRowRule(
            name="c_not_null",
            criticality="error",
            check_func=check_funcs.is_not_null,
            column="c",
            message=_static_custom_message,
        ),
    ]

    checked_df = dq_engine.apply_checks(test_df, rules)
    expected_errors = [
        build_quality_violation(
            name="c_not_null",
            message="Custom error: c_not_null",
            columns=["c"],
            function="is_not_null",
        )
    ]
    expected_df = spark.createDataFrame(
        [[1, 3, None, expected_errors, None]],
        EXPECTED_SCHEMA,
    )
    assert_df_equality(checked_df, expected_df)


def test_apply_checks_with_dynamic_custom_message(ws, spark):
    """Custom message callable with column value should produce dynamic messages."""
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[1, None, 3]], SCHEMA)

    rules = [
        DQRowRule(
            name="b_not_null",
            criticality="warn",
            check_func=check_funcs.is_not_null,
            column="b",
            message=_custom_message,
        ),
    ]

    checked_df = dq_engine.apply_checks(test_df, rules)
    expected_warnings = [
        build_quality_violation(
            name="b_not_null",
            message="Rule 'b_not_null' (is_not_null) failed for value: null",
            columns=["b"],
            function="is_not_null",
        )
    ]
    expected_df = spark.createDataFrame(
        [[1, None, 3, None, expected_warnings]],
        EXPECTED_SCHEMA,
    )
    assert_df_equality(checked_df, expected_df)


def test_apply_checks_without_custom_message_unchanged(ws, spark):
    """Rules without custom message should produce the default message as before."""
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[1, 3, None]], SCHEMA)

    rules = [
        DQRowRule(
            name="c_not_null",
            criticality="error",
            check_func=check_funcs.is_not_null,
            column="c",
        ),
    ]

    checked_df = dq_engine.apply_checks(test_df, rules)
    expected_errors = [
        build_quality_violation(
            name="c_not_null",
            message="Column 'c' value is null",
            columns=["c"],
            function="is_not_null",
        )
    ]
    expected_df = spark.createDataFrame(
        [[1, 3, None, expected_errors, None]],
        EXPECTED_SCHEMA,
    )
    assert_df_equality(checked_df, expected_df)


def test_apply_checks_passing_rows_have_no_custom_message(ws, spark):
    """Rows that pass the check should not have a message even with a custom message callable."""
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[1, 3, 5]], SCHEMA)

    rules = [
        DQRowRule(
            name="c_not_null",
            criticality="error",
            check_func=check_funcs.is_not_null,
            column="c",
            message=_static_custom_message,
        ),
    ]

    checked_df = dq_engine.apply_checks(test_df, rules)
    expected_df = spark.createDataFrame(
        [[1, 3, 5, None, None]],
        EXPECTED_SCHEMA,
    )
    assert_df_equality(checked_df, expected_df)


def test_for_each_col_rule_with_custom_message(ws, spark):
    """DQForEachColRule with message should propagate to generated rules and appear in results."""
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[None, None, 3]], SCHEMA)

    rules = DQForEachColRule(
        columns=["a", "b"],
        check_func=check_funcs.is_not_null,
        criticality="error",
        message=_static_custom_message,
    ).get_rules()

    checked_df = dq_engine.apply_checks(test_df, rules)
    rows = checked_df.collect()
    assert len(rows) == 1
    errors = rows[0]["_errors"]
    assert len(errors) == 2
    messages = sorted([e["message"] for e in errors])
    assert all(msg.startswith("Custom error:") for msg in messages)


def test_apply_checks_with_kwargs_only_custom_message(ws, spark):
    """Custom message callable with **kwargs should receive all supported context args."""
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[1, None, 3]], SCHEMA)

    rules = [
        DQRowRule(
            name="b_not_null",
            criticality="warn",
            check_func=check_funcs.is_not_null,
            column="b",
            message=_kwargs_only_custom_message,
        ),
    ]

    checked_df = dq_engine.apply_checks(test_df, rules)
    expected_warnings = [
        build_quality_violation(
            name="b_not_null",
            message="kwargs keys: check_func_args,check_func_name,column_value,rule_name",
            columns=["b"],
            function="is_not_null",
        )
    ]
    expected_df = spark.createDataFrame(
        [[1, None, 3, None, expected_warnings]],
        EXPECTED_SCHEMA,
    )
    assert_df_equality(checked_df, expected_df)


def test_apply_checks_with_rule_name_only_custom_message(ws, spark):
    """Custom message callable with a single named argument should work."""
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[1, None, 3]], SCHEMA)

    rules = [
        DQRowRule(
            name="b_not_null",
            criticality="warn",
            check_func=check_funcs.is_not_null,
            column="b",
            message=_rule_name_only_custom_message,
        ),
    ]

    checked_df = dq_engine.apply_checks(test_df, rules)
    expected_warnings = [
        build_quality_violation(
            name="b_not_null",
            message="Only rule: b_not_null",
            columns=["b"],
            function="is_not_null",
        )
    ]
    expected_df = spark.createDataFrame(
        [[1, None, 3, None, expected_warnings]],
        EXPECTED_SCHEMA,
    )
    assert_df_equality(checked_df, expected_df)


def test_apply_checks_custom_message_check_func_args_include_column(ws, spark):
    """check_func_args passed to custom message should include the rule 'column' argument."""
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[1, None, 3]], SCHEMA)

    rules = [
        DQRowRule(
            name="b_not_null",
            criticality="warn",
            check_func=check_funcs.is_not_null,
            column="b",
            message=_message_from_column_arg,
        ),
    ]

    checked_df = dq_engine.apply_checks(test_df, rules)
    expected_warnings = [
        build_quality_violation(
            name="b_not_null",
            message="column arg: b",
            columns=["b"],
            function="is_not_null",
        )
    ]
    expected_df = spark.createDataFrame(
        [[1, None, 3, None, expected_warnings]],
        EXPECTED_SCHEMA,
    )
    assert_df_equality(checked_df, expected_df)


def test_apply_checks_custom_message_check_func_args_include_columns(ws, spark):
    """check_func_args passed to custom message should include the rule 'columns' argument."""
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[None, 2, 3]], SCHEMA)

    rules = [
        DQRowRule(
            name="ab_any_null",
            criticality="error",
            check_func=_any_null_in_columns,
            columns=["a", "b"],
            message=_message_from_columns_arg,
        ),
    ]

    checked_df = dq_engine.apply_checks(test_df, rules)
    expected_errors = [
        build_quality_violation(
            name="ab_any_null",
            message="columns arg: a,b",
            columns=["a", "b"],
            function="_any_null_in_columns",
        )
    ]
    expected_df = spark.createDataFrame(
        [[None, 2, 3, expected_errors, None]],
        EXPECTED_SCHEMA,
    )
    assert_df_equality(checked_df, expected_df)


def _custom_message(
    rule_name: str,
    check_func_name: str,
    column_value: Column,
) -> Column:
    """Custom message that includes the rule name and check function name."""
    value_str = F.coalesce(column_value.cast("string"), F.lit("null"))
    return F.concat(F.lit(f"Rule '{rule_name}' ({check_func_name}) failed for value: "), value_str)


def _static_custom_message(
    rule_name: str,
) -> Column:
    """Custom message that returns a static string."""
    return F.lit(f"Custom error: {rule_name}")


def _kwargs_only_custom_message(**kwargs: Any) -> Column:
    """Custom message callable that accepts only **kwargs."""
    return F.lit(f"kwargs keys: {','.join(sorted(kwargs.keys()))}")


def _rule_name_only_custom_message(rule_name: str) -> Column:
    """Custom message callable that accepts a single named argument."""
    return F.lit(f"Only rule: {rule_name}")


def _message_from_column_arg(check_func_args: dict[str, Any]) -> Column:
    """Custom message that reads 'column' from check_func_args."""
    return F.lit(f"column arg: {check_func_args.get('column')}")


def _message_from_columns_arg(check_func_args: dict[str, Any]) -> Column:
    """Custom message that reads 'columns' from check_func_args."""
    columns = check_func_args.get("columns", [])
    return F.lit(f"columns arg: {','.join(columns)}")


def _any_null_in_columns(columns: list[str]) -> Column:
    """Fails if any provided column value is null."""
    condition = F.col(columns[0]).isNull()
    for col_name in columns[1:]:
        condition = condition | F.col(col_name).isNull()
    return check_funcs.make_condition(condition, "one or more columns are null", "any_null_in_columns")
