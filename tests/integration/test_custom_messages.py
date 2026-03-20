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


def _custom_message(
    rule_name: str,
    check_func_name: str,
    check_func_args: dict[str, Any],  # pylint: disable=unused-argument
    column_value: Column,
) -> Column:
    """Custom message that includes the rule name and check function name."""
    return F.concat(F.lit(f"Rule '{rule_name}' ({check_func_name}) failed for value: "), column_value.cast("string"))


def _static_custom_message(
    rule_name: str,
    check_func_name: str,  # pylint: disable=unused-argument
    check_func_args: dict[str, Any],  # pylint: disable=unused-argument
    column_value: Column,  # pylint: disable=unused-argument
) -> Column:
    """Custom message that returns a static string."""
    return F.lit(f"Custom error: {rule_name}")


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
        [
            build_quality_violation(
                name="c_not_null",
                message="Custom error: c_not_null",
                columns=["c"],
                function="is_not_null",
            )
        ]
    ]
    expected_df = spark.createDataFrame(
        [[1, 3, None, expected_errors[0], None]],
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
        [
            build_quality_violation(
                name="b_not_null",
                message="Rule 'b_not_null' (is_not_null) failed for value: null",
                columns=["b"],
                function="is_not_null",
            )
        ]
    ]
    expected_df = spark.createDataFrame(
        [[1, None, 3, None, expected_warnings[0]]],
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
        [
            build_quality_violation(
                name="c_not_null",
                message="Column 'c' value is null",
                columns=["c"],
                function="is_not_null",
            )
        ]
    ]
    expected_df = spark.createDataFrame(
        [[1, 3, None, expected_errors[0], None]],
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

    # Both columns a and b are null, so both should have custom error messages
    rows = checked_df.collect()
    assert len(rows) == 1
    errors = rows[0]["_errors"]
    assert len(errors) == 2
    messages = sorted([e["message"] for e in errors])
    # The auto-generated names include the column info
    assert all(msg.startswith("Custom error:") for msg in messages)
