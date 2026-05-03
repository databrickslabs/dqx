"""Integration tests for custom message expressions on DQRule."""

import pyspark.sql.functions as F

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


def test_apply_checks_with_static_custom_message(ws, spark):
    """A plain SQL literal message should appear in the result DataFrame."""
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[1, 3, None]], SCHEMA)

    rules = [
        DQRowRule(
            name="c_not_null",
            criticality="error",
            check_func=check_funcs.is_not_null,
            column="c",
            message_expr="'Custom error: c_not_null'",
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


def test_apply_checks_with_dynamic_column_value_message(ws, spark):
    """SQL expression referencing the actual column should produce dynamic messages."""
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[1, None, 3]], SCHEMA)

    rules = [
        DQRowRule(
            name="b_not_null",
            criticality="warn",
            check_func=check_funcs.is_not_null,
            column="b",
            message_expr=(
                "concat('Rule b_not_null (is_not_null) failed for value: ',"
                " coalesce(cast(b as string), 'null'))"
            ),
        ),
    ]

    checked_df = dq_engine.apply_checks(test_df, rules)
    expected_warnings = [
        build_quality_violation(
            name="b_not_null",
            message="Rule b_not_null (is_not_null) failed for value: null",
            columns=["b"],
            function="is_not_null",
        )
    ]
    expected_df = spark.createDataFrame(
        [[1, None, 3, None, expected_warnings]],
        EXPECTED_SCHEMA,
    )
    assert_df_equality(checked_df, expected_df)


def test_apply_checks_passing_rows_have_no_custom_message(ws, spark):
    """Rows that pass the check should not have a message even with a custom message."""
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[1, 3, 5]], SCHEMA)

    rules = [
        DQRowRule(
            name="c_not_null",
            criticality="error",
            check_func=check_funcs.is_not_null,
            column="c",
            message_expr="'Custom error: c_not_null'",
        ),
    ]

    checked_df = dq_engine.apply_checks(test_df, rules)
    expected_df = spark.createDataFrame(
        [[1, 3, 5, None, None]],
        EXPECTED_SCHEMA,
    )
    assert_df_equality(checked_df, expected_df)


def test_for_each_col_rule_with_custom_message(ws, spark):
    """DQForEachColRule with message_expr should propagate to generated rules.

    The same expression is used for every generated rule. To produce a per-column
    message, reference each column inline (e.g. concat with a per-column literal) or
    construct rules individually.
    """
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[None, None, 3]], SCHEMA)

    rules = DQForEachColRule(
        columns=["a", "b"],
        check_func=check_funcs.is_not_null,
        criticality="error",
        message_expr="'Custom error: column missing'",
    ).get_rules()

    checked_df = dq_engine.apply_checks(test_df, rules)
    expected_errors = [
        build_quality_violation(
            name="a_is_null",
            message="Custom error: column missing",
            columns=["a"],
            function="is_not_null",
        ),
        build_quality_violation(
            name="b_is_null",
            message="Custom error: column missing",
            columns=["b"],
            function="is_not_null",
        ),
    ]
    expected_df = spark.createDataFrame(
        [[None, None, 3, expected_errors, None]],
        EXPECTED_SCHEMA,
    )
    assert_df_equality(checked_df, expected_df)


def test_apply_checks_with_column_value_non_null(ws, spark):
    """When a check fails with a non-null value, the message can include that value."""
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[1, -5, 3]], SCHEMA)

    rules = [
        DQRowRule(
            name="b_positive",
            criticality="error",
            check_func=check_funcs.is_not_less_than,
            column="b",
            check_func_kwargs={"limit": 0},
            message_expr=(
                "concat('b_positive: value ', coalesce(cast(b as string), 'null'),"
                " ' is not positive')"
            ),
        ),
    ]

    checked_df = dq_engine.apply_checks(test_df, rules)
    expected_errors = [
        build_quality_violation(
            name="b_positive",
            message="b_positive: value -5 is not positive",
            columns=["b"],
            function="is_not_less_than",
        )
    ]
    expected_df = spark.createDataFrame(
        [[1, -5, 3, expected_errors, None]],
        EXPECTED_SCHEMA,
    )
    assert_df_equality(checked_df, expected_df)


def test_apply_checks_simple_literal_message(ws, spark):
    """A plain string literal (no expression) should work as a static message."""
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[1, 3, None]], SCHEMA)

    rules = [
        DQRowRule(
            name="c_not_null",
            criticality="error",
            check_func=check_funcs.is_not_null,
            column="c",
            message_expr="'Column c must not be null'",
        ),
    ]

    checked_df = dq_engine.apply_checks(test_df, rules)
    expected_errors = [
        build_quality_violation(
            name="c_not_null",
            message="Column c must not be null",
            columns=["c"],
            function="is_not_null",
        )
    ]
    expected_df = spark.createDataFrame(
        [[1, 3, None, expected_errors, None]],
        EXPECTED_SCHEMA,
    )
    assert_df_equality(checked_df, expected_df)


def test_apply_checks_with_column_message_expr(ws, spark):
    """A Spark Column passed as message_expr should be used directly without conversion."""
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[1, 3, None]], SCHEMA)

    rules = [
        DQRowRule(
            name="c_not_null",
            criticality="error",
            check_func=check_funcs.is_not_null,
            column="c",
            message_expr=F.concat(
                F.lit("Custom error: c_not_null (value="),
                F.coalesce(F.col("c").cast("string"), F.lit("null")),
                F.lit(")"),
            ),
        ),
    ]

    checked_df = dq_engine.apply_checks(test_df, rules)
    expected_errors = [
        build_quality_violation(
            name="c_not_null",
            message="Custom error: c_not_null (value=null)",
            columns=["c"],
            function="is_not_null",
        )
    ]
    expected_df = spark.createDataFrame(
        [[1, 3, None, expected_errors, None]],
        EXPECTED_SCHEMA,
    )
    assert_df_equality(checked_df, expected_df)


def test_metadata_static_custom_message(ws, spark):
    """Static message defined in YAML-style metadata should appear in the result."""
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[1, 3, None]], SCHEMA)

    checks = [
        {
            "name": "c_not_null",
            "criticality": "error",
            "message_expr": "'Custom error: c_not_null'",
            "check": {"function": "is_not_null", "arguments": {"column": "c"}},
        }
    ]

    checked_df = dq_engine.apply_checks_by_metadata(test_df, checks)
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


def test_metadata_dynamic_column_value_message(ws, spark):
    """Dynamic message expression referencing the column from metadata should resolve correctly."""
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[1, None, 3]], SCHEMA)

    checks = [
        {
            "name": "b_not_null",
            "criticality": "warn",
            "message_expr": (
                "concat('Rule b_not_null (is_not_null) failed for value: ',"
                " coalesce(cast(b as string), 'null'))"
            ),
            "check": {"function": "is_not_null", "arguments": {"column": "b"}},
        }
    ]

    checked_df = dq_engine.apply_checks_by_metadata(test_df, checks)
    expected_warnings = [
        build_quality_violation(
            name="b_not_null",
            message="Rule b_not_null (is_not_null) failed for value: null",
            columns=["b"],
            function="is_not_null",
        )
    ]
    expected_df = spark.createDataFrame(
        [[1, None, 3, None, expected_warnings]],
        EXPECTED_SCHEMA,
    )
    assert_df_equality(checked_df, expected_df)


def test_metadata_without_message_uses_default(ws, spark):
    """Metadata checks without a message_expr field should produce the default message."""
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[1, 3, None]], SCHEMA)

    checks = [
        {
            "name": "c_not_null",
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": "c"}},
        }
    ]

    checked_df = dq_engine.apply_checks_by_metadata(test_df, checks)
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


def test_metadata_for_each_column_with_custom_message(ws, spark):
    """for_each_column in metadata with message_expr should propagate to all generated rules."""
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[None, None, 3]], SCHEMA)

    checks = [
        {
            "criticality": "error",
            "message_expr": "'Custom error: column missing'",
            "check": {
                "function": "is_not_null",
                "for_each_column": ["a", "b"],
            },
        }
    ]

    checked_df = dq_engine.apply_checks_by_metadata(test_df, checks)
    expected_errors = [
        build_quality_violation(
            name="a_is_null",
            message="Custom error: column missing",
            columns=["a"],
            function="is_not_null",
        ),
        build_quality_violation(
            name="b_is_null",
            message="Custom error: column missing",
            columns=["b"],
            function="is_not_null",
        ),
    ]
    expected_df = spark.createDataFrame(
        [[None, None, 3, expected_errors, None]],
        EXPECTED_SCHEMA,
    )
    assert_df_equality(checked_df, expected_df)
