from datetime import datetime, timezone
from decimal import Decimal

import pyspark.sql.functions as F

from databricks.labs.dqx.checks_serializer import DataFrameConverter

SCHEMA = "a: int, b: int, c: int"


def test_build_quality_rules_from_dataframe_round_trip(spark):
    test_checks = [
        {
            "name": "column_is_not_null",
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": "test_col"}},
        },
        {
            "name": "column_is_not_null_or_empty",
            "criticality": "warn",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"column": "test_col"}},
        },
        {
            "name": "column_col_not_less_than",
            "criticality": "warn",
            "check": {"function": "is_not_less_than", "arguments": {"column": "test_col", "limit": "5"}},
        },
        {
            "name": "column_col2_not_less_than",
            "criticality": "warn",
            "check": {"function": "is_not_greater_than", "arguments": {"column": "test_col2", "limit": 1.01}},
        },
        {
            "name": "column_in_list",
            "criticality": "warn",
            "check": {"function": "is_in_list", "arguments": {"column": "test_col2", "allowed": [1, 2]}},
        },
        {
            "name": "column_unique",
            "criticality": "warn",
            "check": {
                "function": "is_unique",
                "arguments": {"columns": ["test_col", "test_col2"], "nulls_distinct": True},
            },
            "user_metadata": {"check_type": "uniqueness", "check_owner": "someone_else@email.com"},
        },
        {
            "name": "column_unique_filter",
            "criticality": "warn",
            "filter": "test_col > 0",
            "check": {
                "function": "is_unique",
                "arguments": {"columns": ["test_col", "test_col2"], "nulls_distinct": True},
            },
            "user_metadata": {"check_type": "uniqueness", "check_owner": "someone_else@email.com"},
        },
        {
            "name": "column_unique_filter_and_row_filter",
            "criticality": "warn",
            "filter": "test_col > 0",
            "check": {
                "function": "is_unique",
                "arguments": {
                    "columns": ["test_col", "test_col2"],
                    "nulls_distinct": True,
                    "row_filter": "test_col2 < 5",
                },
            },
            "user_metadata": {"check_type": "uniqueness", "check_owner": "someone_else@email.com"},
        },
        {
            "name": "column_unique_row_filter",
            "criticality": "warn",
            "check": {
                "function": "is_unique",
                "arguments": {
                    "columns": ["test_col", "test_col2"],
                    "nulls_distinct": True,
                    "row_filter": "test_col2 < 5",
                },
            },
            "user_metadata": {"check_type": "uniqueness", "check_owner": "someone_else@email.com"},
        },
        {
            "name": "d_not_in_a",
            "criticality": "error",
            "check": {
                "function": "sql_expression",
                "arguments": {
                    "expression": "a != substring(b, 8, 2)",
                    "msg": "a not found in b",
                    "columns": ["a", "b"],
                },
            },
        },
        {
            "name": "is_aggr_not_greater_than",
            "criticality": "error",
            "filter": "test_col > 0",
            "check": {
                "function": "is_aggr_not_greater_than",
                "arguments": {"column": "test_col", "group_by": ["a"], "limit": 0, "aggr_type": "count"},
            },
        },
        {
            "name": "is_aggr_not_less_than",
            "criticality": "error",
            "filter": "test_col > 0",
            "check": {
                "function": "is_aggr_not_less_than",
                "arguments": {"column": "test_col", "group_by": ["a"], "limit": 0, "aggr_type": "count"},
            },
        },
        {
            "name": "column_has_outliers",
            "criticality": "error",
            "filter": "test_col > 0",
            "check": {
                "function": "has_no_outliers",
                "arguments": {"column": "c"},
            },
        },
        {
            "name": "price_isnt_in_range",
            "criticality": "error",
            "check": {
                "function": "is_in_range",
                "arguments": {
                    "column": "price",
                    "min_limit": Decimal("0.01"),
                    "max_limit": Decimal("999.99"),
                },
            },
        },
    ]

    df = DataFrameConverter.to_dataframe(spark, test_checks)
    checks = DataFrameConverter.from_dataframe(df)
    assert checks == test_checks, "The loaded checks do not match the expected checks."


def test_build_quality_rules_from_dataframe_with_run_config(spark):
    default_checks = [
        {
            "name": "column_is_not_null",
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": "test_col"}},
            "user_metadata": {"check_type": "completeness", "check_owner": "someone@email.com"},
        },
        {
            "name": "column_is_not_null_or_empty",
            "criticality": "warn",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"column": "test_col"}},
        },
    ]
    workflow_checks = [
        {
            "name": "column_not_less_than",
            "criticality": "warn",
            "check": {"function": "is_not_less_than", "arguments": {"column": "test_col", "limit": "5"}},
        },
    ]
    default_checks_df = DataFrameConverter.to_dataframe(spark, default_checks)
    workflow_checks_df = DataFrameConverter.to_dataframe(spark, workflow_checks, run_config_name="workflow_001")
    df = default_checks_df.union(workflow_checks_df)

    checks = DataFrameConverter.from_dataframe(df, run_config_name="workflow_001")
    assert checks == workflow_checks, "The loaded checks do not match the expected workflow checks."

    checks = DataFrameConverter.from_dataframe(df)
    assert checks == default_checks, "The loaded checks do not match the expected default checks."


def test_from_dataframe_latest_rule_set_tiebreaker_by_fingerprint(spark):
    """When two rule sets share the same created_at, the one with the larger rule_set_fingerprint is chosen."""
    checks_a = [
        {
            "name": "a_not_null",
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": "a"}},
        },
    ]
    checks_b = [
        {
            "name": "b_not_null",
            "criticality": "warn",
            "check": {"function": "is_not_null", "arguments": {"column": "b"}},
        },
    ]
    run_config = "default"
    df_a = DataFrameConverter.to_dataframe(spark, checks_a, run_config_name=run_config)
    df_b = DataFrameConverter.to_dataframe(spark, checks_b, run_config_name=run_config)
    same_ts = datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    df_a = df_a.withColumn("created_at", F.lit(same_ts))
    df_b = df_b.withColumn("created_at", F.lit(same_ts))
    df = df_a.union(df_b)

    loaded = DataFrameConverter.from_dataframe(df, run_config_name=run_config)

    fingerprint_a = df_a.select("rule_set_fingerprint").first()[0]
    fingerprint_b = df_b.select("rule_set_fingerprint").first()[0]
    assert fingerprint_a != fingerprint_b
    expected_checks = checks_b if fingerprint_b > fingerprint_a else checks_a
    assert (
        loaded == expected_checks
    ), "When created_at ties, the rule set with the larger rule_set_fingerprint should be selected."
