from databricks.labs.dqx.checks_serializer import deserialize_checks_to_dataframe, serialize_checks_from_dataframe

SCHEMA = "a: int, b: int, c: int"


def test_build_quality_rules_from_dataframe(spark):
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
            "check": {"function": "is_not_greater_than", "arguments": {"column": "test_col2", "limit": 1}},
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
            "name": "column_unique",
            "criticality": "warn",
            "filter": "test_col > 0",
            "check": {
                "function": "is_unique",
                "arguments": {"columns": ["test_col", "test_col2"], "nulls_distinct": True},
            },
            "user_metadata": {"check_type": "uniqueness", "check_owner": "someone_else@email.com"},
        },
        {
            "name": "d_not_in_a",
            "criticality": "error",
            "check": {
                "function": "sql_expression",
                "arguments": {"expression": "a != substring(b, 8, 1)", "msg": "a not found in b", "columns": ["a", "b"]},
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
    ]

    df = deserialize_checks_to_dataframe(spark, test_checks)
    checks = serialize_checks_from_dataframe(df)
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
    default_checks_df = deserialize_checks_to_dataframe(spark, default_checks)
    workflow_checks_df = deserialize_checks_to_dataframe(spark, workflow_checks, run_config_name="workflow_001")
    df = default_checks_df.union(workflow_checks_df)

    checks = serialize_checks_from_dataframe(df, run_config_name="workflow_001")
    assert checks == workflow_checks, "The loaded checks do not match the expected workflow checks."

    checks = serialize_checks_from_dataframe(df)
    assert checks == default_checks, "The loaded checks do not match the expected default checks."
