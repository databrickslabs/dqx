from databricks.labs.dqx.engine import DQEngineCore

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
            "user_metadata": {"check_type": "uniqueness", "check_owner": "someone_else@email.com"}
        },
        {
            "name": "d_not_in_a",
            "criticality": "error",
            "check": {
                "function": "sql_expression",
                "arguments": {"expression": "a != substring(b, 8, 1)", "msg": "a not found in b"},
            },
        },
    ]
    df = DQEngineCore.build_dataframe_from_quality_rules(test_checks, spark=spark)
    checks = DQEngineCore.build_quality_rules_from_dataframe(df)
    assert checks == test_checks, "The loaded checks do not match the expected checks."


def test_build_quality_rules_from_dataframe_with_run_config(spark):
    default_checks = [
        {
            "name": "column_is_not_null",
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": "test_col"}},
            "user_metadata": {"check_type": "completeness", "check_owner": "someone@email.com"}
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
    default_checks_df = DQEngineCore.build_dataframe_from_quality_rules(default_checks, spark=spark)
    workflow_checks_df = DQEngineCore.build_dataframe_from_quality_rules(
        workflow_checks, run_config_name="workflow_001", spark=spark
    )
    df = default_checks_df.union(workflow_checks_df)

    checks = DQEngineCore.build_quality_rules_from_dataframe(df, run_config_name="workflow_001")
    assert checks == workflow_checks, "The loaded checks do not match the expected workflow checks."

    checks = DQEngineCore.build_quality_rules_from_dataframe(df)
    assert checks == default_checks, "The loaded checks do not match the expected default checks."
