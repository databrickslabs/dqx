import pytest
from databricks.labs.dqx.engine import DQEngineCore

TEST_CHECKS = [
    {
        "name": "column_is_not_null",
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"col_name": "test_col"}},
    },
    {
        "name": "column_is_not_null_or_empty",
        "criticality": "warn",
        "check": {"function": "is_not_null_or_empty", "arguments": {"col_name": "test_col"}},
    },
    {
        "name": "column_not_less_than",
        "criticality": "warn",
        "check": {"function": "is_not_less_than", "arguments": {"col_name": "test_col", "min_limit": "5"}},
    },
]


def test_load_checks_from_dataframe(spark_local):
    df = DQEngineCore.build_dataframe_from_quality_rules(TEST_CHECKS, spark=spark_local)
    checks = DQEngineCore.build_quality_rules_from_dataframe(df)
    assert checks == TEST_CHECKS, "The loaded checks do not match the expected checks."


def test_load_checks_from_dataframe_with_warning(spark_local):
    too_many_checks = [TEST_CHECKS[0] for _ in range(1000)]
    with pytest.warns(UserWarning):
        df = DQEngineCore.build_dataframe_from_quality_rules(too_many_checks, spark=spark_local)
        DQEngineCore.build_quality_rules_from_dataframe(df)
