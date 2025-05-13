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
        "criticality": "warning",
        "check": {"function": "is_not_null_or_empty", "arguments": {"col_name": "test_col"}},
    },
    {
        "name": "column_not_less_than",
        "criticality": "warning",
        "check": {"function": "is_not_less_than", "arguments": {"col_name": "test_col", "min_limit": "5"}},
    },
]


def test_load_checks_from_dataframe():
    df = DQEngineCore.save_checks_in_dataframe(TEST_CHECKS)
    checks = DQEngineCore.load_checks_from_dataframe(df)
    assert checks == TEST_CHECKS, "The loaded checks do not match the expected checks."


def test_load_checks_from_dataframe_with_query():
    df = DQEngineCore.save_checks_in_dataframe(TEST_CHECKS)
    checks = DQEngineCore.load_checks_from_dataframe(df, query="criticality <> 'warning'")
    assert checks == [c for c in TEST_CHECKS if c["criticality"] != "warning"], "Checks were not loaded correctly"


def test_load_checks_from_dataframe_with_warning():
    too_many_checks = [TEST_CHECKS[0] for _ in range(1000)]
    with pytest.warns():
        df = DQEngineCore.save_checks_in_dataframe(too_many_checks)
        DQEngineCore.load_checks_from_dataframe(df, query="criticality <> 'warning'")
