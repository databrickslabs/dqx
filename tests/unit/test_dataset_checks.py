import pytest

from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.check_funcs import sql_query, is_data_fresh_per_time_window
from databricks.labs.dqx.rule import DQDatasetRule
from databricks.labs.dqx.errors import InvalidParameterError, UnsafeSqlQueryError, MissingParameterError


@pytest.mark.parametrize(
    "ref_df_name, ref_table, ref_columns, columns, expected_exception, expected_message",
    [
        ("ref_df", "table", ["a"], ["a"], InvalidParameterError, "Both 'ref_df_name' and 'ref_table' were provided"),
        (None, None, ["a"], ["a"], MissingParameterError, "Either 'ref_df_name' or 'ref_table' is required but neither was provided."),
        ("", None, ["a"], ["a"], MissingParameterError, "Either 'ref_df_name' or 'ref_table' is required but neither was provided."),
        (None, "", ["a"], ["a"], MissingParameterError, "Either 'ref_df_name' or 'ref_table' is required but neither was provided."),
        (
            None,
            "table",
            ["a", "b"],
            ["a"],
            InvalidParameterError,
            "'columns' has 1 entries but 'ref_columns' has 2. Both must have the same length to allow comparison.",
        ),
    ],
)
def test_foreign_key_exceptions(ref_df_name, ref_table, ref_columns, columns, expected_exception, expected_message):

    with pytest.raises(expected_exception, match=expected_message):
        DQDatasetRule(
            criticality="warn",
            check_func=check_funcs.foreign_key,
            columns=columns,
            check_func_kwargs={
                "ref_columns": ref_columns,
                "ref_df_name": ref_df_name,
                "ref_table": ref_table,
            },
        )


@pytest.mark.parametrize(
    "ref_df_name, ref_table, ref_columns, columns, expected_exception, expected_message",
    [
        (
            "ref_df",
            "table",
            ["a"],
            ["a"],
            InvalidParameterError,
            "Both 'ref_df_name' and 'ref_table' were provided. Please provide only one to avoid ambiguity.",
        ),
        (None, None, ["a"], ["a"], MissingParameterError, "Either 'ref_df_name' or 'ref_table' is required but neither was provided."),
        ("", None, ["a"], ["a"], MissingParameterError, "Either 'ref_df_name' or 'ref_table' is required but neither was provided."),
        (None, "", ["a"], ["a"], MissingParameterError, "Either 'ref_df_name' or 'ref_table' is required but neither was provided."),
        (
            None,
            "table",
            ["a", "b"],
            ["a"],
            InvalidParameterError,
            "'columns' has 1 entries but 'ref_columns' has 2. Both must have the same length to allow comparison.",
        ),
    ],
)
def test_compare_datasets_exceptions(
    ref_df_name, ref_table, ref_columns, columns, expected_exception, expected_message
):
    with pytest.raises(expected_exception, match=expected_message):
        DQDatasetRule(
            criticality="warn",
            check_func=check_funcs.compare_datasets,
            columns=columns,
            check_func_kwargs={
                "ref_columns": ref_columns,
                "ref_df_name": ref_df_name,
                "ref_table": ref_table,
            },
        )


@pytest.mark.parametrize(
    "abs_tolerance, rel_tolerance",
    [
        (-1, None),
        (None, -1),
        (-1, -1),
    ],
)
def test_compare_datasets_invalid_tolerance_exceptions(abs_tolerance, rel_tolerance):
    with pytest.raises(
        InvalidParameterError, match="Absolute and/or relative tolerances if provided must be non-negative"
    ):
        DQDatasetRule(
            criticality="warn",
            check_func=check_funcs.compare_datasets,
            columns=["col1"],
            check_func_kwargs={
                "ref_columns": ["col1"],
                "ref_table": "ref_table",
                "abs_tolerance": abs_tolerance,
                "rel_tolerance": rel_tolerance,
            },
        )


def test_sql_query_missing_merge_columns():
    with pytest.raises(InvalidParameterError, match="'merge_columns' must contain at least one column"):
        DQDatasetRule(
            criticality="error",
            check_func=sql_query,
            check_func_kwargs={"query": "SELECT 1", "merge_columns": [], "condition_column": "condition"},
        )


def test_sql_query_unsafe():
    query = "SELECT * FROM {{ input }} JOIN {{ validname; DROP TABLE sensitive_data -- }} ON id = id"
    with pytest.raises(UnsafeSqlQueryError, match="Provided SQL query is not safe for execution"):
        DQDatasetRule(
            criticality="error",
            check_func=sql_query,
            check_func_kwargs={"query": query, "merge_columns": ["col1"], "condition_column": "condition"},
        )


@pytest.mark.parametrize(
    "lookback_windows, min_records_per_window, window_minutes, expected_message",
    [
        (-1, 10, 15, "lookback_windows must be a positive integer if provided"),
        (0, 10, 15, "lookback_windows must be a positive integer if provided"),
        (5, 0, 15, "min_records_per_window must be a positive integer"),
        (5, -1, 15, "min_records_per_window must be a positive integer"),
        (5, None, 15, "min_records_per_window must be a positive integer"),
        (5, 10, 0, "window_minutes must be a positive integer"),
        (5, 10, -1, "window_minutes must be a positive integer"),
        (5, 10, None, "window_minutes must be a positive integer"),
    ],
)
def test_is_data_fresh_per_time_window_exceptions(
    lookback_windows, min_records_per_window, window_minutes, expected_message
):
    with pytest.raises(InvalidParameterError, match=expected_message):
        is_data_fresh_per_time_window(
            column="timestamp",
            window_minutes=window_minutes,
            min_records_per_window=min_records_per_window,
            lookback_windows=lookback_windows,
        )
