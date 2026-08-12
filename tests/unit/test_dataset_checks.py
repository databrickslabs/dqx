import pytest
import pyspark.sql.functions as F

from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.check_funcs import sql_query, is_data_fresh_per_time_window, has_no_gaps_per_time_window
from databricks.labs.dqx.rule import DQDatasetRule
from databricks.labs.dqx.errors import InvalidParameterError, UnsafeSqlQueryError, MissingParameterError


@pytest.mark.parametrize(
    "ref_df_name, ref_table, ref_columns, columns, expected_exception, expected_message",
    [
        ("ref_df", "table", ["a"], ["a"], InvalidParameterError, "Both 'ref_df_name' and 'ref_table' were provided"),
        (
            None,
            None,
            ["a"],
            ["a"],
            MissingParameterError,
            "Either 'ref_df_name' or 'ref_table' is required but neither was provided.",
        ),
        (
            "",
            None,
            ["a"],
            ["a"],
            MissingParameterError,
            "Either 'ref_df_name' or 'ref_table' is required but neither was provided.",
        ),
        (
            None,
            "",
            ["a"],
            ["a"],
            MissingParameterError,
            "Either 'ref_df_name' or 'ref_table' is required but neither was provided.",
        ),
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
        (
            None,
            None,
            ["a"],
            ["a"],
            MissingParameterError,
            "Either 'ref_df_name' or 'ref_table' is required but neither was provided.",
        ),
        (
            "",
            None,
            ["a"],
            ["a"],
            MissingParameterError,
            "Either 'ref_df_name' or 'ref_table' is required but neither was provided.",
        ),
        (
            None,
            "",
            ["a"],
            ["a"],
            MissingParameterError,
            "Either 'ref_df_name' or 'ref_table' is required but neither was provided.",
        ),
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


def test_sql_query_empty_merge_columns():
    """Test that empty list for merge_columns is treated the same as None (dataset-level check)."""
    # Should not raise an error - empty list is normalized to None
    rule = DQDatasetRule(
        criticality="error",
        check_func=sql_query,
        check_func_kwargs={"query": "SELECT FALSE AS condition", "merge_columns": [], "condition_column": "condition"},
    )
    assert rule.check_func is sql_query


def test_sql_query_without_merge_columns():
    """Test that merge_columns is optional and can be None."""
    # Should not raise an error
    rule = DQDatasetRule(
        criticality="error",
        check_func=sql_query,
        check_func_kwargs={"query": "SELECT FALSE AS condition", "condition_column": "condition"},
    )
    assert rule.check_func is sql_query


def test_sql_query_unsafe():
    query = "SELECT * FROM {{ input }} JOIN {{ validname; DROP TABLE sensitive_data -- }} ON id = id"
    with pytest.raises(UnsafeSqlQueryError, match="Provided SQL query is not safe for execution"):
        DQDatasetRule(
            criticality="error",
            check_func=sql_query,
            check_func_kwargs={"query": query, "merge_columns": ["col1"], "condition_column": "condition"},
        )


def test_sql_query_merge_columns_as_string_raises():
    """Ensure merge_columns must be provided as a sequence, not a single string."""
    with pytest.raises(
        InvalidParameterError, match="'merge_columns' must be a sequence of column names \\(e.g., list or tuple\\)"
    ):
        DQDatasetRule(
            criticality="error",
            check_func=sql_query,
            check_func_kwargs={
                "query": "SELECT FALSE AS condition",
                "merge_columns": "id",
                "condition_column": "condition",
            },
        )


def test_sql_query_merge_columns_invalid_entries_raise():
    """Ensure merge_columns entries must be non-empty strings."""
    with pytest.raises(InvalidParameterError, match="'merge_columns' entries must be non-empty strings."):
        DQDatasetRule(
            criticality="error",
            check_func=sql_query,
            check_func_kwargs={
                "query": "SELECT FALSE AS condition",
                "merge_columns": ["id", ""],
                "condition_column": "condition",
            },
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


@pytest.mark.parametrize("window_minutes", [0, -1, None, 1440.5, True, False])
def test_has_no_gaps_per_time_window_exceptions(window_minutes):
    with pytest.raises(InvalidParameterError, match="window_minutes must be a positive integer"):
        has_no_gaps_per_time_window(column="event_date", window_minutes=window_minutes)


def test_has_no_gaps_per_time_window_invalid_group_by():
    with pytest.raises(InvalidParameterError, match="group_by must be a list"):
        has_no_gaps_per_time_window(column="event_date", window_minutes=1440, group_by="device")


def test_has_no_gaps_per_time_window_curr_timestamp_without_trailing_gap():
    with pytest.raises(InvalidParameterError, match="curr_timestamp can only be provided when trailing_gap is enabled"):
        has_no_gaps_per_time_window(
            column="event_date",
            window_minutes=1440,
            curr_timestamp=F.current_timestamp(),
        )


@pytest.mark.parametrize(
    "expected_schema, ref_df_name, ref_table",
    [
        ("a: string, b: int", None, "catalog.schema.table"),
        ("a: string, b: int", "ref_df", None),
        (None, "ref_df", "catalog.schema.table"),
        ("a: string, b: int", "ref_df", "catalog.schema.table"),
        (None, None, None),
    ],
)
def test_has_valid_schema_parameter_validation(expected_schema, ref_df_name, ref_table):
    with pytest.raises(
        InvalidParameterError,
        match="Must specify one of 'expected_schema', 'ref_df_name', or 'ref_table' when using 'has_valid_schema'",
    ):
        DQDatasetRule(
            criticality="warn",
            check_func=check_funcs.has_valid_schema,
            check_func_kwargs={
                "expected_schema": expected_schema,
                "ref_df_name": ref_df_name,
                "ref_table": ref_table,
            },
        )


@pytest.mark.parametrize(
    "ref_df_name, ref_table, expected_exception, expected_message",
    [
        (
            "ref_df",
            "table",
            InvalidParameterError,
            "Both 'ref_df_name' and 'ref_table' were provided. Please provide only one to avoid ambiguity.",
        ),
        (
            None,
            None,
            MissingParameterError,
            "Either 'ref_df_name' or 'ref_table' is required but neither was provided.",
        ),
        (
            "",
            None,
            MissingParameterError,
            "Either 'ref_df_name' or 'ref_table' is required but neither was provided.",
        ),
        (
            None,
            "",
            MissingParameterError,
            "Either 'ref_df_name' or 'ref_table' is required but neither was provided.",
        ),
    ],
)
def test_aggr_matches_dataset_ref_params_exceptions(ref_df_name, ref_table, expected_exception, expected_message):
    """ref_df_name/ref_table must be exactly one of the two, otherwise the rule fails to build."""
    with pytest.raises(expected_exception, match=expected_message):
        DQDatasetRule(
            criticality="warn",
            check_func=check_funcs.aggr_matches_dataset,
            column="id",
            check_func_kwargs={
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
def test_aggr_matches_dataset_invalid_tolerance_exceptions(abs_tolerance, rel_tolerance):
    """Negative abs_tolerance/rel_tolerance must be rejected at rule-build time."""
    with pytest.raises(
        InvalidParameterError, match="Absolute and/or relative tolerances if provided must be non-negative"
    ):
        DQDatasetRule(
            criticality="warn",
            check_func=check_funcs.aggr_matches_dataset,
            column="id",
            check_func_kwargs={
                "ref_table": "catalog.schema.ref_table",
                "abs_tolerance": abs_tolerance,
                "rel_tolerance": rel_tolerance,
            },
        )


@pytest.mark.parametrize("column", ["*", F.expr("*"), F.col("*")])
def test_resolve_aggregate_column_canonicalizes_star_forms(column):
    """Contract for #1435: every "*" form (the string "*", F.expr("*"), F.col("*")) must resolve to the
    same canonical ("", "*") name pair, so count(*) checks are identical however they are constructed and
    the CASE WHEN placeholder substitution can rely on aggr_col_str == "*". This pins the Spark string
    rendering the canonicalization depends on (notably F.col("*") -> "unresolvedstar()")."""
    col_str_norm, col_str, _ = check_funcs.resolve_aggregate_column(column)
    assert (col_str_norm, col_str) == ("", "*")


def test_resolve_aggregate_column_preserves_non_star_columns():
    """A non-star column is returned unchanged by resolve_aggregate_column."""
    col_str_norm, col_str, _ = check_funcs.resolve_aggregate_column(F.col("revenue"))
    assert (col_str_norm, col_str) == ("revenue", "revenue")


@pytest.mark.parametrize("uses_placeholder", [True, False])
def test_validate_star_aggregate_rejects_single_arg_aggregates(uses_placeholder):
    """A star column never works with single-arg aggregates (sum, avg, ...) on either path. See #1435."""
    with pytest.raises(InvalidParameterError, match="Column '\\*'.*only supported with"):
        check_funcs.validate_star_aggregate("*", "sum", uses_placeholder=uses_placeholder)


def test_validate_star_aggregate_allows_count_everywhere_and_non_star():
    """count(*) is valid on both paths, and a non-star column is never rejected."""
    check_funcs.validate_star_aggregate("*", "count", uses_placeholder=True)
    check_funcs.validate_star_aggregate("*", "count", uses_placeholder=False)
    check_funcs.validate_star_aggregate("revenue", "sum", uses_placeholder=True)


def test_validate_star_aggregate_count_distinct_native_only():
    """count(DISTINCT *) works natively (allowed) but the filtered-count placeholder collapses it to 1,
    so it is rejected on the placeholder path. See #1435."""
    check_funcs.validate_star_aggregate("*", "count_distinct", uses_placeholder=False)  # native: allowed
    with pytest.raises(InvalidParameterError, match="with a row filter is only supported with 'count'"):
        check_funcs.validate_star_aggregate("*", "count_distinct", uses_placeholder=True)


def test_validate_star_aggregate_is_case_sensitive():
    """Aggregate names resolve case-sensitively elsewhere (getattr(F, aggr_type)), so 'COUNT' with the star
    column must be rejected rather than passing validation and failing later with a confusing error (#1435)."""
    with pytest.raises(InvalidParameterError, match="only supported with"):
        check_funcs.validate_star_aggregate("*", "COUNT", uses_placeholder=False)


@pytest.mark.parametrize("column", ["*", F.col("*"), F.expr("*")])
def test_is_aggr_star_rejects_single_arg_aggregate(column):
    """is_aggr_* with column='*' and a single-arg aggregate is rejected at build time, regardless of the
    star form, rather than failing later in Spark. See #1435."""
    with pytest.raises(InvalidParameterError, match="only supported with"):
        check_funcs.is_aggr_not_less_than(column, limit=1, aggr_type="sum")


@pytest.mark.parametrize("column", ["*", F.col("*"), F.expr("*")])
def test_is_aggr_star_count_distinct_allowed_unfiltered_rejected_when_filtered(column):
    """count(DISTINCT *) is valid unfiltered (must not raise), but the filtered-count placeholder would make
    it silently wrong, so it is rejected when a row_filter is present. See #1435."""
    check_funcs.is_aggr_not_less_than(column, limit=1, aggr_type="count_distinct")  # unfiltered: allowed
    with pytest.raises(InvalidParameterError, match="with a row filter is only supported with 'count'"):
        check_funcs.is_aggr_not_less_than(column, limit=1, aggr_type="count_distinct", row_filter="id > 0")


@pytest.mark.parametrize("ref_column", ["*", F.col("*"), F.expr("*")])
def test_aggr_matches_dataset_star_ref_column_rejects_single_arg(ref_column):
    """A star *reference* column with a single-arg aggregate is rejected at build time, since the reference
    aggregate is built directly (bypassing _is_aggr_compare's validation). See #1435."""
    with pytest.raises(InvalidParameterError, match="only supported with"):
        check_funcs.aggr_matches_dataset("revenue", ref_table="cat.sch.ref", ref_column=ref_column, aggr_type="sum")


@pytest.mark.parametrize("ref_column", ["*", F.col("*"), F.expr("*")])
def test_aggr_matches_dataset_star_ref_column_allows_count_distinct(ref_column):
    """count(DISTINCT *) as a reference column is valid: the reference side aggregates natively via
    DataFrame.filter, so it must not be rejected. See #1435."""
    check_funcs.aggr_matches_dataset("id", ref_table="cat.sch.ref", ref_column=ref_column, aggr_type="count_distinct")
