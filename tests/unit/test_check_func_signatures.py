import inspect

import pytest

from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.rule import CHECK_FUNC_REGISTRY


EXPECTED_PARAMETER_ORDER = {
    "is_not_null_and_not_empty": ("column", "trim_strings"),
    "is_not_empty": ("column", "trim_strings"),
    "is_not_null": ("column",),
    "is_null": ("column",),
    "is_empty": ("column", "trim_strings"),
    "is_null_or_empty": ("column", "trim_strings"),
    "has_valid_string_case": ("column", "case"),
    "is_not_null_and_is_in_list": ("column", "allowed", "case_sensitive"),
    "is_in_list": ("column", "allowed", "case_sensitive"),
    "is_not_in_list": ("column", "forbidden", "case_sensitive"),
    "sql_expression": ("expression", "msg", "name", "negate", "columns"),
    "is_older_than_col2_for_n_days": ("column1", "column2", "days", "negate"),
    "is_older_than_n_days": ("column", "days", "curr_date", "negate"),
    "is_not_in_future": ("column", "offset", "curr_timestamp"),
    "is_not_in_near_future": ("column", "offset", "curr_timestamp"),
    "is_equal_to": ("column", "value", "abs_tolerance", "rel_tolerance"),
    "is_not_equal_to": ("column", "value", "abs_tolerance", "rel_tolerance"),
    "is_not_less_than": ("column", "limit"),
    "is_not_greater_than": ("column", "limit"),
    "is_in_range": ("column", "min_limit", "max_limit"),
    "is_not_in_range": ("column", "min_limit", "max_limit"),
    "regex_match": ("column", "regex", "negate"),
    "is_not_null_and_not_empty_array": ("column",),
    "is_valid_date": ("column", "date_format"),
    "is_valid_timestamp": ("column", "timestamp_format"),
    "is_valid_ipv4_address": ("column",),
    "is_valid_email": ("column",),
    "is_valid_national_id": ("column", "country"),
    "is_valid_country_code": ("column", "code_format", "case_sensitive"),
    "is_valid_currency_code": ("column", "code_format", "case_sensitive"),
    "is_ipv4_address_in_cidr": ("column", "cidr_block"),
    "is_valid_ipv6_address": ("column",),
    "is_ipv6_address_in_cidr": ("column", "cidr_block"),
    "is_data_fresh": ("column", "max_age_minutes", "base_timestamp"),
    "has_no_outliers": ("column", "row_filter"),
    "is_unique": ("columns", "nulls_distinct", "row_filter"),
    "foreign_key": ("columns", "ref_columns", "ref_df_name", "ref_table", "negate", "row_filter", "null_safe"),
    "sql_query": (
        "query",
        "merge_columns",
        "msg",
        "name",
        "negate",
        "condition_column",
        "input_placeholder",
        "row_filter",
    ),
    "is_aggr_not_greater_than": ("column", "limit", "aggr_type", "group_by", "row_filter", "aggr_params"),
    "is_aggr_not_less_than": ("column", "limit", "aggr_type", "group_by", "row_filter", "aggr_params"),
    "is_aggr_equal": (
        "column",
        "limit",
        "aggr_type",
        "group_by",
        "row_filter",
        "aggr_params",
        "abs_tolerance",
        "rel_tolerance",
    ),
    "is_aggr_not_equal": (
        "column",
        "limit",
        "aggr_type",
        "group_by",
        "row_filter",
        "aggr_params",
        "abs_tolerance",
        "rel_tolerance",
    ),
    "has_no_aggr_outliers": (
        "column",
        "time_column",
        "aggr_type",
        "sigma",
        "lookback_num_intervals",
        "warmup_num_intervals",
        "time_interval",
        "group_by",
        "row_filter",
        "aggr_params",
    ),
    "aggr_matches_dataset": (
        "column",
        "ref_table",
        "ref_df_name",
        "ref_column",
        "aggr_type",
        "aggr_params",
        "group_by",
        "ref_group_by",
        "row_filter",
        "ref_row_filter",
        "abs_tolerance",
        "rel_tolerance",
    ),
    "compare_datasets": (
        "columns",
        "ref_columns",
        "ref_df_name",
        "ref_table",
        "check_missing_records",
        "exclude_columns",
        "null_safe_row_matching",
        "null_safe_column_value_matching",
        "row_filter",
        "abs_tolerance",
        "rel_tolerance",
    ),
    "is_data_fresh_per_time_window": (
        "column",
        "window_minutes",
        "min_records_per_window",
        "lookback_windows",
        "row_filter",
        "curr_timestamp",
    ),
    "has_no_gaps_per_time_window": ("column", "window_minutes", "group_by", "trailing_gap", "curr_timestamp"),
    "has_valid_schema": ("expected_schema", "ref_df_name", "ref_table", "columns", "strict", "exclude_columns"),
    "is_valid_json": ("column",),
    "has_json_keys": ("column", "keys", "require_all"),
    "has_valid_json_schema": ("column", "schema"),
}


def test_signature_contract_covers_all_registered_check_functions():
    core_check_functions = {
        function_name
        for function_name in CHECK_FUNC_REGISTRY
        if getattr(check_funcs, function_name, None)
        and getattr(check_funcs, function_name).__module__ == check_funcs.__name__
    }

    assert set(EXPECTED_PARAMETER_ORDER) == core_check_functions


@pytest.mark.parametrize(("function_name", "expected_order"), EXPECTED_PARAMETER_ORDER.items())
def test_registered_check_function_parameter_order(function_name: str, expected_order: tuple[str, ...]):
    check_function = getattr(check_funcs, function_name)

    assert tuple(inspect.signature(check_function).parameters) == expected_order
