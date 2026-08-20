"""Signature compatibility contract for registered public check functions."""

import inspect
from collections.abc import Callable

import pytest

from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.geo import check_funcs as geo_check_funcs
from databricks.labs.dqx.pii import pii_detection_funcs
from databricks.labs.dqx.rule import CHECK_FUNC_REGISTRY


CHECK_FUNCTION_MODULES = (check_funcs, geo_check_funcs, pii_detection_funcs)
POSITIONAL_OR_KEYWORD = inspect.Parameter.POSITIONAL_OR_KEYWORD
KEYWORD_ONLY = inspect.Parameter.KEYWORD_ONLY

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
    "is_valid_uuid": ("column", "strict"),
    "is_valid_country_code": ("column", "code_format", "case_sensitive"),
    "is_valid_currency_code": ("column", "code_format", "case_sensitive"),
    "is_valid_subdivision_code": ("column", "case_sensitive", "country_column"),
    "is_valid_language_code": ("column", "code_format", "case_sensitive"),
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
    "has_no_sequence_gaps": ("column", "step", "group_by"),
    "has_valid_schema": ("expected_schema", "ref_df_name", "ref_table", "columns", "strict", "exclude_columns"),
    "is_valid_json": ("column",),
    "has_json_keys": ("column", "keys", "require_all"),
    "has_valid_json_schema": ("column", "schema"),
    "is_latitude": ("column",),
    "is_longitude": ("column",),
    "is_geometry": ("column",),
    "is_geography": ("column",),
    "is_point": ("column",),
    "is_linestring": ("column",),
    "is_polygon": ("column",),
    "is_multipoint": ("column",),
    "is_multilinestring": ("column",),
    "is_multipolygon": ("column",),
    "is_geometrycollection": ("column",),
    "is_ogc_valid": ("column",),
    "is_non_empty_geometry": ("column",),
    "is_not_null_island": ("column",),
    "has_dimension": ("column", "dimension"),
    "has_x_coordinate_between": ("column", "min_value", "max_value"),
    "has_y_coordinate_between": ("column", "min_value", "max_value"),
    "is_area_equal_to": ("column", "value", "srid", "geodesic"),
    "is_area_not_equal_to": ("column", "value", "srid", "geodesic"),
    "is_area_not_greater_than": ("column", "value", "srid", "geodesic"),
    "is_area_not_less_than": ("column", "value", "srid", "geodesic"),
    "is_num_points_equal_to": ("column", "value"),
    "is_num_points_not_equal_to": ("column", "value"),
    "is_num_points_not_greater_than": ("column", "value"),
    "is_num_points_not_less_than": ("column", "value"),
    "are_polygons_mutually_disjoint": ("column", "row_filter"),
    "is_geo_contains": ("column", "reference_geometry", "convert_column", "convert_reference_geometry"),
    "is_geo_covers": (
        "column",
        "reference_geometry",
        "precise",
        "resolution",
        "convert_column",
        "convert_reference_geometry",
    ),
    "is_geo_intersects": (
        "column",
        "reference_geometry",
        "precise",
        "resolution",
        "convert_column",
        "convert_reference_geometry",
    ),
    "is_geo_touches": ("column", "reference_geometry", "convert_column", "convert_reference_geometry"),
    "is_geo_within": ("column", "reference_geometry", "convert_column", "convert_reference_geometry"),
    "does_not_contain_pii": ("column", "language", "threshold", "entities", "nlp_engine_config"),
}

EXPECTED_PARAMETER_KIND_OVERRIDES = {
    "has_no_aggr_outliers": (
        POSITIONAL_OR_KEYWORD,
        POSITIONAL_OR_KEYWORD,
        KEYWORD_ONLY,
        KEYWORD_ONLY,
        KEYWORD_ONLY,
        KEYWORD_ONLY,
        KEYWORD_ONLY,
        KEYWORD_ONLY,
        KEYWORD_ONLY,
        KEYWORD_ONLY,
    )
}


def _registered_check_functions() -> dict[str, Callable]:
    functions: dict[str, Callable] = {}
    for module in CHECK_FUNCTION_MODULES:
        functions.update(
            (function_name, function)
            for function_name in CHECK_FUNC_REGISTRY
            if (function := getattr(module, function_name, None)) is not None
        )
    return functions


# The registry is fixed at import time, so resolve the check functions once and reuse the mapping
# across the parametrized cases below instead of rebuilding it for every function.
REGISTERED_CHECK_FUNCTIONS = _registered_check_functions()


def test_signature_contract_covers_all_registered_check_functions():
    # Compare against the production check functions only (registry names that resolve to a function in
    # check_funcs / geo / pii), not the raw CHECK_FUNC_REGISTRY. The registry is a process-global that
    # other test modules mutate by decorating their own dummy checks with @register_rule; asserting
    # against it directly makes this contract order-of-execution dependent and fail under the full suite.
    assert set(EXPECTED_PARAMETER_ORDER) == set(REGISTERED_CHECK_FUNCTIONS)


@pytest.mark.parametrize(("function_name", "expected_order"), EXPECTED_PARAMETER_ORDER.items())
def test_registered_check_function_parameter_order(function_name: str, expected_order: tuple[str, ...]):
    check_function = REGISTERED_CHECK_FUNCTIONS[function_name]
    parameters = inspect.signature(check_function).parameters
    expected_kinds = EXPECTED_PARAMETER_KIND_OVERRIDES.get(
        function_name, (POSITIONAL_OR_KEYWORD,) * len(expected_order)
    )
    expected_signature = tuple(zip(expected_order, expected_kinds, strict=True))
    actual_signature = tuple((parameter.name, parameter.kind) for parameter in parameters.values())

    assert actual_signature == expected_signature
