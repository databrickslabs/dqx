import re

import pyspark.sql.functions as F
import pytest
from pyspark.sql import Column
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
)

from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.array_path import ArrayPath, has_array_wildcard
from databricks.labs.dqx.check_funcs import make_condition
from databricks.labs.dqx.checks_serializer import deserialize_checks, serialize_checks
from databricks.labs.dqx.errors import InvalidCheckError
from databricks.labs.dqx.geo import check_funcs as geo_check_funcs
from databricks.labs.dqx.pii import pii_detection_funcs
from databricks.labs.dqx.rule import DQDatasetRule, DQForEachColRule, DQRowRule

TWO_LEVEL_PATH = "users[*].events[*].payload.event_date"

SAMPLE_SCHEMA = StructType(
    [
        StructField("feed_id", StringType()),
        StructField(
            "users",
            ArrayType(
                StructType(
                    [
                        StructField("user_id", StringType()),
                        StructField("email", StringType()),
                        StructField(
                            "events",
                            ArrayType(
                                StructType(
                                    [
                                        StructField("event_id", StringType()),
                                        StructField(
                                            "payload",
                                            StructType(
                                                [
                                                    StructField("event_date", StringType()),
                                                    StructField("event_ts", StringType()),
                                                ]
                                            ),
                                        ),
                                    ]
                                )
                            ),
                        ),
                    ]
                )
            ),
        ),
        StructField("tags", ArrayType(StringType())),
        StructField("matrix", ArrayType(ArrayType(StringType()))),
        StructField(
            "payload",
            StructType([StructField("items", ArrayType(StructType([StructField("sku", StringType())])))]),
        ),
        StructField("attrs", MapType(StringType(), ArrayType(StringType()))),
    ]
)


def custom_check_with_optional_column(column: str | Column | None = None, limit: int = 10) -> Column:
    column_expr = F.col(column) if isinstance(column, str) else column
    if column_expr is None:
        raise ValueError("column is required")
    return make_condition(column_expr > F.lit(limit), "value is above limit", "items_below_limit")


def is_valid_json(column: str | Column) -> Column:
    """Custom check that shares its name with the built-in *is_valid_json*."""
    column_expr = F.col(column) if isinstance(column, str) else column
    return make_condition(column_expr.isNull(), "value is null", "custom_is_valid_json")


@pytest.mark.parametrize(
    "column, expected_column, expected_steps",
    [
        pytest.param("tags[*]", "tags", (None,), id="single_level_array_path"),
        pytest.param("users[*].email", "users", (None, "email"), id="struct_field_under_array"),
        pytest.param(TWO_LEVEL_PATH, "users", (None, "events", None, "payload", "event_date"), id="two_level"),
        pytest.param("payload.items[*].sku", "payload", ("items", None, "sku"), id="starts_with_struct_field"),
        pytest.param("matrix[*][*]", "matrix", (None, None), id="consecutive_wildcards"),
        pytest.param("orders[*].`line items`[*].sku", "orders", (None, "line items", None, "sku"), id="back_quoted"),
        pytest.param("`my_weird_col`[*].field", "my_weird_col", (None, "field"), id="wildcard_after_back_quotes"),
        pytest.param("`odd``name`[*].x", "odd`name", (None, "x"), id="doubled_backtick_is_escaped_backtick"),
        pytest.param("users[*].`it's`", "users", (None, "it's"), id="back_quoted_field_with_quote_character"),
    ],
)
def test_dq_row_rule_uses_array_path_mode_when_column_is_array_path(
    column: str, expected_column: str, expected_steps: tuple[str | None, ...]
) -> None:
    rule = DQRowRule(check_func=check_funcs.is_not_null, column=column)

    assert rule.array_path == ArrayPath(path=column, column=expected_column, steps=expected_steps)


def test_dq_row_rule_passes_element_by_keyword_when_check_function_column_parameter_is_optional() -> None:
    rule = DQRowRule(check_func=custom_check_with_optional_column, column="items[*]", check_func_kwargs={"limit": 5})

    assert rule.array_path is not None
    assert rule.name == "items_below_limit"


@pytest.mark.parametrize(
    "check_func, check_func_kwargs",
    [
        pytest.param(check_funcs.is_valid_date, {"date_format": "yyyy-MM-dd"}, id="is_valid_date"),
        pytest.param(
            check_funcs.is_valid_timestamp, {"timestamp_format": "yyyy-MM-dd HH:mm:ss"}, id="is_valid_timestamp"
        ),
        pytest.param(check_funcs.is_null, {}, id="is_null"),
        pytest.param(check_funcs.is_not_null, {}, id="is_not_null"),
        pytest.param(check_funcs.regex_match, {"regex": "^[0-9-]+$"}, id="regex_match"),
    ],
)
def test_dq_row_rule_uses_array_path_mode_when_check_function_is_a_priority_check(
    check_func, check_func_kwargs: dict
) -> None:
    rule = DQRowRule(check_func=check_func, column=TWO_LEVEL_PATH, check_func_kwargs=check_func_kwargs)

    assert rule.array_path is not None
    assert rule.name.startswith("users_events_payload_event_date_")


def test_dq_row_rule_uses_array_path_mode_when_custom_check_shares_name_with_unsupported_built_in_check() -> None:
    rule = DQRowRule(check_func=is_valid_json, column="items[*]")

    assert rule.array_path is not None


def test_dq_row_rule_passes_other_arguments_positionally_when_check_func_args_are_given_with_array_path() -> None:
    rule = DQRowRule(check_func=check_funcs.regex_match, column="tags[*]", check_func_args=["^[a-z]+$"])

    assert rule.array_path is not None
    assert rule.name == "tags_not_matching_regex"
    assert rule.to_dict() == {
        "name": "tags_not_matching_regex",
        "criticality": "error",
        "check": {"function": "regex_match", "arguments": {"column": "tags[*]", "regex": "^[a-z]+$"}},
    }


@pytest.mark.parametrize(
    "column",
    [
        pytest.param("`my_weird_col[*]`.`field`", id="wildcard_inside_back_quoted_name"),
        pytest.param("payload.event_date", id="plain_dotted_path"),
        pytest.param("arr[0].field", id="index_without_wildcard"),
        pytest.param("regexp_replace(name, '[*]', '')", id="wildcard_inside_single_quoted_string"),
        pytest.param('concat(name, "[*]")', id="wildcard_inside_double_quoted_string"),
        pytest.param("concat(name, 'it''s [*]')", id="wildcard_after_doubled_quote_escape_in_string"),
        pytest.param("concat(name, 'it\\'s [*]')", id="wildcard_after_backslash_quote_escape_in_string"),
        pytest.param("regexp_replace(name, '\\\\[*]', '')", id="wildcard_after_escaped_backslash_in_string"),
        pytest.param("concat(name, '[*]", id="wildcard_inside_unclosed_string"),
    ],
)
def test_dq_row_rule_builds_as_today_when_column_has_no_wildcard_outside_quotes(column: str) -> None:
    rule = DQRowRule(check_func=check_funcs.is_not_null, column=column)

    assert not has_array_wildcard(column)
    assert rule.array_path is None
    assert rule.to_dict()["check"]["arguments"] == {"column": column}


def test_dq_row_rule_builds_as_today_when_column_is_spark_column_object() -> None:
    rule = DQRowRule(check_func=check_funcs.is_not_null, column=F.col("tags"))

    assert rule.array_path is None
    assert rule.name == "tags_is_null"


def test_dq_row_rule_generates_name_as_today_when_column_is_plain_dotted_path() -> None:
    rule = DQRowRule(check_func=check_funcs.is_valid_date, column="payload.event_date")

    assert rule.array_path is None
    assert rule.name == "payload_event_date_is_not_valid_date"


def test_dq_row_rule_builds_as_today_when_check_function_has_no_column_parameter() -> None:
    rule = DQRowRule(
        check_func=check_funcs.sql_expression,
        column="tags[*]",
        check_func_kwargs={"expression": "size(tags) > 0", "name": "tags_not_empty"},
    )

    assert rule.array_path is None
    assert rule.name == "tags_not_empty"


def test_dq_row_rule_builds_as_today_when_incompatible_check_has_no_wildcard() -> None:
    rule = DQRowRule(check_func=geo_check_funcs.is_geometry, column="value")

    assert rule.array_path is None
    assert rule.name == "value_is_not_geometry"


def test_dq_dataset_rule_builds_as_today_when_column_contains_wildcard() -> None:
    rule = DQDatasetRule(
        check_func=check_funcs.is_aggr_not_greater_than, column="users[*]..user_id", check_func_kwargs={"limit": 1}
    )

    assert rule.to_dict()["check"]["arguments"]["column"] == "users[*]..user_id"


def test_dq_dataset_rule_builds_as_today_when_columns_contain_wildcard() -> None:
    rule = DQDatasetRule(check_func=check_funcs.is_unique, columns=["users[*]..user_id"])

    assert rule.to_dict()["check"]["arguments"]["columns"] == ["users[*]..user_id"]


def test_dq_row_rule_builds_when_path_has_20_array_levels() -> None:
    rule = DQRowRule(check_func=check_funcs.is_not_null, column=".".join(["a[*]"] * 20))

    assert rule.array_path is not None
    assert rule.array_path.steps.count(None) == 20


def test_dq_row_rule_raises_invalid_check_error_when_path_has_21_array_levels() -> None:
    with pytest.raises(InvalidCheckError, match="21 array levels exceed the maximum of 20"):
        DQRowRule(check_func=check_funcs.is_not_null, column=".".join(["a[*]"] * 21))


def test_dq_row_rule_ignores_struct_levels_in_depth_limit_when_path_has_20_array_levels_and_extra_struct_fields() -> (
    None
):
    column = ".".join(["a[*]"] * 20 + ["s"] * 30)

    rule = DQRowRule(check_func=check_funcs.is_not_null, column=column)

    assert rule.array_path is not None


def test_dq_row_rule_counts_each_consecutive_wildcard_in_depth_limit_when_path_has_21_wildcards_in_one_segment() -> (
    None
):
    with pytest.raises(InvalidCheckError, match="21 array levels"):
        DQRowRule(check_func=check_funcs.is_not_null, column="m" + "[*]" * 21)


@pytest.mark.parametrize(
    "column, reason",
    [
        pytest.param("users[*]..event_date", "empty segment", id="empty_segment"),
        pytest.param(".users[*].email", "empty segment", id="starts_with_dot"),
        pytest.param("users[*].", "empty segment", id="ends_with_dot"),
        pytest.param("users[*].[*]", "'[*]' must follow a name", id="wildcard_has_no_field_name"),
        pytest.param("[*].email", "path must start with a name", id="starts_with_wildcard"),
        pytest.param("users[*].arr[0]", "unsupported brackets", id="index_bracket"),
        pytest.param("users[*].arr[]", "unsupported brackets", id="empty_bracket"),
        pytest.param("users[*].arr[ * ]", "unsupported brackets", id="spaced_wildcard"),
        pytest.param("users[*].arr[**]", "unsupported brackets", id="double_star"),
        pytest.param("users[*].arr['key']", "unsupported brackets", id="quoted_key"),
        pytest.param("users[*].events[*", "unclosed", id="unclosed_bracket"),
        pytest.param("users[*].tags]", "unexpected ']'", id="stray_closing_bracket"),
        pytest.param("users[*].]x", "unexpected ']'", id="segment_starts_with_closing_bracket"),
        pytest.param("users[*]email", "expected '.' or '[*]'", id="text_follows_wildcard"),
    ],
)
def test_dq_row_rule_raises_invalid_check_error_when_array_path_is_malformed(column: str, reason: str) -> None:
    with pytest.raises(InvalidCheckError, match=re.escape(reason)) as exc_info:
        DQRowRule(check_func=check_funcs.is_not_null, column=column)

    assert column in str(exc_info.value)


def test_dq_row_rule_keeps_unclosed_back_quote_in_field_name_when_array_path_has_unclosed_back_quote() -> None:
    rule = DQRowRule(check_func=check_funcs.is_not_null, column="users[*].`event date")

    assert rule.array_path is not None
    assert rule.array_path.steps == (None, "`event date")
    assert not rule.array_path.matches_schema(SAMPLE_SCHEMA, case_sensitive=False)


@pytest.mark.parametrize(
    "column",
    [
        pytest.param("tags[*]", id="single_level_path_reaches_array"),
        pytest.param(TWO_LEVEL_PATH, id="two_level_path_reaches_arrays_and_structs"),
        pytest.param("matrix[*][*]", id="path_reaches_array_of_arrays"),
        pytest.param("payload.items[*].sku", id="path_starts_with_struct_field"),
        pytest.param("USERS[*].EVENTS[*].payload.event_date", id="names_differ_in_case_and_matching_is_insensitive"),
    ],
)
def test_array_path_matches_schema_when_path_follows_schema(column: str) -> None:
    assert ArrayPath.parse(column).matches_schema(SAMPLE_SCHEMA, case_sensitive=False)


@pytest.mark.parametrize(
    "column",
    [
        pytest.param("users[*].events[*].payload.missing", id="field_is_missing"),
        pytest.param("feed_id[*]", id="wildcard_is_on_non_array_field"),
        pytest.param("attrs[*]", id="wildcard_is_on_map_field"),
        pytest.param("users.events[*].payload.event_date", id="wildcard_is_left_out_on_array_of_structs"),
        pytest.param("attrs.key[*]", id="field_step_is_on_map"),
        pytest.param("missing[*]", id="top_level_column_is_missing"),
    ],
)
def test_array_path_does_not_match_schema_when_path_does_not_follow_schema(column: str) -> None:
    assert not ArrayPath.parse(column).matches_schema(SAMPLE_SCHEMA, case_sensitive=False)


def test_array_path_does_not_match_schema_when_names_differ_in_case_and_matching_is_case_sensitive() -> None:
    assert not ArrayPath.parse("USERS[*].email").matches_schema(SAMPLE_SCHEMA, case_sensitive=True)


def test_array_path_does_not_match_schema_when_field_name_is_ambiguous_and_matching_is_case_insensitive() -> None:
    schema = StructType([StructField("tags", ArrayType(IntegerType())), StructField("TAGS", ArrayType(IntegerType()))])

    assert not ArrayPath.parse("tags[*]").matches_schema(schema, case_sensitive=False)
    assert ArrayPath.parse("tags[*]").matches_schema(schema, case_sensitive=True)


def test_dq_row_rule_generates_name_from_path_and_check_when_name_not_provided() -> None:
    rule = DQRowRule(check_func=check_funcs.is_valid_date, column=TWO_LEVEL_PATH)

    assert rule.name == "users_events_payload_event_date_is_not_valid_date"


def test_dq_row_rule_keeps_given_name_when_name_provided_with_array_path() -> None:
    rule = DQRowRule(name="event_date_is_valid", check_func=check_funcs.is_valid_date, column=TWO_LEVEL_PATH)

    assert rule.name == "event_date_is_valid"


def test_dq_row_rule_keeps_path_unchanged_in_to_dict_when_column_is_array_path() -> None:
    rule = DQRowRule(
        check_func=check_funcs.is_valid_date, column=TWO_LEVEL_PATH, check_func_kwargs={"date_format": "yyyy-MM-dd"}
    )

    assert rule.to_dict() == {
        "name": "users_events_payload_event_date_is_not_valid_date",
        "criticality": "error",
        "check": {"function": "is_valid_date", "arguments": {"column": TWO_LEVEL_PATH, "date_format": "yyyy-MM-dd"}},
    }


def test_dq_row_rule_has_stable_fingerprint_when_built_twice_with_same_array_path() -> None:
    first = DQRowRule(check_func=check_funcs.is_valid_date, column=TWO_LEVEL_PATH)
    second = DQRowRule(check_func=check_funcs.is_valid_date, column=TWO_LEVEL_PATH)

    assert first.rule_fingerprint == second.rule_fingerprint


def test_dq_row_rule_has_different_fingerprint_when_array_paths_differ() -> None:
    first = DQRowRule(name="date_check", check_func=check_funcs.is_valid_date, column=TWO_LEVEL_PATH)
    second = DQRowRule(name="date_check", check_func=check_funcs.is_valid_date, column="users[*].events[*].date")

    assert first.rule_fingerprint != second.rule_fingerprint


SAMPLE_CHECKS = [
    {
        "name": "event_date_is_valid",
        "criticality": "error",
        "check": {
            "function": "is_valid_date",
            "arguments": {"column": TWO_LEVEL_PATH, "date_format": "yyyy-MM-dd"},
        },
    },
    {
        "name": "event_ts_is_valid",
        "criticality": "error",
        "check": {
            "function": "is_valid_timestamp",
            "arguments": {"column": "users[*].events[*].payload.event_ts", "timestamp_format": "yyyy-MM-dd HH:mm:ss"},
        },
    },
    {
        "name": "tags_are_lowercase_words",
        "criticality": "warn",
        "check": {"function": "regex_match", "arguments": {"column": "tags[*]", "regex": "^[a-z]+$"}},
    },
]


def test_deserialize_checks_builds_array_path_rule_when_metadata_column_has_wildcard() -> None:
    rules = deserialize_checks(SAMPLE_CHECKS)

    assert [type(rule) for rule in rules] == [DQRowRule, DQRowRule, DQRowRule]
    assert [rule.array_path.path for rule in rules if isinstance(rule, DQRowRule) and rule.array_path] == [
        TWO_LEVEL_PATH,
        "users[*].events[*].payload.event_ts",
        "tags[*]",
    ]


def test_serialize_checks_round_trips_array_path_rule_without_changes() -> None:
    serialized = serialize_checks(deserialize_checks(SAMPLE_CHECKS))

    assert serialize_checks(deserialize_checks(serialized)) == serialized
    assert serialized == SAMPLE_CHECKS


def test_dq_for_each_col_rule_builds_one_array_path_row_rule_per_path_when_columns_contain_wildcards() -> None:
    rules = DQForEachColRule(check_func=check_funcs.is_not_null, columns=["tags[*]", "users[*].email"]).get_rules()

    assert [rule.array_path.path for rule in rules if isinstance(rule, DQRowRule) and rule.array_path] == [
        "tags[*]",
        "users[*].email",
    ]
    assert [rule.name for rule in rules] == ["tags_is_null", "users_email_is_null"]


def test_dq_row_rule_builds_when_message_expr_is_set_with_array_path() -> None:
    rule = DQRowRule(check_func=check_funcs.is_not_null, column="tags[*]", message_expr="concat('bad feed ', feed_id)")

    assert rule.array_path is not None
    assert rule.to_dict()["message_expr"] == "concat('bad feed ', feed_id)"


def test_dq_row_rule_builds_when_filter_is_set_with_array_path() -> None:
    rule = DQRowRule(check_func=check_funcs.is_not_null, column="tags[*]", filter="feed_id = 'F-002'")

    assert rule.array_path is not None
    assert rule.to_dict()["filter"] == "feed_id = 'F-002'"


@pytest.mark.parametrize(
    "check_func, check_func_kwargs",
    [
        pytest.param(check_funcs.is_valid_ipv6_address, {}, id="is_valid_ipv6_address"),
        pytest.param(check_funcs.is_ipv6_address_in_cidr, {"cidr_block": "2001:db8::/32"}, id="is_ipv6_in_cidr"),
        pytest.param(check_funcs.is_valid_json, {}, id="is_valid_json"),
        pytest.param(check_funcs.has_json_keys, {"keys": ["a"]}, id="has_json_keys"),
        pytest.param(check_funcs.has_valid_json_schema, {"schema": "STRUCT<a: BIGINT>"}, id="has_valid_json_schema"),
        pytest.param(pii_detection_funcs.does_not_contain_pii, {}, id="does_not_contain_pii"),
        pytest.param(geo_check_funcs.is_geometry, {}, id="is_geometry"),
    ],
)
def test_dq_row_rule_raises_invalid_check_error_when_check_cannot_run_on_array_elements(
    check_func, check_func_kwargs: dict
) -> None:
    with pytest.raises(InvalidCheckError, match="does not support array paths"):
        DQRowRule(check_func=check_func, column="users[*].value", check_func_kwargs=check_func_kwargs)
