import pyspark.sql.functions as F
import pytest
import yaml
from pyspark.errors import AnalysisException
from pyspark.sql import Column, DataFrame

from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.check_funcs import make_condition
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQDatasetRule, DQRowRule
from tests.integration.conftest import EXTRA_PARAMS, RUN_ID

DATE_PATH = "users[*].events[*].payload.event_date"
TS_PATH = "users[*].events[*].payload.event_ts"
VALID_TS = "2024-01-01 00:00:00"

USERS_TYPE = (
    "array<struct<user_id: string, events: array<struct<event_id: string, "
    "payload: struct<event_date: string, event_ts: string>>>>>"
)
SAMPLE_SCHEMA = f"feed_id string, users {USERS_TYPE}, tags array<string>, attrs map<string, array<string>>"

SAMPLE_CHECKS_YAML = """
- name: event_date_is_valid
  criticality: error
  check:
    function: is_valid_date
    arguments:
      column: users[*].events[*].payload.event_date
      date_format: yyyy-MM-dd

- name: event_ts_is_valid
  criticality: error
  check:
    function: is_valid_timestamp
    arguments:
      column: users[*].events[*].payload.event_ts
      timestamp_format: yyyy-MM-dd HH:mm:ss

- name: tags_are_lowercase_words
  criticality: warn
  check:
    function: regex_match
    arguments:
      column: tags[*]
      regex: ^[a-z]+$
"""

SAMPLE_CHECKS = [
    DQRowRule(
        name="event_date_is_valid",
        criticality="error",
        check_func=check_funcs.is_valid_date,
        column=DATE_PATH,
        check_func_kwargs={"date_format": "yyyy-MM-dd"},
    ),
    DQRowRule(
        name="event_ts_is_valid",
        criticality="error",
        check_func=check_funcs.is_valid_timestamp,
        column=TS_PATH,
        check_func_kwargs={"timestamp_format": "yyyy-MM-dd HH:mm:ss"},
    ),
    DQRowRule(
        name="tags_are_lowercase_words",
        criticality="warn",
        check_func=check_funcs.regex_match,
        column="tags[*]",
        check_func_kwargs={"regex": "^[a-z]+$"},
    ),
]

INVALID_FEED_IDS = ["F-002", "F-003", "F-008", "F-009"]


def event(event_id: str, event_date: str | None, event_ts: str = VALID_TS) -> tuple:
    return event_id, (event_date, event_ts)


def array_message(path: str, function: str, count: int, values: list[str], distinct: int | None = None) -> str:
    """Expected message; *distinct* defaults to the number of values shown (all distinct values are shown)."""
    distinct_count = len(values) if distinct is None else distinct
    return (
        f"Array elements in Column '{path}' failed check '{function}'. "
        f"{count} failing values, {distinct_count} distinct (showing first {len(values)}): {', '.join(values)}"
    )


def date_message(count: int, values: list[str]) -> str:
    return array_message(DATE_PATH, "is_valid_date", count, values)


def skipped_message(path: str) -> str:
    return f"Check evaluation skipped due to invalid check columns: ['{path}']"


def collect_messages(checked_df: DataFrame, key: str = "feed_id", column: str = "_errors") -> dict:
    """Returns the (name, message) pairs of the result column for each row, keyed by *key*."""
    return {
        row[key]: [(item["name"], item["message"]) for item in (row[column] or [])]
        for row in checked_df.select(key, column).collect()
    }


def apply_rule(ws, df: DataFrame, rule: DQRowRule, key: str = "id") -> dict:
    checked_df = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS).apply_checks(df, [rule])
    return {
        row_key: [message for _, message in messages] for row_key, messages in collect_messages(checked_df, key).items()
    }


@pytest.fixture
def sample_feed_df(spark) -> DataFrame:
    rows = [
        ["F-001", [("u1", [event("e1", "2024-01-15"), event("e2", "2024-02-01")])], ["retail", "online"], None],
        ["F-002", [("u1", [event("e1", "2024-01-15"), event("e2", "2024-13-45")])], ["retail"], None],
        [
            "F-003",
            [
                ("u1", [event("e1", "15/01/2024", "2024-01-15 10:00:00")]),
                (
                    "u2",
                    [
                        event("e2", "2024-03-10", "2024-03-10 08:30:00"),
                        event("e3", "not-a-date", "2024-03-10 25:61:00"),
                    ],
                ),
            ],
            ["retail", ""],
            None,
        ],
        ["F-004", None, None, None],
        ["F-005", [], [], None],
        ["F-006", [("u1", None)], ["online"], None],
        ["F-007", [("u1", [event("e1", None)])], ["online"], None],
        ["F-008", [("u1", None), ("u2", [event("e2", "BAD")])], ["online"], None],
        [
            "F-009",
            [("u1", [event(f"e{i}", date) for i, date in enumerate(["x", "x", "y", "z", "w", "2024-01-01"])])],
            ["online"],
            None,
        ],
        ["F-010", [("u1", [None, event("e2", "2024-01-01")])], ["online"], {"k": ["v"]}],
    ]
    return spark.createDataFrame(rows, SAMPLE_SCHEMA)


EXPECTED_SAMPLE_ERRORS: dict[str, list[tuple[str, str]]] = {
    "F-001": [],
    "F-002": [("event_date_is_valid", date_message(1, ["'2024-13-45'"]))],
    "F-003": [
        ("event_date_is_valid", date_message(2, ["'15/01/2024'", "'not-a-date'"])),
        ("event_ts_is_valid", array_message(TS_PATH, "is_valid_timestamp", 1, ["'2024-03-10 25:61:00'"])),
    ],
    "F-004": [],
    "F-005": [],
    "F-006": [],
    "F-007": [],
    "F-008": [("event_date_is_valid", date_message(1, ["'BAD'"]))],
    "F-009": [("event_date_is_valid", date_message(5, ["'x'", "'y'", "'z'", "'w'"]))],
    "F-010": [],
}

EXPECTED_SAMPLE_WARNINGS: dict[str, list[tuple[str, str]]] = {feed_id: [] for feed_id in EXPECTED_SAMPLE_ERRORS} | {
    "F-003": [("tags_are_lowercase_words", array_message("tags[*]", "regex_match", 1, ["''"]))]
}


def test_apply_checks_by_metadata_returns_expected_results_when_sample_feed_has_nested_array_failures(
    ws, sample_feed_df, set_utc_timezone
):
    checked_df = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS).apply_checks_by_metadata(
        sample_feed_df, yaml.safe_load(SAMPLE_CHECKS_YAML)
    )

    assert collect_messages(checked_df) == EXPECTED_SAMPLE_ERRORS
    assert collect_messages(checked_df, column="_warnings") == EXPECTED_SAMPLE_WARNINGS
    error = checked_df.where("feed_id = 'F-002'").select(F.col("_errors")[0].alias("error")).first()["error"]
    assert error["columns"] == [DATE_PATH]
    assert error["function"] == "is_valid_date"
    assert error["filter"] is None
    assert error["run_id"] == RUN_ID
    assert error["skipped"] is None


def test_apply_checks_and_split_quarantines_rows_with_failing_elements_when_criticality_is_error(
    ws, sample_feed_df, set_utc_timezone
):
    valid_df, invalid_df = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS).apply_checks_and_split(
        sample_feed_df, SAMPLE_CHECKS
    )

    assert sorted(row.feed_id for row in invalid_df.select("feed_id").collect()) == INVALID_FEED_IDS
    assert sorted(row.feed_id for row in valid_df.select("feed_id").collect()) == [
        "F-001",
        "F-004",
        "F-005",
        "F-006",
        "F-007",
        "F-010",
    ]


def test_apply_checks_by_metadata_and_split_quarantines_rows_with_failing_elements_when_criticality_is_error(
    ws, sample_feed_df, set_utc_timezone
):
    _, invalid_df = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS).apply_checks_by_metadata_and_split(
        sample_feed_df, yaml.safe_load(SAMPLE_CHECKS_YAML)
    )

    assert sorted(row.feed_id for row in invalid_df.select("feed_id").collect()) == INVALID_FEED_IDS


def test_apply_checks_keeps_row_count_unchanged_when_array_path_checks_fail(ws, sample_feed_df, set_utc_timezone):
    checked_df = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS).apply_checks(sample_feed_df, SAMPLE_CHECKS)

    assert checked_df.count() == 10


def test_apply_checks_reports_failing_values_when_path_is_single_level_array(ws, spark):
    df = spark.createDataFrame([[1, ["retail", ""]]], "id int, tags array<string>")
    rule = DQRowRule(check_func=check_funcs.regex_match, column="tags[*]", check_func_kwargs={"regex": "^[a-z]+$"})

    assert apply_rule(ws, df, rule) == {1: [array_message("tags[*]", "regex_match", 1, ["''"])]}


def test_apply_checks_reports_failing_values_when_path_is_two_level_array(ws, sample_feed_df, set_utc_timezone):
    rule = DQRowRule(
        check_func=check_funcs.is_valid_date, column=DATE_PATH, check_func_kwargs={"date_format": "yyyy-MM-dd"}
    )

    actual = apply_rule(ws, sample_feed_df.where("feed_id = 'F-003'"), rule, key="feed_id")

    assert actual == {"F-003": [date_message(2, ["'15/01/2024'", "'not-a-date'"])]}


def test_apply_checks_reports_failing_values_when_path_starts_with_struct_field(ws, spark):
    df = spark.createDataFrame(
        [[1, {"items": [{"sku": "a"}, {"sku": None}]}]], "id int, payload struct<items: array<struct<sku: string>>>"
    )
    rule = DQRowRule(check_func=check_funcs.is_not_null, column="payload.items[*].sku")

    assert apply_rule(ws, df, rule) == {1: [array_message("payload.items[*].sku", "is_not_null", 1, ["null"])]}


def test_apply_checks_reports_failing_values_when_path_is_array_of_arrays(ws, spark):
    df = spark.createDataFrame([[1, [["1", "a"], ["2"]]]], "id int, matrix array<array<string>>")
    rule = DQRowRule(check_func=check_funcs.regex_match, column="matrix[*][*]", check_func_kwargs={"regex": "^[0-9]+$"})

    assert apply_rule(ws, df, rule) == {1: [array_message("matrix[*][*]", "regex_match", 1, ["'a'"])]}


def test_apply_checks_reports_failing_values_when_path_has_back_quoted_names(ws, spark):
    df = spark.createDataFrame(
        [[1, [{"line items": [{"sku": "ok1"}, {"sku": "bad1"}]}]]],
        "id int, orders array<struct<`line items`: array<struct<sku: string>>>>",
    )
    path = "orders[*].`line items`[*].sku"
    rule = DQRowRule(check_func=check_funcs.regex_match, column=path, check_func_kwargs={"regex": "^ok"})

    assert apply_rule(ws, df, rule) == {1: [array_message(path, "regex_match", 1, ["'bad1'"])]}


def test_apply_checks_reports_failing_values_when_path_has_20_array_levels(ws, spark):
    value: list = ["ok", "bad"]
    for _ in range(19):
        value = [value]
    df = spark.createDataFrame([[1, value]], "id int, a " + "array<" * 20 + "string" + ">" * 20)
    path = "a" + "[*]" * 20
    rule = DQRowRule(check_func=check_funcs.regex_match, column=path, check_func_kwargs={"regex": "^ok$"})

    assert apply_rule(ws, df, rule) == {1: [array_message(path, "regex_match", 1, ["'bad'"])]}


@pytest.mark.parametrize(
    "feed_id",
    [
        pytest.param("F-004", id="top_level_array_is_null"),
        pytest.param("F-005", id="top_level_array_is_empty"),
        pytest.param("F-006", id="inner_array_is_null"),
    ],
)
def test_apply_checks_passes_row_when_array_on_path_is_null_or_empty(ws, sample_feed_df, feed_id: str):
    rule = DQRowRule(check_func=check_funcs.is_not_null, column=DATE_PATH)

    assert apply_rule(ws, sample_feed_df.where(F.col("feed_id") == feed_id), rule, key="feed_id") == {feed_id: []}


def test_apply_checks_passes_row_when_inner_array_is_empty(ws, spark):
    df = spark.createDataFrame([[1, [("u1", [])]]], f"id int, users {USERS_TYPE}")
    rule = DQRowRule(check_func=check_funcs.is_not_null, column=DATE_PATH)

    assert apply_rule(ws, df, rule) == {1: []}


def test_apply_checks_reports_failure_when_one_inner_array_is_null_and_another_has_failing_value(
    ws, sample_feed_df, set_utc_timezone
):
    rule = DQRowRule(
        check_func=check_funcs.is_valid_date, column=DATE_PATH, check_func_kwargs={"date_format": "yyyy-MM-dd"}
    )

    actual = apply_rule(ws, sample_feed_df.where("feed_id = 'F-008'"), rule, key="feed_id")

    assert actual == {"F-008": [date_message(1, ["'BAD'"])]}


def test_apply_checks_passes_null_leaf_when_check_allows_nulls(ws, sample_feed_df, set_utc_timezone):
    rule = DQRowRule(check_func=check_funcs.is_valid_date, column=DATE_PATH)

    assert apply_rule(ws, sample_feed_df.where("feed_id = 'F-007'"), rule, key="feed_id") == {"F-007": []}


def test_apply_checks_reports_null_value_when_is_not_null_fails_on_null_leaf(ws, sample_feed_df):
    rule = DQRowRule(check_func=check_funcs.is_not_null, column=DATE_PATH)

    actual = apply_rule(ws, sample_feed_df.where("feed_id = 'F-007'"), rule, key="feed_id")

    assert actual == {"F-007": [array_message(DATE_PATH, "is_not_null", 1, ["null"])]}


def test_apply_checks_treats_null_struct_element_as_null_leaf_when_array_contains_null_element(
    ws, sample_feed_df, set_utc_timezone
):
    df = sample_feed_df.where("feed_id = 'F-010'")
    date_rule = DQRowRule(check_func=check_funcs.is_valid_date, column=DATE_PATH)
    not_null_rule = DQRowRule(check_func=check_funcs.is_not_null, column=DATE_PATH)

    assert apply_rule(ws, df, date_rule, key="feed_id") == {"F-010": []}
    assert apply_rule(ws, df, not_null_rule, key="feed_id") == {
        "F-010": [array_message(DATE_PATH, "is_not_null", 1, ["null"])]
    }


def test_apply_checks_passes_row_when_inner_array_of_arrays_is_null(ws, spark):
    df = spark.createDataFrame([[1, [["1"], None]]], "id int, matrix array<array<string>>")
    rule = DQRowRule(check_func=check_funcs.is_not_null, column="matrix[*][*]")

    assert apply_rule(ws, df, rule) == {1: []}


def test_apply_checks_reports_failing_value_count_including_duplicates_when_several_values_fail(
    ws, sample_feed_df, set_utc_timezone
):
    rule = DQRowRule(
        check_func=check_funcs.is_valid_date, column=DATE_PATH, check_func_kwargs={"date_format": "yyyy-MM-dd"}
    )

    actual = apply_rule(ws, sample_feed_df.where("feed_id = 'F-009'"), rule, key="feed_id")

    assert actual == {"F-009": [date_message(5, ["'x'", "'y'", "'z'", "'w'"])]}


def test_apply_checks_shows_first_ten_distinct_values_when_more_than_ten_distinct_values_fail(ws, spark):
    values = [f"bad{i}" for i in range(12)]
    df = spark.createDataFrame([[1, ["2024-01-01", *values, values[0]]]], "id int, dates array<string>")
    rule = DQRowRule(check_func=check_funcs.is_valid_date, column="dates[*]")

    expected = array_message("dates[*]", "is_valid_date", 13, [f"'{value}'" for value in values[:10]], distinct=12)
    assert apply_rule(ws, df, rule) == {1: [expected]}


def test_apply_checks_shows_each_value_once_when_failing_values_repeat(ws, spark):
    df = spark.createDataFrame([[1, ["x", "x", "x"]]], "id int, dates array<string>")
    rule = DQRowRule(check_func=check_funcs.is_valid_date, column="dates[*]")

    assert apply_rule(ws, df, rule) == {1: [array_message("dates[*]", "is_valid_date", 3, ["'x'"])]}


def test_apply_checks_reports_struct_value_in_spark_string_form_when_leaf_is_struct(ws, spark):
    df = spark.createDataFrame(
        [[1, [("u1", [])]]], "id int, users array<struct<user_id: string, events: array<string>>>"
    )
    rule = DQRowRule(check_func=check_funcs.is_null, column="users[*]")

    assert apply_rule(ws, df, rule) == {1: [array_message("users[*]", "is_null", 1, ["'{u1, []}'"])]}


def test_apply_checks_uses_custom_message_when_message_expr_is_set_on_array_path_rule(ws, sample_feed_df):
    rule = DQRowRule(
        check_func=check_funcs.is_valid_date,
        column=DATE_PATH,
        check_func_kwargs={"date_format": "yyyy-MM-dd"},
        message_expr="concat('bad event dates in feed ', feed_id)",
    )

    actual = apply_rule(ws, sample_feed_df.where("feed_id IN ('F-001', 'F-002')"), rule, key="feed_id")

    assert actual == {"F-001": [], "F-002": ["bad event dates in feed F-002"]}


def test_apply_checks_applies_filter_to_whole_row_when_array_path_rule_has_filter(ws, sample_feed_df):
    rule = DQRowRule(
        check_func=check_funcs.is_valid_date,
        column=DATE_PATH,
        check_func_kwargs={"date_format": "yyyy-MM-dd"},
        filter="feed_id = 'F-002'",
    )

    actual = apply_rule(ws, sample_feed_df.where("feed_id IN ('F-002', 'F-003')"), rule, key="feed_id")

    assert actual == {"F-002": [date_message(1, ["'2024-13-45'"])], "F-003": []}


def test_apply_checks_reports_independent_entries_when_several_array_path_rules_fail_on_one_row(
    ws, sample_feed_df, set_utc_timezone
):
    checked_df = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS).apply_checks(
        sample_feed_df.where("feed_id = 'F-003'"), SAMPLE_CHECKS[:2]
    )

    assert collect_messages(checked_df) == {"F-003": EXPECTED_SAMPLE_ERRORS["F-003"]}


@pytest.mark.parametrize(
    "check_func, check_func_kwargs, values, failing_value",
    [
        pytest.param(check_funcs.is_valid_date, {}, ["2024-01-01", "bad"], "'bad'", id="is_valid_date"),
        pytest.param(check_funcs.is_valid_timestamp, {}, [VALID_TS, "bad"], "'bad'", id="is_valid_timestamp"),
        pytest.param(check_funcs.is_null, {}, [None, "x"], "'x'", id="is_null"),
        pytest.param(check_funcs.is_not_null, {}, ["x", None], "null", id="is_not_null"),
        pytest.param(check_funcs.regex_match, {"regex": "^[a-z]+$"}, ["abc", "ABC"], "'ABC'", id="regex_match"),
    ],
)
def test_apply_checks_reports_failing_values_when_priority_check_runs_on_array_path(
    ws, spark, set_utc_timezone, check_func, check_func_kwargs: dict, values: list, failing_value: str
):
    df = spark.createDataFrame([[1, values]], "id int, items array<string>")
    rule = DQRowRule(check_func=check_func, column="items[*]", check_func_kwargs=check_func_kwargs)

    assert apply_rule(ws, df, rule) == {1: [array_message("items[*]", check_func.__name__, 1, [failing_value])]}


def test_apply_checks_uses_outer_row_value_when_check_argument_references_top_level_column(ws, spark):
    df = spark.createDataFrame([[1, 5, [6, 4]], [2, 1, [2, 3]]], "id int, min_score int, scores array<int>")
    rule = DQRowRule(
        check_func=check_funcs.is_in_range,
        column="scores[*]",
        check_func_kwargs={"min_limit": F.col("min_score"), "max_limit": 100},
    )

    assert apply_rule(ws, df, rule) == {1: [array_message("scores[*]", "is_in_range", 1, ["'4'"])], 2: []}


def test_apply_checks_reports_failing_values_when_check_func_args_are_given_positionally_with_array_path(ws, spark):
    df = spark.createDataFrame([[1, ["online", "Online"]]], "id int, tags array<string>")
    rule = DQRowRule(check_func=check_funcs.regex_match, column="tags[*]", check_func_args=["^[a-z]+$"])

    assert apply_rule(ws, df, rule) == {1: [array_message("tags[*]", "regex_match", 1, ["'Online'"])]}


@pytest.mark.parametrize(
    "path",
    [
        pytest.param("users[*].events[*].payload.missing", id="array_path_field_is_missing"),
        pytest.param("feed_id[*]", id="wildcard_is_on_non_array_field"),
        pytest.param("attrs[*]", id="wildcard_is_on_map_field"),
        pytest.param("users[*].`event date", id="back_quote_is_unclosed"),
        pytest.param("size(users)[*].x", id="name_has_expression_characters"),
        pytest.param("users[*]. .x", id="segment_is_whitespace_only"),
        pytest.param("users.events[*].payload.event_date", id="wildcard_is_left_out_on_array_of_structs"),
        pytest.param("attrs.k[*]", id="field_step_is_on_map"),
    ],
)
def test_apply_checks_skips_check_with_invalid_columns_message_when_path_does_not_match_data(
    ws, sample_feed_df, path: str
):
    rule = DQRowRule(name="path_check", check_func=check_funcs.is_not_null, column=path)

    actual = apply_rule(ws, sample_feed_df.where("feed_id = 'F-010'"), rule, key="feed_id")

    assert actual == {"F-010": [skipped_message(path)]}


def test_apply_checks_resolves_back_quoted_name_containing_wildcard_when_column_exists(ws, spark):
    df = spark.createDataFrame([[1, None], [2, "x"]], "id int, `my_weird_col[*]` string")
    rule = DQRowRule(check_func=check_funcs.is_not_null, column="`my_weird_col[*]`")

    assert rule.array_path is None
    assert apply_rule(ws, df, rule) == {1: ["Column 'my_weird_col[*]' value is null"], 2: []}


def test_apply_checks_handles_dataset_rule_as_today_when_column_contains_wildcard(ws, sample_feed_df):
    rule = DQDatasetRule(
        name="aggr_check",
        check_func=check_funcs.is_aggr_not_greater_than,
        column="users[*].user_id",
        check_func_kwargs={"limit": 1},
    )

    checked_df = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS).apply_checks(sample_feed_df, [rule])

    messages = {message for entries in collect_messages(checked_df).values() for _, message in entries}
    assert len(messages) == 1
    message = messages.pop()
    assert message.startswith("Check evaluation skipped due to invalid check columns:")
    assert "users[*].user_id" in message


def custom_check_with_optional_column(column: str | Column | None = None, limit: int = 10) -> Column:
    column_expr = F.col(column) if isinstance(column, str) else column
    if column_expr is None:
        raise ValueError("column is required")
    return make_condition(column_expr > F.lit(limit), "value is above limit", "items_below_limit")


def is_lowercase(column: str | Column) -> Column:
    column_expr = F.col(column) if isinstance(column, str) else column
    return make_condition(column_expr != F.lower(column_expr), "value is not lowercase", "is_lowercase")


def references_missing_column(column: str | Column) -> Column:
    column_expr = F.col(column) if isinstance(column, str) else column
    return make_condition(column_expr > F.col("does_not_exist"), "value is too large", "references_missing_column")


def test_apply_checks_reports_failing_values_when_custom_check_column_parameter_is_optional(ws, spark):
    df = spark.createDataFrame([[1, [11, 1, 12]]], "id int, items array<int>")
    rule = DQRowRule(check_func=custom_check_with_optional_column, column="items[*]")

    expected = array_message("items[*]", "custom_check_with_optional_column", 2, ["'11'", "'12'"])
    assert apply_rule(ws, df, rule) == {1: [expected]}


@pytest.mark.parametrize("column", ["score", "scores[*]"])
def test_apply_checks_raises_same_error_as_plain_column_when_check_condition_is_invalid_for_array_path(
    ws, spark, column: str
):
    df = spark.createDataFrame([[1, 5, [5]]], "id int, score int, scores array<int>")
    rule = DQRowRule(check_func=references_missing_column, column=column)

    with pytest.raises(AnalysisException, match="does_not_exist"):
        DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS).apply_checks(df, [rule]).collect()


def test_apply_checks_reports_failing_values_when_custom_spark_check_runs_on_array_path(ws, spark):
    df = spark.createDataFrame([[1, ["online", "Online"]]], "id int, tags array<string>")
    rule = DQRowRule(check_func=is_lowercase, column="tags[*]")

    assert rule.name == "is_lowercase"
    assert apply_rule(ws, df, rule) == {1: [array_message("tags[*]", "is_lowercase", 1, ["'Online'"])]}
