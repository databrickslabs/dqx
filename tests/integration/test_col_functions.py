from datetime import datetime
from decimal import Decimal
import pyspark.sql.functions as F
from chispa.dataframe_comparer import assert_df_equality  # type: ignore
from databricks.labs.dqx.col_functions import (
    is_in_range,
    is_not_empty,
    is_not_in_range,
    is_not_null,
    is_not_null_and_not_empty,
    is_older_than_col2_for_n_days,
    is_older_than_n_days,
    is_not_in_future,
    is_not_in_near_future,
    is_not_less_than,
    is_not_greater_than,
    regex_match,
    sql_expression,
    is_in_list,
    is_not_null_and_is_in_list,
    is_not_null_and_not_empty_array,
    is_valid_date,
    is_valid_timestamp,
    is_unique,
)

SCHEMA = "a: string, b: int"


def test_col_is_not_null_and_not_empty(spark):
    input_schema = "a: string, b: int, c: map<string, string>, d: array<string>"
    test_df = spark.createDataFrame(
        [
            ["str1", 1, {"val": "a"}, ["a", "b"]],
            ["", None, {"val": ""}, [None, "a"]],
            [" ", 3, {"val": None}, ["", "a"]],
        ],
        input_schema,
    )

    actual = test_df.select(
        is_not_null_and_not_empty("a"),
        is_not_null_and_not_empty("b", True),
        is_not_null_and_not_empty(F.col("c").getItem("val")),
        is_not_null_and_not_empty(F.try_element_at("d", F.lit(1))),
    )

    checked_schema = (
        "a_is_null_or_empty: string, "
        + "b_is_null_or_empty: string, "
        + "unresolvedextractvalue_c_val_is_null_or_empty: string, "
        + "try_element_at_d_1_is_null_or_empty: string"
    )
    expected = spark.createDataFrame(
        [
            [None, None, None, None],
            [
                "Column a is null or empty",
                "Column b is null or empty",
                "Column unresolvedextractvalue_c_val is null or empty",
                "Column try_element_at_d_1 is null or empty",
            ],
            [
                None,
                None,
                "Column unresolvedextractvalue_c_val is null or empty",
                "Column try_element_at_d_1 is null or empty",
            ],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_is_not_empty(spark):
    input_schema = "a: string, b: int, c: map<string, string>, d: array<string>"
    test_df = spark.createDataFrame(
        [
            ["str1", 1, {"val": "a"}, ["a", "b"]],
            ["", None, {"val": ""}, [None, "a"]],
            [" ", 3, {"val": None}, ["", "a"]],
        ],
        input_schema,
    )

    actual = test_df.select(
        is_not_empty("a"),
        is_not_empty("b"),
        is_not_empty(F.col("c").getItem("val")),
        is_not_empty(F.try_element_at("d", F.lit(1))),
    )

    checked_schema = (
        "a_is_empty: string, "
        + "b_is_empty: string, "
        + "unresolvedextractvalue_c_val_is_empty: string, "
        + "try_element_at_d_1_is_empty: string"
    )
    expected = spark.createDataFrame(
        [
            [None, None, None, None],
            ["Column a is empty", None, "Column unresolvedextractvalue_c_val is empty", None],
            [None, None, None, "Column try_element_at_d_1 is empty"],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_is_not_null(spark):
    input_schema = "a: string, b: int, c: map<string, string>, d: array<string>"
    test_df = spark.createDataFrame(
        [
            ["str1", 1, {"val": "a"}, ["a", "b"]],
            ["", None, {"val": ""}, [None, "a"]],
            [" ", 3, {"val": None}, ["", "a"]],
        ],
        input_schema,
    )

    actual = test_df.select(
        is_not_null("a"),
        is_not_null("b"),
        is_not_null(F.col("c").getItem("val")),
        is_not_null(F.try_element_at("d", F.lit(1))),
    )

    checked_schema = (
        "a_is_null: string, "
        + "b_is_null: string, "
        + "unresolvedextractvalue_c_val_is_null: string, "
        + "try_element_at_d_1_is_null: string"
    )
    expected = spark.createDataFrame(
        [
            [None, None, None, None],
            [None, "Column b is null", None, "Column try_element_at_d_1 is null"],
            [None, None, "Column unresolvedextractvalue_c_val is null", None],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_is_not_null_and_is_in_list(spark):
    input_schema = "a: string, b: int, c: map<string, string>, d: array<string>"
    test_df = spark.createDataFrame(
        [
            ["str1", 1, {"val": "a"}, ["a", "b"]],
            ["str2", None, {"val": "str2"}, [None, "a"]],
            [" ", 3, {"val": " "}, [None, " "]],
        ],
        input_schema,
    )

    actual = test_df.select(
        is_not_null_and_is_in_list("a", ["str1"]),
        is_not_null_and_is_in_list("b", [F.lit(3)]),
        is_not_null_and_is_in_list(F.col("c").getItem("val"), [F.lit("a")]),
        is_not_null_and_is_in_list(F.try_element_at("d", F.lit(2)), ["b"]),
    )

    checked_schema = (
        "a_is_null_or_is_not_in_the_list: string, "
        + "b_is_null_or_is_not_in_the_list: string, "
        + "unresolvedextractvalue_c_val_is_null_or_is_not_in_the_list: string, "
        + "try_element_at_d_2_is_null_or_is_not_in_the_list: string"
    )
    expected = spark.createDataFrame(
        [
            [None, "Value 1 is null or not in the allowed list: [3]", None, None],
            [
                "Value str2 is null or not in the allowed list: [str1]",
                "Value null is null or not in the allowed list: [3]",
                "Value str2 is null or not in the allowed list: [a]",
                "Value a is null or not in the allowed list: [b]",
            ],
            [
                "Value   is null or not in the allowed list: [str1]",
                None,
                "Value   is null or not in the allowed list: [a]",
                "Value   is null or not in the allowed list: [b]",
            ],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_is_not_in_list(spark):
    input_schema = "a: string, b: int, c: map<string, string>, d: array<string>"
    test_df = spark.createDataFrame(
        [
            ["str1", 1, {"val": "a"}, ["a", "b"]],
            ["str2", None, {"val": "str2"}, [None, "a"]],
            [" ", 3, {"val": None}, [None, "a"]],
        ],
        input_schema,
    )

    actual = test_df.select(
        is_in_list("a", ["str1"]),
        is_in_list("b", [F.lit(3)]),
        is_in_list(F.col("c").getItem("val"), [F.lit("a")]),
        is_in_list(F.try_element_at("d", F.lit(2)), ["b"]),
    )

    checked_schema = (
        "a_is_not_in_the_list: string, "
        + "b_is_not_in_the_list: string, "
        + "unresolvedextractvalue_c_val_is_not_in_the_list: string, "
        + "try_element_at_d_2_is_not_in_the_list: string"
    )
    expected = spark.createDataFrame(
        [
            [None, "Value 1 is not in the allowed list: [3]", None, None],
            [
                "Value str2 is not in the allowed list: [str1]",
                None,
                "Value str2 is not in the allowed list: [a]",
                "Value a is not in the allowed list: [b]",
            ],
            ["Value   is not in the allowed list: [str1]", None, None, "Value a is not in the allowed list: [b]"],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_sql_expression(spark):
    test_df = spark.createDataFrame([["str1", 1, 1], ["str2", None, None], ["", 2, 3]], SCHEMA + ", c: string")

    actual = test_df.select(
        sql_expression("a = 'str2'"),
        sql_expression("b is null", name="test", negate=True),
        sql_expression("c is null", msg="failed validation", negate=True),
        sql_expression("b < c", msg="b is greater or equal c", negate=False),
    )

    checked_schema = "a_str2_: string, test: string, c_is_null: string, b_c: string"
    expected = spark.createDataFrame(
        [
            ["Value is not matching expression: a = 'str2'", None, None, "b is greater or equal c"],
            [None, "Value is matching expression: ~(b is null)", "failed validation", None],
            ["Value is not matching expression: a = 'str2'", None, None, None],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_is_col_older_than_col2_for_n_days(spark):
    schema_dates = "a: string, b: string, c: map<string, string>, d: array<string>"
    test_df = spark.createDataFrame(
        [
            ["2023-01-10", "2023-01-13", {"val": "2023-01-10"}, ["2023-01-13"]],
            ["2023-01-10", "2023-01-12", {"val": "2023-01-10"}, ["2023-01-12"]],
            ["2023-01-10", "2023-01-05", {"val": "2023-01-10"}, ["2023-01-05"]],
            ["2023-01-10", None, {"val": "2023-01-10"}, [None]],
            [None, None, {"val": None}, [None]],
        ],
        schema_dates,
    )

    actual = test_df.select(
        is_older_than_col2_for_n_days("a", "b", 2),
        is_older_than_col2_for_n_days(F.col("c").getItem("val"), F.try_element_at("d", F.lit(1)), 2),
    )

    checked_schema = (
        "is_col_a_older_than_b_for_n_days: string, "
        + "is_col_unresolvedextractvalue_c_val_older_than_try_element_at_d_1_for_n_days: string"
    )
    expected = spark.createDataFrame(
        [
            [
                "Value of a: '2023-01-10' less than value of b: '2023-01-13' for more than 2 days",
                "Value of unresolvedextractvalue_c_val: '2023-01-10' less than value of try_element_at_d_1: "
                + "'2023-01-13' for more than 2 days",
            ],
            [None, None],
            [None, None],
            [None, None],
            [None, None],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_is_col_older_than_n_days(spark):
    schema_dates = "a: string, b: map<string, string>, c: array<string>"
    test_df = spark.createDataFrame(
        [
            ["2023-01-10", {"val": "2023-01-10"}, ["2023-01-10"]],
            ["2023-01-13", {"val": "2023-01-13"}, ["2023-01-13"]],
            [None, None, None],
        ],
        schema_dates,
    )

    actual = test_df.select(
        is_older_than_n_days("a", 2, F.lit("2023-01-13")),
        is_older_than_n_days(F.col("b").getItem("val"), 2, F.lit("2023-01-13")),
        is_older_than_n_days(F.try_element_at("c", F.lit(1)), 2, F.lit("2023-01-13")),
    )

    checked_schema = (
        "is_col_a_older_than_n_days: string, "
        + "is_col_unresolvedextractvalue_b_val_older_than_n_days: string, "
        + "is_col_try_element_at_c_1_older_than_n_days: string"
    )
    expected = spark.createDataFrame(
        [
            [
                "Value of a: '2023-01-10' less than current date: '2023-01-13' for more than 2 days",
                "Value of unresolvedextractvalue_b_val: '2023-01-10' less than current date: '2023-01-13' "
                + "for more than 2 days",
                "Value of try_element_at_c_1: '2023-01-10' less than current date: '2023-01-13' for more than 2 days",
            ],
            [None, None, None],
            [None, None, None],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_is_not_in_future(spark):
    schema_dates = "a: string, b: map<string, string>"
    test_df = spark.createDataFrame(
        [
            ["2023-01-10 11:08:37", {"dt": "2023-01-10 11:01:10"}],
            ["2023-01-10 11:08:43", {"dt": "2024-01-02 02:41:20"}],
            [None, {"dt": None}],
        ],
        schema_dates,
    )

    actual = test_df.select(
        is_not_in_future("a", 2, F.lit("2023-01-10 11:08:40")),
        is_not_in_future(F.col("b").getItem("dt"), 2, F.lit("2023-01-10 11:08:40")),
    )

    checked_schema = "a_in_future: string, unresolvedextractvalue_b_dt_in_future: string"
    expected = spark.createDataFrame(
        [
            [None, None],
            [
                "Value '2023-01-10 11:08:43' is greater than time '2023-01-10 11:08:42'",
                "Value '2024-01-02 02:41:20' is greater than time '2023-01-10 11:08:42'",
            ],
            [None, None],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_is_not_in_near_future(spark):
    schema_dates = "a: string, b: long, c: map<string, string>"
    test_df = spark.createDataFrame(
        [
            ["2023-01-10 11:08:40", 1673348920, {"dt": "2023-01-10 11:08:40"}],
            ["2023-01-10 11:08:41", 1673348921, {"dt": "2023-01-10 11:08:41"}],
            ["2023-01-10 11:08:42", 1673348922, {"dt": "2023-01-10 11:08:42"}],
            [None, None, {"dt": None}],
        ],
        schema_dates,
    )

    actual = test_df.select(
        is_not_in_near_future("a", 2, F.lit("2023-01-10 11:08:40")),
        is_not_in_near_future(F.col("b").cast("timestamp"), 2, F.lit("2023-01-10 11:08:40")),
        is_not_in_near_future(F.col("c").getItem("dt"), 2, F.lit("2023-01-10 11:08:40")),
    )

    checked_schema = "a_in_near_future: string, cast_b_as_timestamp_in_near_future: string, unresolvedextractvalue_c_dt_in_near_future: string"
    expected = spark.createDataFrame(
        [
            [None, None, None],
            [
                "Value '2023-01-10 11:08:41' is greater than '2023-01-10 11:08:40 and smaller than '2023-01-10 11:08:42'",
                "Value '2023-01-10 11:08:41' is greater than '2023-01-10 11:08:40 and smaller than '2023-01-10 11:08:42'",
                "Value '2023-01-10 11:08:41' is greater than '2023-01-10 11:08:40 and smaller than '2023-01-10 11:08:42'",
            ],
            [None, None, None],
            [None, None, None],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_is_col_older_than_n_days_cur(spark):
    schema_dates = "a: string, b: map<string, string>"
    cur_date = spark.sql("SELECT current_date() AS current_date").collect()[0]['current_date'].strftime("%Y-%m-%d")

    test_df = spark.createDataFrame([["2023-01-10", {"dt": "2023-01-10"}], [None, {"dt": None}]], schema_dates)

    actual = test_df.select(is_older_than_n_days("a", 2, None), is_older_than_n_days(F.col("b").getItem("dt"), 2, None))

    checked_schema = "is_col_a_older_than_n_days: string, is_col_unresolvedextractvalue_b_dt_older_than_n_days: string"

    expected = spark.createDataFrame(
        [
            [
                f"Value of a: '2023-01-10' less than current date: '{cur_date}' for more than 2 days",
                f"Value of unresolvedextractvalue_b_dt: '2023-01-10' less than current date: '{cur_date}' for more than 2 days",
            ],
            [None, None],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_is_not_less_than(spark, set_utc_timezone):
    schema_num = "a: int, b: int, c: date, d: timestamp, e: decimal(10,2), f: array<int>, g: map<string, int>"
    test_df = spark.createDataFrame(
        [
            [1, 1, datetime(2025, 1, 1).date(), datetime(2025, 1, 1), Decimal("1.00"), [1], {"val": 1}],
            [2, 4, datetime(2025, 2, 1).date(), datetime(2025, 2, 1), Decimal("1.99"), [2], {"val": 2}],
            [4, 3, None, None, Decimal("2.01"), [4], {"val": 4}],
            [None, None, None, None, None, [None], {"val": None}],
        ],
        schema_num,
    )

    actual = test_df.select(
        is_not_less_than("a", 2),
        is_not_less_than("a", F.col("b") * 2),
        is_not_less_than("b", "a"),
        is_not_less_than("c", datetime(2025, 2, 1).date()),
        is_not_less_than("d", datetime(2025, 2, 1)),
        is_not_less_than("e", 2),
        is_not_less_than(F.try_element_at("f", F.lit(1)), 2),
        is_not_less_than(F.col("g").getItem("val"), 2),
    )

    checked_schema = (
        "a_less_than_limit: string, a_less_than_limit: string, b_less_than_limit: string, "
        "c_less_than_limit: string, d_less_than_limit: string, e_less_than_limit: string, "
        "try_element_at_f_1_less_than_limit: string, "
        "unresolvedextractvalue_g_val_less_than_limit: string"
    )

    expected = spark.createDataFrame(
        [
            [
                "Value 1 is less than limit: 2",
                None,
                None,
                "Value 2025-01-01 is less than limit: 2025-02-01",
                "Value 2025-01-01 00:00:00 is less than limit: 2025-02-01 00:00:00",
                "Value 1.00 is less than limit: 2",
                "Value 1 is less than limit: 2",
                "Value 1 is less than limit: 2",
            ],
            [None, "Value 2 is less than limit: 8", None, None, None, "Value 1.99 is less than limit: 2", None, None],
            [None, "Value 4 is less than limit: 6", "Value 3 is less than limit: 4", None, None, None, None, None],
            [None, None, None, None, None, None, None, None],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_is_not_greater_than(spark, set_utc_timezone):
    schema_num = "a: int, b: int, c: date, d: timestamp, e: decimal(10,2), f: array<int>"
    test_df = spark.createDataFrame(
        [
            [1, 1, datetime(2025, 1, 1).date(), datetime(2025, 1, 1), Decimal("1.00"), [1]],
            [2, 4, datetime(2025, 2, 1).date(), datetime(2025, 2, 1), Decimal("1.01"), [2]],
            [8, 3, None, None, Decimal("0.99"), [8]],
            [None, None, None, None, None, [None]],
        ],
        schema_num,
    )

    actual = test_df.select(
        is_not_greater_than("a", 1),
        is_not_greater_than("a", F.col("b") * 2),
        is_not_greater_than("b", "a"),
        is_not_greater_than("c", datetime(2025, 1, 1).date()),
        is_not_greater_than("d", datetime(2025, 1, 1)),
        is_not_greater_than("e", 1),
        is_not_greater_than(F.try_element_at("f", F.lit(1)), 1),
    )

    checked_schema = (
        "a_greater_than_limit: string, a_greater_than_limit: string, b_greater_than_limit: string, "
        "c_greater_than_limit: string, d_greater_than_limit: string, e_greater_than_limit: string, "
        "try_element_at_f_1_greater_than_limit: string"
    )
    expected = spark.createDataFrame(
        [
            [None, None, None, None, None, None, None],
            [
                "Value 2 is greater than limit: 1",
                None,
                "Value 4 is greater than limit: 2",
                "Value 2025-02-01 is greater than limit: 2025-01-01",
                "Value 2025-02-01 00:00:00 is greater than limit: 2025-01-01 00:00:00",
                "Value 1.01 is greater than limit: 1",
                "Value 2 is greater than limit: 1",
            ],
            [
                "Value 8 is greater than limit: 1",
                "Value 8 is greater than limit: 6",
                None,
                None,
                None,
                None,
                "Value 8 is greater than limit: 1",
            ],
            [None, None, None, None, None, None, None],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_is_in_range(spark, set_utc_timezone):
    schema_num = "a: int, b: date, c: timestamp, d: int, e: int, f: int, g: decimal(10,2), h: map<string, int>"
    test_df = spark.createDataFrame(
        [
            [0, datetime(2024, 12, 1).date(), datetime(2024, 12, 1), -1, 5, 6, Decimal("2.00"), {"val": 0}],
            [1, datetime(2025, 1, 1).date(), datetime(2025, 1, 1), 2, 6, 3, Decimal("1.00"), {"val": 1}],
            [2, datetime(2025, 2, 1).date(), datetime(2025, 2, 1), 2, 7, 3, Decimal("3.00"), {"val": 2}],
            [3, datetime(2025, 3, 1).date(), datetime(2025, 3, 1), 3, 8, 3, Decimal("1.01"), {"val": 3}],
            [4, datetime(2025, 4, 1).date(), datetime(2025, 4, 1), 2, 9, 3, Decimal("3.01"), {"val": 4}],
            [None, None, None, None, None, None, None, {"val": None}],
        ],
        schema_num,
    )

    start_date = datetime(2025, 1, 1)
    end_date = datetime(2025, 3, 1)
    actual = test_df.select(
        is_in_range("a", 1, 3),
        is_in_range("b", start_date.date(), end_date.date()),
        is_in_range("c", start_date, end_date),
        is_in_range("d", F.col("a"), F.expr("e - 1")),
        is_in_range("f", "a", 5),
        is_in_range("g", 1, 3),
        is_in_range(F.col("h").getItem("val"), 1, 3),
    )

    checked_schema = (
        "a_not_in_range: string, b_not_in_range: string, c_not_in_range: string, "
        "d_not_in_range: string, f_not_in_range: string, g_not_in_range: string, "
        "unresolvedextractvalue_h_val_not_in_range: string"
    )
    expected = spark.createDataFrame(
        [
            [
                "Value 0 not in range: [ 1 , 3 ]",
                "Value 2024-12-01 not in range: [ 2025-01-01 , 2025-03-01 ]",
                "Value 2024-12-01 00:00:00 not in range: [ 2025-01-01 00:00:00 , 2025-03-01 00:00:00 ]",
                "Value -1 not in range: [ 0 , 4 ]",
                "Value 6 not in range: [ 0 , 5 ]",
                None,
                "Value 0 not in range: [ 1 , 3 ]",
            ],
            [None, None, None, None, None, None, None],
            [None, None, None, None, None, None, None],
            [None, None, None, None, None, None, None],
            [
                "Value 4 not in range: [ 1 , 3 ]",
                "Value 2025-04-01 not in range: [ 2025-01-01 , 2025-03-01 ]",
                "Value 2025-04-01 00:00:00 not in range: [ 2025-01-01 00:00:00 , 2025-03-01 00:00:00 ]",
                "Value 2 not in range: [ 4 , 8 ]",
                "Value 3 not in range: [ 4 , 5 ]",
                "Value 3.01 not in range: [ 1 , 3 ]",
                "Value 4 not in range: [ 1 , 3 ]",
            ],
            [None, None, None, None, None, None, None],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_is_not_in_range(spark, set_utc_timezone):
    schema_num = "a: int, b: date, c: timestamp, d: timestamp, e: decimal(10,2), f: array<int>"
    test_df = spark.createDataFrame(
        [
            [0, datetime(2024, 12, 31).date(), datetime(2025, 1, 4), datetime(2025, 1, 7), Decimal("0.99"), [0, 1]],
            [1, datetime(2025, 1, 1).date(), datetime(2025, 1, 3), datetime(2025, 1, 1), Decimal("1.00"), [1, 2]],
            [3, datetime(2025, 2, 1).date(), datetime(2025, 2, 1), datetime(2025, 2, 3), Decimal("3.00"), [3, 4]],
            [None, None, None, None, None, [None, 1]],
        ],
        schema_num,
    )

    start_date = datetime(2025, 1, 1)
    end_date = datetime(2025, 1, 3)
    actual = test_df.select(
        is_not_in_range("a", 1, 3),
        is_not_in_range("b", start_date.date(), end_date.date()),
        is_not_in_range("c", start_date, end_date),
        is_not_in_range("d", "c", F.expr("cast(b as timestamp) + INTERVAL 2 DAY")),
        is_not_in_range("e", 1, 3),
        is_not_in_range(F.try_element_at("f", F.lit(1)), 1, 3),
    )

    checked_schema = (
        "a_in_range: string, b_in_range: string, c_in_range: string, d_in_range: string, e_in_range: string, "
        "try_element_at_f_1_in_range: string"
    )
    expected = spark.createDataFrame(
        [
            [None, None, None, None, None, None],
            [
                "Value 1 in range: [ 1 , 3 ]",
                "Value 2025-01-01 in range: [ 2025-01-01 , 2025-01-03 ]",
                "Value 2025-01-03 00:00:00 in range: [ 2025-01-01 00:00:00 , 2025-01-03 00:00:00 ]",
                None,
                "Value 1.00 in range: [ 1 , 3 ]",
                "Value 1 in range: [ 1 , 3 ]",
            ],
            [
                "Value 3 in range: [ 1 , 3 ]",
                None,
                None,
                "Value 2025-02-03 00:00:00 in range: [ 2025-02-01 00:00:00 , 2025-02-03 00:00:00 ]",
                "Value 3.00 in range: [ 1 , 3 ]",
                "Value 3 in range: [ 1 , 3 ]",
            ],
            [None, None, None, None, None, None],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_matching_regex(spark):
    schema_str = "a: string, b: map<string, string>"
    test_df = spark.createDataFrame(
        [["2023-01-02", {"s": "2023-01-02"}], ["2023/01/02", {"s": "2023/01/02"}], [None, {"s": None}]],
        schema_str,
    )

    # matching ISO date: yyyy-MM-dd format
    date_re = "^\\d{4}-([0]\\d|1[0-2])-([0-2]\\d|3[01])$"

    actual = test_df.select(
        regex_match("a", date_re), regex_match("a", date_re, negate=True), regex_match(F.col("b").getItem("s"), date_re)
    )

    checked_schema = (
        "a_not_matching_regex: string, a_matching_regex: string, unresolvedextractvalue_b_s_not_matching_regex: string"
    )
    expected = spark.createDataFrame(
        [
            [None, "Column a is matching regex", None],
            ["Column a is not matching regex", None, "Column unresolvedextractvalue_b_s is not matching regex"],
            [None, None, None],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_struct(spark):
    test_df = spark.createDataFrame([[("str1",)]], "data: struct<x:string>")

    actual = test_df.select(is_not_empty("data.x"))

    checked_schema = "data_x_is_empty: string"
    expected = spark.createDataFrame([[None]], checked_schema)

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_is_not_in_future_cur(spark):
    schema_dates = "a: string"

    test_df = spark.createDataFrame([["9999-12-31 23:59:59"]], schema_dates)

    actual = test_df.select(is_not_in_future("a", 0, None))

    checked_schema = "a_in_future: string"

    expected = spark.createDataFrame([[None]], checked_schema)

    assert actual.select("a_in_future") != expected.select("a_in_future")


def test_col_is_not_in_near_future_cur(spark):
    schema_dates = "a: string"

    test_df = spark.createDataFrame([["1900-01-01 23:59:59"], ["9999-12-31 23:59:59"], [None]], schema_dates)

    actual = test_df.select(is_not_in_near_future("a", 2, None))

    checked_schema = "a_in_near_future: string"
    expected = spark.createDataFrame(
        [[None], [None], [None]],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_is_not_null_and_not_empty_array(spark):
    schema_array = (
        "str_col: array<string>, int_col: array<int> , timestamp_col: array<timestamp>, "
        "date_col: array<string>, struct_col: array<struct<a: string, b: int>>, "
        "nested_array_col: map<string, array<string>>"
    )
    data = [
        (
            ["a", "b", None],
            [1, 2, None],
            [None, datetime.strptime("2025-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")],
            [datetime.strptime("2025-01-01", "%Y-%m-%d"), None],
            [{"a": "x", "b": 1}, None],
            {"arr": ["a", "b", None]},
        ),
        ([], [], [], [], [], {"arr": []}),
        (None, None, None, None, None, {"arr": None}),
        (
            ["non-empty"],
            [10],
            [datetime.strptime("2025-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")],
            [datetime.strptime("2025-01-01", "%Y-%m-%d")],
            [{"a": "y", "b": 2}],
            {"arr": ["non-empty"]},
        ),
    ]

    test_df = spark.createDataFrame(data, schema_array)

    actual = test_df.select(
        is_not_null_and_not_empty_array("str_col"),
        is_not_null_and_not_empty_array("int_col"),
        is_not_null_and_not_empty_array("timestamp_col"),
        is_not_null_and_not_empty_array("date_col"),
        is_not_null_and_not_empty_array("struct_col"),
        is_not_null_and_not_empty_array(F.col("nested_array_col").getItem("arr")),
    )

    checked_schema = (
        "str_col_is_null_or_empty_array: string, int_col_is_null_or_empty_array: string, "
        "timestamp_col_is_null_or_empty_array: string, date_col_is_null_or_empty_array: string, "
        "struct_col_is_null_or_empty_array: string, "
        "unresolvedextractvalue_nested_array_col_arr_is_null_or_empty_array: string"
    )
    # Create the data
    checked_data = [
        (None, None, None, None, None, None),
        (
            "Column str_col is null or empty array",
            "Column int_col is null or empty array",
            "Column timestamp_col is null or empty array",
            "Column date_col is null or empty array",
            "Column struct_col is null or empty array",
            "Column unresolvedextractvalue_nested_array_col_arr is null or empty array",
        ),
        (
            "Column str_col is null or empty array",
            "Column int_col is null or empty array",
            "Column timestamp_col is null or empty array",
            "Column date_col is null or empty array",
            "Column struct_col is null or empty array",
            "Column unresolvedextractvalue_nested_array_col_arr is null or empty array",
        ),
        (None, None, None, None, None, None),
    ]
    expected = spark.createDataFrame(checked_data, checked_schema)

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_is_valid_date(spark, set_utc_timezone):
    schema_array = "a: string, b: string, c: string, d: string, e: map<string, string>"
    data = [
        ["2024-01-01", "12/31/2025", "invalid_date", None, {"dt": "2024-01-01"}],
        ["12/31/2025", "2024-01-01", "invalid_date", None, {"dt": "12/31/2025"}],
        ["12/31/2025", "invalid_date", "2024-01-01", None, {"dt": "12/31/2025"}],
    ]

    test_df = spark.createDataFrame(data, schema_array)

    actual = test_df.select(
        is_valid_date("a"),
        is_valid_date("b", "MM/dd/yyyy"),
        is_valid_date("c", "yyyy-MM-dd"),
        is_valid_date("d"),
        is_valid_date(F.col("e").getItem("dt")),
    )

    checked_schema = """
        a_is_not_valid_date: string, 
        b_is_not_valid_date: string, 
        c_is_not_valid_date: string, 
        d_is_not_valid_date: string,
        unresolvedextractvalue_e_dt_is_not_valid_date: string
        """
    checked_data = [
        [None, None, "Value 'invalid_date' is not a valid date with format 'yyyy-MM-dd'", None, None],
        [
            "Value '12/31/2025' is not a valid date",
            "Value '2024-01-01' is not a valid date with format 'MM/dd/yyyy'",
            "Value 'invalid_date' is not a valid date with format 'yyyy-MM-dd'",
            None,
            "Value '12/31/2025' is not a valid date",
        ],
        [
            "Value '12/31/2025' is not a valid date",
            "Value 'invalid_date' is not a valid date with format 'MM/dd/yyyy'",
            None,
            None,
            "Value '12/31/2025' is not a valid date",
        ],
    ]
    expected = spark.createDataFrame(checked_data, checked_schema)

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_is_valid_timestamp(spark, set_utc_timezone):
    schema_array = "a: string, b: string, c: string, d: string, e: string, f: map<string, string>"
    data = [
        [
            "2024-01-01 00:00:00",
            "12/31/2025 00:00:00",
            "invalid_timestamp",
            None,
            "2025-01-31T00:00:00",
            {"dt": "2024-01-01 00:00:00"},
        ],
        [
            "12/31/2025 00:00:00",
            "2024-01-01 00:00:00",
            "invalid_timestamp",
            None,
            "2025-01-31 00:00:00",
            {"dt": "12/31/2025 00:00:00"},
        ],
        [
            "2024-01-01T00:00:00",
            "invalid_timestamp",
            "2024-01-01 00:00:00",
            None,
            "1/31/2025 00:00:00",
            {"dt": "2024-01-01 00:00:00"},
        ],
    ]

    test_df = spark.createDataFrame(data, schema_array)

    actual = test_df.select(
        is_valid_timestamp("a"),
        is_valid_timestamp("b", "MM/dd/yyyy HH:mm:ss"),
        is_valid_timestamp("c", "yyyy-MM-dd HH:mm:ss"),
        is_valid_timestamp("d"),
        is_valid_timestamp("e", "yyyy-MM-dd'T'HH:mm:ss"),
        is_valid_timestamp(F.col("f").getItem("dt")),
    )

    checked_schema = """
        a_is_not_valid_timestamp: string, 
        b_is_not_valid_timestamp: string, 
        c_is_not_valid_timestamp: string, 
        d_is_not_valid_timestamp: string,
        e_is_not_valid_timestamp: string,
        unresolvedextractvalue_f_dt_is_not_valid_timestamp: string
        """
    checked_data = [
        [
            None,
            None,
            "Value 'invalid_timestamp' is not a valid timestamp with format 'yyyy-MM-dd HH:mm:ss'",
            None,
            None,
            None,
        ],
        [
            "Value '12/31/2025 00:00:00' is not a valid timestamp",
            "Value '2024-01-01 00:00:00' is not a valid timestamp with format 'MM/dd/yyyy HH:mm:ss'",
            "Value 'invalid_timestamp' is not a valid timestamp with format 'yyyy-MM-dd HH:mm:ss'",
            None,
            "Value '2025-01-31 00:00:00' is not a valid timestamp with format 'yyyy-MM-dd'T'HH:mm:ss'",
            "Value '12/31/2025 00:00:00' is not a valid timestamp",
        ],
        [
            None,
            "Value 'invalid_timestamp' is not a valid timestamp with format 'MM/dd/yyyy HH:mm:ss'",
            None,
            None,
            "Value '1/31/2025 00:00:00' is not a valid timestamp with format 'yyyy-MM-dd'T'HH:mm:ss'",
            None,
        ],
    ]
    expected = spark.createDataFrame(checked_data, checked_schema)

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_is_unique(spark):
    test_df = spark.createDataFrame([["str1", 1], ["str2", 1], ["str2", 2], ["str3", 3]], SCHEMA)

    actual = test_df.select(is_unique("a"), is_unique("b"))

    checked_schema = "a_is_not_unique: string, b_is_not_unique: string"
    expected = spark.createDataFrame(
        [
            [None, "Column b has duplicate values"],
            ["Column a has duplicate values", "Column b has duplicate values"],
            ["Column a has duplicate values", None],
            [None, None],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_is_unique_handle_nulls(spark):
    test_df = spark.createDataFrame([["", None], ["", None], ["str1", 1], [None, None]], SCHEMA)

    actual = test_df.select(is_unique("a"), is_unique("b"))

    checked_schema = "a_is_not_unique: string, b_is_not_unique: string"
    expected = spark.createDataFrame(
        [
            ["Column a has duplicate values", None],  # Null values are not considered duplicates as they are unknown
            ["Column a has duplicate values", None],
            [None, None],
            [None, None],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)


def test_col_is_unique_custom_window_spec(spark):
    schema_num = "a: int, b: timestamp"
    test_df = spark.createDataFrame(
        [
            [0, datetime(2025, 1, 1)],
            [0, datetime(2025, 1, 2)],
            [0, datetime(2025, 1, 3)],  # duplicate but not within the first window
            [1, None],  # considered duplicate with "b" as "1970-01-01"
            [1, None],  # considered duplicate with "b" as "1970-01-01"
            [None, datetime(2025, 1, 6)],
            [None, None],
        ],
        schema_num,
    )

    actual = test_df.select(
        # must use coalesce to handle nulls, otherwise records with null for the time column b will be dropped
        is_unique("a", window_spec=F.window(F.coalesce(F.col("b"), F.lit(datetime(1970, 1, 1))), "2 days"))
    )

    checked_schema = "a_is_not_unique: string"
    expected = spark.createDataFrame(
        [
            ["Column a has duplicate values"],
            ["Column a has duplicate values"],
            ["Column a has duplicate values"],
            ["Column a has duplicate values"],
            [None],
            [None],
            [None],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)


def test_col_is_unique_custom_window_spec_without_handling_nulls(spark):
    schema_num = "a: int, b: timestamp"
    test_df = spark.createDataFrame(
        [
            [0, datetime(2025, 1, 1)],
            [0, datetime(2025, 1, 2)],
            [0, datetime(2025, 1, 3)],  # duplicate but not within the first window
            [1, None],  # considered duplicate with "b" as "1970-01-01"
            [1, None],  # considered duplicate with "b" as "1970-01-01"
            [None, datetime(2025, 1, 6)],
            [None, None],
        ],
        schema_num,
    )

    actual = test_df.select(
        # window functions do not handle nulls by default
        # incorrect implementation of the window_spec will result in rows being dropped!!!
        is_unique("a", window_spec=F.window(F.col("b"), "2 days"))
    )

    checked_schema = "a_is_not_unique: string"
    expected = spark.createDataFrame(
        [
            ["Column a has duplicate values"],
            ["Column a has duplicate values"],
            [None],
            [None],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)


def test_col_is_unique_custom_window_as_string(spark):
    schema_num = "a: int, b: timestamp"
    test_df = spark.createDataFrame(
        [
            [0, datetime(2025, 1, 1)],
            [0, datetime(2025, 1, 2)],
            [0, datetime(2025, 1, 3)],  # duplicate but not within the first window
            [1, None],  # considered duplicate with "b" as "1970-01-01"
            [1, None],  # considered duplicate with "b" as "1970-01-01"
            [None, datetime(2025, 1, 6)],
            [None, None],
        ],
        schema_num,
    )

    actual = test_df.select(is_unique("a", window_spec="window(coalesce(b, '1970-01-01'), '2 days')"))

    checked_schema = "a_is_not_unique: string"
    expected = spark.createDataFrame(
        [
            ["Column a has duplicate values"],
            ["Column a has duplicate values"],
            ["Column a has duplicate values"],
            ["Column a has duplicate values"],
            [None],
            [None],
            [None],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)
