from datetime import datetime

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
    not_greater_than,
    not_in_future,
    not_in_near_future,
    not_less_than,
    regex_match,
    sql_expression,
    value_is_in_list,
    value_is_not_null_and_is_in_list,
)

SCHEMA = "a: string, b: int"


def test_col_is_not_null_and_not_empty(spark):
    test_df = spark.createDataFrame([["str1", 1], ["", None], [" ", 3]], SCHEMA)

    actual = test_df.select(is_not_null_and_not_empty("a"), is_not_null_and_not_empty("b", True))

    checked_schema = "a_is_null_or_empty: string, b_is_null_or_empty: string"
    expected = spark.createDataFrame(
        [[None, None], ["Column a is null or empty", "Column b is null or empty"], [None, None]], checked_schema
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_is_not_empty(spark):
    test_df = spark.createDataFrame([["str1", 1], ["", None], [" ", 3]], SCHEMA)

    actual = test_df.select(is_not_empty("a"), is_not_empty("b"))

    checked_schema = "a_is_empty: string, b_is_empty: string"
    expected = spark.createDataFrame([[None, None], ["Column a is empty", None], [None, None]], checked_schema)

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_is_not_null(spark):
    test_df = spark.createDataFrame([["str1", 1], ["", None], [" ", 3]], SCHEMA)

    actual = test_df.select(is_not_null("a"), is_not_null("b"))

    checked_schema = "a_is_null: string, b_is_null: string"
    expected = spark.createDataFrame([[None, None], [None, "Column b is null"], [None, None]], checked_schema)

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_value_is_not_null_and_is_in_list(spark):
    test_df = spark.createDataFrame([["str1", 1], ["str2", None], ["", 3]], SCHEMA)

    actual = test_df.select(
        value_is_not_null_and_is_in_list("a", ["str1"]), value_is_not_null_and_is_in_list("b", [F.lit(3)])
    )

    checked_schema = "a_value_is_not_in_the_list: string, b_value_is_not_in_the_list: string"
    expected = spark.createDataFrame(
        [
            [None, "Value 1 is not in the allowed list: [3]"],
            ["Value str2 is not in the allowed list: [str1]", "Value null is not in the allowed list: [3]"],
            ["Value  is not in the allowed list: [str1]", None],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_value_is_not_in_list(spark):
    test_df = spark.createDataFrame([["str1", 1], ["str2", None], ["", 3]], SCHEMA)

    actual = test_df.select(value_is_in_list("a", ["str1"]), value_is_in_list("b", [F.lit(3)]))

    checked_schema = "a_value_is_not_in_the_list: string, b_value_is_not_in_the_list: string"
    expected = spark.createDataFrame(
        [
            [None, "Value 1 is not in the allowed list: [3]"],
            ["Value str2 is not in the allowed list: [str1]", None],
            ["Value  is not in the allowed list: [str1]", None],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_sql_expression(spark):
    test_df = spark.createDataFrame([["str1", 1, 1], ["str2", None, None], ["", 3, 2]], SCHEMA + ", c: string")

    actual = test_df.select(
        sql_expression("a = 'str1'"),
        sql_expression("b is not null", name="test", negate=True),
        sql_expression("c is not null", msg="failed validation", negate=True),
        sql_expression("b > c", msg="b is not greater than c", negate=True),
    )

    checked_schema = "a_str1_: string, test: string, c_is_not_null: string, b_c: string"
    expected = spark.createDataFrame(
        [
            ["Value matches expression: a = 'str1'", None, None, "b is not greater than c"],
            [None, "Value matches expression: ~(b is not null)", "failed validation", None],
            [None, None, None, None],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_is_col_older_than_col2_for_n_days(spark):
    schema_dates = "a: string, b: string"
    test_df = spark.createDataFrame(
        [
            ["2023-01-10", "2023-01-13"],
            ["2023-01-10", "2023-01-12"],
            ["2023-01-10", "2023-01-05"],
            ["2023-01-10", None],
            [None, None],
        ],
        schema_dates,
    )

    actual = test_df.select(is_older_than_col2_for_n_days("a", "b", 2))

    checked_schema = "is_col_a_older_than_b_for_N_days: string"
    expected = spark.createDataFrame(
        [
            ["Value of a: '2023-01-10' less than value of b: '2023-01-13' for more than 2 days"],
            [None],
            [None],
            [None],
            [None],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_is_col_older_than_n_days(spark):
    schema_dates = "a: string"
    test_df = spark.createDataFrame([["2023-01-10"], ["2023-01-13"], [None]], schema_dates)

    actual = test_df.select(is_older_than_n_days("a", 2, F.lit("2023-01-13")))

    checked_schema = "is_col_a_older_than_N_days: string"
    expected = spark.createDataFrame(
        [["Value of a: '2023-01-10' less than current date: '2023-01-13' for more than 2 days"], [None], [None]],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_not_in_future(spark):
    schema_dates = "a: string"
    test_df = spark.createDataFrame([["2023-01-10 11:08:37"], ["2023-01-10 11:08:43"], [None]], schema_dates)

    actual = test_df.select(not_in_future("a", 2, F.lit("2023-01-10 11:08:40")))

    checked_schema = "a_in_future: string"
    expected = spark.createDataFrame(
        [[None], ["Value '2023-01-10 11:08:43' is greater than time '2023-01-10 11:08:42'"], [None]], checked_schema
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_not_in_near_future(spark):
    schema_dates = "a: string"
    test_df = spark.createDataFrame(
        [["2023-01-10 11:08:40"], ["2023-01-10 11:08:41"], ["2023-01-10 11:08:42"], [None]], schema_dates
    )

    actual = test_df.select(not_in_near_future("a", 2, F.lit("2023-01-10 11:08:40")))

    checked_schema = "a_in_near_future: string"
    expected = spark.createDataFrame(
        [
            [None],
            ["Value '2023-01-10 11:08:41' is greater than '2023-01-10 11:08:40 and smaller than '2023-01-10 11:08:42'"],
            [None],
            [None],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_is_col_older_than_n_days_cur(spark):
    schema_dates = "a: string"
    cur_date = datetime.now().strftime("%Y-%m-%d")

    test_df = spark.createDataFrame([["2023-01-10"], [None]], schema_dates)

    actual = test_df.select(is_older_than_n_days("a", 2, None))

    checked_schema = "is_col_a_older_than_N_days: string"

    expected = spark.createDataFrame(
        [[f"Value of a: '2023-01-10' less than current date: '{cur_date}' for more than 2 days"], [None]],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_not_less_than(spark):
    schema_num = "a: int"
    test_df = spark.createDataFrame([[1], [2], [None]], schema_num)

    actual = test_df.select(not_less_than("a", 2))

    checked_schema = "a_less_than_limit: string"
    expected = spark.createDataFrame([["Value 1 is less than limit: 2"], [None], [None]], checked_schema)

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_not_greater_than(spark):
    schema_num = "a: int"
    test_df = spark.createDataFrame([[1], [2], [None]], schema_num)

    actual = test_df.select(not_greater_than("a", 1))

    checked_schema = "a_greater_than_limit: string"
    expected = spark.createDataFrame([[None], ["Value 2 is greater than limit: 1"], [None]], checked_schema)

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_is_in_range(spark):
    schema_num = "a: int"
    test_df = spark.createDataFrame([[0], [1], [2], [3], [4], [None]], schema_num)

    actual = test_df.select(is_in_range("a", 1, 3))

    checked_schema = "a_not_in_range: string"
    expected = spark.createDataFrame(
        [["Value 0 not in range: [ 1 , 3 ]"], [None], [None], [None], ["Value 4 not in range: [ 1 , 3 ]"], [None]],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_is_not_in_range(spark):
    schema_num = "a: int"
    test_df = spark.createDataFrame([[1], [2], [3], [None]], schema_num)

    actual = test_df.select(is_not_in_range("a", 1, 3))

    checked_schema = "a_in_range: string"
    expected = spark.createDataFrame([[None], ["Value 2 in range: [ 1 , 3 ]"], [None], [None]], checked_schema)

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_matching_regex(spark):
    schema_str = "a: string"
    test_df = spark.createDataFrame([["2023-01-02"], ["2023/01/02"], [None]], schema_str)

    # matching ISO date: yyyy-MM-dd format
    date_re = "^\\d{4}-([0]\\d|1[0-2])-([0-2]\\d|3[01])$"

    actual = test_df.select(regex_match("a", date_re), regex_match("a", date_re, negate=True))

    checked_schema = "a_not_matching_regex: string, a_matching_regex: string"
    expected = spark.createDataFrame(
        [[None, "Column a is matching regex"], ["Column a is not matching regex", None], [None, None]], checked_schema
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_struct(spark):
    test_df = spark.createDataFrame([[("str1",)]], "data: struct<x:string>")

    actual = test_df.select(is_not_empty("data.x"))

    checked_schema = "data_x_is_empty: string"
    expected = spark.createDataFrame([[None]], checked_schema)

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_not_in_future_cur(spark):
    schema_dates = "a: string"

    test_df = spark.createDataFrame([["9999-12-31 23:59:59"]], schema_dates)

    actual = test_df.select(not_in_future("a", 0, None))

    checked_schema = "a_in_future: string"

    expected = spark.createDataFrame([[None]], checked_schema)

    assert actual.select("a_in_future") != expected.select("a_in_future")


def test_col_not_in_near_future_cur(spark):
    schema_dates = "a: string"

    test_df = spark.createDataFrame([["1900-01-01 23:59:59"], ["9999-12-31 23:59:59"], [None]], schema_dates)

    actual = test_df.select(not_in_near_future("a", 2, None))

    checked_schema = "a_in_near_future: string"
    expected = spark.createDataFrame(
        [[None], [None], [None]],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)