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
    not_in_future,
    not_in_near_future,
    not_less_than,
    not_greater_than,
    regex_match,
    sql_expression,
    value_is_in_list,
    value_is_not_null_and_is_in_list,
    is_not_null_and_not_empty_array,
    is_valid_date,
    is_valid_timestamp,
    is_unique,
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
    cur_date = spark.sql("SELECT current_date() AS current_date").collect()[0]['current_date'].strftime("%Y-%m-%d")

    test_df = spark.createDataFrame([["2023-01-10"], [None]], schema_dates)

    actual = test_df.select(is_older_than_n_days("a", 2, None))

    checked_schema = "is_col_a_older_than_N_days: string"

    expected = spark.createDataFrame(
        [[f"Value of a: '2023-01-10' less than current date: '{cur_date}' for more than 2 days"], [None]],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_not_less_than(spark, set_utc_timezone):
    schema_num = "a: int, b: int, c: date, d: timestamp"
    test_df = spark.createDataFrame(
        [
            [1, 1, datetime(2025, 1, 1).date(), datetime(2025, 1, 1)],
            [2, 4, datetime(2025, 2, 1).date(), datetime(2025, 2, 1)],
            [4, 3, None, None],
            [None, None, None, None],
        ],
        schema_num,
    )

    actual = test_df.select(
        not_less_than("a", 2),
        not_less_than("a", limit_col_expr=F.col("b") * 2),
        not_less_than("b", limit_col_expr="a", limit=5),  # limit is skipped if limit_col_expr is provided
        not_less_than("c", datetime(2025, 2, 1).date()),
        not_less_than("d", datetime(2025, 2, 1)),
    )

    checked_schema = "a_less_than_limit: string, a_less_than_limit: string, b_less_than_limit: string, " \
                     "c_less_than_limit: string, d_less_than_limit: string"

    expected = spark.createDataFrame(
        [
            [
                "Value 1 is less than limit: 2",
                None,
                None,
                "Value 2025-01-01 is less than limit: 2025-02-01",
                "Value 2025-01-01 00:00:00 is less than limit: 2025-02-01 00:00:00",
            ],
            [None, "Value 2 is less than limit: 8", None, None, None],
            [
                None,
                "Value 4 is less than limit: 6",
                "Value 3 is less than limit: 4",
                None,
                None,
            ],
            [None, None, None, None, None],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_not_greater_than(spark, set_utc_timezone):
    schema_num = "a: int, b: int, c: date, d: timestamp"
    test_df = spark.createDataFrame(
        [
            [1, 1, datetime(2025, 1, 1).date(), datetime(2025, 1, 1)],
            [2, 4, datetime(2025, 2, 1).date(), datetime(2025, 2, 1)],
            [8, 3, None, None],
            [None, None, None, None],
        ],
        schema_num,
    )

    actual = test_df.select(
        not_greater_than("a", 1),
        not_greater_than("a", limit_col_expr=F.col("b") * 2),
        not_greater_than("b", limit_col_expr="a", limit=5),  # limit is skipped if limit_col_expr is provided
        not_greater_than("c", datetime(2025, 1, 1).date()),
        not_greater_than("d", datetime(2025, 1, 1)),
    )

    checked_schema = "a_greater_than_limit: string, a_greater_than_limit: string, b_greater_than_limit: string, " \
                     "c_greater_than_limit: string, d_greater_than_limit: string"
    expected = spark.createDataFrame(
        [
            [None, None, None, None, None],
            [
                "Value 2 is greater than limit: 1",
                None,
                "Value 4 is greater than limit: 2",
                "Value 2025-02-01 is greater than limit: 2025-01-01",
                "Value 2025-02-01 00:00:00 is greater than limit: 2025-01-01 00:00:00",
            ],
            [
                "Value 8 is greater than limit: 1",
                "Value 8 is greater than limit: 6",
                None,
                None,
                None
            ],
            [None, None, None, None, None],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_is_in_range(spark, set_utc_timezone):
    schema_num = "a: int, b: date, c: timestamp, d: int, e: int, f: int"
    test_df = spark.createDataFrame(
        [
            [0, datetime(2024, 12, 1).date(), datetime(2024, 12, 1), -1, 5, 6],
            [1, datetime(2025, 1, 1).date(), datetime(2025, 1, 1), 2, 6, 3],
            [2, datetime(2025, 2, 1).date(), datetime(2025, 2, 1), 2, 7, 3],
            [3, datetime(2025, 3, 1).date(), datetime(2025, 3, 1), 3, 8, 3],
            [4, datetime(2025, 4, 1).date(), datetime(2025, 4, 1), 2, 9, 3],
            [None, None, None, None, None, None],
        ],
        schema_num,
    )

    start_date = datetime(2025, 1, 1)
    end_date = datetime(2025, 3, 1)
    actual = test_df.select(
        is_in_range("a", 1, 3),
        is_in_range("b", start_date.date(), end_date.date()),
        is_in_range("c", start_date, end_date),
        is_in_range("d", min_limit_col_expr=F.col("a"), max_limit_col_expr=F.expr("e - 1")),
        is_in_range("f", min_limit_col_expr="a", max_limit=5),
    )
    checked_schema = "a_not_in_range: string, b_not_in_range: string, c_not_in_range: string, d_not_in_range: string, f_not_in_range: string"
    expected = spark.createDataFrame(
        [
            [
                "Value 0 not in range: [ 1 , 3 ]",
                "Value 2024-12-01 not in range: [ 2025-01-01 , 2025-03-01 ]",
                "Value 2024-12-01 00:00:00 not in range: [ 2025-01-01 00:00:00 , 2025-03-01 00:00:00 ]",
                "Value -1 not in range: [ 0 , 4 ]",
                "Value 6 not in range: [ 0 , 5 ]",
            ],
            [None, None, None, None, None],
            [None, None, None, None, None],
            [None, None, None, None, None],
            [
                "Value 4 not in range: [ 1 , 3 ]",
                "Value 2025-04-01 not in range: [ 2025-01-01 , 2025-03-01 ]",
                "Value 2025-04-01 00:00:00 not in range: [ 2025-01-01 00:00:00 , 2025-03-01 00:00:00 ]",
                "Value 2 not in range: [ 4 , 8 ]",
                "Value 3 not in range: [ 4 , 5 ]",
            ],
            [None, None, None, None, None],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_is_not_in_range(spark, set_utc_timezone):
    schema_num = "a: int, b: date, c: timestamp, d: timestamp"
    test_df = spark.createDataFrame(
        [
            [1, datetime(2025, 1, 1).date(), datetime(2024, 1, 1), datetime(2024, 1, 1)],
            [2, datetime(2025, 2, 1).date(), datetime(2025, 2, 1), datetime(2025, 2, 2)],
            [3, datetime(2025, 3, 1).date(), datetime(2025, 3, 1), datetime(2025, 3, 1)],
            [None, None, None, None],
        ],
        schema_num,
    )

    start_date = datetime(2025, 1, 1)
    end_date = datetime(2025, 3, 1)
    actual = test_df.select(
        is_not_in_range("a", 1, 3),
        is_not_in_range("b", start_date.date(), end_date.date()),
        is_not_in_range("c", start_date, end_date),
        is_not_in_range(
            "d", min_limit_col_expr="c", max_limit_col_expr=F.expr("cast(b as timestamp) + INTERVAL 2 DAY")
        ),
    )

    checked_schema = "a_in_range: string, b_in_range: string, c_in_range: string, d_in_range: string"
    expected = spark.createDataFrame(
        [
            [None, None, None, None],
            [
                "Value 2 in range: [ 1 , 3 ]",
                "Value 2025-02-01 in range: [ 2025-01-01 , 2025-03-01 ]",
                "Value 2025-02-01 00:00:00 in range: [ 2025-01-01 00:00:00 , 2025-03-01 00:00:00 ]",
                "Value 2025-02-02 00:00:00 in range: [ 2025-02-01 00:00:00 , 2025-02-03 00:00:00 ]",
            ],
            [None, None, None, None],
            [None, None, None, None],
        ],
        checked_schema,
    )

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


def test_col_is_not_null_and_not_empty_array(spark):
    schema_array = "str_col: array<string>, int_col: array<int> , timestamp_col: array<timestamp>, date_col: array<string>, struct_col: array<struct<a: string, b: int>>"
    data = [
        (
            ["a", "b", None],
            [1, 2, None],
            [None, datetime.strptime("2025-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")],
            [datetime.strptime("2025-01-01", "%Y-%m-%d"), None],
            [{"a": "x", "b": 1}, None],
        ),
        ([], [], [], [], []),
        (None, None, None, None, None),
        (
            ["non-empty"],
            [10],
            [datetime.strptime("2025-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")],
            [datetime.strptime("2025-01-01", "%Y-%m-%d")],
            [{"a": "y", "b": 2}],
        ),
    ]

    test_df = spark.createDataFrame(data, schema_array)

    actual = test_df.select(
        is_not_null_and_not_empty_array("str_col"),
        is_not_null_and_not_empty_array("int_col"),
        is_not_null_and_not_empty_array("timestamp_col"),
        is_not_null_and_not_empty_array("date_col"),
        is_not_null_and_not_empty_array("struct_col"),
    )

    checked_schema = "str_col_is_null_or_empty_array: string, int_col_is_null_or_empty_array: string, timestamp_col_is_null_or_empty_array: string, date_col_is_null_or_empty_array: string, struct_col_is_null_or_empty_array: string"
    # Create the data
    checked_data = [
        (None, None, None, None, None),
        (
            "Column str_col is null or empty array",
            "Column int_col is null or empty array",
            "Column timestamp_col is null or empty array",
            "Column date_col is null or empty array",
            "Column struct_col is null or empty array",
        ),
        (
            "Column str_col is null or empty array",
            "Column int_col is null or empty array",
            "Column timestamp_col is null or empty array",
            "Column date_col is null or empty array",
            "Column struct_col is null or empty array",
        ),
        (None, None, None, None, None),
    ]
    expected = spark.createDataFrame(checked_data, checked_schema)

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_is_valid_date(spark, set_utc_timezone):
    schema_array = "a: string, b: string, c: string, d: string"
    data = [
        ["2024-01-01", "12/31/2025", "invalid_date", None],
        ["12/31/2025", "2024-01-01", "invalid_date", None],
        ["12/31/2025", "invalid_date", "2024-01-01", None],
    ]

    test_df = spark.createDataFrame(data, schema_array)

    actual = test_df.select(
        is_valid_date("a"), is_valid_date("b", "MM/dd/yyyy"), is_valid_date("c", "yyyy-MM-dd"), is_valid_date("d")
    )

    checked_schema = """
        a_is_not_valid_date: string, 
        b_is_not_valid_date: string, 
        c_is_not_valid_date: string, 
        d_is_not_valid_date: string
        """
    checked_data = [
        [None, None, "Value 'invalid_date' is not a valid date with format 'yyyy-MM-dd'", None],
        [
            "Value '12/31/2025' is not a valid date",
            "Value '2024-01-01' is not a valid date with format 'MM/dd/yyyy'",
            "Value 'invalid_date' is not a valid date with format 'yyyy-MM-dd'",
            None,
        ],
        [
            "Value '12/31/2025' is not a valid date",
            "Value 'invalid_date' is not a valid date with format 'MM/dd/yyyy'",
            None,
            None,
        ],
    ]
    expected = spark.createDataFrame(checked_data, checked_schema)

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_is_valid_timestamp(spark, set_utc_timezone):
    schema_array = "a: string, b: string, c: string, d: string, e: string"
    data = [
        ["2024-01-01 00:00:00", "12/31/2025 00:00:00", "invalid_timestamp", None, "2025-01-31T00:00:00"],
        ["12/31/2025 00:00:00", "2024-01-01 00:00:00", "invalid_timestamp", None, "2025-01-31 00:00:00"],
        ["2024-01-01T00:00:00", "invalid_timestamp", "2024-01-01 00:00:00", None, "1/31/2025 00:00:00"],
    ]

    test_df = spark.createDataFrame(data, schema_array)

    actual = test_df.select(
        is_valid_timestamp("a"),
        is_valid_timestamp("b", "MM/dd/yyyy HH:mm:ss"),
        is_valid_timestamp("c", "yyyy-MM-dd HH:mm:ss"),
        is_valid_timestamp("d"),
        is_valid_timestamp("e", "yyyy-MM-dd'T'HH:mm:ss"),
    )

    checked_schema = """
        a_is_not_valid_timestamp: string, 
        b_is_not_valid_timestamp: string, 
        c_is_not_valid_timestamp: string, 
        d_is_not_valid_timestamp: string,
        e_is_not_valid_timestamp: string
        """
    checked_data = [
        [
            None,
            None,
            "Value 'invalid_timestamp' is not a valid timestamp with format 'yyyy-MM-dd HH:mm:ss'",
            None,
            None,
        ],
        [
            "Value '12/31/2025 00:00:00' is not a valid timestamp",
            "Value '2024-01-01 00:00:00' is not a valid timestamp with format 'MM/dd/yyyy HH:mm:ss'",
            "Value 'invalid_timestamp' is not a valid timestamp with format 'yyyy-MM-dd HH:mm:ss'",
            None,
            "Value '2025-01-31 00:00:00' is not a valid timestamp with format 'yyyy-MM-dd'T'HH:mm:ss'",
        ],
        [
            None,
            "Value 'invalid_timestamp' is not a valid timestamp with format 'MM/dd/yyyy HH:mm:ss'",
            None,
            None,
            "Value '1/31/2025 00:00:00' is not a valid timestamp with format 'yyyy-MM-dd'T'HH:mm:ss'",
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
    test_df = spark.createDataFrame([["", None], ["", None], ["str1", 1]], SCHEMA)

    actual = test_df.select(is_unique("a"), is_unique("b"))

    checked_schema = "a_is_not_unique: string, b_is_not_unique: string"
    expected = spark.createDataFrame(
        [
            ["Column a has duplicate values", None],  # Null values are not considered duplicates as they are unknown
            ["Column a has duplicate values", None],
            [None, None],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)
