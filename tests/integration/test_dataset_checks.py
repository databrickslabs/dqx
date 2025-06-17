from datetime import datetime
import pyspark.sql.functions as F
from chispa.dataframe_comparer import assert_df_equality  # type: ignore

from databricks.labs.dqx.check_funcs import (
    is_unique,
    is_aggr_not_greater_than,
    is_aggr_not_less_than,
)

SCHEMA = "a: string, b: int"


def test_col_is_unique(spark):
    test_df = spark.createDataFrame([["str1", 1], ["str2", 1], ["str2", 2], ["str3", 3]], SCHEMA)

    actual = test_df.select(is_unique(["a"]), is_unique(["b"]))

    checked_schema = "struct_a_is_not_unique: string, struct_b_is_not_unique: string"
    expected = spark.createDataFrame(
        [
            [None, "Value '{1}' in Column 'struct(b)' is not unique"],
            ["Value '{str2}' in Column 'struct(a)' is not unique", "Value '{1}' in Column 'struct(b)' is not unique"],
            ["Value '{str2}' in Column 'struct(a)' is not unique", None],
            [None, None],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_is_aggr_not_greater_than(spark):
    test_df = spark.createDataFrame(
        [
            ["a", 1],
            ["b", 3],
            ["c", None],
        ],
        SCHEMA,
    )

    actual = test_df.select(
        is_aggr_not_greater_than("a", limit=1, aggr_type="count"),
        is_aggr_not_greater_than(F.col("a"), limit=0, aggr_type="count", row_filter="b is not null"),
        is_aggr_not_greater_than("a", limit=F.lit(0), aggr_type="count", row_filter="b is not null", group_by=["a"]),
        is_aggr_not_greater_than(F.col("b"), limit=F.lit(0), aggr_type="count", group_by=[F.col("b")]),
        is_aggr_not_greater_than("b", limit=0.0, aggr_type="avg"),
        is_aggr_not_greater_than("b", limit=0.0, aggr_type="sum"),
        is_aggr_not_greater_than("b", limit=0.0, aggr_type="min"),
        is_aggr_not_greater_than("b", limit=0.0, aggr_type="max"),
    )

    expected_schema = (
        "a_count_greater_than_limit STRING, "
        "a_count_greater_than_limit STRING, "
        "a_count_group_by_a_greater_than_limit STRING,"
        "b_count_group_by_b_greater_than_limit STRING, "
        "b_avg_greater_than_limit STRING, "
        "b_sum_greater_than_limit STRING, "
        "b_min_greater_than_limit STRING, "
        "b_max_greater_than_limit STRING"
    )

    expected = spark.createDataFrame(
        [
            [
                "Count 3 in column 'a' is greater than limit: 1",
                "Count 2 in column 'a' is greater than limit: 0",
                None,
                None,
                "Avg 2.0 in column 'b' is greater than limit: 0.0",
                "Sum 4 in column 'b' is greater than limit: 0.0",
                "Min 1 in column 'b' is greater than limit: 0.0",
                "Max 3 in column 'b' is greater than limit: 0.0",
            ],
            [
                "Count 3 in column 'a' is greater than limit: 1",
                "Count 2 in column 'a' is greater than limit: 0",
                "Count 1 per group of columns 'a' in column 'a' is greater than limit: 0",
                "Count 1 per group of columns 'b' in column 'b' is greater than limit: 0",
                "Avg 2.0 in column 'b' is greater than limit: 0.0",
                "Sum 4 in column 'b' is greater than limit: 0.0",
                "Min 1 in column 'b' is greater than limit: 0.0",
                "Max 3 in column 'b' is greater than limit: 0.0",
            ],
            [
                "Count 3 in column 'a' is greater than limit: 1",
                "Count 2 in column 'a' is greater than limit: 0",
                "Count 1 per group of columns 'a' in column 'a' is greater than limit: 0",
                "Count 1 per group of columns 'b' in column 'b' is greater than limit: 0",
                "Avg 2.0 in column 'b' is greater than limit: 0.0",
                "Sum 4 in column 'b' is greater than limit: 0.0",
                "Min 1 in column 'b' is greater than limit: 0.0",
                "Max 3 in column 'b' is greater than limit: 0.0",
            ],
        ],
        expected_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_is_aggr_not_less_than(spark):
    test_df = spark.createDataFrame(
        [
            ["a", 1],
            ["b", 3],
            ["c", None],
        ],
        SCHEMA,
    )

    actual = test_df.select(
        is_aggr_not_less_than("a", limit=4, aggr_type="count"),
        is_aggr_not_less_than(F.col("a"), limit=3, aggr_type="count", row_filter="b is not null"),
        is_aggr_not_less_than(
            "a", limit=F.lit(2), aggr_type="count", row_filter="b is not null", group_by=[F.col("a")]
        ),
        is_aggr_not_less_than(F.col("b"), limit=F.lit(2), aggr_type="count", group_by=["b"]),
        is_aggr_not_less_than("b", limit=3.0, aggr_type="avg"),
        is_aggr_not_less_than("b", limit=5.0, aggr_type="sum"),
        is_aggr_not_less_than("b", limit=2.0, aggr_type="min"),
        is_aggr_not_less_than("b", limit=4.0, aggr_type="max"),
    )

    expected_schema = (
        "a_count_less_than_limit STRING, "
        "a_count_less_than_limit STRING, "
        "a_count_group_by_a_less_than_limit STRING,"
        "b_count_group_by_b_less_than_limit STRING, "
        "b_avg_less_than_limit STRING, "
        "b_sum_less_than_limit STRING, "
        "b_min_less_than_limit STRING, "
        "b_max_less_than_limit STRING"
    )

    expected = spark.createDataFrame(
        [
            [
                "Count 3 in column 'a' is less than limit: 4",
                "Count 2 in column 'a' is less than limit: 3",
                "Count 0 per group of columns 'a' in column 'a' is less than limit: 2",
                "Count 0 per group of columns 'b' in column 'b' is less than limit: 2",
                "Avg 2.0 in column 'b' is less than limit: 3.0",
                "Sum 4 in column 'b' is less than limit: 5.0",
                "Min 1 in column 'b' is less than limit: 2.0",
                "Max 3 in column 'b' is less than limit: 4.0",
            ],
            [
                "Count 3 in column 'a' is less than limit: 4",
                "Count 2 in column 'a' is less than limit: 3",
                "Count 1 per group of columns 'a' in column 'a' is less than limit: 2",
                "Count 1 per group of columns 'b' in column 'b' is less than limit: 2",
                "Avg 2.0 in column 'b' is less than limit: 3.0",
                "Sum 4 in column 'b' is less than limit: 5.0",
                "Min 1 in column 'b' is less than limit: 2.0",
                "Max 3 in column 'b' is less than limit: 4.0",
            ],
            [
                "Count 3 in column 'a' is less than limit: 4",
                "Count 2 in column 'a' is less than limit: 3",
                "Count 1 per group of columns 'a' in column 'a' is less than limit: 2",
                "Count 1 per group of columns 'b' in column 'b' is less than limit: 2",
                "Avg 2.0 in column 'b' is less than limit: 3.0",
                "Sum 4 in column 'b' is less than limit: 5.0",
                "Min 1 in column 'b' is less than limit: 2.0",
                "Max 3 in column 'b' is less than limit: 4.0",
            ],
        ],
        expected_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_is_unique_handle_nulls(spark):
    test_df = spark.createDataFrame([["", None], ["", None], ["str1", 1], [None, None]], SCHEMA)

    actual = test_df.select(is_unique(["a"]), is_unique(["b"]))

    checked_schema = "struct_a_is_not_unique: string, struct_b_is_not_unique: string"
    expected = spark.createDataFrame(
        [
            [
                "Value '{}' in Column 'struct(a)' is not unique",
                None,
            ],  # Null values are not considered duplicates as they are unknown
            ["Value '{}' in Column 'struct(a)' is not unique", None],
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
            [1, None],  # considered duplicate with "b" as "1970-01-01", and duplicate over col a and b
            [1, None],  # considered duplicate with "b" as "1970-01-01", and duplicate over col a and b
            [None, datetime(2025, 1, 6)],
            [None, None],
        ],
        schema_num,
    )

    actual = test_df.select(
        # must use coalesce to handle nulls, otherwise records with null for the time column b will be dropped
        is_unique(["a"], window_spec=F.window(F.coalesce(F.col("b"), F.lit(datetime(1970, 1, 1))), "2 days")),
        is_unique([F.col("a"), F.col("b")], nulls_distinct=False),
    )

    checked_schema = "struct_a_is_not_unique: string, struct_a_b_is_not_unique: string"
    expected = spark.createDataFrame(
        [
            [
                "Value '{1}' in Column 'struct(a)' is not unique",
                "Value '{1, null}' in Column 'struct(a, b)' is not unique",
            ],
            [
                "Value '{1}' in Column 'struct(a)' is not unique",
                "Value '{1, null}' in Column 'struct(a, b)' is not unique",
            ],
            ["Value '{0}' in Column 'struct(a)' is not unique", None],
            ["Value '{0}' in Column 'struct(a)' is not unique", None],
            [None, None],
            [None, None],
            [None, None],
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
        is_unique(["a"], window_spec=F.window(F.col("b"), "2 days"))
    )

    checked_schema = "struct_a_is_not_unique: string"
    expected = spark.createDataFrame(
        [
            ["Value '{0}' in Column 'struct(a)' is not unique"],
            ["Value '{0}' in Column 'struct(a)' is not unique"],
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

    actual = test_df.select(is_unique(["a"], window_spec="window(coalesce(b, '1970-01-01'), '2 days')"))

    checked_schema = "struct_a_is_not_unique: string"
    expected = spark.createDataFrame(
        [
            ["Value '{0}' in Column 'struct(a)' is not unique"],
            ["Value '{0}' in Column 'struct(a)' is not unique"],
            ["Value '{1}' in Column 'struct(a)' is not unique"],
            ["Value '{1}' in Column 'struct(a)' is not unique"],
            [None],
            [None],
            [None],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)
