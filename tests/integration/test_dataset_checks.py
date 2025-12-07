from collections.abc import Callable
from datetime import date, datetime, timedelta
from typing import Any
import json
import itertools
import pytest

import pyspark.sql.functions as F
from chispa.dataframe_comparer import assert_df_equality  # type: ignore
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

from databricks.labs.dqx.check_funcs import (
    is_unique,
    is_aggr_not_greater_than,
    is_aggr_not_less_than,
    is_aggr_equal,
    is_aggr_not_equal,
    foreign_key,
    compare_datasets,
    is_data_fresh_per_time_window,
    has_valid_schema,
)
from databricks.labs.dqx.utils import get_column_name_or_alias
from databricks.labs.dqx.errors import InvalidParameterError, MissingParameterError

from tests.conftest import TEST_CATALOG


SCHEMA = "a: string, b: int"


def test_is_unique(spark: SparkSession):
    test_df = spark.createDataFrame(
        [
            ["str1", 1],
            ["str2", 1],
            ["str2", 2],
            ["str3", 3],
        ],
        SCHEMA,
    )

    condition, apply_method = is_unique(["a"])
    actual_apply_df = apply_method(test_df)
    actual_condition_df = actual_apply_df.select("a", "b", condition)

    expected_condition_df = spark.createDataFrame(
        [
            ["str1", 1, None],
            ["str2", 2, "Value 'str2' in column 'a' is not unique, found 2 duplicates"],
            ["str3", 3, None],
            ["str2", 1, "Value 'str2' in column 'a' is not unique, found 2 duplicates"],
        ],
        SCHEMA + ", a_is_not_unique: string",
    )
    assert_df_equality(actual_condition_df, expected_condition_df, ignore_nullable=True, ignore_row_order=True)


def test_is_unique_null_distinct(spark: SparkSession):
    test_df = spark.createDataFrame(
        [
            ["str1", 1],
            ["str1", 1],
            [None, 1],
            [None, 1],
        ],
        SCHEMA,
    )

    condition, apply_method = is_unique(["a", "b"], nulls_distinct=True)
    actual_apply_df = apply_method(test_df)
    actual_condition_df = actual_apply_df.select("a", "b", condition)

    expected_condition_df = spark.createDataFrame(
        [
            [None, 1, None],
            [None, 1, None],
            ["str1", 1, "Value '{str1, 1}' in column 'struct(a, b)' is not unique, found 2 duplicates"],
            ["str1", 1, "Value '{str1, 1}' in column 'struct(a, b)' is not unique, found 2 duplicates"],
        ],
        SCHEMA + ", struct_a_b_is_not_unique: string",
    )
    assert_df_equality(actual_condition_df, expected_condition_df, ignore_nullable=True)


def test_is_unique_nulls_not_distinct(spark: SparkSession):
    test_df = spark.createDataFrame([["", None], ["", None], [None, 1], [None, 1], [None, None]], SCHEMA)

    condition, apply_method = is_unique(["a", "b"], nulls_distinct=False)
    actual_apply_df = apply_method(test_df)
    actual_condition_df = actual_apply_df.select("a", "b", condition)

    expected_condition_df = spark.createDataFrame(
        [
            [None, None, None],
            [None, 1, "Value '{null, 1}' in column 'struct(a, b)' is not unique, found 2 duplicates"],
            [None, 1, "Value '{null, 1}' in column 'struct(a, b)' is not unique, found 2 duplicates"],
            ["", None, "Value '{, null}' in column 'struct(a, b)' is not unique, found 2 duplicates"],
            ["", None, "Value '{, null}' in column 'struct(a, b)' is not unique, found 2 duplicates"],
        ],
        SCHEMA + ", struct_a_b_is_not_unique: string",
    )
    assert_df_equality(actual_condition_df, expected_condition_df, ignore_nullable=True)


def test_foreign_key(spark: SparkSession):
    test_df = spark.createDataFrame(
        [
            ["key1", 1],
            ["key2", 2],
            ["key3", 3],
            [None, 4],
        ],
        SCHEMA,
    )

    ref_df = spark.createDataFrame(
        [
            ["key1"],
            ["key3"],
        ],
        "ref_col: string",
    )

    ref_dfs = {"ref_df": ref_df}
    checks = [
        foreign_key(["a"], ["ref_col"], "ref_df"),
        foreign_key([F.lit("a")], [F.lit("ref_col")], "ref_df", row_filter="b = 3"),
    ]

    actual_df = _apply_checks(test_df, checks, ref_dfs, spark)

    expected_condition_df = spark.createDataFrame(
        [
            ["key1", 1, None, None],
            ["key2", 2, "Value 'key2' in column 'a' not found in reference column 'ref_col'", None],
            ["key3", 3, None, None],
            [None, 4, None, None],
        ],
        SCHEMA + ", a_not_exists_in_ref_ref_col: string, a_not_exists_in_ref_ref_col: string",
    )
    assert_df_equality(actual_df, expected_condition_df, ignore_nullable=True)


def test_foreign_key_negate(spark: SparkSession):
    test_df = spark.createDataFrame(
        [
            ["key1", 1],
            ["key2", 2],
            ["key3", 3],
            [None, 4],
        ],
        SCHEMA,
    )

    ref_df = spark.createDataFrame(
        [
            ["key1"],
            ["key4"],
        ],
        "ref_col: string",
    )

    ref_dfs = {"ref_df": ref_df}
    checks = [
        foreign_key(["a"], ["ref_col"], "ref_df", negate=True),
        foreign_key([F.lit("a")], [F.lit("ref_col")], "ref_df", row_filter="b = 3", negate=True),
    ]

    actual_df = _apply_checks(test_df, checks, ref_dfs, spark)

    expected_condition_df = spark.createDataFrame(
        [
            ["key1", 1, "Value 'key1' in column 'a' found in reference column 'ref_col'", None],
            ["key2", 2, None, None],
            ["key3", 3, None, None],
            [None, 4, None, None],
        ],
        SCHEMA + ", a_exists_in_ref_ref_col: string, a_exists_in_ref_ref_col: string",
    )
    assert_df_equality(actual_df, expected_condition_df, ignore_nullable=True)


def test_is_aggr_not_greater_than(spark: SparkSession):
    test_df = spark.createDataFrame(
        [
            ["a", 1],
            ["b", 3],
            ["c", None],
        ],
        SCHEMA,
    )

    checks = [
        is_aggr_not_greater_than("a", limit=1, aggr_type="count"),
        is_aggr_not_greater_than(F.col("a"), limit=0, aggr_type="count", row_filter="b is not null"),
        is_aggr_not_greater_than("a", limit=F.lit(0), aggr_type="count", row_filter="b is not null", group_by=["a"]),
        is_aggr_not_greater_than(F.col("b"), limit=F.lit(0), aggr_type="count", group_by=[F.col("b")]),
        is_aggr_not_greater_than("b", limit=0.0, aggr_type="avg"),
        is_aggr_not_greater_than("b", limit=0.0, aggr_type="sum"),
        is_aggr_not_greater_than("b", limit=0.0, aggr_type="min"),
        is_aggr_not_greater_than("b", limit=0.0, aggr_type="max"),
    ]

    actual = _apply_checks(test_df, checks)

    expected_schema = (
        f"{SCHEMA}, a_count_greater_than_limit STRING, "
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
                "c",
                None,
                "Count value 3 in column 'a' is greater than limit: 1",
                # displayed since filtering is done after, filter only applied for calculation inside the check
                "Count value 2 in column 'a' is greater than limit: 0",
                None,
                None,
                "Average value 2.0 in column 'b' is greater than limit: 0.0",
                "Sum value 4 in column 'b' is greater than limit: 0.0",
                "Min value 1 in column 'b' is greater than limit: 0.0",
                "Max value 3 in column 'b' is greater than limit: 0.0",
            ],
            [
                "a",
                1,
                "Count value 3 in column 'a' is greater than limit: 1",
                "Count value 2 in column 'a' is greater than limit: 0",
                "Count value 1 in column 'a' per group of columns 'a' is greater than limit: 0",
                "Count value 1 in column 'b' per group of columns 'b' is greater than limit: 0",
                "Average value 2.0 in column 'b' is greater than limit: 0.0",
                "Sum value 4 in column 'b' is greater than limit: 0.0",
                "Min value 1 in column 'b' is greater than limit: 0.0",
                "Max value 3 in column 'b' is greater than limit: 0.0",
            ],
            [
                "b",
                3,
                "Count value 3 in column 'a' is greater than limit: 1",
                "Count value 2 in column 'a' is greater than limit: 0",
                "Count value 1 in column 'a' per group of columns 'a' is greater than limit: 0",
                "Count value 1 in column 'b' per group of columns 'b' is greater than limit: 0",
                "Average value 2.0 in column 'b' is greater than limit: 0.0",
                "Sum value 4 in column 'b' is greater than limit: 0.0",
                "Min value 1 in column 'b' is greater than limit: 0.0",
                "Max value 3 in column 'b' is greater than limit: 0.0",
            ],
        ],
        expected_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_is_aggr_not_less_than(spark: SparkSession):
    test_df = spark.createDataFrame(
        [
            ["a", 1],
            ["b", 3],
            ["c", None],
        ],
        SCHEMA,
    )

    checks = [
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
    ]
    actual = _apply_checks(test_df, checks)

    expected_schema = (
        f"{SCHEMA}, a_count_less_than_limit STRING, "
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
                "c",
                None,
                "Count value 3 in column 'a' is less than limit: 4",
                "Count value 2 in column 'a' is less than limit: 3",
                "Count value 0 in column 'a' per group of columns 'a' is less than limit: 2",
                "Count value 0 in column 'b' per group of columns 'b' is less than limit: 2",
                "Average value 2.0 in column 'b' is less than limit: 3.0",
                "Sum value 4 in column 'b' is less than limit: 5.0",
                "Min value 1 in column 'b' is less than limit: 2.0",
                "Max value 3 in column 'b' is less than limit: 4.0",
            ],
            [
                "a",
                1,
                "Count value 3 in column 'a' is less than limit: 4",
                "Count value 2 in column 'a' is less than limit: 3",
                "Count value 1 in column 'a' per group of columns 'a' is less than limit: 2",
                "Count value 1 in column 'b' per group of columns 'b' is less than limit: 2",
                "Average value 2.0 in column 'b' is less than limit: 3.0",
                "Sum value 4 in column 'b' is less than limit: 5.0",
                "Min value 1 in column 'b' is less than limit: 2.0",
                "Max value 3 in column 'b' is less than limit: 4.0",
            ],
            [
                "b",
                3,
                "Count value 3 in column 'a' is less than limit: 4",
                "Count value 2 in column 'a' is less than limit: 3",
                "Count value 1 in column 'a' per group of columns 'a' is less than limit: 2",
                "Count value 1 in column 'b' per group of columns 'b' is less than limit: 2",
                "Average value 2.0 in column 'b' is less than limit: 3.0",
                "Sum value 4 in column 'b' is less than limit: 5.0",
                "Min value 1 in column 'b' is less than limit: 2.0",
                "Max value 3 in column 'b' is less than limit: 4.0",
            ],
        ],
        expected_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def _apply_checks(
    test_df: DataFrame,
    checks: list[tuple[Column, Callable]],
    ref_dfs: dict[str, DataFrame] | None = None,
    spark: SparkSession | None = None,
) -> DataFrame:
    df_checked = test_df

    kwargs: dict[str, Any] = {}
    if spark:
        kwargs["spark"] = spark
    if ref_dfs:
        kwargs["ref_dfs"] = ref_dfs

    for _, apply_closure in checks:
        df_checked = apply_closure(df_checked, **kwargs)

    # Now simply select the conditions directly without adding withColumn
    condition_columns = [condition for (condition, _) in checks]
    actual = df_checked.select("a", "b", *condition_columns)
    return actual


def test_is_aggr_equal(spark: SparkSession):
    test_df = spark.createDataFrame(
        [
            ["a", 1],
            ["b", 3],
            ["c", None],
        ],
        SCHEMA,
    )

    checks = [
        is_aggr_equal("a", limit=3, aggr_type="count"),
        is_aggr_equal(F.col("a"), limit=1, aggr_type="count", row_filter="b is not null"),
        is_aggr_equal("a", limit=F.lit(1), aggr_type="count", row_filter="b is not null", group_by=["a"]),
        is_aggr_equal(F.col("b"), limit=F.lit(2), aggr_type="count", group_by=[F.col("b")]),
        is_aggr_equal("b", limit=2.0, aggr_type="avg"),
        is_aggr_equal("b", limit=10.0, aggr_type="sum"),
        is_aggr_equal("b", limit=1.0, aggr_type="min"),
        is_aggr_equal("b", limit=5.0, aggr_type="max"),
    ]

    actual = _apply_checks(test_df, checks)

    expected_schema = (
        f"{SCHEMA}, a_count_not_equal_to_limit STRING, "
        "a_count_not_equal_to_limit STRING, "
        "a_count_group_by_a_not_equal_to_limit STRING,"
        "b_count_group_by_b_not_equal_to_limit STRING, "
        "b_avg_not_equal_to_limit STRING, "
        "b_sum_not_equal_to_limit STRING, "
        "b_min_not_equal_to_limit STRING, "
        "b_max_not_equal_to_limit STRING"
    )

    expected = spark.createDataFrame(
        [
            [
                "c",
                None,
                None,
                "Count value 2 in column 'a' is not equal to limit: 1",
                "Count value 0 in column 'a' per group of columns 'a' is not equal to limit: 1",
                "Count value 0 in column 'b' per group of columns 'b' is not equal to limit: 2",
                None,
                "Sum value 4 in column 'b' is not equal to limit: 10.0",
                None,
                "Max value 3 in column 'b' is not equal to limit: 5.0",
            ],
            [
                "a",
                1,
                None,
                "Count value 2 in column 'a' is not equal to limit: 1",
                None,
                "Count value 1 in column 'b' per group of columns 'b' is not equal to limit: 2",
                None,
                "Sum value 4 in column 'b' is not equal to limit: 10.0",
                None,
                "Max value 3 in column 'b' is not equal to limit: 5.0",
            ],
            [
                "b",
                3,
                None,
                "Count value 2 in column 'a' is not equal to limit: 1",
                None,
                "Count value 1 in column 'b' per group of columns 'b' is not equal to limit: 2",
                None,
                "Sum value 4 in column 'b' is not equal to limit: 10.0",
                None,
                "Max value 3 in column 'b' is not equal to limit: 5.0",
            ],
        ],
        expected_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_is_aggr_not_equal(spark: SparkSession):
    test_df = spark.createDataFrame(
        [
            ["a", 1],
            ["b", 3],
            ["c", None],
        ],
        SCHEMA,
    )

    checks = [
        is_aggr_not_equal("a", limit=3, aggr_type="count"),
        is_aggr_not_equal(F.col("a"), limit=1, aggr_type="count", row_filter="b is not null"),
        is_aggr_not_equal("a", limit=F.lit(1), aggr_type="count", row_filter="b is not null", group_by=["a"]),
        is_aggr_not_equal(F.col("b"), limit=F.lit(2), aggr_type="count", group_by=[F.col("b")]),
        is_aggr_not_equal("b", limit=2.0, aggr_type="avg"),
        is_aggr_not_equal("b", limit=10.0, aggr_type="sum"),
        is_aggr_not_equal("b", limit=1.0, aggr_type="min"),
        is_aggr_not_equal("b", limit=5.0, aggr_type="max"),
    ]

    actual = _apply_checks(test_df, checks)

    expected_schema = (
        f"{SCHEMA}, a_count_equal_to_limit STRING, "
        "a_count_equal_to_limit STRING, "
        "a_count_group_by_a_equal_to_limit STRING,"
        "b_count_group_by_b_equal_to_limit STRING, "
        "b_avg_equal_to_limit STRING, "
        "b_sum_equal_to_limit STRING, "
        "b_min_equal_to_limit STRING, "
        "b_max_equal_to_limit STRING"
    )

    expected = spark.createDataFrame(
        [
            [
                "c",
                None,
                "Count value 3 in column 'a' is equal to limit: 3",
                None,
                None,
                None,
                "Average value 2.0 in column 'b' is equal to limit: 2.0",
                None,
                "Min value 1 in column 'b' is equal to limit: 1.0",
                None,
            ],
            [
                "a",
                1,
                "Count value 3 in column 'a' is equal to limit: 3",
                None,
                "Count value 1 in column 'a' per group of columns 'a' is equal to limit: 1",
                None,
                "Average value 2.0 in column 'b' is equal to limit: 2.0",
                None,
                "Min value 1 in column 'b' is equal to limit: 1.0",
                None,
            ],
            [
                "b",
                3,
                "Count value 3 in column 'a' is equal to limit: 3",
                None,
                "Count value 1 in column 'a' per group of columns 'a' is equal to limit: 1",
                None,
                "Average value 2.0 in column 'b' is equal to limit: 2.0",
                None,
                "Min value 1 in column 'b' is equal to limit: 1.0",
                None,
            ],
        ],
        expected_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_is_aggr_with_count_distinct(spark: SparkSession):
    """Test count_distinct for exact cardinality (works without group_by)."""
    test_df = spark.createDataFrame(
        [
            ["val1", "data1"],
            ["val1", "data2"],  # Same first column
            ["val2", "data3"],  # Different first column
            ["val3", "data4"],
        ],
        "a: string, b: string",
    )

    checks = [
        # Global count_distinct (no group_by) - 3 distinct values in 'a', limit is 5, should pass
        is_aggr_not_greater_than("a", limit=5, aggr_type="count_distinct"),
    ]

    actual = _apply_checks(test_df, checks)

    expected = spark.createDataFrame(
        [
            ["val1", "data1", None],
            ["val1", "data2", None],
            ["val2", "data3", None],
            ["val3", "data4", None],
        ],
        "a: string, b: string, a_count_distinct_greater_than_limit: string",
    )

    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)


def test_is_aggr_with_count_distinct_and_group_by(spark: SparkSession):
    """Test that count_distinct with group_by works using two-stage aggregation."""
    test_df = spark.createDataFrame(
        [
            ["group1", "val1"],
            ["group1", "val1"],  # Same value
            ["group1", "val2"],  # Different value - 2 distinct
            ["group2", "val3"],
            ["group2", "val3"],  # Same value - only 1 distinct
        ],
        "a: string, b: string",
    )

    checks = [
        # group1 has 2 distinct values, should exceed limit of 1
        is_aggr_not_greater_than("b", limit=1, aggr_type="count_distinct", group_by=["a"]),
    ]

    actual = _apply_checks(test_df, checks)

    expected = spark.createDataFrame(
        [
            [
                "group1",
                "val1",
                "Distinct count value 2 in column 'b' per group of columns 'a' is greater than limit: 1",
            ],
            [
                "group1",
                "val1",
                "Distinct count value 2 in column 'b' per group of columns 'a' is greater than limit: 1",
            ],
            [
                "group1",
                "val2",
                "Distinct count value 2 in column 'b' per group of columns 'a' is greater than limit: 1",
            ],
            ["group2", "val3", None],
            ["group2", "val3", None],
        ],
        "a: string, b: string, b_count_distinct_group_by_a_greater_than_limit: string",
    )

    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)


def test_is_aggr_with_count_distinct_and_column_expression_in_group_by(spark: SparkSession):
    """Test count_distinct with Column expression (F.col) in group_by.

    This tests that the two-stage aggregation (groupBy + join) correctly handles
    Column expressions (not just string column names) in group_by.
    """
    test_df = spark.createDataFrame(
        [
            ["group1", "val1"],
            ["group1", "val2"],  # 2 distinct values in group1
            ["group2", "val3"],
            ["group2", "val3"],  # 1 distinct value in group2
        ],
        "a: string, b: string",
    )

    # Use Column expression (F.col) in group_by instead of string
    checks = [
        is_aggr_not_greater_than(
            "b",
            limit=1,
            aggr_type="count_distinct",
            group_by=[F.col("a")],  # Column expression without alias
        ),
    ]

    actual = _apply_checks(test_df, checks)

    # group1 has 2 distinct values > 1, should fail
    # group2 has 1 distinct value <= 1, should pass
    expected = spark.createDataFrame(
        [
            [
                "group1",
                "val1",
                "Distinct count value 2 in column 'b' per group of columns 'a' is greater than limit: 1",
            ],
            [
                "group1",
                "val2",
                "Distinct count value 2 in column 'b' per group of columns 'a' is greater than limit: 1",
            ],
            ["group2", "val3", None],
            ["group2", "val3", None],
        ],
        "a: string, b: string, b_count_distinct_group_by_a_greater_than_limit: string",
    )

    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)


def test_is_aggr_with_approx_count_distinct(spark: SparkSession):
    """Test approx_count_distinct for fast cardinality estimation with group_by."""
    test_df = spark.createDataFrame(
        [
            ["group1", "val1"],
            ["group1", "val1"],  # Same value
            ["group1", "val2"],  # Different value
            ["group2", "val3"],
            ["group2", "val3"],  # Same value - only 1 distinct
        ],
        "a: string, b: string",
    )

    checks = [
        # group1 has 2 distinct values, should exceed limit of 1
        is_aggr_not_greater_than("b", limit=1, aggr_type="approx_count_distinct", group_by=["a"]),
    ]

    actual = _apply_checks(test_df, checks)

    expected = spark.createDataFrame(
        [
            [
                "group1",
                "val1",
                "Approximate distinct count value 2 in column 'b' per group of columns 'a' is greater than limit: 1",
            ],
            [
                "group1",
                "val1",
                "Approximate distinct count value 2 in column 'b' per group of columns 'a' is greater than limit: 1",
            ],
            [
                "group1",
                "val2",
                "Approximate distinct count value 2 in column 'b' per group of columns 'a' is greater than limit: 1",
            ],
            ["group2", "val3", None],
            ["group2", "val3", None],
        ],
        "a: string, b: string, b_approx_count_distinct_group_by_a_greater_than_limit: string",
    )

    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)


def test_is_aggr_with_aggr_params_generic(spark: SparkSession):
    """Test aggr_params passed through to generic aggregate function (not percentile)."""
    test_df = spark.createDataFrame(
        [
            ["group1", "val1"],
            ["group1", "val1"],
            ["group1", "val2"],
            ["group1", "val3"],
            ["group2", "valA"],
            ["group2", "valA"],
        ],
        "a: string, b: string",
    )

    # Test approx_count_distinct with rsd (relative standard deviation) parameter
    # This tests the generic aggr_params pass-through (line 2313 in check_funcs.py)
    checks = [
        is_aggr_not_greater_than(
            "b",
            limit=5,
            aggr_type="approx_count_distinct",
            aggr_params={"rsd": 0.01},  # More accurate approximation (1% relative error)
            group_by=["a"],
        ),
    ]

    actual = _apply_checks(test_df, checks)

    # All rows should pass (group1 has ~3 distinct, group2 has ~1 distinct, both <= 5)
    expected = spark.createDataFrame(
        [
            ["group1", "val1", None],
            ["group1", "val1", None],
            ["group1", "val2", None],
            ["group1", "val3", None],
            ["group2", "valA", None],
            ["group2", "valA", None],
        ],
        "a: string, b: string, b_approx_count_distinct_group_by_a_greater_than_limit: string",
    )

    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)


def test_is_aggr_with_statistical_functions(spark: SparkSession):
    """Test statistical aggregate functions: stddev, variance, median."""
    test_df = spark.createDataFrame(
        [
            ["A", 10.0],
            ["A", 20.0],
            ["A", 30.0],
            ["B", 5.0],
            ["B", 5.0],
            ["B", 5.0],
        ],
        "a: string, b: double",
    )

    checks = [
        # Standard deviation check (group A stddev ~8.16, group B stddev=0, both <= 10.0)
        is_aggr_not_greater_than("b", limit=10.0, aggr_type="stddev", group_by=["a"]),
        # Variance check (group A variance ~66.67, group B variance=0, both <= 100.0)
        is_aggr_not_greater_than("b", limit=100.0, aggr_type="variance", group_by=["a"]),
        # Median check (dataset-level median 7.5, passes < 25.0)
        is_aggr_not_greater_than("b", limit=25.0, aggr_type="median"),
    ]

    actual = _apply_checks(test_df, checks)

    # All checks should pass
    expected = spark.createDataFrame(
        [
            ["A", 10.0, None, None, None],
            ["A", 20.0, None, None, None],
            ["A", 30.0, None, None, None],
            ["B", 5.0, None, None, None],
            ["B", 5.0, None, None, None],
            ["B", 5.0, None, None, None],
        ],
        "a: string, b: double, b_stddev_group_by_a_greater_than_limit: string, "
        "b_variance_group_by_a_greater_than_limit: string, b_median_greater_than_limit: string",
    )

    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)


def test_is_aggr_with_mode_function(spark: SparkSession):
    """Test mode aggregate function for detecting most common numeric value."""
    test_df = spark.createDataFrame(
        [
            # groupA: most common error code is 401 (appears 3 times)
            ["groupA", 401],
            ["groupA", 401],
            ["groupA", 401],
            ["groupA", 500],
            # groupB: most common error code is 200 (appears 2 times)
            ["groupB", 200],
            ["groupB", 200],
            ["groupB", 404],
        ],
        "a: string, b: int",
    )

    # Check that the most common error code value doesn't exceed threshold
    checks = [
        is_aggr_not_greater_than("b", limit=400, aggr_type="mode", group_by=["a"]),
    ]

    actual = _apply_checks(test_df, checks)

    # groupA should fail (mode=401 > limit=400), groupB should pass (mode=200 <= limit=400)
    expected = spark.createDataFrame(
        [
            ["groupA", 401, "Mode value 401 in column 'b' per group of columns 'a' is greater than limit: 400"],
            ["groupA", 401, "Mode value 401 in column 'b' per group of columns 'a' is greater than limit: 400"],
            ["groupA", 401, "Mode value 401 in column 'b' per group of columns 'a' is greater than limit: 400"],
            ["groupA", 500, "Mode value 401 in column 'b' per group of columns 'a' is greater than limit: 400"],
            ["groupB", 200, None],
            ["groupB", 200, None],
            ["groupB", 404, None],
        ],
        "a: string, b: int, b_mode_group_by_a_greater_than_limit: string",
    )

    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)


def test_is_aggr_with_percentile_functions(spark: SparkSession):
    """Test percentile and approx_percentile with aggr_params."""
    test_df = spark.createDataFrame(
        [(f"row{i}", float(i)) for i in range(1, 101)],
        "a: string, b: double",
    )

    checks = [
        # P95 should be around 95.95 (dataset-level), all pass (< 100.0)
        is_aggr_not_greater_than("b", limit=100.0, aggr_type="percentile", aggr_params={"percentile": 0.95}),
        # P99 with approx_percentile should be around 99 (dataset-level), all pass (< 100.0)
        is_aggr_not_greater_than("b", limit=100.0, aggr_type="approx_percentile", aggr_params={"percentile": 0.99}),
        # P50 (median) should be around 50.5 (dataset-level), all pass (>= 40.0)
        is_aggr_not_less_than(
            "b", limit=40.0, aggr_type="approx_percentile", aggr_params={"percentile": 0.50, "accuracy": 100}
        ),
    ]

    actual = _apply_checks(test_df, checks)

    # All checks should pass (P95 ~95 < 100, P99 ~99 < 100, P50 ~50 >= 40)
    expected = spark.createDataFrame(
        [(f"row{i}", float(i), None, None, None) for i in range(1, 101)],
        "a: string, b: double, b_percentile_greater_than_limit: string, "
        "b_approx_percentile_greater_than_limit: string, b_approx_percentile_less_than_limit: string",
    )

    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)


def test_is_aggr_message_includes_params(spark: SparkSession):
    """Test that violation messages include aggr_params for differentiation."""
    test_df = spark.createDataFrame(
        [("group1", 100.0), ("group1", 200.0), ("group1", 300.0)],
        "a: string, b: double",
    )

    # Create a check that will fail - P50 is 200, limit is 100
    checks = [
        is_aggr_not_greater_than(
            "b", limit=100.0, aggr_type="percentile", aggr_params={"percentile": 0.5}, group_by=["a"]
        ),
    ]

    actual = _apply_checks(test_df, checks)

    # Verify message includes the percentile parameter
    # Column name includes group_by info: b_percentile_group_by_a_greater_than_limit
    messages = [row.b_percentile_group_by_a_greater_than_limit for row in actual.collect()]
    assert all(msg is not None for msg in messages), "All rows should have violation messages"
    assert all("percentile=0.5" in msg for msg in messages), "Message should include percentile parameter"
    assert all("Percentile (percentile=0.5) value" in msg for msg in messages), "Message should have correct format"


def test_is_aggr_percentile_missing_params(spark: SparkSession):
    """Test that percentile functions require percentile parameter."""
    test_df = spark.createDataFrame([(1, 10.0)], "id: int, value: double")

    # Should raise error when percentile param is missing
    with pytest.raises(MissingParameterError, match="percentile.*requires aggr_params"):
        _, apply_fn = is_aggr_not_greater_than("value", limit=100.0, aggr_type="percentile")
        apply_fn(test_df)


def test_is_aggr_percentile_invalid_params_caught_by_spark(spark: SparkSession):
    """Test that invalid aggr_params are caught by Spark at runtime.

    This verifies our permissive strategy: we don't validate extra parameters,
    but Spark will raise an error for truly invalid ones.
    """
    test_df = spark.createDataFrame([(1, 10.0), (2, 20.0)], "id: int, value: double")

    # Pass an invalid parameter type (string instead of float for percentile)
    # Spark should raise an error when the DataFrame is evaluated
    with pytest.raises(Exception):  # Spark will raise AnalysisException or similar
        _, apply_fn = is_aggr_not_greater_than(
            "value",
            limit=100.0,
            aggr_type="approx_percentile",
            aggr_params={"percentile": "invalid_string"},  # Invalid: should be float
        )
        result_df = apply_fn(test_df)
        result_df.collect()  # Force evaluation to trigger Spark error


def test_is_aggr_with_invalid_parameter_name(spark: SparkSession):
    """Test that invalid parameter names in aggr_params raise clear errors."""
    test_df = spark.createDataFrame([(1, 10.0), (2, 20.0)], "id: int, value: double")

    # Pass an invalid parameter name - should raise InvalidParameterError with context
    with pytest.raises(InvalidParameterError, match="Failed to build 'approx_percentile' expression"):
        _, apply_fn = is_aggr_not_greater_than(
            "value",
            limit=100.0,
            aggr_type="approx_percentile",
            aggr_params={"percentile": 0.95, "invalid_param": 1},  # Invalid param name
        )
        apply_fn(test_df)


def test_is_aggr_with_invalid_aggregate_function(spark: SparkSession):
    """Test that invalid aggregate function names raise clear errors."""
    test_df = spark.createDataFrame([(1, 10)], "id: int, value: int")

    # Non-existent function should raise error
    with pytest.raises(InvalidParameterError, match="not found in pyspark.sql.functions"):
        _, apply_fn = is_aggr_not_greater_than("value", limit=100, aggr_type="nonexistent_function")
        apply_fn(test_df)


def test_is_aggr_with_collect_list_fails(spark: SparkSession):
    """Test that collect_list (returns array) fails with clear error message - no group_by."""
    test_df = spark.createDataFrame([(1, 10), (2, 20)], "id: int, value: int")

    # collect_list returns array which cannot be compared to numeric limit
    with pytest.raises(InvalidParameterError, match="array.*cannot be compared"):
        _, apply_fn = is_aggr_not_greater_than("value", limit=100, aggr_type="collect_list")
        apply_fn(test_df)


def test_is_aggr_with_collect_list_fails_with_group_by(spark: SparkSession):
    """Test that collect_list with group_by also fails with clear error message - bug fix verification."""
    test_df = spark.createDataFrame(
        [("A", 10), ("A", 20), ("B", 30)],
        "category: string, value: int",
    )

    # This is the bug fix: collect_list with group_by should fail gracefully, not with cryptic Spark error
    with pytest.raises(InvalidParameterError, match="array.*cannot be compared"):
        _, apply_fn = is_aggr_not_greater_than("value", limit=100, aggr_type="collect_list", group_by=["category"])
        apply_fn(test_df)


def test_is_aggr_non_curated_aggregate_with_warning(spark: SparkSession):
    """Test that non-curated (built-in but not in curated list) aggregates work but produce warning."""
    test_df = spark.createDataFrame(
        [("A", 10), ("B", 20), ("C", 30)],
        "a: string, b: int",
    )

    # Use a valid aggregate that's not in curated list (e.g., any_value)
    # any_value returns one arbitrary non-null value (likely 10, 20, or 30), all < 100
    with pytest.warns(UserWarning, match="non-curated.*any_value"):
        checks = [is_aggr_not_greater_than("b", limit=100, aggr_type="any_value")]
        actual = _apply_checks(test_df, checks)

    # Should still work - any_value returns an arbitrary value, all should pass (< 100)
    expected = spark.createDataFrame(
        [("A", 10, None), ("B", 20, None), ("C", 30, None)],
        "a: string, b: int, b_any_value_greater_than_limit: string",
    )

    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)


def test_dataset_compare(spark: SparkSession, set_utc_timezone):
    schema = "id1 long, id2 long, name string, dt date, ts timestamp, score float, likes bigint, active boolean"

    df = spark.createDataFrame(
        [
            [1, 1, "Grzegorz", datetime(2017, 1, 1), datetime(2018, 1, 1, 12, 34, 56), 26.7, 123234234345, True],
            # extra row
            [2, 1, "Tim", datetime(2018, 1, 1), datetime(2018, 2, 1, 12, 34, 56), 36.7, 54545, True],
            [3, 1, "Mike", datetime(2019, 1, 1), datetime(2018, 3, 1, 12, 34, 56), 46.7, 5667888989, False],
        ],
        schema,
    )

    df_ref = spark.createDataFrame(
        [
            # diff in dt and score
            [1, 1, "Grzegorz", datetime(2018, 1, 1), datetime(2018, 1, 1, 12, 34, 56), 26.9, 123234234345, True],
            # no diff
            [3, 1, "Mike", datetime(2019, 1, 1), datetime(2018, 3, 1, 12, 34, 56), 46.7, 5667888989, False],
            # missing record
            [2, 2, "Timmy", datetime(2018, 1, 1), datetime(2018, 2, 1, 12, 34, 56), 36.7, 8754857845, True],
        ],
        schema,
    )

    columns = ["id1", "id2"]

    condition, apply = compare_datasets(
        columns=columns,
        ref_columns=columns,
        ref_df_name="df_ref",
        check_missing_records=False,
    )

    actual: DataFrame = apply(df, spark, {"df_ref": df_ref})
    actual = actual.select(*df.columns, condition)

    compare_status_column = get_column_name_or_alias(condition)
    expected_schema = f"{schema}, {compare_status_column} string"

    expected = spark.createDataFrame(
        [
            {
                "id1": 1,
                "id2": 1,
                "name": "Grzegorz",
                "dt": datetime(2017, 1, 1),
                "ts": datetime(2018, 1, 1, 12, 34, 56),
                "score": 26.7,
                "likes": 123234234345,
                "active": True,
                compare_status_column: json.dumps(
                    {
                        "row_missing": False,
                        "row_extra": False,
                        "changed": {
                            "dt": {"df": "2017-01-01", "ref": "2018-01-01"},
                            "score": {"df": "26.7", "ref": "26.9"},
                        },
                    },
                    separators=(',', ':'),
                ),
            },
            {
                "id1": 2,
                "id2": 1,
                "name": "Tim",
                "dt": datetime(2018, 1, 1),
                "ts": datetime(2018, 2, 1, 12, 34, 56),
                "score": 36.7,
                "likes": 54545,
                "active": True,
                compare_status_column: json.dumps(
                    {
                        "row_missing": False,
                        "row_extra": True,
                        "changed": {
                            "name": {"df": "Tim"},
                            "dt": {"df": "2018-01-01"},
                            "ts": {"df": "2018-02-01 12:34:56"},
                            "score": {"df": "36.7"},
                            "likes": {"df": "54545"},
                            "active": {"df": "true"},
                        },
                    },
                    separators=(',', ':'),
                ),
            },
            {
                "id1": 3,
                "id2": 1,
                "name": "Mike",
                "dt": datetime(2019, 1, 1),
                "ts": datetime(2018, 3, 1, 12, 34, 56),
                "score": 46.7,
                "likes": 5667888989,
                "active": False,
                compare_status_column: None,
            },
        ],
        expected_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)


def test_compare_datasets_with_diff_col_names_and_check_missing(spark: SparkSession, set_utc_timezone):
    schema = "id1 long, id2 long, name string, dt date, ts timestamp, score float, likes bigint, active boolean"

    df = spark.createDataFrame(
        [
            [1, 1, "Grzegorz", datetime(2017, 1, 1), datetime(2018, 1, 1, 12, 34, 56), 26.7, 123234234345, True],
            # extra row
            [2, 1, "Tim", datetime(2018, 1, 1), datetime(2018, 2, 1, 12, 34, 56), 36.7, 54545, True],
            [3, 1, "Mike", datetime(2019, 1, 1), datetime(2018, 3, 1, 12, 34, 56), 46.7, 5667888989, False],
        ],
        schema,
    )

    schema_ref = "id1_ref long, id2_ref long, name string, dt date, ts timestamp, score float, likes bigint, active boolean, extra string"
    df_ref = spark.createDataFrame(
        [
            # diff in dt and score
            [1, 1, "Grzegorz", datetime(2018, 1, 1), datetime(2018, 1, 1, 12, 34, 56), 26.9, 123234234345, True, "a"],
            # no diff
            [3, 1, "Mike", datetime(2019, 1, 1), datetime(2018, 3, 1, 12, 34, 56), 1.7, 5667888989, False, "b"],
            # missing record
            [2, 2, "Timmy", datetime(2018, 1, 1), datetime(2018, 2, 1, 12, 34, 56), 36.7, 8754857845, True, "c"],
        ],
        schema_ref,
    )

    columns = [F.col("id1"), F.col("id2")]
    # ref columns having different name than columns
    ref_columns = [F.col("id1_ref"), F.col("id2_ref")]

    condition, apply = compare_datasets(
        columns=columns,
        ref_columns=ref_columns,
        ref_df_name="df_ref",
        check_missing_records=True,
        exclude_columns=[F.col("score")],
    )

    actual: DataFrame = apply(df, spark, {"df_ref": df_ref})
    actual = actual.select(*df.columns, condition)

    compare_status_column = get_column_name_or_alias(condition)
    expected_schema = f"{schema}, {compare_status_column} string"

    expected = spark.createDataFrame(
        [
            {
                "id1": 1,
                "id2": 1,
                "name": "Grzegorz",
                "dt": datetime(2017, 1, 1),
                "ts": datetime(2018, 1, 1, 12, 34, 56),
                "score": 26.7,
                "likes": 123234234345,
                "active": True,
                compare_status_column: json.dumps(
                    {
                        "row_missing": False,
                        "row_extra": False,
                        "changed": {
                            "dt": {"df": "2017-01-01", "ref": "2018-01-01"},
                        },
                    },
                    separators=(',', ':'),
                ),
            },
            {
                "id1": 3,
                "id2": 1,
                "name": "Mike",
                "dt": datetime(2019, 1, 1),
                "ts": datetime(2018, 3, 1, 12, 34, 56),
                "score": 46.7,
                "likes": 5667888989,
                "active": False,
                compare_status_column: None,
            },
            {
                "id1": 2,
                "id2": 2,
                "name": None,
                "dt": None,
                "ts": None,
                "score": None,
                "likes": None,
                "active": None,
                compare_status_column: json.dumps(
                    {
                        "row_missing": True,
                        "row_extra": False,
                        "changed": {
                            "name": {"ref": "Timmy"},
                            "dt": {"ref": "2018-01-01"},
                            "ts": {"ref": "2018-02-01 12:34:56"},
                            "likes": {"ref": "8754857845"},
                            "active": {"ref": "true"},
                        },
                    },
                    separators=(',', ':'),
                ),
            },
            {
                "id1": 2,
                "id2": 1,
                "name": "Tim",
                "dt": datetime(2018, 1, 1),
                "ts": datetime(2018, 2, 1, 12, 34, 56),
                "score": 36.7,
                "likes": 54545,
                "active": True,
                compare_status_column: json.dumps(
                    {
                        "row_missing": False,
                        "row_extra": True,
                        "changed": {
                            "name": {"df": "Tim"},
                            "dt": {"df": "2018-01-01"},
                            "ts": {"df": "2018-02-01 12:34:56"},
                            "likes": {"df": "54545"},
                            "active": {"df": "true"},
                        },
                    },
                    separators=(',', ':'),
                ),
            },
        ],
        expected_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)


def test_dataset_compare_ref_as_table_and_skip_map_col(spark: SparkSession, set_utc_timezone, make_schema, make_random):
    schema = (
        "id1 long, id2 long, name string, dt date, ts timestamp, score float, likes bigint, active boolean, "
        "extra string, extra_map: map<string, string>"
    )

    df = spark.createDataFrame(
        [
            [
                1,
                1,
                "Pawel",
                datetime(2017, 1, 1),
                datetime(2018, 1, 1, 12, 34, 56),
                26.7,
                123234234345,
                True,
                "a",
                {"key1": "value1"},
            ],
            # extra row
            [
                2,
                1,
                "Tom",
                datetime(2018, 1, 1),
                datetime(2018, 2, 1, 12, 34, 56),
                36.7,
                54545,
                True,
                "b",
                {"key2": "value2"},
            ],
            [
                3,
                1,
                "Mike",
                datetime(2019, 1, 1),
                datetime(2018, 3, 1, 12, 34, 56),
                46.7,
                5667888989,
                False,
                "c",
                {"key3": "value3"},
            ],
        ],
        schema,
    )

    schema_ref = (
        "id1 long, id2 long, name string, dt date, ts timestamp, score float, likes bigint, active boolean, "
        "extra_map: map<string, string>"
    )
    df_ref = spark.createDataFrame(
        [
            # diff in dt and score
            [
                1,
                1,
                "Pawel",
                datetime(2018, 1, 1),
                datetime(2018, 1, 1, 12, 34, 56),
                26.9,
                123234234345,
                True,
                {"key": "value"},
            ],
            # no diff
            [
                3,
                1,
                "Mike",
                datetime(2019, 1, 1),
                datetime(2018, 3, 1, 12, 34, 56),
                46.7,
                5667888989,
                False,
                {"key": "value"},
            ],
            # missing record
            [
                2,
                2,
                "Timmy",
                datetime(2018, 1, 1),
                datetime(2018, 2, 1, 12, 34, 56),
                36.7,
                8754857845,
                True,
                {"key": "value"},
            ],
        ],
        schema_ref,
    )

    catalog_name = TEST_CATALOG
    ref_table_schema = make_schema(catalog_name=catalog_name)
    ref_table = f"{catalog_name}.{ref_table_schema.name}.{make_random(10).lower()}"
    df_ref.write.saveAsTable(ref_table)

    columns = ["id1", "id2"]

    condition, apply = compare_datasets(
        columns=columns,
        ref_columns=columns,
        ref_table=ref_table,
        check_missing_records=False,
    )

    actual: DataFrame = apply(df, spark, {})
    actual = actual.select(*df.columns, condition)

    compare_status_column = get_column_name_or_alias(condition)
    expected_schema = f"{schema}, {compare_status_column} string"

    expected = spark.createDataFrame(
        [
            {
                "id1": 1,
                "id2": 1,
                "name": "Pawel",
                "dt": datetime(2017, 1, 1),
                "ts": datetime(2018, 1, 1, 12, 34, 56),
                "score": 26.7,
                "likes": 123234234345,
                "active": True,
                "extra": "a",
                "extra_map": {"key1": "value1"},
                compare_status_column: json.dumps(
                    {
                        "row_missing": False,
                        "row_extra": False,
                        "changed": {
                            "dt": {"df": "2017-01-01", "ref": "2018-01-01"},
                            "score": {"df": "26.7", "ref": "26.9"},
                        },
                    },
                    separators=(',', ':'),
                ),
            },
            {
                "id1": 2,
                "id2": 1,
                "name": "Tom",
                "dt": datetime(2018, 1, 1),
                "ts": datetime(2018, 2, 1, 12, 34, 56),
                "score": 36.7,
                "likes": 54545,
                "active": True,
                "extra": "b",
                "extra_map": {"key2": "value2"},
                compare_status_column: json.dumps(
                    {
                        "row_missing": False,
                        "row_extra": True,
                        "changed": {
                            "name": {"df": "Tom"},
                            "dt": {"df": "2018-01-01"},
                            "ts": {"df": "2018-02-01 12:34:56"},
                            "score": {"df": "36.7"},
                            "likes": {"df": "54545"},
                            "active": {"df": "true"},
                        },
                    },
                    separators=(',', ':'),
                ),
            },
            {
                "id1": 3,
                "id2": 1,
                "name": "Mike",
                "dt": datetime(2019, 1, 1),
                "ts": datetime(2018, 3, 1, 12, 34, 56),
                "score": 46.7,
                "likes": 5667888989,
                "active": False,
                "extra": "c",
                "extra_map": {"key3": "value3"},
                compare_status_column: None,
            },
        ],
        expected_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_dataset_compare_with_no_columns_to_compare_and_check_missing(spark: SparkSession):
    schema = "id long"

    df = spark.createDataFrame([[1]], schema)
    df_ref = spark.createDataFrame([[1]], schema)
    columns = ["id"]

    condition, apply = compare_datasets(
        columns=columns,
        ref_columns=columns,
        ref_df_name="df_ref",
        check_missing_records=True,
    )

    actual: DataFrame = apply(df, spark, {"df_ref": df_ref})
    actual = actual.select(*df.columns, condition)

    compare_status_column = get_column_name_or_alias(condition)
    expected_schema = f"{schema}, {compare_status_column} string"

    expected = spark.createDataFrame(
        [
            {
                "id": 1,
                compare_status_column: None,
            },
        ],
        expected_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_dataset_compare_with_empty_ref_and_check_missing(spark: SparkSession):
    schema = "id long, name string"

    df = spark.createDataFrame([[1, "Marcin"]], schema)
    df_ref = spark.createDataFrame([[None, "Marcin"]], schema)
    columns = ["id"]

    condition, apply = compare_datasets(
        columns=columns,
        ref_columns=columns,
        ref_df_name="df_ref",
        check_missing_records=True,
    )

    actual: DataFrame = apply(df, spark, {"df_ref": df_ref})
    actual = actual.select(*df.columns, condition)

    compare_status_column = get_column_name_or_alias(condition)
    expected_schema = f"{schema}, {compare_status_column} string"

    expected = spark.createDataFrame(
        [
            {
                "id": 1,
                "name": "Marcin",
                compare_status_column: json.dumps(
                    {
                        "row_missing": False,
                        "row_extra": True,
                        "changed": {"name": {"df": "Marcin"}},
                    },
                    separators=(',', ':'),
                ),
            },
            {
                "id": None,
                "name": None,
                compare_status_column: json.dumps(
                    {
                        # We cannot reliably determine whether a row is missing or extra if all keys are null on both sides
                        "row_missing": True,
                        "row_extra": True,
                        "changed": {"name": {"ref": "Marcin"}},
                    },
                    separators=(',', ':'),
                ),
            },
        ],
        expected_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)


def test_dataset_compare_with_empty_df_and_check_missing(spark: SparkSession):
    schema = "id long, id2 long, name string"

    df = spark.createDataFrame([[None, 1, "Marcin"]], schema)
    df_ref = spark.createDataFrame([[1, 1, "Marcin"]], schema)
    columns = ["id", "id2"]

    condition, apply = compare_datasets(
        columns=columns,
        ref_columns=columns,
        ref_df_name="df_ref",
        check_missing_records=True,
    )

    actual: DataFrame = apply(df, spark, {"df_ref": df_ref})
    actual = actual.select(*df.columns, condition)

    compare_status_column = get_column_name_or_alias(condition)
    expected_schema = f"{schema}, {compare_status_column} string"

    expected = spark.createDataFrame(
        [
            {
                "id": 1,
                "id2": 1,
                "name": None,
                compare_status_column: json.dumps(
                    {
                        "row_missing": True,
                        "row_extra": False,
                        "changed": {"name": {"ref": "Marcin"}},
                    },
                    separators=(',', ':'),
                ),
            },
            {
                "id": None,
                "id2": 1,
                "name": "Marcin",
                compare_status_column: json.dumps(
                    {
                        "row_missing": False,
                        "row_extra": True,
                        "changed": {"name": {"df": "Marcin"}},
                    },
                    separators=(',', ':'),
                ),
            },
        ],
        expected_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)


def test_dataset_compare_with_empty_df_and_ref(spark: SparkSession):
    schema = "id long, name: string"

    df = spark.createDataFrame([[None, "Marcin"]], schema)
    df_ref = spark.createDataFrame([[None, "Marcin"]], schema)
    columns = ["id"]

    condition, apply = compare_datasets(
        columns=columns,
        ref_columns=columns,
        ref_df_name="df_ref",
        check_missing_records=True,
    )

    actual: DataFrame = apply(df, spark, {"df_ref": df_ref})
    actual = actual.select(*df.columns, condition)

    compare_status_column = get_column_name_or_alias(condition)
    expected_schema = f"{schema}, {compare_status_column} string"

    expected = spark.createDataFrame(
        [
            {
                "id": None,
                "name": "Marcin",
                compare_status_column: json.dumps(
                    {
                        # We cannot reliably determine whether a row is missing or extra if all keys are null on both sides
                        "row_missing": True,
                        "row_extra": True,
                        "changed": {},
                    },
                    separators=(',', ':'),
                ),
            },
        ],
        expected_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_dataset_compare_unsorted_df_columns(spark: SparkSession):
    schema = "id1 long, id2 long, name string"

    df = spark.createDataFrame(
        [
            [1, 1, None],
            [1, None, None],
        ],
        schema,
    )

    schema_ref = "name string, id1 long, id2 long"

    df_ref = spark.createDataFrame(
        [
            [None, 1, 1],
            [None, 1, None],
        ],
        schema_ref,
    )

    columns = ["id1", "id2"]

    condition, apply = compare_datasets(
        columns=columns,
        ref_columns=columns,  # columns are matched by position, so the order of columns must align exactly
        ref_df_name="df_ref",
        check_missing_records=True,
    )

    actual: DataFrame = apply(df, spark, {"df_ref": df_ref})
    actual = actual.select(*df.columns, condition)

    compare_status_column = get_column_name_or_alias(condition)
    expected_schema = f"{schema}, {compare_status_column} string"

    expected = spark.createDataFrame(
        [
            {"id1": 1, "id2": 1, "name": None, compare_status_column: None},
            {"id1": 1, "id2": None, "name": None, compare_status_column: None},
        ],
        expected_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)


def test_compare_dataset_disabled_null_safe_row_matching(spark: SparkSession):
    schema = "id1 long, id2 long, name string"

    df = spark.createDataFrame(
        [
            [1, 1, None],
            [2, 2, 2],
            [3, 3, None],
            [1, None, "val1"],
        ],
        schema,
    )

    df_ref = spark.createDataFrame(
        [
            [1, 1, None],
            [2, 2, None],
            [3, 3, 3],
            [1, None, "val2"],
        ],
        schema,
    )

    columns = ["id1", "id2"]

    condition, apply = compare_datasets(
        columns=columns,
        ref_columns=columns,  # columns are matched by position, so the order of columns must align exactly
        ref_df_name="df_ref",
        check_missing_records=True,
        null_safe_row_matching=False,
        null_safe_column_value_matching=True,
    )

    actual: DataFrame = apply(df, spark, {"df_ref": df_ref})
    actual = actual.select(*df.columns, condition)

    compare_status_column = get_column_name_or_alias(condition)
    expected_schema = f"{schema}, {compare_status_column} string"

    expected = spark.createDataFrame(
        [
            {
                "id1": 1,
                "id2": None,
                "name": None,
                compare_status_column: json.dumps(
                    {
                        "row_missing": True,
                        "row_extra": False,
                        "changed": {"name": {"ref": "val2"}},
                    },
                    separators=(',', ':'),
                ),
            },
            {
                "id1": 2,
                "id2": 2,
                "name": 2,
                compare_status_column: json.dumps(
                    {
                        "row_missing": False,
                        "row_extra": False,
                        "changed": {"name": {"df": "2"}},
                    },
                    separators=(',', ':'),
                ),
            },
            {
                "id1": 3,
                "id2": 3,
                "name": None,
                compare_status_column: json.dumps(
                    {
                        "row_missing": False,
                        "row_extra": False,
                        "changed": {"name": {"ref": "3"}},
                    },
                    separators=(',', ':'),
                ),
            },
            {
                "id1": 1,
                "id2": None,
                "name": "val1",
                compare_status_column: json.dumps(
                    {
                        "row_missing": False,
                        "row_extra": True,
                        "changed": {"name": {"df": "val1"}},
                    },
                    separators=(',', ':'),
                ),
            },
            {"id1": 1, "id2": 1, "name": None, compare_status_column: None},
        ],
        expected_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)


def test_compare_dataset_disabled_null_safe_column_value_matching(spark: SparkSession):
    schema = "id long, name string"

    df = spark.createDataFrame(
        [
            [1, "val1"],
            [2, "val2"],
            [3, None],
        ],
        schema,
    )

    df_ref = spark.createDataFrame(
        [
            [1, None],  # should not show any diff in the name
            [2, "val2"],
            [3, "val3"],
        ],
        schema,
    )

    columns = ["id"]

    condition, apply = compare_datasets(
        columns=columns,
        ref_columns=columns,
        ref_df_name="df_ref",
        check_missing_records=True,
        null_safe_column_value_matching=False,
    )

    actual: DataFrame = apply(df, spark, {"df_ref": df_ref})
    actual = actual.select(*df.columns, condition)

    compare_status_column = get_column_name_or_alias(condition)
    expected_schema = f"{schema}, {compare_status_column} string"

    expected = spark.createDataFrame(
        [
            {
                "id": 1,
                "name": "val1",
                compare_status_column: None,
            },
            {
                "id": 2,
                "name": "val2",
                compare_status_column: None,
            },
            {
                "id": 3,
                "name": None,
                compare_status_column: None,
            },
        ],
        expected_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)


def test_is_data_fresh_per_time_window(spark: SparkSession, set_utc_timezone):
    schedule_schema = "a timestamp, b long"
    data_time = datetime(second=0, minute=59, hour=9, day=31, month=7, year=2025)
    # 2 records in first 2 min window: [base_time - 0, -1]
    first_window = [data_time - timedelta(minutes=i) for i in range(0, 2)]
    # 1 records in second 2 min window: [base_time - 2]
    second_window = [data_time - timedelta(minutes=i) for i in range(2, 3)]
    # 4 records in third 2 min window: [base_time - 4, -4.5, -5, -5.5]
    third_window = list(
        itertools.chain.from_iterable(
            [
                (data_time - timedelta(minutes=i, seconds=-30), data_time - timedelta(minutes=i, seconds=0))
                for i in range(4, 6)
            ]
        )
    )
    # 1 record in last window: [base_time - 6] which is not in the lookback window time range
    last_window = [data_time - timedelta(minutes=i) for i in range(6, 7)]
    timestamps = first_window + second_window + third_window + last_window
    values = list(range(1, len(timestamps) + 1))
    data = list(zip(timestamps, values))
    df = spark.createDataFrame(data, schedule_schema)

    condition, apply_method = is_data_fresh_per_time_window(
        column="a",
        window_minutes=2,
        min_records_per_window=2,
        lookback_windows=3,  # cover the whole period
        curr_timestamp=F.lit(data_time + timedelta(minutes=1)),  # 2023-01-01 00:01:00
        row_filter="b > 1",
    )
    actual: DataFrame = apply_method(df)

    actual = actual.select('a', 'b', condition)
    condition_column = get_column_name_or_alias(condition)
    expected_schema = f"{schedule_schema}, {condition_column} string"

    expected = spark.createDataFrame(
        [
            {
                "a": datetime(2025, 7, 31, 9, 59, 0),
                "b": 1,
                condition_column: None,
            },
            {
                "a": datetime(2025, 7, 31, 9, 58, 0),
                "b": 2,
                # this is because we filtered the record 2025-07-31 09:59:00
                condition_column: "Data arrival completeness check failed: only 1 records found in 2-minute interval starting at 2025-07-31 09:58:00 and ending at 2025-07-31 10:00:00, expected at least 2 records",
            },
            {
                "a": datetime(2025, 7, 31, 9, 57, 0),
                "b": 3,
                condition_column: "Data arrival completeness check failed: only 1 records found in 2-minute interval starting at 2025-07-31 09:56:00 and ending at 2025-07-31 09:58:00, expected at least 2 records",
            },
            {
                "a": datetime(2025, 7, 31, 9, 55, 30),
                "b": 4,
                condition_column: None,
            },
            {
                "a": datetime(2025, 7, 31, 9, 55, 0),
                "b": 5,
                condition_column: None,
            },
            {
                "a": datetime(2025, 7, 31, 9, 54, 30),
                "b": 6,
                condition_column: None,
            },
            {
                "a": datetime(2025, 7, 31, 9, 54, 0),
                "b": 7,
                condition_column: None,
            },
            {
                "a": datetime(2025, 7, 31, 9, 53, 0),
                "b": 8,
                condition_column: None,
            },
        ],
        expected_schema,
    )
    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)


def test_is_data_fresh_per_time_window_with_cutt_off(spark: SparkSession, set_utc_timezone):
    schedule_schema = "a timestamp, b long"
    data_time = datetime(2023, 1, 1, 0, 0, 0)
    # 2 records in first 2 min window: [base_time - 0, -1]
    first_window = [data_time - timedelta(minutes=i) for i in range(0, 2)]
    # 1 records in second 2 min window: [base_time - 2]
    second_window = [data_time - timedelta(minutes=i) for i in range(2, 3)]
    # 4 records in third 2 min window: [base_time - 4, -4.5, -5, -5.5]
    third_window = list(
        itertools.chain.from_iterable(
            [
                (data_time - timedelta(minutes=i, seconds=-30), data_time - timedelta(minutes=i, seconds=0))
                for i in range(4, 6)
            ]
        )
    )
    timestamps = first_window + second_window + third_window
    values = list(range(1, len(timestamps) + 1))
    data = list(zip(timestamps, values))
    df = spark.createDataFrame(data, schedule_schema)

    condition, apply_method = is_data_fresh_per_time_window(
        column="a",
        window_minutes=3,
        min_records_per_window=5,
        lookback_windows=1,  # only look back one window (3 minutes), until 2023-01-01 00:02:00
        curr_timestamp=F.lit(data_time + timedelta(minutes=2)),
    )

    actual: DataFrame = apply_method(df)
    actual = actual.select('a', 'b', condition)
    condition_column = get_column_name_or_alias(condition)
    expected_schema = f"{schedule_schema}, {condition_column} string"

    expected = spark.createDataFrame(
        [
            {
                "a": data_time,
                "b": 1,
                condition_column: "Data arrival completeness check failed: only 1 records found in 3-minute interval starting at 2023-01-01 00:00:00 and ending at 2023-01-01 00:03:00, expected at least 5 records",
            },
            {
                "a": data_time - timedelta(minutes=1),
                "b": 2,
                condition_column: "Data arrival completeness check failed: only 1 records found in 3-minute interval starting at 2022-12-31 23:57:00 and ending at 2023-01-01 00:00:00, expected at least 5 records",
            },
            {
                "a": data_time - timedelta(minutes=2),
                "b": 3,
                condition_column: None,
            },
            {
                "a": data_time - timedelta(minutes=4, seconds=-30),
                "b": 4,
                condition_column: None,
            },
            {
                "a": data_time - timedelta(minutes=4, seconds=0),
                "b": 5,
                condition_column: None,
            },
            {
                "a": data_time - timedelta(minutes=5, seconds=-30),
                "b": 6,
                condition_column: None,
            },
            {
                "a": data_time - timedelta(minutes=5, seconds=0),
                "b": 7,
                condition_column: None,
            },
        ],
        expected_schema,
    )
    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)


def test_is_data_fresh_per_time_window_check_entire_dataset(spark: SparkSession, set_utc_timezone):
    schedule_schema = "a timestamp, b long"
    data_time = datetime(2023, 1, 1, 0, 0, 0)
    # 2 records in first 2 min window: [base_time - 0, -1]
    first_window = [data_time - timedelta(minutes=i) for i in range(0, 2)]
    # 1 records in second 2 min window: [base_time - 2]
    second_window = [data_time - timedelta(minutes=i) for i in range(2, 3)]
    # 4 records in third 2 min window: [base_time - 4, -4.5, -5, -5.5]
    third_window = list(
        itertools.chain.from_iterable(
            [
                (data_time - timedelta(minutes=i, seconds=-30), data_time - timedelta(minutes=i, seconds=0))
                for i in range(4, 6)
            ]
        )
    )
    timestamps = first_window + second_window + third_window
    values = list(range(1, len(timestamps) + 1))
    data = list(zip(timestamps, values))
    df = spark.createDataFrame(data, schedule_schema)

    condition, apply_method = is_data_fresh_per_time_window(
        column="a",
        window_minutes=3,
        min_records_per_window=4,
        # no lookback, use the entire data
        curr_timestamp=F.lit(data_time + timedelta(minutes=1)),  # 2023-01-01 00:01:00
    )

    actual: DataFrame = apply_method(df)
    actual = actual.select('a', 'b', condition)
    condition_column = get_column_name_or_alias(condition)
    expected_schema = f"{schedule_schema}, {condition_column} string"

    expected = spark.createDataFrame(
        [
            {
                "a": data_time,
                "b": 1,
                condition_column: "Data arrival completeness check failed: only 1 records found in 3-minute interval starting at 2023-01-01 00:00:00 and ending at 2023-01-01 00:03:00, expected at least 4 records",
            },
            {
                "a": data_time - timedelta(minutes=1),
                "b": 2,
                condition_column: "Data arrival completeness check failed: only 2 records found in 3-minute interval starting at 2022-12-31 23:57:00 and ending at 2023-01-01 00:00:00, expected at least 4 records",
            },
            {
                "a": data_time - timedelta(minutes=2),
                "b": 3,
                condition_column: "Data arrival completeness check failed: only 2 records found in 3-minute interval starting at 2022-12-31 23:57:00 and ending at 2023-01-01 00:00:00, expected at least 4 records",
            },
            {
                "a": data_time - timedelta(minutes=4, seconds=-30),
                "b": 4,
                condition_column: None,
            },
            {
                "a": data_time - timedelta(minutes=4, seconds=0),
                "b": 5,
                condition_column: None,
            },
            {
                "a": data_time - timedelta(minutes=5, seconds=-30),
                "b": 6,
                condition_column: None,
            },
            {
                "a": data_time - timedelta(minutes=5, seconds=0),
                "b": 7,
                condition_column: None,
            },
        ],
        expected_schema,
    )
    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)


def test_has_valid_schema_invalid_schema_exceptions():
    expected_schema = "INVALID_SCHEMA"
    with pytest.raises(InvalidParameterError, match=f"Invalid schema string '{expected_schema}'.*"):
        has_valid_schema(expected_schema=expected_schema)


def test_has_valid_schema_permissive_mode_extra_column(spark):
    test_df = spark.createDataFrame(
        [
            ["str1", 1, 100.0],
            ["str2", 2, 200.0],
            ["str3", 3, 300.0],
        ],
        "a string, b int, c double",
    )

    expected_schema = "a string, b int"  # Expected schema without extra column
    condition, apply_method = has_valid_schema(expected_schema)
    actual_apply_df = apply_method(test_df)
    actual_condition_df = actual_apply_df.select("a", "b", "c", condition)

    expected_condition_df = spark.createDataFrame(
        [
            ["str1", 1, 100.0, None],
            ["str2", 2, 200.0, None],
            ["str3", 3, 300.0, None],
        ],
        "a string, b int, c double, has_invalid_schema string",
    )
    assert_df_equality(actual_condition_df, expected_condition_df, ignore_nullable=True)


def test_has_valid_schema_permissive_mode_type_widening(spark):
    test_df = spark.createDataFrame(
        [
            [
                "str1",
                1,
                100.0,
                1.375,
                date(2025, 1, 1),
                datetime(2025, 1, 1, 0, 0, 0),
                True,
                b"hello world!",
                ["a", "b"],
                {"key1": 1, "key2": 1},
                {"field1": "val1", "field2": 1, "field3": date(2025, 1, 1)},
                '{"key1": 1.0}',
                "invalid_str1",
            ],
            [
                "str2",
                2,
                200.0,
                2.5,
                date(2025, 2, 1),
                datetime(2025, 2, 1, 0, 0, 0),
                True,
                b"hello world!",
                ["c", "d"],
                {"key1": 2, "key2": 2},
                {"field1": "val2", "field2": 2, "field3": date(2025, 2, 1)},
                '{"key2": 1}',
                "invalid_str2",
            ],
            [
                "str3",
                3,
                300.0,
                3.875,
                date(2025, 3, 1),
                datetime(2025, 3, 1, 0, 0, 0),
                True,
                b"hello world!",
                ["e", "f"],
                {"key1": 3, "key2": 3},
                {"field1": "val3", "field2": 3, "field3": date(2025, 3, 1)},
                '{"key3": "val3"}',
                "invalid_str3",
            ],
        ],
        schema="a string, b int, c float, d double, e date, f timestamp_ntz, g boolean, h binary, i array<string>, j map<string, short>, k struct<field1: string, field2: int, field3: date>, l string, invalid_col string",
    )
    test_df = test_df.withColumn("l", F.parse_json("l"))

    expected_schema = "a varchar(10), b long, c decimal(5, 1), d float, e timestamp, f timestamp, g boolean, h binary, i array<char(1)>, j map<varchar(10), int>, k struct<field1: varchar(5), field2: byte, field3: timestamp>, l map<string, string>, invalid_col int"
    condition, apply_method = has_valid_schema(expected_schema)
    actual_apply_df = apply_method(test_df)
    actual_condition_df = actual_apply_df.select(
        "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "invalid_col", condition
    )

    expected_condition_df = spark.createDataFrame(
        [
            [
                "str1",
                1,
                100.0,
                1.375,
                date(2025, 1, 1),
                datetime(2025, 1, 1, 0, 0, 0),
                True,
                b"hello world!",
                ["a", "b"],
                {"key1": 1, "key2": 1},
                {"field1": "val1", "field2": 1, "field3": date(2025, 1, 1)},
                '{"key1": 1.0}',
                "invalid_str1",
                "Schema validation failed: Column 'invalid_col' has incompatible type, expected 'integer', got 'string'",
            ],
            [
                "str2",
                2,
                200.0,
                2.5,
                date(2025, 2, 1),
                datetime(2025, 2, 1, 0, 0, 0),
                True,
                b"hello world!",
                ["c", "d"],
                {"key1": 2, "key2": 2},
                {"field1": "val2", "field2": 2, "field3": date(2025, 2, 1)},
                '{"key2": 1}',
                "invalid_str2",
                "Schema validation failed: Column 'invalid_col' has incompatible type, expected 'integer', got 'string'",
            ],
            [
                "str3",
                3,
                300.0,
                3.875,
                date(2025, 3, 1),
                datetime(2025, 3, 1, 0, 0, 0),
                True,
                b"hello world!",
                ["e", "f"],
                {"key1": 3, "key2": 3},
                {"field1": "val3", "field2": 3, "field3": date(2025, 3, 1)},
                '{"key3": "val3"}',
                "invalid_str3",
                "Schema validation failed: Column 'invalid_col' has incompatible type, expected 'integer', got 'string'",
            ],
        ],
        schema="a string, b int, c float, d double, e date, f timestamp_ntz, g boolean, h binary, i array<string>, j map<string, short>, k struct<field1: string, field2: int, field3: date>, l string, invalid_col string, has_invalid_schema string",
    )
    expected_condition_df = expected_condition_df.withColumn("l", F.parse_json("l"))

    # NOTE: As of Databricks Connect version 15.4, we cannot compare `VariantType` columns using `assert_df_equality`;
    # We cast variants to `MapType` to safely compare the columns
    expected_condition_df = expected_condition_df.withColumn("l", F.col("l").cast("map<string, string>"))
    actual_condition_df = actual_condition_df.withColumn("l", F.col("l").cast("map<string, string>"))
    assert_df_equality(actual_condition_df, expected_condition_df, ignore_nullable=True)


def test_has_valid_schema_permissive_mode_missing_column(spark):
    test_df = spark.createDataFrame(
        [
            ["str1", 1],
            ["str2", 2],
        ],
        "a string, b int",
    )

    expected_schema = StructType(
        [
            StructField("a", StringType(), True),
            StructField("b", IntegerType(), True),
            StructField("c", DoubleType(), True),
        ]
    )
    condition, apply_method = has_valid_schema(expected_schema)
    actual_apply_df = apply_method(test_df)
    actual_condition_df = actual_apply_df.select("a", "b", condition)

    expected_condition_df = spark.createDataFrame(
        [
            [
                "str1",
                1,
                "Schema validation failed: Column 'c' in expected schema not present in checked data",
            ],
            [
                "str2",
                2,
                "Schema validation failed: Column 'c' in expected schema not present in checked data",
            ],
        ],
        "a string, b int, has_invalid_schema string",
    )
    assert_df_equality(actual_condition_df, expected_condition_df, ignore_nullable=True)


def test_has_valid_schema_permissive_mode_incompatible_column_type(spark):
    test_df = spark.createDataFrame(
        [
            ["str1", "not_an_int"],
            ["str2", "also_not_int"],
        ],
        "a string, b string",
    )

    expected_schema = "a string, b int"
    condition, apply_method = has_valid_schema(expected_schema)
    actual_apply_df = apply_method(test_df)
    actual_condition_df = actual_apply_df.select("a", "b", condition)

    expected_condition_df = spark.createDataFrame(
        [
            [
                "str1",
                "not_an_int",
                "Schema validation failed: Column 'b' has incompatible type, expected 'integer', got 'string'",
            ],
            [
                "str2",
                "also_not_int",
                "Schema validation failed: Column 'b' has incompatible type, expected 'integer', got 'string'",
            ],
        ],
        "a string, b string, has_invalid_schema string",
    )
    assert_df_equality(actual_condition_df, expected_condition_df, ignore_nullable=True)


def test_has_valid_schema_strict_mode_missing_column(spark):
    test_df = spark.createDataFrame(
        [
            ["str1", 1],
            ["str2", 2],
        ],
        "a string, b int",
    )

    expected_schema = "a string, b int, c double"
    condition, apply_method = has_valid_schema(expected_schema, strict=True)
    actual_apply_df = apply_method(test_df)
    actual_condition_df = actual_apply_df.select("a", "b", condition)

    expected_condition_df = spark.createDataFrame(
        [
            [
                "str1",
                1,
                "Schema validation failed: Column 'c' in expected schema not present in checked data",
            ],
            [
                "str2",
                2,
                "Schema validation failed: Column 'c' in expected schema not present in checked data",
            ],
        ],
        "a string, b int, has_invalid_schema string",
    )
    assert_df_equality(actual_condition_df, expected_condition_df, ignore_nullable=True)


def test_has_valid_schema_strict_mode_extra_column(spark):
    test_df = spark.createDataFrame(
        [
            ["str1", 1, 100.0],
            ["str2", 2, 200.0],
        ],
        "a string, b int, c double",
    )

    expected_schema = "a string, b int"
    condition, apply_method = has_valid_schema(expected_schema, strict=True)
    actual_apply_df = apply_method(test_df)
    actual_condition_df = actual_apply_df.select("a", "b", "c", condition)

    expected_condition_df = spark.createDataFrame(
        [
            [
                "str1",
                1,
                100.0,
                "Schema validation failed: Column 'c' in checked data not present in expected schema",
            ],
            [
                "str2",
                2,
                200.0,
                "Schema validation failed: Column 'c' in checked data not present in expected schema",
            ],
        ],
        "a string, b int, c double, has_invalid_schema string",
    )
    assert_df_equality(actual_condition_df, expected_condition_df, ignore_nullable=True)


def test_has_valid_schema_strict_mode_wrong_column_order(spark):
    test_df = spark.createDataFrame(
        [
            [1, "str1"],
            [2, "str2"],
        ],
        "b int, a string",
    )

    expected_schema = "a string, b int"
    condition, apply_method = has_valid_schema(expected_schema, strict=True)
    actual_apply_df = apply_method(test_df)
    actual_condition_df = actual_apply_df.select("b", "a", condition)

    expected_condition_df = spark.createDataFrame(
        [
            [
                1,
                "str1",
                "Schema validation failed: Column with index 0 has incorrect name, expected 'a', got 'b'; Column 'b' has incorrect type, expected 'string', got 'integer'; Column with index 1 has incorrect name, expected 'b', got 'a'; Column 'a' has incorrect type, expected 'integer', got 'string'",
            ],
            [
                2,
                "str2",
                "Schema validation failed: Column with index 0 has incorrect name, expected 'a', got 'b'; Column 'b' has incorrect type, expected 'string', got 'integer'; Column with index 1 has incorrect name, expected 'b', got 'a'; Column 'a' has incorrect type, expected 'integer', got 'string'",
            ],
        ],
        "b int, a string, has_invalid_schema string",
    )
    assert_df_equality(actual_condition_df, expected_condition_df, ignore_nullable=True)


def test_has_valid_schema_with_specified_columns(spark):
    test_df = spark.createDataFrame(
        [
            ["str1", 1, 100.0, "extra"],
            ["str2", 2, 200.0, "data"],
        ],
        "a string, b int, c double, d string",
    )

    expected_schema = "a string, b int, c string, e int"
    condition, apply_method = has_valid_schema(expected_schema, columns=["a", "b"], strict=False)
    actual_apply_df = apply_method(test_df)
    actual_condition_df = actual_apply_df.select("a", "b", "c", "d", condition)

    expected_condition_df = spark.createDataFrame(
        [
            ["str1", 1, 100.0, "extra", None],
            ["str2", 2, 200.0, "data", None],
        ],
        "a string, b int, c double, d string, has_invalid_schema string",
    )
    assert_df_equality(actual_condition_df, expected_condition_df, ignore_nullable=True)


def test_has_valid_schema_with_specific_columns_mismatch(spark: SparkSession):
    test_df = spark.createDataFrame(
        [
            ["str1", "not_int", 100.0],
            ["str2", "also_not_int", 200.0],
        ],
        "a string, b string, c double",
    )

    expected_schema = "a string, b int, c string"
    condition, apply_method = has_valid_schema(expected_schema, columns=["a", "b"], strict=True)
    actual_apply_df = apply_method(test_df)
    actual_condition_df = actual_apply_df.select("a", "b", "c", condition)

    expected_condition_df = spark.createDataFrame(
        [
            [
                "str1",
                "not_int",
                100.0,
                "Schema validation failed: Column 'b' has incorrect type, expected 'integer', got 'string'",
            ],
            [
                "str2",
                "also_not_int",
                200.0,
                "Schema validation failed: Column 'b' has incorrect type, expected 'integer', got 'string'",
            ],
        ],
        "a string, b string, c double, has_invalid_schema string",
    )
    assert_df_equality(actual_condition_df, expected_condition_df, ignore_nullable=True)
