from collections.abc import Callable
from typing import Any

import pyspark.sql.functions as F
from chispa.dataframe_comparer import assert_df_equality  # type: ignore
from pyspark.sql import Column, DataFrame, SparkSession

from databricks.labs.dqx.check_funcs import (
    is_unique,
    is_aggr_not_greater_than,
    is_aggr_not_less_than,
    foreign_key,
)

SCHEMA = "a: string, b: int"


def test_is_unique(spark):
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
            ["str2", 1, "Value '{str2}' in column 'struct(a)' is not unique, found 2 duplicates"],
            ["str2", 2, "Value '{str2}' in column 'struct(a)' is not unique, found 2 duplicates"],
            ["str3", 3, None],
        ],
        SCHEMA + ", struct_a_is_not_unique: string",
    )
    assert_df_equality(actual_condition_df, expected_condition_df, ignore_nullable=True)


def test_is_unique_null_distinct(spark):
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


def test_is_unique_nulls_not_distinct(spark):
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


def test_foreign_key(spark):
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
        foreign_key("a", "ref_col", "ref_df"),
        foreign_key(F.lit("a"), F.lit("ref_col"), "ref_df", row_filter="b = 3"),
    ]

    actual_df = _apply_checks(test_df, checks, ref_dfs, spark)

    expected_condition_df = spark.createDataFrame(
        [
            ["key1", 1, None, None],
            ["key2", 2, "FK violation: Value 'key2' in column 'a' not found in reference column 'ref_col'", None],
            ["key3", 3, None, None],
            [None, 4, None, None],
        ],
        SCHEMA + ", a_ref_col_foreign_key_violation: string, a_ref_col_foreign_key_violation: string",
    )
    assert_df_equality(actual_df, expected_condition_df, ignore_nullable=True)


def test_is_aggr_not_greater_than(spark):
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
                "Count 3 in column 'a' is greater than limit: 1",
                # displayed since filtering is done after, filter only applied for calculation inside the check
                "Count 2 in column 'a' is greater than limit: 0",
                None,
                None,
                "Avg 2.0 in column 'b' is greater than limit: 0.0",
                "Sum 4 in column 'b' is greater than limit: 0.0",
                "Min 1 in column 'b' is greater than limit: 0.0",
                "Max 3 in column 'b' is greater than limit: 0.0",
            ],
            [
                "a",
                1,
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
                "b",
                3,
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
                "a",
                1,
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
                "b",
                3,
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
