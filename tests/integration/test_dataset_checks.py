from collections.abc import Callable
from datetime import datetime
from typing import Any
import json

import pyspark.sql.functions as F
from chispa.dataframe_comparer import assert_df_equality  # type: ignore
from pyspark.sql import Column, DataFrame, SparkSession

from databricks.labs.dqx.check_funcs import (
    is_unique,
    is_aggr_not_greater_than,
    is_aggr_not_less_than,
    is_aggr_equal,
    is_aggr_not_equal,
    foreign_key,
    compare_datasets,
)
from databricks.labs.dqx.utils import get_column_name_or_alias

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
                "Count 2 in column 'a' is not equal to limit: 1",
                "Count 0 per group of columns 'a' in column 'a' is not equal to limit: 1",
                "Count 0 per group of columns 'b' in column 'b' is not equal to limit: 2",
                None,
                "Sum 4 in column 'b' is not equal to limit: 10.0",
                None,
                "Max 3 in column 'b' is not equal to limit: 5.0",
            ],
            [
                "a",
                1,
                None,
                "Count 2 in column 'a' is not equal to limit: 1",
                None,
                "Count 1 per group of columns 'b' in column 'b' is not equal to limit: 2",
                None,
                "Sum 4 in column 'b' is not equal to limit: 10.0",
                None,
                "Max 3 in column 'b' is not equal to limit: 5.0",
            ],
            [
                "b",
                3,
                None,
                "Count 2 in column 'a' is not equal to limit: 1",
                None,
                "Count 1 per group of columns 'b' in column 'b' is not equal to limit: 2",
                None,
                "Sum 4 in column 'b' is not equal to limit: 10.0",
                None,
                "Max 3 in column 'b' is not equal to limit: 5.0",
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
                "Count 3 in column 'a' is equal to limit: 3",
                None,
                None,
                None,
                "Avg 2.0 in column 'b' is equal to limit: 2.0",
                None,
                "Min 1 in column 'b' is equal to limit: 1.0",
                None,
            ],
            [
                "a",
                1,
                "Count 3 in column 'a' is equal to limit: 3",
                None,
                "Count 1 per group of columns 'a' in column 'a' is equal to limit: 1",
                None,
                "Avg 2.0 in column 'b' is equal to limit: 2.0",
                None,
                "Min 1 in column 'b' is equal to limit: 1.0",
                None,
            ],
            [
                "b",
                3,
                "Count 3 in column 'a' is equal to limit: 3",
                None,
                "Count 1 per group of columns 'a' in column 'a' is equal to limit: 1",
                None,
                "Avg 2.0 in column 'b' is equal to limit: 2.0",
                None,
                "Min 1 in column 'b' is equal to limit: 1.0",
                None,
            ],
        ],
        expected_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


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

    catalog_name = "main"
    ref_table_schema = make_schema(catalog_name=catalog_name)
    ref_table = f"{catalog_name}.{ref_table_schema.name}.{make_random(6).lower()}"
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
            [1, None, "val1"],
        ],
        schema,
    )

    df_ref = spark.createDataFrame(
        [
            [1, 1, None],
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
        ],
        schema,
    )

    df_ref = spark.createDataFrame(
        [
            [1, None],  # should not show any diff in the name
            [2, "val2"],
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
        ],
        expected_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)
