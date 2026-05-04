"""Integration tests for profiling_utils."""

from pyspark.sql import SparkSession

from databricks.labs.dqx.profiling_utils import (
    compute_exact_distinct_counts,
    compute_null_and_distinct_counts,
)


def test_compute_exact_distinct_counts_empty_columns_returns_empty_dict(spark: SparkSession):
    """When columns is empty, compute_exact_distinct_counts returns {} (lines 46-47)."""
    df = spark.createDataFrame([(1, "a"), (2, "b")], "id int, name string")
    result = compute_exact_distinct_counts(df, [])
    assert result == {}


def test_compute_null_and_distinct_counts_exact_when_approx_false(spark: SparkSession):
    """When approx=False, compute_null_and_distinct_counts uses countDistinct (lines 28-30)."""
    df = spark.createDataFrame(
        [(1, "a", 10), (2, "a", 20), (3, "b", 10), (None, "c", 30)],
        "id int, cat string, val int",
    )
    column_names = ["id", "cat", "val"]
    distinct_columns = ["id", "cat", "val"]
    null_counts, distinct_counts = compute_null_and_distinct_counts(df, column_names, distinct_columns, approx=False)
    assert null_counts["id"] == 1
    assert null_counts["cat"] == 0
    assert null_counts["val"] == 0
    assert distinct_counts["id"] == 3
    assert distinct_counts["cat"] == 3
    assert distinct_counts["val"] == 3
