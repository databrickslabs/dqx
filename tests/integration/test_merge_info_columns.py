"""Integration tests for merge_info_columns to verify that info struct columns are merged into a single 
destination column and source columns are dropped. Uses assertDataFrameEqual for result comparison.
"""

import pyspark.sql.functions as F
from pyspark.testing.utils import assertDataFrameEqual
from databricks.labs.dqx.reporting_columns import DefaultColumnNames, merge_info_columns


def test_merge_info_columns_single_column(ws, spark):
    """Merge one info struct column into dest; source column is dropped. Result is array of one struct."""
    df = spark.createDataFrame([(1,)], "id int").withColumn(
        "__dqx_info_abc",
        F.struct(F.struct(F.lit(0.75).alias("score"), F.lit(90.0).alias("severity_percentile")).alias("anomaly")),
    )

    result = merge_info_columns(
        DefaultColumnNames.INFO.value,
        df,
        info_col_names=["__dqx_info_abc"],
    )

    expected = spark.createDataFrame([(1,)], "id int").withColumn(
        DefaultColumnNames.INFO.value,
        F.array(
            F.struct(
                F.struct(
                    F.lit(0.75).alias("score"),
                    F.lit(90.0).alias("severity_percentile"),
                ).alias("anomaly")
            )
        ),
    )
    assertDataFrameEqual(result, expected)
    row = result.select(DefaultColumnNames.INFO.value).first()
    assert row is not None and row[0][0].anomaly.score == 0.75


def test_merge_info_columns_no_names_returns_unchanged(ws, spark):
    """None or empty info_col_names leaves DataFrame unchanged."""
    df = spark.createDataFrame([(1, "a")], "id int, x string")

    result_none = merge_info_columns("_dq_info", df, info_col_names=None)
    result_empty = merge_info_columns("_dq_info", df, info_col_names=[])

    assertDataFrameEqual(result_none, df)
    assertDataFrameEqual(result_empty, df)


def test_merge_info_columns_skips_missing_columns(ws, spark):
    """Columns in info_col_names that are not in the DataFrame are skipped."""
    df = spark.createDataFrame([(1,)], "id int").withColumn(
        "__dqx_info_real",
        F.struct(F.struct(F.lit(0.5).alias("score")).alias("anomaly")),
    )

    result = merge_info_columns(
        "_dq_info",
        df,
        info_col_names=["__dqx_info_real", "__dqx_info_missing"],
    )

    expected = spark.createDataFrame([(1,)], "id int").withColumn(
        "_dq_info",
        F.array(F.struct(F.struct(F.lit(0.5).alias("score")).alias("anomaly"))),
    )
    assertDataFrameEqual(result, expected)


def test_merge_info_columns_called_twice_preserves_both(ws, spark):
    """Second call merges existing _dq_info with new info columns."""
    # First pass: one info column -> _dq_info
    df = spark.createDataFrame([(1,)], "id int").withColumn(
        "__dqx_info_a",
        F.struct(F.struct(F.lit(0.75).alias("score")).alias("anomaly")),
    )
    after_first = merge_info_columns(
        DefaultColumnNames.INFO.value,
        df,
        info_col_names=["__dqx_info_a"],
    )
    assert after_first.columns == ["id", DefaultColumnNames.INFO.value]

    # Select to avoid carrying references to dropped temp columns into the next merge
    after_first = after_first.select("id", DefaultColumnNames.INFO.value)

    # Simulate engine adding _warnings between passes
    with_warnings = after_first.withColumn(
        DefaultColumnNames.WARNINGS.value,
        F.lit(None).cast("array<struct<check:string,message:string>>"),
    )
    assert with_warnings.columns == ["id", DefaultColumnNames.INFO.value, DefaultColumnNames.WARNINGS.value]

    # Second pass: another info column (same struct shape so concat type matches); existing _dq_info must be merged in
    with_second_info = with_warnings.withColumn(
        "__dqx_info_b",
        F.struct(F.struct(F.lit(0.5).alias("score")).alias("anomaly")),
    )
    after_second = merge_info_columns(
        DefaultColumnNames.INFO.value,
        with_second_info,
        info_col_names=["__dqx_info_b"],
    )

    assert set(after_second.columns) == {"id", DefaultColumnNames.WARNINGS.value, DefaultColumnNames.INFO.value}
    # Merged array must have two elements: first and second anomaly structs
    row = after_second.select(DefaultColumnNames.INFO.value).first()
    assert row is not None
    arr = row[0]
    assert len(arr) == 2
    assert arr[0].anomaly is not None and arr[0].anomaly.score == 0.75
    assert arr[1].anomaly is not None and arr[1].anomaly.score == 0.5


def test_merge_info_columns_twice_each_with_valid_info_fields(ws, spark):
    """Run merge_info_columns twice; each call has a valid info column with fields; final _dq_info has both."""
    # First pass: one info column with field "tag"
    df = spark.createDataFrame([(1,)], "id int").withColumn(
        "__dqx_info_first",
        F.struct(F.lit("a").alias("tag")),
    )
    after_first = merge_info_columns(
        DefaultColumnNames.INFO.value,
        df,
        info_col_names=["__dqx_info_first"],
    )
    assert DefaultColumnNames.INFO.value in after_first.columns
    row1 = after_first.select(DefaultColumnNames.INFO.value).first()
    assert row1 is not None and len(row1[0]) == 1 and row1[0][0].tag == "a"

    # Select to avoid carrying references to dropped temp columns into the next merge
    after_first = after_first.select("id", DefaultColumnNames.INFO.value)

    # Second pass: add another info column with same struct shape (tag); existing _dq_info must be merged in
    with_second = after_first.withColumn(
        "__dqx_info_second",
        F.struct(F.lit("b").alias("tag")),
    )
    after_second = merge_info_columns(
        DefaultColumnNames.INFO.value,
        with_second,
        info_col_names=["__dqx_info_second"],
    )
    assert DefaultColumnNames.INFO.value in after_second.columns
    row2 = after_second.select(DefaultColumnNames.INFO.value).first()
    assert row2 is not None
    arr = row2[0]
    assert len(arr) == 2
    assert arr[0].tag == "a"
    assert arr[1].tag == "b"
