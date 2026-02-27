"""Integration tests for merge_info_columns to verify that info struct columns are merged into a single 
destination column and source columns are dropped. Uses assert_df_equality for result comparison.
"""

import pyspark.sql.functions as F
from chispa.dataframe_comparer import assert_df_equality  # type: ignore
from databricks.labs.dqx.reporting_columns import DefaultColumnNames, merge_info_columns


def test_merge_info_columns_single_column(ws, spark):
    """Merge one info struct column into dest; source column is dropped."""
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
        F.struct(F.struct(F.lit(0.75).alias("score"), F.lit(90.0).alias("severity_percentile")).alias("anomaly")),
    )
    assert_df_equality(result, expected, ignore_nullable=True)


def test_merge_info_columns_no_names_returns_unchanged(ws, spark):
    """None or empty info_col_names leaves DataFrame unchanged."""
    df = spark.createDataFrame([(1, "a")], "id int, x string")

    result_none = merge_info_columns("_dq_info", df, info_col_names=None)
    result_empty = merge_info_columns("_dq_info", df, info_col_names=[])

    assert_df_equality(result_none, df)
    assert_df_equality(result_empty, df)


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
        F.struct(F.struct(F.lit(0.5).alias("score")).alias("anomaly")),
    )
    assert_df_equality(result, expected, ignore_nullable=True)


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

    # Simulate engine adding _warnings between passes
    with_warnings = after_first.withColumn(
        DefaultColumnNames.WARNINGS.value,
        F.lit(None).cast("array<struct<check:string,message:string>>"),
    )
    assert with_warnings.columns == ["id", DefaultColumnNames.INFO.value, DefaultColumnNames.WARNINGS.value]

    # Second pass: another info column; existing _dq_info must be merged in, not replaced
    with_second_info = with_warnings.withColumn(
        "__dqx_info_b",
        F.struct(F.lit("extra").alias("other")),
    )
    after_second = merge_info_columns(
        DefaultColumnNames.INFO.value,
        with_second_info,
        info_col_names=["__dqx_info_b"],
    )

    assert set(after_second.columns) == {"id", DefaultColumnNames.WARNINGS.value, DefaultColumnNames.INFO.value}
    # Merged struct must contain both anomaly (from first pass) and other (from second pass)
    row = after_second.select(DefaultColumnNames.INFO.value).first()
    assert row is not None
    info = row[0]
    assert info.anomaly is not None and info.anomaly.score == 0.75
    assert info.other == "extra"


def test_merge_info_columns_twice_each_with_valid_info_fields(ws, spark):
    """Run merge_info_columns twice; each call has a valid info column with fields; final _dq_info has both."""
    # First pass: one info column with field "first"
    df = spark.createDataFrame([(1,)], "id int").withColumn(
        "__dqx_info_first",
        F.struct(F.lit("a").alias("first")),
    )
    after_first = merge_info_columns(
        DefaultColumnNames.INFO.value,
        df,
        info_col_names=["__dqx_info_first"],
    )
    assert DefaultColumnNames.INFO.value in after_first.columns
    row1 = after_first.select(DefaultColumnNames.INFO.value).first()
    assert row1 is not None and row1[0].first == "a"

    # Second pass: add another info column with field "second"; existing _dq_info must be merged in
    with_second = after_first.withColumn(
        "__dqx_info_second",
        F.struct(F.lit("b").alias("second")),
    )
    after_second = merge_info_columns(
        DefaultColumnNames.INFO.value,
        with_second,
        info_col_names=["__dqx_info_second"],
    )
    assert DefaultColumnNames.INFO.value in after_second.columns
    row2 = after_second.select(DefaultColumnNames.INFO.value).first()
    assert row2 is not None
    info = row2[0]
    assert info.first == "a"
    assert info.second == "b"
