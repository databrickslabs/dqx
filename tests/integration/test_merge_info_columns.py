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
