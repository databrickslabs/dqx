"""Integration tests for anomaly detection error cases.

Tests integration-level errors that occur with real Spark operations,
model training, and Delta table access. Pure parameter validation errors
are covered by unit tests (tests/unit/test_anomaly_validation.py).
"""

import pytest
from pyspark.sql import SparkSession

from databricks.labs.dqx.anomaly import has_no_anomalies
from databricks.labs.dqx.errors import InvalidParameterError

from tests.integration_anomaly.test_anomaly_constants import DEFAULT_SCORE_THRESHOLD
from tests.integration_anomaly.test_anomaly_utils import qualify_model_name, train_simple_2d_model


def test_missing_columns_error(
    ws, spark: SparkSession, make_random, anomaly_engine, test_df_factory, anomaly_registry_prefix
):
    """Test error when input DataFrame is missing model columns."""
    unique_id = make_random(8).lower()
    model_name = f"{anomaly_registry_prefix}.test_col_mismatch_{make_random(4).lower()}"
    registry_table = f"{anomaly_registry_prefix}.{unique_id}_registry"

    # Train on [amount, quantity] - use helper
    train_simple_2d_model(spark, anomaly_engine, model_name, registry_table)

    # Try to score with [amount, discount] (different columns) - use factory
    test_df = test_df_factory(
        spark,
        normal_rows=[(100.0, 0.1)],
        anomaly_rows=[],
        columns_schema="amount double, discount double",
    )

    # Should raise error about missing model columns in the input DataFrame
    with pytest.raises(InvalidParameterError, match="missing required columns"):
        _, apply_fn = has_no_anomalies(
            model=qualify_model_name(model_name, registry_table),
            registry_table=registry_table,
            score_threshold=DEFAULT_SCORE_THRESHOLD,
        )
        result_df = apply_fn(test_df)
        result_df.collect()


def test_column_order_independence(
    ws,
    spark: SparkSession,
    make_random,
    anomaly_engine,
    test_df_factory,
    anomaly_scorer,
    anomaly_registry_prefix,
):
    """Test that column order doesn't matter (set comparison)."""
    unique_id = make_random(8).lower()
    model_name = f"{anomaly_registry_prefix}.test_col_order_{make_random(4).lower()}"
    registry_table = f"{anomaly_registry_prefix}.{unique_id}_registry"

    # Train on [amount, quantity] - use helper
    train_simple_2d_model(spark, anomaly_engine, model_name, registry_table)

    # Score with [quantity, amount] (different order) - note DataFrame column order
    test_df = test_df_factory(
        spark,
        normal_rows=[(2.0, 100.0)],  # quantity, amount order
        anomaly_rows=[],
        columns_schema="quantity double, amount double",  # Different column order
    )

    # Should succeed - order doesn't matter for anomaly check
    result_df = anomaly_scorer(
        test_df,
        model_name=model_name,
        registry_table=registry_table,
        score_threshold=DEFAULT_SCORE_THRESHOLD,
        extract_score=False,
    )
    result_df.collect()
    assert True  # No error - order doesn't matter


def test_empty_dataframe_error(spark: SparkSession, make_random, anomaly_engine, anomaly_registry_prefix):
    """Test error when training on empty DataFrame."""
    unique_id = make_random(8).lower()
    model_name = f"{anomaly_registry_prefix}.test_empty_{make_random(4).lower()}"
    registry_table = f"{anomaly_registry_prefix}.{unique_id}_registry"

    empty_df = spark.createDataFrame([], "amount double, quantity double")

    # Should raise clear error about empty data
    with pytest.raises(InvalidParameterError, match="Sampling produced 0 rows"):
        anomaly_engine.train(
            df=empty_df,
            columns=["amount", "quantity"],
            model_name=qualify_model_name(model_name, registry_table),
            registry_table=registry_table,
        )


def test_missing_registry_table_for_scoring_error(
    ws, spark: SparkSession, make_random, test_df_factory, anomaly_registry_prefix
):
    """Test error when registry table doesn't exist during scoring."""
    unique_id = make_random(8).lower()
    model_name = f"{anomaly_registry_prefix}.test_missing_registry_{make_random(4).lower()}"
    registry_table = f"{anomaly_registry_prefix}.{unique_id}_nonexistent_registry"

    # Use factory to create test DataFrame
    df = test_df_factory(
        spark,
        normal_rows=[(100.0, 2.0)],
        anomaly_rows=[],
        columns_schema="amount double, quantity double",
    )

    # Try to score with non-existent registry
    with pytest.raises((InvalidParameterError, Exception)):  # Delta/Spark exception
        _, apply_fn = has_no_anomalies(
            model=qualify_model_name(model_name, registry_table),
            registry_table=registry_table,
            score_threshold=DEFAULT_SCORE_THRESHOLD,
        )
        result_df = apply_fn(df)
        result_df.collect()
