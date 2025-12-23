"""Integration tests for anomaly detection error cases.

Tests integration-level errors that occur with real Spark operations,
model training, and Delta table access. Pure parameter validation errors
are covered by unit tests (tests/unit/test_anomaly_validation.py).
"""

from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession

from databricks.labs.dqx.anomaly import has_no_anomalies
from databricks.labs.dqx.errors import InvalidParameterError
from databricks.sdk import WorkspaceClient


@pytest.fixture
def mock_workspace_client():
    """Create a mock WorkspaceClient for testing."""
    return MagicMock(spec=WorkspaceClient)


def test_column_mismatch_error(spark: SparkSession, mock_workspace_client, anomaly_engine):
    """Test error when scoring columns don't match training columns."""
    # Train on [amount, quantity]
    train_df = spark.createDataFrame(
        [(100.0, 2.0) for i in range(50)],
        "amount double, quantity double",
    )

    anomaly_engine.train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name="test_col_mismatch",
        registry_table="main.default.test_col_mismatch_registry",
    )

    # Try to score with [amount, discount] (different columns)
    test_df = spark.createDataFrame(
        [(100.0, 0.1)],
        "amount double, discount double",
    )

    # Should raise error about column mismatch
    with pytest.raises(InvalidParameterError, match="Columns .* don't match trained model"):
        _, apply_fn = has_no_anomalies(
            merge_columns=["transaction_id"],
            columns=["amount", "discount"],
            model="test_col_mismatch",
            registry_table="main.default.test_col_mismatch_registry",
            score_threshold=0.5,
        )
        result_df = apply_fn(test_df)
        result_df.collect()


def test_column_order_independence(spark: SparkSession, mock_workspace_client, anomaly_engine):
    """Test that column order doesn't matter (set comparison)."""
    # Train on [amount, quantity]
    train_df = spark.createDataFrame(
        [(100.0, 2.0) for i in range(50)],
        "amount double, quantity double",
    )

    anomaly_engine.train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name="test_col_order",
        registry_table="main.default.test_col_order_registry",
    )

    # Score with [quantity, amount] (different order)
    test_df = spark.createDataFrame(
        [(2.0, 100.0)],
        "quantity double, amount double",
    )

    # Should succeed - order doesn't matter
    _, apply_fn = has_no_anomalies(
        merge_columns=["transaction_id"],
        columns=["quantity", "amount"],  # Different order
        model="test_col_order",
        registry_table="main.default.test_col_order_registry",
        score_threshold=0.5,
    )
    result_df = apply_fn(test_df)
    result_df.collect()
    assert True  # No error - order doesn't matter


def test_empty_dataframe_error(spark: SparkSession, anomaly_engine):
    """Test error when training on empty DataFrame."""
    empty_df = spark.createDataFrame([], "amount double, quantity double")

    # Should raise clear error about empty data
    with pytest.raises(InvalidParameterError, match="Sampling produced 0 rows"):
        anomaly_engine.train(
            df=empty_df,
            columns=["amount", "quantity"],
            model_name="test_empty",
            registry_table="main.default.test_empty_registry",
        )


def test_missing_registry_table_for_scoring_error(spark: SparkSession, mock_workspace_client):
    """Test error when registry table doesn't exist during scoring."""
    df = spark.createDataFrame(
        [(100.0, 2.0)],
        "amount double, quantity double",
    )

    # Try to score with non-existent registry
    with pytest.raises((InvalidParameterError, Exception)):  # Delta/Spark exception
        _, apply_fn = has_no_anomalies(
            merge_columns=["transaction_id"],
            columns=["amount", "quantity"],
            model="test_missing_registry",
            registry_table="main.default.completely_nonexistent_registry",
            score_threshold=0.5,
        )
        result_df = apply_fn(df)
        result_df.collect()
