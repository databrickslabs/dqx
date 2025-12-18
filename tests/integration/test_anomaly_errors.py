"""Integration tests for anomaly detection error cases."""

import pytest
from pyspark.sql import SparkSession
from unittest.mock import MagicMock

from databricks.labs.dqx.anomaly import train, has_no_anomalies
from databricks.labs.dqx.errors import InvalidParameterError
from databricks.sdk import WorkspaceClient


@pytest.fixture
def mock_workspace_client():
    """Create a mock WorkspaceClient for testing."""
    return MagicMock(spec=WorkspaceClient)


def test_missing_model_error(spark: SparkSession, mock_workspace_client):
    """Test error when scoring with non-existent model."""
    df = spark.createDataFrame(
        [(100.0, 2.0)],
        "amount double, quantity double",
    )

    # Should raise clear error about missing model
    # Model name gets normalized to full catalog.schema.model format
    with pytest.raises(InvalidParameterError, match="Model 'main.default.nonexistent_model' not found"):
        _, apply_fn = has_no_anomalies(
            merge_columns=["transaction_id"],
            columns=["amount", "quantity"],
            model="nonexistent_model",
            registry_table="main.default.nonexistent_registry",
            score_threshold=0.5,
        )
        result_df = apply_fn(df)
        result_df.collect()


def test_column_mismatch_error(spark: SparkSession, mock_workspace_client):
    """Test error when scoring columns don't match training columns."""
    # Train on [amount, quantity]
    train_df = spark.createDataFrame(
        [(100.0, 2.0) for i in range(50)],
        "amount double, quantity double",
    )

    train(
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


def test_column_count_mismatch_error(spark: SparkSession, mock_workspace_client):
    """Test error when number of columns differs."""
    # Train on 2 columns
    train_df = spark.createDataFrame(
        [(100.0, 2.0) for i in range(50)],
        "amount double, quantity double",
    )

    train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name="test_count_mismatch",
        registry_table="main.default.test_count_mismatch_registry",
    )

    # Try to score with 3 columns
    test_df = spark.createDataFrame(
        [(100.0, 2.0, 0.1)],
        "amount double, quantity double, discount double",
    )

    # Should raise error about column mismatch
    with pytest.raises(InvalidParameterError, match="Columns .* don't match trained model"):
        condition_col, apply_fn = has_no_anomalies(
            columns=["amount", "quantity", "discount"],
            model="test_count_mismatch",
            registry_table="main.default.test_count_mismatch_registry",
            score_threshold=0.5,
        )
        result_df = apply_fn(test_df)
        result_df.collect()


def test_column_order_independence(spark: SparkSession, mock_workspace_client):
    """Test that column order doesn't matter (set comparison)."""
    # Train on [amount, quantity]
    train_df = spark.createDataFrame(
        [(100.0, 2.0) for i in range(50)],
        "amount double, quantity double",
    )

    train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name="test_col_order",
        registry_table="main.default.test_col_order_registry",
    )

    # Score with [quantity, amount] (reversed order)
    test_df = spark.createDataFrame(
        [(2.0, 100.0)],
        "quantity double, amount double",
    )

    # Should NOT raise error (column sets are the same)
    condition_col, apply_fn = has_no_anomalies(
        columns=["quantity", "amount"],  # Different order
        model="test_col_order",
        registry_table="main.default.test_col_order_registry",
        score_threshold=0.5,
    )
    result_df = apply_fn(test_df)
    assert "anomaly_score" in result_df.columns


def test_empty_dataframe_error(spark: SparkSession):
    """Test error when training on empty DataFrame."""
    df = spark.createDataFrame([], "amount double, quantity double")

    # Should raise error about empty data
    with pytest.raises(InvalidParameterError, match="Sampling produced 0 rows"):
        train(
            df=df,
            columns=["amount", "quantity"],
            model_name="test_empty",
            registry_table="main.default.test_empty_registry",
        )


def test_nonexistent_column_error(spark: SparkSession):
    """Test error when specified column doesn't exist."""
    df = spark.createDataFrame(
        [(100.0, 2.0)],
        "amount double, quantity double",
    )

    # Try to train on non-existent column
    with pytest.raises(Exception):  # Should raise AnalysisException
        train(
            df=df,
            columns=["amount", "nonexistent_column"],
            model_name="test_nonexistent_col",
            registry_table="main.default.test_nonexistent_col_registry",
        )


def test_non_numeric_column_error(spark: SparkSession):
    """Test error when specified column is not numeric."""
    df = spark.createDataFrame(
        [(100.0, "text")],
        "amount double, description string",
    )

    # Try to train on string column (not numeric)
    with pytest.raises(Exception):  # Should raise error during VectorAssembler
        train(
            df=df,
            columns=["amount", "description"],
            model_name="test_non_numeric",
            registry_table="main.default.test_non_numeric_registry",
        )


def test_missing_registry_table_for_scoring_error(spark: SparkSession, mock_workspace_client):
    """Test error when registry table doesn't exist during scoring."""
    df = spark.createDataFrame(
        [(100.0, 2.0)],
        "amount double, quantity double",
    )

    # Should raise error about missing model/registry
    with pytest.raises(InvalidParameterError, match="Model .* not found"):
        condition_col, apply_fn = has_no_anomalies(
            columns=["amount", "quantity"],
            model="test_missing_registry",
            registry_table="main.default.completely_nonexistent_registry",
            score_threshold=0.5,
        )
        result_df = apply_fn(df)
        result_df.collect()
