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


def test_column_mismatch_error(spark: SparkSession, mock_workspace_client, make_random, anomaly_engine):
    """Test error when scoring columns don't match training columns."""
    unique_id = make_random(8).lower()
    model_name = f"test_col_mismatch_{make_random(4).lower()}"
    registry_table = f"main.default.{unique_id}_registry"
    
    # Train on [amount, quantity]
    train_df = spark.createDataFrame(
        [(100.0, 2.0) for i in range(50)],
        "amount double, quantity double",
    )

    anomaly_engine.train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name=model_name,
        registry_table=registry_table,
    )

    # Try to score with [amount, discount] (different columns)
    test_df = spark.createDataFrame(
        [(1, 100.0, 0.1)],
        "transaction_id int, amount double, discount double",
    )

    # Should raise error about column mismatch
    with pytest.raises(InvalidParameterError, match="Columns .* don't match trained model"):
        _, apply_fn = has_no_anomalies(
            merge_columns=["transaction_id"],
            columns=["amount", "discount"],
            model=model_name,
            registry_table=registry_table,
            score_threshold=0.5,
        )
        result_df = apply_fn(test_df)
        result_df.collect()


def test_column_order_independence(spark: SparkSession, mock_workspace_client, make_random, anomaly_engine):
    """Test that column order doesn't matter (set comparison)."""
    unique_id = make_random(8).lower()
    model_name = f"test_col_order_{make_random(4).lower()}"
    registry_table = f"main.default.{unique_id}_registry"
    
    # Train on [amount, quantity]
    train_df = spark.createDataFrame(
        [(100.0, 2.0) for i in range(50)],
        "amount double, quantity double",
    )

    anomaly_engine.train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name=model_name,
        registry_table=registry_table,
    )

    # Score with [quantity, amount] (different order)
    test_df = spark.createDataFrame(
        [(1, 2.0, 100.0)],
        "transaction_id int, quantity double, amount double",
    )

    # Should succeed - order doesn't matter
    _, apply_fn = has_no_anomalies(
        merge_columns=["transaction_id"],
        columns=["quantity", "amount"],  # Different order
        model=model_name,
        registry_table=registry_table,
        score_threshold=0.5,
    )
    result_df = apply_fn(test_df)
    result_df.collect()
    assert True  # No error - order doesn't matter


def test_empty_dataframe_error(spark: SparkSession, make_random, anomaly_engine):
    """Test error when training on empty DataFrame."""
    unique_id = make_random(8).lower()
    model_name = f"test_empty_{make_random(4).lower()}"
    registry_table = f"main.default.{unique_id}_registry"
    
    empty_df = spark.createDataFrame([], "amount double, quantity double")

    # Should raise clear error about empty data
    with pytest.raises(InvalidParameterError, match="Sampling produced 0 rows"):
        anomaly_engine.train(
            df=empty_df,
            columns=["amount", "quantity"],
            model_name=model_name,
            registry_table=registry_table,
        )


def test_missing_registry_table_for_scoring_error(spark: SparkSession, mock_workspace_client, make_random):
    """Test error when registry table doesn't exist during scoring."""
    unique_id = make_random(8).lower()
    model_name = f"test_missing_registry_{make_random(4).lower()}"
    registry_table = f"main.default.{unique_id}_nonexistent_registry"
    
    df = spark.createDataFrame(
        [(1, 100.0, 2.0)],
        "transaction_id int, amount double, quantity double",
    )

    # Try to score with non-existent registry
    with pytest.raises((InvalidParameterError, Exception)):  # Delta/Spark exception
        _, apply_fn = has_no_anomalies(
            merge_columns=["transaction_id"],
            columns=["amount", "quantity"],
            model=model_name,
            registry_table=registry_table,
            score_threshold=0.5,
        )
        result_df = apply_fn(df)
        result_df.collect()
