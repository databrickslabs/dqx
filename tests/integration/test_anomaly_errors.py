"""Integration tests for anomaly detection error cases."""

import pytest
from pyspark.sql import SparkSession
from unittest.mock import MagicMock, patch

from databricks.labs.dqx.anomaly import train, has_no_anomalies
from databricks.labs.dqx.engine import DQEngine
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
    
    dq_engine = DQEngine(mock_workspace_client)
    checks = [
        has_no_anomalies(
            columns=["amount", "quantity"],
            model="nonexistent_model",
            registry_table="main.default.nonexistent_registry",
            score_threshold=0.5,
        )
    ]
    
    # Should raise clear error about missing model
    with pytest.raises(InvalidParameterError, match="Model 'nonexistent_model' not found"):
        result_df = dq_engine.apply_checks_by_metadata(df, checks)
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
    
    dq_engine = DQEngine(mock_workspace_client)
    checks = [
        has_no_anomalies(
            columns=["amount", "discount"],
            model="test_col_mismatch",
            registry_table="main.default.test_col_mismatch_registry",
            score_threshold=0.5,
        )
    ]
    
    # Should raise error about column mismatch
    with pytest.raises(InvalidParameterError, match="Columns .* don't match trained model"):
        result_df = dq_engine.apply_checks_by_metadata(test_df, checks)
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
    
    dq_engine = DQEngine(mock_workspace_client)
    checks = [
        has_no_anomalies(
            columns=["amount", "quantity", "discount"],
            model="test_count_mismatch",
            registry_table="main.default.test_count_mismatch_registry",
            score_threshold=0.5,
        )
    ]
    
    # Should raise error about column mismatch
    with pytest.raises(InvalidParameterError, match="Columns .* don't match trained model"):
        result_df = dq_engine.apply_checks_by_metadata(test_df, checks)
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
    
    dq_engine = DQEngine(mock_workspace_client)
    checks = [
        has_no_anomalies(
            columns=["quantity", "amount"],  # Different order
            model="test_col_order",
            registry_table="main.default.test_col_order_registry",
            score_threshold=0.5,
        )
    ]
    
    # Should NOT raise error (column sets are the same)
    result_df = dq_engine.apply_checks_by_metadata(test_df, checks)
    assert "anomaly_score" in result_df.columns


def test_spark_version_check_on_train(spark: SparkSession):
    """Test that training fails fast on unsupported Spark version."""
    df = spark.createDataFrame(
        [(100.0, 2.0) for i in range(50)],
        "amount double, quantity double",
    )
    
    # Mock Spark version to be < 3.5
    with patch.object(spark, "version", "3.4.0"):
        with pytest.raises(InvalidParameterError, match="Spark .* or DBR .* required"):
            train(
                df=df,
                columns=["amount", "quantity"],
                model_name="test_version_check",
                registry_table="main.default.test_version_check_registry",
            )


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
    
    dq_engine = DQEngine(mock_workspace_client)
    checks = [
        has_no_anomalies(
            columns=["amount", "quantity"],
            model="test_missing_registry",
            registry_table="main.default.completely_nonexistent_registry",
            score_threshold=0.5,
        )
    ]
    
    # Should raise error about missing model/registry
    with pytest.raises(InvalidParameterError, match="Model .* not found"):
        result_df = dq_engine.apply_checks_by_metadata(df, checks)
        result_df.collect()

