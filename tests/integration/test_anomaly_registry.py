"""Integration tests for anomaly model registry management."""

import pytest
import warnings
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from unittest.mock import MagicMock, patch

from databricks.labs.dqx.anomaly import train, has_no_anomalies
from databricks.labs.dqx.anomaly.model_registry import AnomalyModelRegistry
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient


@pytest.fixture
def mock_workspace_client():
    """Create a mock WorkspaceClient for testing."""
    return MagicMock(spec=WorkspaceClient)


def test_explicit_model_names(spark: SparkSession, mock_workspace_client):
    """Test training with explicit model_name and registry_table."""
    df = spark.createDataFrame(
        [(100.0 + i, 2.0) for i in range(50)],
        "amount double, quantity double",
    )
    
    model_uri = train(
        df=df,
        columns=["amount", "quantity"],
        model_name="my_custom_model",
        registry_table="main.default.my_custom_registry",
    )
    
    # Verify model URI is returned
    assert model_uri is not None
    
    # Verify model can be loaded with explicit name
    dq_engine = DQEngine(mock_workspace_client)
    checks = [
        has_no_anomalies(
            columns=["amount", "quantity"],
            model="my_custom_model",
            registry_table="main.default.my_custom_registry",
            score_threshold=0.5,
        )
    ]
    
    result_df = dq_engine.apply_checks_by_metadata(df, checks)
    
    assert "anomaly_score" in result_df.columns


def test_multiple_models_in_same_registry(spark: SparkSession, mock_workspace_client):
    """Test training multiple models in the same registry table."""
    registry_table = "main.default.test_multi_models_registry"
    
    # Train first model
    df1 = spark.createDataFrame(
        [(100.0, 2.0) for i in range(50)],
        "amount double, quantity double",
    )
    
    train(
        df=df1,
        columns=["amount", "quantity"],
        model_name="model_a",
        registry_table=registry_table,
    )
    
    # Train second model with different columns
    df2 = spark.createDataFrame(
        [(0.1, 50.0) for i in range(50)],
        "discount double, weight double",
    )
    
    train(
        df=df2,
        columns=["discount", "weight"],
        model_name="model_b",
        registry_table=registry_table,
    )
    
    # Verify both models exist in registry
    registry_df = spark.table(registry_table)
    model_names = [row["model_name"] for row in registry_df.select("model_name").distinct().collect()]
    
    assert "model_a" in model_names
    assert "model_b" in model_names
    
    # Verify correct model is loaded for each check
    dq_engine = DQEngine(mock_workspace_client)
    
    # Score with model_a
    checks_a = [
        has_no_anomalies(
            columns=["amount", "quantity"],
            model="model_a",
            registry_table=registry_table,
            score_threshold=0.5,
        )
    ]
    
    result_a = dq_engine.apply_checks_by_metadata(df1, checks_a)
    assert "anomaly_score" in result_a.columns
    
    # Score with model_b
    checks_b = [
        has_no_anomalies(
            columns=["discount", "weight"],
            model="model_b",
            registry_table=registry_table,
            score_threshold=0.5,
        )
    ]
    
    result_b = dq_engine.apply_checks_by_metadata(df2, checks_b)
    assert "anomaly_score" in result_b.columns


def test_active_model_retrieval(spark: SparkSession):
    """Test that get_active_model() returns the most recent model."""
    registry_table = "main.default.test_active_model_registry"
    df = spark.createDataFrame(
        [(100.0, 2.0) for i in range(50)],
        "amount double, quantity double",
    )
    
    # Train first version
    train(
        df=df,
        columns=["amount", "quantity"],
        model_name="test_active",
        registry_table=registry_table,
    )
    
    # Get active model
    registry = AnomalyModelRegistry(spark)
    model_v1 = registry.get_active_model(registry_table, "test_active")
    
    assert model_v1 is not None
    assert model_v1.model_name == "test_active"
    assert model_v1.status == "active"
    v1_training_time = model_v1.training_time
    
    # Train second version (should archive first)
    train(
        df=df,
        columns=["amount", "quantity"],
        model_name="test_active",
        registry_table=registry_table,
    )
    
    # Get active model (should be v2)
    model_v2 = registry.get_active_model(registry_table, "test_active")
    
    assert model_v2 is not None
    assert model_v2.model_name == "test_active"
    assert model_v2.status == "active"
    assert model_v2.training_time > v1_training_time
    
    # Verify first model is archived
    archived_count = (
        spark.table(registry_table)
        .filter("model_name = 'test_active' AND status = 'archived'")
        .count()
    )
    assert archived_count == 1


def test_model_staleness_warning(spark: SparkSession, mock_workspace_client):
    """Test that staleness warning is issued when model is >30 days old."""
    df = spark.createDataFrame(
        [(100.0, 2.0) for i in range(50)],
        "amount double, quantity double",
    )
    
    model_name = "test_stale_model"
    registry_table = "main.default.test_stale_registry"
    
    # Train model
    train(
        df=df,
        columns=["amount", "quantity"],
        model_name=model_name,
        registry_table=registry_table,
    )
    
    # Mock old training_time in registry
    old_time = datetime.utcnow() - timedelta(days=35)
    spark.sql(
        f"UPDATE {registry_table} "
        f"SET training_time = timestamp('{old_time.strftime('%Y-%m-%d %H:%M:%S')}') "
        f"WHERE model_name = '{model_name}'"
    )
    
    # Score with old model (should issue warning)
    dq_engine = DQEngine(mock_workspace_client)
    checks = [
        has_no_anomalies(
            columns=["amount", "quantity"],
            model=model_name,
            registry_table=registry_table,
            score_threshold=0.5,
        )
    ]
    
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        result_df = dq_engine.apply_checks_by_metadata(df, checks)
        result_df.collect()  # Force evaluation
        
        # Check that staleness warning was issued
        stale_warnings = [warning for warning in w if "days old" in str(warning.message)]
        assert len(stale_warnings) > 0
        assert "Consider retraining" in str(stale_warnings[0].message)


def test_registry_table_schema(spark: SparkSession):
    """Test that registry table has all expected columns."""
    df = spark.createDataFrame(
        [(100.0, 2.0) for i in range(50)],
        "amount double, quantity double",
    )
    
    registry_table = "main.default.test_schema_registry"
    
    train(
        df=df,
        columns=["amount", "quantity"],
        model_name="test_schema",
        registry_table=registry_table,
    )
    
    # Verify all expected columns exist
    registry_df = spark.table(registry_table)
    expected_columns = [
        "model_name",
        "model_uri",
        "input_table",
        "columns",
        "algorithm",
        "hyperparameters",
        "training_rows",
        "training_time",
        "mlflow_run_id",
        "status",
        "metrics",
        "mode",
        "baseline_stats",
        "feature_importance",
        "temporal_config",
    ]
    
    actual_columns = registry_df.columns
    
    for col in expected_columns:
        assert col in actual_columns, f"Missing column: {col}"


def test_registry_stores_metadata(spark: SparkSession):
    """Test that registry stores comprehensive metadata."""
    df = spark.createDataFrame(
        [(100.0, 2.0, 0.1) for i in range(50)],
        "amount double, quantity double, discount double",
    )
    
    registry_table = "main.default.test_metadata_registry"
    model_name = "test_metadata"
    
    train(
        df=df,
        columns=["amount", "quantity", "discount"],
        model_name=model_name,
        registry_table=registry_table,
    )
    
    # Query registry
    record = spark.table(registry_table).filter(f"model_name = '{model_name}'").first()
    
    # Verify key metadata is present
    assert record["model_name"] == model_name
    assert record["model_uri"] is not None
    assert record["algorithm"] == "IsolationForest"
    assert record["status"] == "active"
    assert record["training_time"] is not None
    assert record["columns"] == ["amount", "quantity", "discount"]
    
    # Verify baseline_stats exists
    assert record["baseline_stats"] is not None
    assert len(record["baseline_stats"]) == 3  # Three columns
    
    # Verify feature_importance exists
    assert record["feature_importance"] is not None
    assert len(record["feature_importance"]) == 3  # Three features
    
    # Verify metrics exist
    assert record["metrics"] is not None
    assert "recommended_threshold" in record["metrics"]


def test_nonexistent_registry_returns_none(spark: SparkSession):
    """Test that get_active_model returns None for non-existent registry."""
    registry = AnomalyModelRegistry(spark)
    
    model = registry.get_active_model(
        "main.default.nonexistent_registry",
        "nonexistent_model"
    )
    
    assert model is None


def test_nonexistent_model_returns_none(spark: SparkSession):
    """Test that get_active_model returns None for non-existent model."""
    df = spark.createDataFrame(
        [(100.0, 2.0) for i in range(50)],
        "amount double, quantity double",
    )
    
    registry_table = "main.default.test_nonexistent_model_registry"
    
    # Train a model
    train(
        df=df,
        columns=["amount", "quantity"],
        model_name="existing_model",
        registry_table=registry_table,
    )
    
    # Try to get non-existent model
    registry = AnomalyModelRegistry(spark)
    model = registry.get_active_model(registry_table, "nonexistent_model")
    
    assert model is None

