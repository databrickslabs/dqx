"""Integration tests for anomaly model registry management."""

from datetime import datetime, timedelta
from unittest.mock import MagicMock
import warnings

import pytest
from pyspark.sql import SparkSession

from databricks.labs.dqx.anomaly import has_no_anomalies
from databricks.labs.dqx.anomaly.model_registry import AnomalyModelRegistry
from databricks.sdk import WorkspaceClient
from tests.integration.test_anomaly_utils import (
    get_standard_2d_training_data,
    get_standard_3d_training_data,
)


@pytest.fixture
def mock_workspace_client():
    """Create a mock WorkspaceClient for testing."""
    return MagicMock(spec=WorkspaceClient)


def test_explicit_model_names(spark: SparkSession, mock_workspace_client, make_random: str, anomaly_engine):
    """Test training with explicit model_name and registry_table."""
    df = spark.createDataFrame(
        [(i, 100.0 + i, 2.0) for i in range(50)],
        "transaction_id int, amount double, quantity double",
    )

    unique_id = make_random(8).lower()
    model_name = f"my_custom_model_{make_random(4).lower()}"
    registry_table = f"main.default.{unique_id}_registry"

    model_uri = anomaly_engine.train(
        df=df,
        columns=["amount", "quantity"],
        model_name=model_name,
        registry_table=registry_table,
    )

    # Verify model URI is returned
    assert model_uri is not None

    # Verify model can be loaded with explicit name
    _, apply_fn = has_no_anomalies(
        merge_columns=["transaction_id"],
        columns=["amount", "quantity"],
        model=model_name,
        registry_table=registry_table,
        score_threshold=0.5,
    )

    result_df = apply_fn(df)

    # Verify _info column exists (anomaly_score is now internal, use _info.anomaly.score)
    assert "_info" in result_df.columns


def test_multiple_models_in_same_registry(spark: SparkSession, mock_workspace_client, make_random: str, anomaly_engine):
    """Test training multiple models in the same registry table."""
    unique_id = make_random(8).lower()
    registry_table = f"main.default.{unique_id}_registry"
    model_a = f"model_a_{make_random(4).lower()}"
    model_b = f"model_b_{make_random(4).lower()}"

    # Train first model
    df1 = spark.createDataFrame(
        [(100.0 + i * 0.5, 2.0) for i in range(50)],
        "amount double, quantity double",
    )

    anomaly_engine.train(
        df=df1,
        columns=["amount", "quantity"],
        model_name=model_a,
        registry_table=registry_table,
    )

    # Train second model with different columns
    df2 = spark.createDataFrame(
        [(0.1 + i * 0.01, 50.0) for i in range(50)],
        "discount double, weight double",
    )

    anomaly_engine.train(
        df=df2,
        columns=["discount", "weight"],
        model_name=model_b,
        registry_table=registry_table,
    )

    # Verify both models exist in registry
    registry_df = spark.table(registry_table)
    model_names = [row["model_name"] for row in registry_df.select("model_name").distinct().collect()]

    # Models are stored with full three-level names
    assert f"main.default.{model_a}" in model_names
    assert f"main.default.{model_b}" in model_names

    # Verify correct model is loaded for each check - call directly

    # Score with model_a
    _, apply_fn_a = has_no_anomalies(
        merge_columns=["transaction_id"],
        columns=["amount", "quantity"],
        model=model_a,
        registry_table=registry_table,
        score_threshold=0.5,
    )

    result_a = apply_fn_a(df1)
    assert "anomaly_score" in result_a.columns

    # Score with model_b
    _, apply_fn_b = has_no_anomalies(
        merge_columns=["transaction_id"],
        columns=["discount", "weight"],
        model=model_b,
        registry_table=registry_table,
        score_threshold=0.5,
    )

    result_b = apply_fn_b(df2)
    assert "anomaly_score" in result_b.columns


def test_active_model_retrieval(spark: SparkSession, make_random: str, anomaly_engine):
    """Test that get_active_model() returns the most recent model."""
    unique_id = make_random(8).lower()
    registry_table = f"main.default.{unique_id}_registry"
    model_name = f"test_active_{make_random(4).lower()}"

    df = spark.createDataFrame(
        [(100.0 + i * 0.5, 2.0) for i in range(50)],
        "amount double, quantity double",
    )

    # Train first version
    anomaly_engine.train(
        df=df,
        columns=["amount", "quantity"],
        model_name=model_name,
        registry_table=registry_table,
    )

    # Get active model (use full three-level name)
    registry = AnomalyModelRegistry(spark)
    full_model_name = f"main.default.{model_name}"
    model_v1 = registry.get_active_model(registry_table, full_model_name)

    assert model_v1 is not None
    assert model_v1.model_name == full_model_name
    assert model_v1.status == "active"
    v1_training_time = model_v1.training_time

    # Train second version (should archive first)
    anomaly_engine.train(
        df=df,
        columns=["amount", "quantity"],
        model_name=model_name,
        registry_table=registry_table,
    )

    # Get active model (should be v2, use full three-level name)
    model_v2 = registry.get_active_model(registry_table, full_model_name)

    assert model_v2 is not None
    assert model_v2.model_name == full_model_name
    assert model_v2.status == "active"
    assert model_v2.training_time > v1_training_time

    # Verify first model is archived
    archived_count = (
        spark.table(registry_table).filter(f"model_name = '{full_model_name}' AND status = 'archived'").count()
    )
    assert archived_count == 1


def test_model_staleness_warning(spark: SparkSession, mock_workspace_client, make_random: str, anomaly_engine):
    """Test that staleness warning is issued when model is >30 days old."""
    # Use standard 2D training data with sufficient variance
    df = spark.createDataFrame(
        get_standard_2d_training_data(),
        "amount double, quantity double",
    )

    unique_id = make_random(8).lower()
    model_name = f"test_stale_{make_random(4).lower()}"
    registry_table = f"main.default.{unique_id}_registry"

    # Train model
    anomaly_engine.train(
        df=df,
        columns=["amount", "quantity"],
        model_name=model_name,
        registry_table=registry_table,
    )

    # Mock old training_time in registry (use full three-level name)
    full_model_name = f"main.default.{model_name}"
    old_time = datetime.utcnow() - timedelta(days=35)
    spark.sql(
        f"UPDATE {registry_table} "
        f"SET training_time = timestamp('{old_time.strftime('%Y-%m-%d %H:%M:%S')}') "
        f"WHERE model_name = '{full_model_name}'"
    )

    # Score with old model (should issue warning) - call directly
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        _, apply_fn = has_no_anomalies(
            merge_columns=["transaction_id"],
            columns=["amount", "quantity"],
            model=model_name,
            registry_table=registry_table,
            score_threshold=0.5,
        )
        result_df = apply_fn(df)
        result_df.collect()  # Force evaluation

        # Check that staleness warning was issued
        stale_warnings = [warning for warning in w if "days old" in str(warning.message)]
        assert len(stale_warnings) > 0
        assert "Consider retraining" in str(stale_warnings[0].message)


def test_registry_table_schema(spark: SparkSession, make_random: str, anomaly_engine):
    """Test that registry table has all expected columns."""
    df = spark.createDataFrame(
        [(100.0 + i * 0.5, 2.0) for i in range(50)],
        "amount double, quantity double",
    )

    unique_id = make_random(8).lower()
    registry_table = f"main.default.{unique_id}_registry"
    model_name = f"test_schema_{make_random(4).lower()}"

    anomaly_engine.train(
        df=df,
        columns=["amount", "quantity"],
        model_name=model_name,
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


def test_registry_stores_metadata(spark: SparkSession, make_random: str, anomaly_engine):
    """Test that registry stores comprehensive metadata."""
    # Use standard 3D training data with sufficient variance
    df = spark.createDataFrame(
        get_standard_3d_training_data(),
        "amount double, quantity double, discount double",
    )

    unique_id = make_random(8).lower()
    registry_table = f"main.default.{unique_id}_registry"
    model_name = f"test_metadata_{make_random(4).lower()}"

    anomaly_engine.train(
        df=df,
        columns=["amount", "quantity", "discount"],
        model_name=model_name,
        registry_table=registry_table,
    )

    # Query registry (use full three-level name)
    full_model_name = f"main.default.{model_name}"
    record = spark.table(registry_table).filter(f"model_name = '{full_model_name}'").first()
    assert record is not None

    # Verify key metadata is present
    assert record["model_name"] == full_model_name
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

    model = registry.get_active_model("main.default.nonexistent_registry", "nonexistent_model")

    assert model is None


def test_nonexistent_model_returns_none(spark: SparkSession, make_random: str, anomaly_engine):
    """Test that get_active_model returns None for non-existent model."""
    df = spark.createDataFrame(
        [(100.0 + i * 0.5, 2.0) for i in range(50)],
        "amount double, quantity double",
    )

    unique_id = make_random(8).lower()
    registry_table = f"main.default.{unique_id}_registry"
    existing_model = f"existing_model_{make_random(4).lower()}"

    # Train a model
    anomaly_engine.train(
        df=df,
        columns=["amount", "quantity"],
        model_name=existing_model,
        registry_table=registry_table,
    )

    # Try to get non-existent model
    registry = AnomalyModelRegistry(spark)
    model = registry.get_active_model(registry_table, "nonexistent_model")

    assert model is None
