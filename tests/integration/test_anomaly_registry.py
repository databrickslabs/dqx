"""Integration tests for anomaly model registry management."""

from collections.abc import Callable
import logging
from datetime import datetime, timedelta
from unittest.mock import MagicMock
import warnings

import pytest
from pyspark.sql import SparkSession

from databricks.labs.dqx.anomaly import has_no_anomalies
from databricks.labs.dqx.anomaly.model_registry import AnomalyModelRegistry, compute_config_hash
from databricks.labs.dqx.errors import InvalidParameterError
from databricks.sdk import WorkspaceClient
from tests.integration.test_anomaly_utils import (
    get_standard_2d_training_data,
    get_standard_3d_training_data,
    score_with_anomaly_check,
    train_simple_2d_model,
    train_simple_3d_model,
)


@pytest.fixture
def mock_workspace_client():
    """Create a mock WorkspaceClient for testing."""
    return MagicMock(spec=WorkspaceClient)


def test_explicit_model_names(
    spark: SparkSession, mock_workspace_client, make_random: Callable[[int], str], anomaly_engine
):
    """Test training with explicit model_name and registry_table."""
    unique_id = make_random(8).lower()
    model_name = f"my_custom_model_{make_random(4).lower()}"
    registry_table = f"main.default.{unique_id}_registry"

    # Train model - use helper
    train_simple_2d_model(spark, anomaly_engine, model_name, registry_table)

    # Create df with transaction_id for scoring
    df = spark.createDataFrame(
        [(i, 100.0 + i, 2.0) for i in range(50)],
        "transaction_id int, amount double, quantity double",
    )

    # Verify model can be loaded with explicit name - use helper
    result_df = score_with_anomaly_check(df, model_name, registry_table, ["amount", "quantity"])

    # Verify _info column exists (anomaly_score is now internal, use _info.anomaly.score)
    assert "_info" in result_df.columns


def test_multiple_models_in_same_registry(
    spark: SparkSession,
    mock_workspace_client,
    make_random: Callable[[int], str],
    anomaly_engine,
    test_df_factory,
    anomaly_scorer,
):
    """Test training multiple models in the same registry table."""
    unique_id = make_random(8).lower()
    registry_table = f"main.default.{unique_id}_registry"
    model_a = f"model_a_{make_random(4).lower()}"
    model_b = f"model_b_{make_random(4).lower()}"

    # Train first model - use helper
    train_simple_2d_model(spark, anomaly_engine, model_a, registry_table)
    df1 = test_df_factory(
        spark,
        normal_rows=[(100.0 + i * 0.5, 2.0) for i in range(50)],
        anomaly_rows=[],
        columns_schema="amount double, quantity double",
    )

    # Train second model with different columns - custom data
    df2_train = spark.createDataFrame(
        [(0.1 + i * 0.01, 50.0) for i in range(50)],
        "discount double, weight double",
    )
    anomaly_engine.train(
        df=df2_train,
        columns=["discount", "weight"],
        model_name=model_b,
        registry_table=registry_table,
    )
    df2 = test_df_factory(
        spark,
        normal_rows=[(0.1 + i * 0.01, 50.0) for i in range(50)],
        anomaly_rows=[],
        columns_schema="discount double, weight double",
    )

    # Verify both models exist in registry
    registry_df = spark.table(registry_table)
    model_names = [row["model_name"] for row in registry_df.select("identity.model_name").distinct().collect()]

    # Models are stored with full three-level names
    assert f"main.default.{model_a}" in model_names
    assert f"main.default.{model_b}" in model_names

    # Verify correct model is loaded for each check - use anomaly_scorer

    # Score with model_a
    result_a = anomaly_scorer(
        df1, model_name=model_a, registry_table=registry_table, columns=["amount", "quantity"], score_threshold=0.5
    )
    assert "anomaly_score" in result_a.columns

    # Score with model_b
    result_b = anomaly_scorer(
        df2, model_name=model_b, registry_table=registry_table, columns=["discount", "weight"], score_threshold=0.5
    )
    assert "anomaly_score" in result_b.columns


def test_active_model_retrieval(spark: SparkSession, make_random: Callable[[int], str], anomaly_engine):
    """Test that get_active_model() returns the most recent model."""
    unique_id = make_random(8).lower()
    registry_table = f"main.default.{unique_id}_registry"
    model_name = f"test_active_{make_random(4).lower()}"

    # Train first version - use helper
    train_simple_2d_model(spark, anomaly_engine, model_name, registry_table)

    # Get active model (use full three-level name)
    registry = AnomalyModelRegistry(spark)
    full_model_name = f"main.default.{model_name}"
    model_v1 = registry.get_active_model(registry_table, full_model_name)

    assert model_v1 is not None
    assert model_v1.identity.model_name == full_model_name
    assert model_v1.identity.status == "active"
    v1_training_time = model_v1.training.training_time

    # Train second version (should archive first) - use helper
    train_simple_2d_model(spark, anomaly_engine, model_name, registry_table)

    # Get active model (should be v2, use full three-level name)
    model_v2 = registry.get_active_model(registry_table, full_model_name)

    assert model_v2 is not None
    assert model_v2.identity.model_name == full_model_name
    assert model_v2.identity.status == "active"
    assert model_v2.training.training_time > v1_training_time

    # Verify first model is archived
    archived_count = (
        spark.table(registry_table)
        .filter(f"identity.model_name = '{full_model_name}' AND identity.status = 'archived'")
        .count()
    )
    assert archived_count == 1


def test_model_staleness_warning(
    spark: SparkSession, mock_workspace_client, make_random: Callable[[int], str], anomaly_engine, test_df_factory
):
    """Test that staleness warning is issued when model is >30 days old."""
    unique_id = make_random(8).lower()
    model_name = f"test_stale_{make_random(4).lower()}"
    registry_table = f"main.default.{unique_id}_registry"

    # Train model - use helper with standard training data
    train_simple_2d_model(spark, anomaly_engine, model_name, registry_table, train_data=get_standard_2d_training_data())

    # Create df for scoring - use factory
    df = test_df_factory(
        spark,
        normal_rows=get_standard_2d_training_data(),
        anomaly_rows=[],
        columns_schema="amount double, quantity double",
    )

    # Mock old training_time in registry (use full three-level name)
    full_model_name = f"main.default.{model_name}"
    old_time = datetime.utcnow() - timedelta(days=35)
    spark.sql(
        f"UPDATE {registry_table} "
        f"SET training.training_time = timestamp('{old_time.strftime('%Y-%m-%d %H:%M:%S')}') "
        f"WHERE identity.model_name = '{full_model_name}'"
    )

    # Score with old model (should issue warning) - use helper
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        _ = score_with_anomaly_check(df, model_name, registry_table, ["amount", "quantity"])

        # Check that staleness warning was issued
        stale_warnings = [warning for warning in w if "days old" in str(warning.message)]
        assert len(stale_warnings) > 0
        assert "Consider retraining" in str(stale_warnings[0].message)


def test_registry_table_schema(spark: SparkSession, make_random: Callable[[int], str], anomaly_engine):
    """Test that registry table has all expected columns."""
    unique_id = make_random(8).lower()
    registry_table = f"main.default.{unique_id}_registry"
    model_name = f"test_schema_{make_random(4).lower()}"

    # Train model - use helper
    train_simple_2d_model(spark, anomaly_engine, model_name, registry_table)

    # Verify all expected nested struct columns exist
    registry_df = spark.table(registry_table)
    expected_top_level_columns = ["identity", "training", "features", "segmentation"]

    actual_columns = registry_df.columns

    for col in expected_top_level_columns:
        assert col in actual_columns, f"Missing top-level column: {col}"

    # Verify schema contains expected nested fields
    schema_str = str(registry_df.schema)
    expected_nested_fields = [
        "model_name",
        "model_uri",
        "algorithm",
        "mlflow_run_id",
        "status",
        "columns",
        "hyperparameters",
        "training_rows",
        "training_time",
        "metrics",
        "baseline_stats",
        "mode",
        "feature_importance",
        "temporal_config",
        "column_types",
        "feature_metadata",
        "segment_by",
        "segment_values",
        "is_global_model",
        "sklearn_version",
        "config_hash",
    ]

    for field in expected_nested_fields:
        assert field in schema_str, f"Missing nested field in schema: {field}"


def test_registry_stores_metadata(spark: SparkSession, make_random: Callable[[int], str], anomaly_engine):
    """Test that registry stores comprehensive metadata."""
    unique_id = make_random(8).lower()
    registry_table = f"main.default.{unique_id}_registry"
    model_name = f"test_metadata_{make_random(4).lower()}"

    # Train model - use helper with standard 3D training data
    train_simple_3d_model(spark, anomaly_engine, model_name, registry_table, train_data=get_standard_3d_training_data())

    # Query registry (use full three-level name)
    full_model_name = f"main.default.{model_name}"
    record = spark.table(registry_table).filter(f"identity.model_name = '{full_model_name}'").first()
    assert record is not None

    # Verify key metadata is present
    assert record["identity"]["model_name"] == full_model_name
    assert record["identity"]["model_uri"] is not None
    assert record["identity"]["algorithm"] == "IsolationForest"
    assert record["identity"]["status"] == "active"
    assert record["training"]["training_time"] is not None
    assert record["training"]["columns"] == ["amount", "quantity", "discount"]

    # Verify baseline_stats exists
    assert record["training"]["baseline_stats"] is not None
    assert len(record["training"]["baseline_stats"]) == 3  # Three columns

    # Verify feature_importance exists
    assert record["features"]["feature_importance"] is not None
    assert len(record["features"]["feature_importance"]) == 3  # Three features

    # Verify metrics exist
    assert record["training"]["metrics"] is not None
    assert "recommended_threshold" in record["training"]["metrics"]


def test_nonexistent_registry_returns_none(spark: SparkSession):
    """Test that get_active_model returns None for non-existent registry."""
    registry = AnomalyModelRegistry(spark)

    model = registry.get_active_model("main.default.nonexistent_registry", "nonexistent_model")

    assert model is None


def test_nonexistent_model_returns_none(spark: SparkSession, make_random: Callable[[int], str], anomaly_engine):
    """Test that get_active_model returns None for non-existent model."""
    unique_id = make_random(8).lower()
    registry_table = f"main.default.{unique_id}_registry"
    existing_model = f"existing_model_{make_random(4).lower()}"

    # Train a model - use helper
    train_simple_2d_model(spark, anomaly_engine, existing_model, registry_table)

    # Try to get non-existent model
    registry = AnomalyModelRegistry(spark)
    model = registry.get_active_model(registry_table, "nonexistent_model")

    assert model is None


def test_config_hash_stability(spark: SparkSession):
    """Test that config_hash is stable for same inputs."""
    columns = ["amount", "quantity", "discount"]
    segment_by = ["region", "category"]

    # Compute hash multiple times
    hash1 = compute_config_hash(columns, segment_by)
    hash2 = compute_config_hash(columns, segment_by)

    assert hash1 == hash2
    assert len(hash1) == 16  # 16 hex characters


def test_config_hash_order_independence(spark: SparkSession):
    """Test that config_hash is order-independent (columns are sorted internally)."""
    # Different order should produce same hash
    hash1 = compute_config_hash(["amount", "quantity", "discount"], ["region", "category"])
    hash2 = compute_config_hash(["discount", "amount", "quantity"], ["category", "region"])

    assert hash1 == hash2


def test_config_hash_differentiation(spark: SparkSession):
    """Test that different configs produce different hashes."""
    hash1 = compute_config_hash(["amount", "quantity"], None)
    hash2 = compute_config_hash(["amount", "quantity", "discount"], None)
    hash3 = compute_config_hash(["amount", "quantity"], ["region"])

    assert hash1 != hash2  # Different columns
    assert hash1 != hash3  # Different segment_by
    assert hash2 != hash3  # Different columns and segment_by


def test_config_hash_stored_during_training(spark: SparkSession, make_random: Callable[[int], str], anomaly_engine):
    """Test that config_hash is stored in registry during training."""
    unique_id = make_random(8).lower()
    registry_table = f"main.default.{unique_id}_registry"
    model_name = f"test_config_hash_{make_random(4).lower()}"
    columns = ["amount", "quantity"]

    # Train model - use helper
    train_simple_2d_model(spark, anomaly_engine, model_name, registry_table)

    # Verify config_hash is stored
    full_model_name = f"main.default.{model_name}"
    record = spark.table(registry_table).filter(f"identity.model_name = '{full_model_name}'").first()

    assert record is not None
    assert record["segmentation"]["config_hash"] is not None

    # Verify hash matches expected
    expected_hash = compute_config_hash(columns, None)
    assert record["segmentation"]["config_hash"] == expected_hash


def test_config_change_warning(spark: SparkSession, make_random: Callable[[int], str], anomaly_engine, caplog):
    """Test that warning is issued when retraining with different config."""
    unique_id = make_random(8).lower()
    registry_table = f"main.default.{unique_id}_registry"
    model_name = f"test_config_warn_{make_random(4).lower()}"

    # Train with initial columns (2D) - use helper
    train_simple_2d_model(spark, anomaly_engine, model_name, registry_table)

    # Create 3D df for retraining
    df = spark.createDataFrame(
        [(100.0 + i * 0.5, 2.0, 0.1) for i in range(50)],
        "amount double, quantity double, discount double",
    )

    # Retrain with different columns (should warn)
    with caplog.at_level(logging.WARNING):
        anomaly_engine.train(
            df=df,
            columns=["amount", "quantity", "discount"],
            model_name=model_name,
            registry_table=registry_table,
        )

    # Check that config change warning was logged
    warning_messages = [record.message for record in caplog.records if record.levelname == "WARNING"]
    config_warnings = [msg for msg in warning_messages if "different configuration" in msg]
    assert len(config_warnings) > 0, f"Expected config warning, got: {warning_messages}"
    assert "will be archived" in config_warnings[0]


def test_scoring_validates_config_hash(spark: SparkSession, make_random: Callable[[int], str], anomaly_engine):
    """Test that scoring validates config hash and errors on mismatch."""
    unique_id = make_random(8).lower()
    registry_table = f"main.default.{unique_id}_registry"
    model_name = f"test_score_validate_{make_random(4).lower()}"

    # Train with specific columns (2D) - use helper
    train_simple_2d_model(spark, anomaly_engine, model_name, registry_table)

    # Try to score with different columns (should error)
    df_score = spark.createDataFrame(
        [(100.0 + i * 0.5, 2.0, 0.1) for i in range(10)],
        "amount double, quantity double, discount double",
    )

    with pytest.raises(InvalidParameterError) as exc_info:
        _, apply_fn = has_no_anomalies(
            merge_columns=["transaction_id"],
            columns=["amount", "quantity", "discount"],  # Different columns!
            model=model_name,
            registry_table=registry_table,
            score_threshold=0.5,
        )
        result = apply_fn(df_score)
        result.collect()  # Force evaluation

    assert "Configuration mismatch" in str(exc_info.value)
    assert "Expected columns" in str(exc_info.value)
