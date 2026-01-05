"""Integration tests for ensemble anomaly detection."""

from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession

from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.anomaly import AnomalyParams, has_no_anomalies
from databricks.sdk import WorkspaceClient
from tests.integration.test_anomaly_utils import train_simple_2d_model, train_simple_3d_model

# Ensemble tests validate ensemble-specific behavior (confidence scores, multiple model URIs)
# Ensemble mode uses sklearn.ensemble.IsolationForest (same as single models, just trains multiple)


@pytest.fixture
def mock_workspace_client():
    """Create a mock WorkspaceClient for testing."""
    return MagicMock(spec=WorkspaceClient)


@pytest.mark.nightly
def test_ensemble_training(spark: SparkSession, make_random, anomaly_engine):
    """Test training an ensemble of models."""
    unique_id = make_random(8).lower()
    model_name = f"test_ensemble_{make_random(4).lower()}"
    registry_table = f"main.default.{unique_id}_registry"

    # Train ensemble with 3 models - use helper
    params = AnomalyParams(
        sample_fraction=1.0,
        max_rows=100,
        ensemble_size=3,
    )
    train_simple_2d_model(spark, anomaly_engine, model_name, registry_table, train_size=100, params=params)

    # Get model_uri from registry
    full_model_name = f"main.default.{model_name}"
    record = spark.table(registry_table).filter(f"model_name = '{full_model_name}'").first()
    assert record is not None
    model_uri = record["model_uri"]

    # Check that multiple URIs are returned
    assert "," in model_uri
    uris = model_uri.split(",")
    assert len(uris) == 3


@pytest.mark.nightly
def test_ensemble_scoring_with_confidence(spark: SparkSession, mock_workspace_client, make_random, anomaly_engine):
    """Test scoring with ensemble model returns confidence scores."""
    unique_id = make_random(8).lower()
    model_name = f"test_ensemble_scoring_{make_random(4).lower()}"
    registry_table = f"main.default.{unique_id}_registry"

    # Train ensemble - use helper
    params = AnomalyParams(
        sample_fraction=1.0,
        max_rows=50,
        ensemble_size=2,
    )
    train_simple_2d_model(spark, anomaly_engine, model_name, registry_table, train_size=50, params=params)

    # Test data - ADD transaction_id column
    test_df = spark.createDataFrame(
        [(1, 100.0, 2.0), (2, 500.0, 1.0)],  # One normal, one anomaly
        "transaction_id int, amount double, quantity double",
    )

    # Apply check with confidence
    dq_engine = DQEngine(mock_workspace_client)
    checks = [
        has_no_anomalies(
            merge_columns=["transaction_id"],
            columns=["amount", "quantity"],
            model=model_name,
            registry_table=registry_table,
            score_threshold=0.5,
            include_confidence=True,
        )
    ]

    result_df = dq_engine.apply_checks_by_metadata(test_df, checks)

    # Check that confidence column exists
    assert "anomaly_score_std" in result_df.columns

    # Check that scores vary (std > 0 for some rows)
    std_values = [row["anomaly_score_std"] for row in result_df.collect()]
    assert any(std > 0 for std in std_values)


@pytest.mark.nightly
def test_ensemble_with_feature_contributions(spark: SparkSession, mock_workspace_client, make_random, anomaly_engine):
    """Test that ensemble works with feature contributions."""
    unique_id = make_random(8).lower()
    model_name = f"test_ensemble_contributions_{make_random(4).lower()}"
    registry_table = f"main.default.{unique_id}_registry"

    # Train ensemble with 3D model - use helper
    params = AnomalyParams(
        sample_fraction=1.0,
        max_rows=30,
        ensemble_size=2,
    )
    train_simple_3d_model(spark, anomaly_engine, model_name, registry_table, train_size=30, params=params)

    # Test data - ADD transaction_id column
    test_df = spark.createDataFrame(
        [(1, 100.0, 2.0, 0.1), (2, 9999.0, 1.0, 0.95)],
        "transaction_id int, amount double, quantity double, discount double",
    )

    dq_engine = DQEngine(mock_workspace_client)
    checks = [
        has_no_anomalies(
            merge_columns=["transaction_id"],
            columns=["amount", "quantity", "discount"],
            model=model_name,
            registry_table=registry_table,
            score_threshold=0.5,
            include_contributions=True,
            include_confidence=True,
        )
    ]

    result_df = dq_engine.apply_checks_by_metadata(test_df, checks)

    # Check both confidence and contributions exist
    assert "anomaly_score_std" in result_df.columns
    assert "anomaly_contributions" in result_df.columns
