"""Integration tests for ensemble anomaly detection."""

from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession

from databricks.labs.dqx.anomaly import AnomalyParams
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
    record = spark.table(registry_table).filter(f"identity.model_name = '{full_model_name}'").first()
    assert record is not None
    model_uri = record["identity"]["model_uri"]

    # Check that multiple URIs are returned
    assert "," in model_uri
    uris = model_uri.split(",")
    assert len(uris) == 3


@pytest.mark.nightly
def test_ensemble_scoring_with_confidence(
    spark: SparkSession, mock_workspace_client, make_random, anomaly_engine, test_df_factory, anomaly_scorer
):
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

    # Test data - use factory
    test_df = test_df_factory(
        spark,
        normal_rows=[(100.0, 2.0)],
        anomaly_rows=[(500.0, 1.0)],
        columns_schema="amount double, quantity double",
    )

    # Apply check with confidence - use anomaly_scorer
    result_df = anomaly_scorer(
        test_df,
        model_name=model_name,
        registry_table=registry_table,
        columns=["amount", "quantity"],
        score_threshold=0.5,
        include_confidence=True,
        extract_score=False,
    )

    # Check that confidence column exists in _info
    row = result_df.collect()[0]
    assert row["_info"]["anomaly"]["confidence_std"] is not None

    # Check that scores vary (std > 0 for some rows)
    std_values = [r["_info"]["anomaly"]["confidence_std"] for r in result_df.collect()]
    assert any(std is not None and std > 0 for std in std_values)


@pytest.mark.nightly
def test_ensemble_with_feature_contributions(
    spark: SparkSession, mock_workspace_client, make_random, anomaly_engine, test_df_factory, anomaly_scorer
):
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

    # Test data - use factory
    test_df = test_df_factory(
        spark,
        normal_rows=[(100.0, 2.0, 0.1)],
        anomaly_rows=[(9999.0, 1.0, 0.95)],
        columns_schema="amount double, quantity double, discount double",
    )

    # Apply check with confidence and contributions - use anomaly_scorer
    result_df = anomaly_scorer(
        test_df,
        model_name=model_name,
        registry_table=registry_table,
        columns=["amount", "quantity", "discount"],
        score_threshold=0.5,
        include_contributions=True,
        include_confidence=True,
        extract_score=False,
    )

    # Check both confidence and contributions exist in _info
    row = result_df.collect()[0]
    assert row["_info"]["anomaly"]["confidence_std"] is not None
    assert row["_info"]["anomaly"]["contributions"] is not None
