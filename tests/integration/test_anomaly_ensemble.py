"""Integration tests for ensemble anomaly detection."""

import pytest
from pyspark.sql import SparkSession

from databricks.labs.dqx import anomaly
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from unittest.mock import MagicMock

# Check if IsolationForest is available (only in Databricks Runtime)
ISOLATION_FOREST_AVAILABLE = False
try:
    import pyspark.ml.classification as pyspark_ml_classification

    if hasattr(pyspark_ml_classification, 'IsolationForest'):
        ISOLATION_FOREST_AVAILABLE = True
except ImportError:
    pass

pytestmark = pytest.mark.skipif(
    not ISOLATION_FOREST_AVAILABLE, reason="IsolationForest only available in Databricks Runtime (DBR >= 15.4)"
)


@pytest.fixture
def mock_workspace_client():
    """Create a mock WorkspaceClient for testing."""
    return MagicMock(spec=WorkspaceClient)


def test_ensemble_training(spark: SparkSession):
    """Test training an ensemble of models."""
    df = spark.createDataFrame(
        [(100.0 + i, 2.0) for i in range(100)],
        "amount double, quantity double",
    )

    # Train ensemble with 3 models
    params = anomaly.AnomalyParams(
        sample_fraction=1.0,
        max_rows=100,
        ensemble_size=3,
    )

    model_uri = anomaly.train(
        df=df,
        columns=["amount", "quantity"],
        model_name="test_ensemble",
        registry_table="main.default.test_anomaly_ensemble_registry",
        params=params,
    )

    # Check that multiple URIs are returned
    assert "," in model_uri
    uris = model_uri.split(",")
    assert len(uris) == 3


def test_ensemble_scoring_with_confidence(spark: SparkSession, mock_workspace_client):
    """Test scoring with ensemble model returns confidence scores."""
    # Training data
    train_df = spark.createDataFrame(
        [(100.0 + i, 2.0) for i in range(50)],
        "amount double, quantity double",
    )

    # Train ensemble
    params = anomaly.AnomalyParams(
        sample_fraction=1.0,
        max_rows=50,
        ensemble_size=2,
    )

    anomaly.train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name="test_ensemble_scoring",
        registry_table="main.default.test_anomaly_ensemble_scoring_registry",
        params=params,
    )

    # Test data
    test_df = spark.createDataFrame(
        [(100.0, 2.0), (500.0, 1.0)],  # One normal, one anomaly
        "amount double, quantity double",
    )

    # Apply check with confidence
    dq_engine = DQEngine(mock_workspace_client)
    checks = [
        anomaly.has_no_anomalies(
            columns=["amount", "quantity"],
            model="test_ensemble_scoring",
            registry_table="main.default.test_anomaly_ensemble_scoring_registry",
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


def test_ensemble_with_feature_contributions(spark: SparkSession, mock_workspace_client):
    """Test that ensemble works with feature contributions."""
    train_df = spark.createDataFrame(
        [(100.0, 2.0, 0.1) for i in range(30)],
        "amount double, quantity double, discount double",
    )

    params = anomaly.AnomalyParams(
        sample_fraction=1.0,
        max_rows=30,
        ensemble_size=2,
    )

    anomaly.train(
        df=train_df,
        columns=["amount", "quantity", "discount"],
        model_name="test_ensemble_contributions",
        registry_table="main.default.test_ensemble_contrib_registry",
        params=params,
    )

    test_df = spark.createDataFrame(
        [(100.0, 2.0, 0.1), (9999.0, 1.0, 0.95)],
        "amount double, quantity double, discount double",
    )

    dq_engine = DQEngine(mock_workspace_client)
    checks = [
        anomaly.has_no_anomalies(
            columns=["amount", "quantity", "discount"],
            model="test_ensemble_contributions",
            registry_table="main.default.test_ensemble_contrib_registry",
            score_threshold=0.5,
            include_contributions=True,
            include_confidence=True,
        )
    ]

    result_df = dq_engine.apply_checks_by_metadata(test_df, checks)

    # Check both confidence and contributions exist
    assert "anomaly_score_std" in result_df.columns
    assert "anomaly_contributions" in result_df.columns
