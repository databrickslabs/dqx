"""Integration tests for drift detection."""

from unittest.mock import MagicMock
import warnings

import pytest
from pyspark.sql import SparkSession

from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from tests.integration.test_anomaly_utils import create_anomaly_check_rule


@pytest.fixture
def mock_workspace_client():
    """Create a mock WorkspaceClient for testing."""
    return MagicMock(spec=WorkspaceClient)


def test_drift_detection_warns_on_distribution_shift(spark: SparkSession, mock_workspace_client, anomaly_engine):
    """Test that drift warning is issued when data distribution shifts."""
    # Train on distribution centered at 100
    train_df = spark.createDataFrame(
        [(100.0 + i, 2.0) for i in range(100)],
        "amount double, quantity double",
    )

    anomaly_engine.train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name="test_drift",
        registry_table="main.default.test_drift_registry",
    )

    # Test on distribution centered at 500 (significant shift)
    test_df = spark.createDataFrame(
        [(500.0 + i, 5.0) for i in range(50)],
        "amount double, quantity double",
    )

    dq_engine = DQEngine(mock_workspace_client)
    checks = [
        create_anomaly_check_rule(
            model_name="test_drift",
            registry_table="main.default.test_drift_registry",
            columns=["amount", "quantity"],
            score_threshold=0.5,
            drift_threshold=3.0,
        )
    ]

    # Capture warnings
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        result_df = dq_engine.apply_checks(test_df, checks)
        result_df.collect()  # Force evaluation

        # Check that drift warning was issued
        drift_warnings = [warning for warning in w if "Data drift detected" in str(warning.message)]
        assert len(drift_warnings) > 0

        # Check that warning mentions drifted columns
        warning_message = str(drift_warnings[0].message)
        assert "amount" in warning_message or "quantity" in warning_message
        assert "anomaly.train" in warning_message  # Retrain recommendation


def test_no_drift_warning_on_similar_distribution(spark: SparkSession, mock_workspace_client, anomaly_engine):
    """Test that no drift warning is issued when distributions are similar."""
    # Train on distribution
    train_df = spark.createDataFrame(
        [(100.0 + i, 2.0) for i in range(100)],
        "amount double, quantity double",
    )

    anomaly_engine.train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name="test_no_drift",
        registry_table="main.default.test_no_drift_registry",
    )

    # Test on similar distribution
    test_df = spark.createDataFrame(
        [(100.0 + i, 2.0) for i in range(50)],
        "amount double, quantity double",
    )

    dq_engine = DQEngine(mock_workspace_client)
    checks = [
        create_anomaly_check_rule(
            model_name="test_no_drift",
            registry_table="main.default.test_no_drift_registry",
            columns=["amount", "quantity"],
            score_threshold=0.5,
            drift_threshold=3.0,
        )
    ]

    # Capture warnings
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        result_df = dq_engine.apply_checks(test_df, checks)
        result_df.collect()

        # Check that NO drift warning was issued
        drift_warnings = [warning for warning in w if "Data drift detected" in str(warning.message)]
        assert len(drift_warnings) == 0


def test_drift_detection_disabled_when_threshold_none(spark: SparkSession, mock_workspace_client, anomaly_engine):
    """Test that drift detection is disabled when drift_threshold=None."""
    train_df = spark.createDataFrame(
        [(100.0 + i, 2.0) for i in range(100)],
        "amount double, quantity double",
    )

    anomaly_engine.train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name="test_drift_disabled",
        registry_table="main.default.test_drift_disabled_registry",
    )

    # Test on very different distribution
    test_df = spark.createDataFrame(
        [(9999.0, 1.0) for i in range(10)],
        "amount double, quantity double",
    )

    dq_engine = DQEngine(mock_workspace_client)
    checks = [
        create_anomaly_check_rule(
            model_name="test_drift_disabled",
            registry_table="main.default.test_drift_disabled_registry",
            columns=["amount", "quantity"],
            score_threshold=0.5,
            drift_threshold=None,  # Disable drift detection
        )
    ]

    # Capture warnings
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        result_df = dq_engine.apply_checks(test_df, checks)
        result_df.collect()

        # Check that NO drift warning was issued
        drift_warnings = [warning for warning in w if "Data drift detected" in str(warning.message)]
        assert len(drift_warnings) == 0
