"""Integration tests for drift detection.
"""

from unittest.mock import MagicMock
import warnings

import pytest
from pyspark.sql import SparkSession

from databricks.labs.dqx.anomaly.drift_detector import compute_drift_score
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from tests.integration.test_anomaly_utils import create_anomaly_check_rule, train_simple_2d_model


@pytest.fixture
def mock_workspace_client():
    """Create a mock WorkspaceClient for testing."""
    return MagicMock(spec=WorkspaceClient)


@pytest.mark.nightly
def test_drift_detection_warns_on_distribution_shift(
    spark: SparkSession, mock_workspace_client, make_random: str, anomaly_engine
):
    """Test that drift warning is issued when data distribution shifts."""
    # Create unique table names for test isolation
    unique_id = make_random(8).lower()
    model_name = f"test_drift_{make_random(4).lower()}"
    registry_table = f"main.default.{unique_id}_registry"

    # Train on distribution centered at 100 - use helper
    train_simple_2d_model(spark, anomaly_engine, model_name, registry_table, train_size=100)

    # Test on distribution centered at 500 (significant shift)
    test_df = spark.createDataFrame(
        [(i, 500.0 + i, 5.0) for i in range(50)],
        "transaction_id int, amount double, quantity double",
    )

    dq_engine = DQEngine(mock_workspace_client)
    checks = [
        create_anomaly_check_rule(
            model_name=model_name,
            registry_table=registry_table,
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

        # Check that drift warning was issued (case-insensitive check)
        drift_warnings = [warning for warning in w if "drift detected" in str(warning.message).lower()]
        assert len(drift_warnings) > 0

        # Check that warning mentions drifted columns
        warning_message = str(drift_warnings[0].message)
        assert "amount" in warning_message.lower() or "quantity" in warning_message.lower()
        # Note: Global model warnings don't include "anomaly.train", they include retrain_cmd format


@pytest.mark.nightly
def test_no_drift_warning_on_similar_distribution(
    spark: SparkSession, mock_workspace_client, make_random: str, anomaly_engine
):
    """Test that no drift warning is issued when distributions are similar."""
    # Create unique table names for test isolation
    unique_id = make_random(8).lower()
    model_name = f"test_no_drift_{make_random(4).lower()}"
    registry_table = f"main.default.{unique_id}_registry"

    # Train on distribution - use helper
    train_simple_2d_model(spark, anomaly_engine, model_name, registry_table, train_size=100)

    # Test on similar distribution
    test_df = spark.createDataFrame(
        [(i, 100.0 + i, 2.0) for i in range(50)],
        "transaction_id int, amount double, quantity double",
    )

    dq_engine = DQEngine(mock_workspace_client)
    checks = [
        create_anomaly_check_rule(
            model_name=model_name,
            registry_table=registry_table,
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

        # Check that NO drift warning was issued (case-insensitive check)
        drift_warnings = [warning for warning in w if "drift detected" in str(warning.message).lower()]
        assert len(drift_warnings) == 0


@pytest.mark.nightly
def test_drift_detection_disabled_when_threshold_none(
    spark: SparkSession, mock_workspace_client, make_random: str, anomaly_engine
):
    """Test that drift detection is disabled when drift_threshold=None."""
    # Create unique table names for test isolation
    unique_id = make_random(8).lower()
    model_name = f"test_drift_disabled_{make_random(4).lower()}"
    registry_table = f"main.default.{unique_id}_registry"

    # Train model - use helper
    train_simple_2d_model(spark, anomaly_engine, model_name, registry_table, train_size=100)

    # Test on very different distribution
    test_df = spark.createDataFrame(
        [(i, 9999.0, 1.0) for i in range(10)],
        "transaction_id int, amount double, quantity double",
    )

    dq_engine = DQEngine(mock_workspace_client)
    checks = [
        create_anomaly_check_rule(
            model_name=model_name,
            registry_table=registry_table,
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

        # Check that NO drift warning was issued (case-insensitive check)
        drift_warnings = [warning for warning in w if "drift detected" in str(warning.message).lower()]
        assert len(drift_warnings) == 0


# ============================================================================
# Direct Drift Detector Module Tests
# ============================================================================


@pytest.mark.nightly
def test_drift_detector_no_drift_when_distributions_match(spark):
    """Test that drift is not detected when distributions are identical."""
    df = spark.createDataFrame(
        [(100.0,), (101.0,), (102.0,), (99.0,), (100.5,)],
        "value double",
    )

    baseline_stats = {
        "value": {
            "mean": 100.5,
            "std": 1.12,
            "min": 99.0,
            "max": 102.0,
            "p25": 99.5,
            "p50": 100.5,
            "p75": 101.5,
        }
    }

    result = compute_drift_score(df, ["value"], baseline_stats, threshold=3.0)

    assert not result.drift_detected
    assert result.drift_score < 3.0
    assert len(result.drifted_columns) == 0


@pytest.mark.nightly
def test_drift_detector_drift_detected_when_mean_shifts(spark):
    """Test that drift is detected when mean shifts significantly."""
    df = spark.createDataFrame(
        [(500.0,), (501.0,), (502.0,), (499.0,), (500.5,)],
        "value double",
    )

    baseline_stats = {
        "value": {
            "mean": 100.0,
            "std": 1.0,
            "min": 99.0,
            "max": 102.0,
            "p25": 99.5,
            "p50": 100.0,
            "p75": 101.0,
        }
    }

    result = compute_drift_score(df, ["value"], baseline_stats, threshold=3.0)

    assert result.drift_detected
    assert result.drift_score > 3.0
    assert "value" in result.drifted_columns
    assert result.recommendation == "retrain"


@pytest.mark.nightly
def test_drift_detector_with_multiple_columns(spark):
    """Test drift detection with multiple columns."""
    df = spark.createDataFrame(
        [(100.0, 500.0), (101.0, 501.0), (102.0, 502.0)],
        "col1 double, col2 double",
    )

    baseline_stats = {
        "col1": {"mean": 100.0, "std": 1.0, "min": 99.0, "max": 102.0, "p25": 99.5, "p50": 100.0, "p75": 101.0},
        "col2": {"mean": 100.0, "std": 1.0, "min": 99.0, "max": 102.0, "p25": 99.5, "p50": 100.0, "p75": 101.0},
    }

    result = compute_drift_score(df, ["col1", "col2"], baseline_stats, threshold=3.0)

    assert result.drift_detected
    assert "col2" in result.drifted_columns
    assert "col1" not in result.drifted_columns
