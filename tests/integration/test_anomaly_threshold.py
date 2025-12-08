"""Integration tests for anomaly threshold selection."""

import pytest
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from unittest.mock import MagicMock

from databricks.labs.dqx.anomaly import train, has_no_anomalies
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient


@pytest.fixture
def mock_workspace_client():
    """Create a mock WorkspaceClient for testing."""
    return MagicMock(spec=WorkspaceClient)


def test_threshold_affects_flagging(spark: SparkSession, mock_workspace_client):
    """Test that different thresholds flag different numbers of anomalies."""
    train_df = spark.createDataFrame(
        [(100.0, 2.0) for i in range(50)],
        "amount double, quantity double",
    )
    
    train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name="test_threshold",
        registry_table="main.default.test_threshold_registry",
    )
    
    # Test data with varying degrees of anomalousness
    test_df = spark.createDataFrame(
        [
            (100.0, 2.0),  # Normal
            (150.0, 1.8),  # Slightly unusual
            (500.0, 1.0),  # Moderately unusual
            (9999.0, 0.5),  # Highly unusual
        ],
        "amount double, quantity double",
    )
    
    dq_engine = DQEngine(mock_workspace_client)
    
    # Aggressive threshold (0.3) - flags more
    checks_aggressive = [
        has_no_anomalies(
            columns=["amount", "quantity"],
            model="test_threshold",
            registry_table="main.default.test_threshold_registry",
            score_threshold=0.3,
        )
    ]
    
    result_aggressive = dq_engine.apply_checks_by_metadata(test_df, checks_aggressive)
    errors_aggressive = result_aggressive.filter(F.size("_errors") > 0).count()
    
    # Conservative threshold (0.9) - flags less
    checks_conservative = [
        has_no_anomalies(
            columns=["amount", "quantity"],
            model="test_threshold",
            registry_table="main.default.test_threshold_registry",
            score_threshold=0.9,
        )
    ]
    
    result_conservative = dq_engine.apply_checks_by_metadata(test_df, checks_conservative)
    errors_conservative = result_conservative.filter(F.size("_errors") > 0).count()
    
    # More aggressive threshold should flag more rows
    assert errors_aggressive >= errors_conservative


def test_recommended_threshold_stored(spark: SparkSession):
    """Test that recommended_threshold is stored in registry metrics."""
    train_df = spark.createDataFrame(
        [(100.0, 2.0) for i in range(50)],
        "amount double, quantity double",
    )
    
    model_name = "test_recommended"
    registry_table = "main.default.test_recommended_registry"
    
    train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name=model_name,
        registry_table=registry_table,
    )
    
    # Query recommended threshold from registry
    record = spark.table(registry_table).filter(f"model_name = '{model_name}'").first()
    
    # Verify recommended_threshold exists in metrics
    assert record["metrics"] is not None
    assert "recommended_threshold" in record["metrics"]
    
    # Verify it's a reasonable value (between 0 and 1)
    recommended = record["metrics"]["recommended_threshold"]
    assert 0.0 <= recommended <= 1.0


def test_using_recommended_threshold(spark: SparkSession, mock_workspace_client):
    """Test using recommended_threshold from registry in checks."""
    train_df = spark.createDataFrame(
        [(100.0, 2.0) for i in range(50)],
        "amount double, quantity double",
    )
    
    model_name = "test_use_recommended"
    registry_table = "main.default.test_use_recommended_registry"
    
    train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name=model_name,
        registry_table=registry_table,
    )
    
    # Query recommended threshold
    recommended = spark.sql(f"""
        SELECT metrics['recommended_threshold'] as threshold
        FROM {registry_table}
        WHERE model_name = '{model_name}' AND status = 'active'
    """).first()["threshold"]
    
    # Use recommended threshold in check
    test_df = spark.createDataFrame(
        [(100.0, 2.0), (9999.0, 1.0)],
        "amount double, quantity double",
    )
    
    dq_engine = DQEngine(mock_workspace_client)
    checks = [
        has_no_anomalies(
            columns=["amount", "quantity"],
            model=model_name,
            registry_table=registry_table,
            score_threshold=recommended,
        )
    ]
    
    result_df = dq_engine.apply_checks_by_metadata(test_df, checks)
    
    # Should work correctly
    assert "anomaly_score" in result_df.columns


def test_precision_recall_tradeoff(spark: SparkSession, mock_workspace_client):
    """Test that lower threshold increases recall (catches more anomalies)."""
    train_df = spark.createDataFrame(
        [(100.0, 2.0) for i in range(50)],
        "amount double, quantity double",
    )
    
    train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name="test_precision_recall",
        registry_table="main.default.test_precision_recall_registry",
    )
    
    # Test data with clear anomalies
    test_df = spark.createDataFrame(
        [
            (100.0, 2.0),  # Normal
            (9999.0, 0.1),  # Extreme anomaly
            (8888.0, 0.2),  # Extreme anomaly
        ],
        "amount double, quantity double",
    )
    
    dq_engine = DQEngine(mock_workspace_client)
    
    # High threshold (low recall)
    checks_low_recall = [
        has_no_anomalies(
            columns=["amount", "quantity"],
            model="test_precision_recall",
            registry_table="main.default.test_precision_recall_registry",
            score_threshold=0.95,  # Very conservative
        )
    ]
    
    result_low_recall = dq_engine.apply_checks_by_metadata(test_df, checks_low_recall)
    flagged_low_recall = result_low_recall.filter(F.size("_errors") > 0).count()
    
    # Low threshold (high recall)
    checks_high_recall = [
        has_no_anomalies(
            columns=["amount", "quantity"],
            model="test_precision_recall",
            registry_table="main.default.test_precision_recall_registry",
            score_threshold=0.3,  # Very aggressive
        )
    ]
    
    result_high_recall = dq_engine.apply_checks_by_metadata(test_df, checks_high_recall)
    flagged_high_recall = result_high_recall.filter(F.size("_errors") > 0).count()
    
    # Lower threshold should catch more anomalies
    assert flagged_high_recall >= flagged_low_recall


def test_threshold_edge_cases(spark: SparkSession, mock_workspace_client):
    """Test edge case thresholds (0.0 and 1.0)."""
    train_df = spark.createDataFrame(
        [(100.0, 2.0) for i in range(50)],
        "amount double, quantity double",
    )
    
    train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name="test_edge_thresholds",
        registry_table="main.default.test_edge_thresholds_registry",
    )
    
    test_df = spark.createDataFrame(
        [(100.0, 2.0), (9999.0, 1.0)],
        "amount double, quantity double",
    )
    
    dq_engine = DQEngine(mock_workspace_client)
    
    # Threshold 0.0 - flags everything
    checks_zero = [
        has_no_anomalies(
            columns=["amount", "quantity"],
            model="test_edge_thresholds",
            registry_table="main.default.test_edge_thresholds_registry",
            score_threshold=0.0,
        )
    ]
    
    result_zero = dq_engine.apply_checks_by_metadata(test_df, checks_zero)
    flagged_zero = result_zero.filter(F.size("_errors") > 0).count()
    
    # Should flag most/all rows
    assert flagged_zero >= 1
    
    # Threshold 1.0 - flags nothing (almost impossible to exceed)
    checks_one = [
        has_no_anomalies(
            columns=["amount", "quantity"],
            model="test_edge_thresholds",
            registry_table="main.default.test_edge_thresholds_registry",
            score_threshold=1.0,
        )
    ]
    
    result_one = dq_engine.apply_checks_by_metadata(test_df, checks_one)
    flagged_one = result_one.filter(F.size("_errors") > 0).count()
    
    # Should flag few/no rows
    assert flagged_one <= flagged_zero


def test_threshold_consistency(spark: SparkSession, mock_workspace_client):
    """Test that same threshold produces consistent results."""
    train_df = spark.createDataFrame(
        [(100.0, 2.0) for i in range(50)],
        "amount double, quantity double",
    )
    
    train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name="test_consistency",
        registry_table="main.default.test_consistency_registry",
    )
    
    test_df = spark.createDataFrame(
        [(100.0, 2.0), (9999.0, 1.0)],
        "amount double, quantity double",
    )
    
    dq_engine = DQEngine(mock_workspace_client)
    checks = [
        has_no_anomalies(
            columns=["amount", "quantity"],
            model="test_consistency",
            registry_table="main.default.test_consistency_registry",
            score_threshold=0.5,
        )
    ]
    
    # Run twice with same threshold
    result1 = dq_engine.apply_checks_by_metadata(test_df, checks)
    result2 = dq_engine.apply_checks_by_metadata(test_df, checks)
    
    # Should produce same results
    scores1 = [row["anomaly_score"] for row in result1.collect()]
    scores2 = [row["anomaly_score"] for row in result2.collect()]
    
    # Scores should be identical (deterministic)
    for s1, s2 in zip(scores1, scores2):
        if s1 is not None and s2 is not None:
            assert abs(s1 - s2) < 0.001  # Allow small floating point error


def test_validation_metrics_in_registry(spark: SparkSession):
    """Test that validation metrics are stored in registry."""
    train_df = spark.createDataFrame(
        [(100.0, 2.0) for i in range(50)],
        "amount double, quantity double",
    )
    
    model_name = "test_val_metrics"
    registry_table = "main.default.test_val_metrics_registry"
    
    train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name=model_name,
        registry_table=registry_table,
    )
    
    # Check metrics in registry
    record = spark.table(registry_table).filter(f"model_name = '{model_name}'").first()
    
    metrics = record["metrics"]
    assert metrics is not None
    
    # Recommended threshold should be present
    assert "recommended_threshold" in metrics
    
    # May also have precision, recall, f1_score (depending on implementation)
    # These are optional but good to have
    possible_metrics = ["precision", "recall", "f1_score", "val_anomaly_rate"]
    # At least some metrics should be present
    assert len(metrics) >= 1

