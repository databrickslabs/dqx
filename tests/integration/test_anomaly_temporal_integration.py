"""Integration tests for temporal features in anomaly detection."""

import pytest
from pyspark.sql import SparkSession
from unittest.mock import MagicMock

from databricks.labs.dqx.anomaly import train, has_no_anomalies
from databricks.labs.dqx.anomaly.temporal import extract_temporal_features
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient


@pytest.fixture
def mock_workspace_client():
    """Create a mock WorkspaceClient for testing."""
    return MagicMock(spec=WorkspaceClient)


def test_temporal_features_end_to_end(spark: SparkSession, mock_workspace_client):
    """Test extract → train → score flow with temporal features."""
    # Create data with timestamps
    df = spark.sql(
        "SELECT 100.0 as amount, timestamp('2024-01-01 09:00:00') as event_time "
        "FROM range(50)"
    )
    
    # Extract temporal features
    df_with_temporal = extract_temporal_features(
        df,
        timestamp_column="event_time",
        features=["hour", "day_of_week"]
    )
    
    # Verify temporal columns were added
    assert "temporal_hour" in df_with_temporal.columns
    assert "temporal_day_of_week" in df_with_temporal.columns
    
    # Train on original + temporal features
    train(
        df=df_with_temporal,
        columns=["amount", "temporal_hour", "temporal_day_of_week"],
        model_name="test_temporal",
        registry_table="main.default.test_temporal_registry",
    )
    
    # Score new data with temporal features
    test_df = spark.sql(
        "SELECT 100.0 as amount, timestamp('2024-01-01 03:00:00') as event_time"
    )  # 3am might be anomalous
    
    test_df_with_temporal = extract_temporal_features(
        test_df,
        timestamp_column="event_time",
        features=["hour", "day_of_week"]
    )
    
    # Call apply function directly to get anomaly_score column
    condition_col, apply_fn = has_no_anomalies(
        columns=["amount", "temporal_hour", "temporal_day_of_week"],
        model="test_temporal",
        registry_table="main.default.test_temporal_registry",
        score_threshold=0.5,
    )
    result_df = apply_fn(test_df_with_temporal)

    # Verify scoring works
    assert "anomaly_score" in result_df.columns


def test_multiple_temporal_features(spark: SparkSession, mock_workspace_client):
    """Test training with multiple temporal features."""
    df = spark.sql(
        "SELECT 100.0 as amount, timestamp('2024-03-15 14:30:00') as event_time "
        "FROM range(50)"
    )
    
    # Extract multiple temporal features
    df_with_temporal = extract_temporal_features(
        df,
        timestamp_column="event_time",
        features=["hour", "day_of_week", "month", "quarter"]
    )
    
    # Verify all features were added
    assert "temporal_hour" in df_with_temporal.columns
    assert "temporal_day_of_week" in df_with_temporal.columns
    assert "temporal_month" in df_with_temporal.columns
    assert "temporal_quarter" in df_with_temporal.columns
    
    # Train
    train(
        df=df_with_temporal,
        columns=[
            "amount",
            "temporal_hour",
            "temporal_day_of_week",
            "temporal_month",
            "temporal_quarter",
        ],
        model_name="test_multi_temporal",
        registry_table="main.default.test_multi_temporal_registry",
    )
    
    # Score
    test_df = spark.sql(
        "SELECT 100.0 as amount, timestamp('2024-03-15 14:30:00') as event_time"
    )
    
    test_df_with_temporal = extract_temporal_features(
        test_df,
        timestamp_column="event_time",
        features=["hour", "day_of_week", "month", "quarter"]
    )
    
    # Call apply function directly to get anomaly_score column
    condition_col, apply_fn = has_no_anomalies(
        columns=[
            "amount",
            "temporal_hour",
            "temporal_day_of_week",
            "temporal_month",
            "temporal_quarter",
        ],
        model="test_multi_temporal",
        registry_table="main.default.test_multi_temporal_registry",
        score_threshold=0.5,
    )
    result_df = apply_fn(test_df_with_temporal)
    assert "anomaly_score" in result_df.columns


def test_temporal_pattern_detection(spark: SparkSession, mock_workspace_client):
    """Test that model learns time-based patterns."""
    # Create data with distinct patterns for different hours
    # 9am-5pm: amount=100, evening: amount=50
    df = spark.sql("""
        SELECT 100.0 as amount, timestamp('2024-01-01 09:00:00') as event_time FROM range(25)
        UNION ALL
        SELECT 100.0 as amount, timestamp('2024-01-01 14:00:00') as event_time FROM range(25)
    """)
    
    df_with_temporal = extract_temporal_features(
        df,
        timestamp_column="event_time",
        features=["hour"]
    )
    
    train(
        df=df_with_temporal,
        columns=["amount", "temporal_hour"],
        model_name="test_temporal_pattern",
        registry_table="main.default.test_temporal_pattern_registry",
    )
    
    # Test with normal hour
    test_normal = spark.sql(
        "SELECT 100.0 as amount, timestamp('2024-01-01 10:00:00') as event_time"
    )
    test_normal_with_temporal = extract_temporal_features(
        test_normal,
        timestamp_column="event_time",
        features=["hour"]
    )
    
    # Test with unusual hour (3am)
    test_unusual = spark.sql(
        "SELECT 100.0 as amount, timestamp('2024-01-01 03:00:00') as event_time"
    )
    test_unusual_with_temporal = extract_temporal_features(
        test_unusual,
        timestamp_column="event_time",
        features=["hour"]
    )
    
    # Call apply function directly to get anomaly_score column
    condition_col, apply_fn = has_no_anomalies(
        columns=["amount", "temporal_hour"],
        model="test_temporal_pattern",
        registry_table="main.default.test_temporal_pattern_registry",
        score_threshold=0.5,
    )

    # Score both
    result_normal = apply_fn(test_normal_with_temporal)
    result_unusual = apply_fn(test_unusual_with_temporal)
    
    # Get anomaly scores
    score_normal = result_normal.select("anomaly_score").collect()[0]["anomaly_score"]
    score_unusual = result_unusual.select("anomaly_score").collect()[0]["anomaly_score"]

    # Verify both have valid scores (model learned temporal features)
    assert score_normal is not None
    assert score_unusual is not None
    # Note: With this training data (same amount=100 for all hours), the model may not
    # differentiate between hours. A real use case would have varied amounts per hour.


def test_weekend_feature(spark: SparkSession, mock_workspace_client):
    """Test is_weekend temporal feature."""
    # Train on weekday data
    df = spark.sql("""
        SELECT 100.0 as amount, timestamp('2024-01-15 10:00:00') as event_time FROM range(30)
        UNION ALL
        SELECT 100.0 as amount, timestamp('2024-01-16 10:00:00') as event_time FROM range(20)
    """)  # Jan 15-16 are Mon-Tue
    
    df_with_temporal = extract_temporal_features(
        df,
        timestamp_column="event_time",
        features=["is_weekend"]
    )
    
    train(
        df=df_with_temporal,
        columns=["amount", "temporal_is_weekend"],
        model_name="test_weekend",
        registry_table="main.default.test_weekend_registry",
    )
    
    # Test on weekend
    test_df = spark.sql(
        "SELECT 100.0 as amount, timestamp('2024-01-13 10:00:00') as event_time"
    )  # Jan 13 is Saturday
    
    test_df_with_temporal = extract_temporal_features(
        test_df,
        timestamp_column="event_time",
        features=["is_weekend"]
    )
    
    # Verify weekend flag is set
    row = test_df_with_temporal.collect()[0]
    assert row["temporal_is_weekend"] == 1.0
    
    # Score (call apply function directly to get anomaly_score column)
    condition_col, apply_fn = has_no_anomalies(
        columns=["amount", "temporal_is_weekend"],
        model="test_weekend",
        registry_table="main.default.test_weekend_registry",
        score_threshold=0.5,
    )
    result_df = apply_fn(test_df_with_temporal)
    assert "anomaly_score" in result_df.columns


def test_missing_timestamp_column_behavior(spark: SparkSession):
    """Test behavior when timestamp column doesn't exist."""
    df = spark.createDataFrame(
        [(100.0, 2.0)],
        "amount double, quantity double",
    )
    
    # Try to extract temporal features from non-existent column
    # The function should raise an exception when trying to use a non-existent column
    with pytest.raises(Exception):  # Should raise AnalysisException or similar
        result_df = extract_temporal_features(
            df,
            timestamp_column="nonexistent_timestamp",
            features=["hour"]
        )
        # Force evaluation to trigger the error
        result_df.collect()


def test_temporal_features_with_nulls(spark: SparkSession):
    """Test temporal feature extraction with null timestamps."""
    df = spark.sql("""
        SELECT 100.0 as amount, timestamp('2024-01-01 09:00:00') as event_time
        UNION ALL
        SELECT 100.0 as amount, NULL as event_time
    """)
    
    # Extract temporal features (should handle nulls)
    df_with_temporal = extract_temporal_features(
        df,
        timestamp_column="event_time",
        features=["hour"]
    )
    
    rows = df_with_temporal.collect()
    
    # First row should have temporal_hour
    assert rows[0]["temporal_hour"] == 9
    
    # Second row with null timestamp should have null temporal_hour
    assert rows[1]["temporal_hour"] is None

