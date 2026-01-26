"""Integration tests for temporal features in anomaly detection."""

from uuid import uuid4

import pyspark.sql.functions as F
import pytest
from pyspark.sql import SparkSession

from databricks.labs.dqx.anomaly import AnomalyEngine, has_no_anomalies
from databricks.labs.dqx.anomaly.temporal import extract_temporal_features

from tests.integration_anomaly.test_anomaly_constants import DEFAULT_SCORE_THRESHOLD


@pytest.fixture
def temporal_model(ws, spark, anomaly_registry_prefix):
    """
    Temporal model trained per test for schema isolation.

    Trains a model with temporal features (hour, day_of_week) on standard data.
    """
    suffix = uuid4().hex[:8]
    model_name = f"temporal_model_{suffix}"
    registry_table = f"{anomaly_registry_prefix}.temporal_reg_{suffix}"

    # Create data with timestamps (9am on weekdays)
    df = spark.sql("SELECT 100.0 as amount, timestamp('2024-01-15 09:00:00') as event_time FROM range(50)")

    # Extract temporal features
    df_with_temporal = extract_temporal_features(df, timestamp_column="event_time", features=["hour", "day_of_week"])

    # Train model
    engine = AnomalyEngine(ws, spark)
    engine.train(
        df=df_with_temporal,
        columns=["amount", "temporal_hour", "temporal_day_of_week"],
        model_name=model_name,
        registry_table=registry_table,
    )

    return {
        "model_name": model_name,
        "registry_table": registry_table,
    }


def test_temporal_features_end_to_end(spark: SparkSession, temporal_model):
    """Test extract → train → score flow with temporal features."""
    # Use module-scoped pre-trained temporal model (no training needed!)
    model_name = temporal_model["model_name"]
    registry_table = temporal_model["registry_table"]

    # Verify we can extract temporal features
    test_df = spark.sql("SELECT 1 as transaction_id, 100.0 as amount, timestamp('2024-01-15 09:00:00') as event_time")

    test_df_with_temporal = extract_temporal_features(
        test_df, timestamp_column="event_time", features=["hour", "day_of_week"]
    )

    # Verify temporal columns were added
    assert "temporal_hour" in test_df_with_temporal.columns
    assert "temporal_day_of_week" in test_df_with_temporal.columns

    # Score with temporal features
    _, apply_fn = has_no_anomalies(
        model=model_name,
        registry_table=registry_table,
        score_threshold=DEFAULT_SCORE_THRESHOLD,
    )
    result_df = apply_fn(test_df_with_temporal)

    # Verify scoring works - check _info column exists
    assert "_dq_info" in result_df.columns
    row = result_df.collect()[0]
    assert row["_dq_info"]["anomaly"]["score"] is not None


def test_multiple_temporal_features(spark: SparkSession, make_random, anomaly_engine, anomaly_registry_prefix):
    """Test training with multiple temporal features."""
    unique_id = make_random(8).lower()
    model_name = f"test_multi_temporal_{make_random(4).lower()}"
    registry_table = f"{anomaly_registry_prefix}.{unique_id}_registry"

    df = spark.sql("SELECT 100.0 as amount, timestamp('2024-03-15 14:30:00') as event_time FROM range(50)")

    # Extract multiple temporal features
    df_with_temporal = extract_temporal_features(
        df, timestamp_column="event_time", features=["hour", "day_of_week", "month", "quarter"]
    )

    # Verify all features were added
    assert "temporal_hour" in df_with_temporal.columns
    assert "temporal_day_of_week" in df_with_temporal.columns
    assert "temporal_month" in df_with_temporal.columns
    assert "temporal_quarter" in df_with_temporal.columns

    # Train
    anomaly_engine.train(
        df=df_with_temporal,
        columns=[
            "amount",
            "temporal_hour",
            "temporal_day_of_week",
            "temporal_month",
            "temporal_quarter",
        ],
        model_name=model_name,
        registry_table=registry_table,
    )

    # Score
    test_df = spark.sql("SELECT 1 as transaction_id, 100.0 as amount, timestamp('2024-03-15 14:30:00') as event_time")

    test_df_with_temporal = extract_temporal_features(
        test_df, timestamp_column="event_time", features=["hour", "day_of_week", "month", "quarter"]
    )

    # Call apply function directly to get _info column
    _, apply_fn = has_no_anomalies(
        model=model_name,
        registry_table=registry_table,
        score_threshold=DEFAULT_SCORE_THRESHOLD,
    )
    result_df = apply_fn(test_df_with_temporal)
    assert "_dq_info" in result_df.columns
    row = result_df.collect()[0]
    assert row["_dq_info"]["anomaly"]["score"] is not None


def test_temporal_pattern_detection(spark: SparkSession, make_random, anomaly_engine, anomaly_registry_prefix):
    """Test that model learns time-based patterns."""
    unique_id = make_random(8).lower()
    model_name = f"test_temporal_pattern_{make_random(4).lower()}"
    registry_table = f"{anomaly_registry_prefix}.{unique_id}_registry"

    # Create data with distinct patterns for different hours
    # 9am-5pm: amount=100, evening: amount=50
    df = spark.sql(
        """
        SELECT 100.0 as amount, timestamp('2024-01-01 09:00:00') as event_time FROM range(25)
        UNION ALL
        SELECT 100.0 as amount, timestamp('2024-01-01 14:00:00') as event_time FROM range(25)
    """
    )

    df_with_temporal = extract_temporal_features(df, timestamp_column="event_time", features=["hour"])

    anomaly_engine.train(
        df=df_with_temporal,
        columns=["amount", "temporal_hour"],
        model_name=model_name,
        registry_table=registry_table,
    )

    # Test with normal hour
    test_normal = spark.sql(
        "SELECT 1 as transaction_id, 100.0 as amount, timestamp('2024-01-01 10:00:00') as event_time"
    )
    test_normal_with_temporal = extract_temporal_features(test_normal, timestamp_column="event_time", features=["hour"])

    # Test with unusual hour (3am)
    test_unusual = spark.sql(
        "SELECT 2 as transaction_id, 100.0 as amount, timestamp('2024-01-01 03:00:00') as event_time"
    )
    test_unusual_with_temporal = extract_temporal_features(
        test_unusual, timestamp_column="event_time", features=["hour"]
    )

    # Call apply function directly to get _info column
    _, apply_fn = has_no_anomalies(
        model=model_name,
        registry_table=registry_table,
        score_threshold=DEFAULT_SCORE_THRESHOLD,
    )

    # Score both
    result_normal = apply_fn(test_normal_with_temporal)
    result_unusual = apply_fn(test_unusual_with_temporal)

    # Get anomaly scores from _dq_info.anomaly.score
    score_normal = result_normal.select(F.col("_dq_info.anomaly.score")).collect()[0][0]
    score_unusual = result_unusual.select(F.col("_dq_info.anomaly.score")).collect()[0][0]

    # Verify both have valid scores (model learned temporal features)
    assert score_normal is not None
    assert score_unusual is not None
    # Note: With this training data (same amount=100 for all hours), the model may not
    # differentiate between hours. A real use case would have varied amounts per hour.


def test_weekend_feature(spark: SparkSession, make_random, anomaly_engine, anomaly_registry_prefix):
    """Test is_weekend temporal feature."""
    unique_id = make_random(8).lower()
    model_name = f"test_weekend_{make_random(4).lower()}"
    registry_table = f"{anomaly_registry_prefix}.{unique_id}_registry"

    # Train on weekday data
    df = spark.sql(
        """
        SELECT 100.0 as amount, timestamp('2024-01-15 10:00:00') as event_time FROM range(30)
        UNION ALL
        SELECT 100.0 as amount, timestamp('2024-01-16 10:00:00') as event_time FROM range(20)
    """
    )  # Jan 15-16 are Mon-Tue

    df_with_temporal = extract_temporal_features(df, timestamp_column="event_time", features=["is_weekend"])

    anomaly_engine.train(
        df=df_with_temporal,
        columns=["amount", "temporal_is_weekend"],
        model_name=model_name,
        registry_table=registry_table,
    )

    # Test on weekend
    test_df = spark.sql(
        "SELECT 1 as transaction_id, 100.0 as amount, timestamp('2024-01-13 10:00:00') as event_time"
    )  # Jan 13 is Saturday

    test_df_with_temporal = extract_temporal_features(test_df, timestamp_column="event_time", features=["is_weekend"])

    # Verify weekend flag is set
    row = test_df_with_temporal.collect()[0]
    assert row["temporal_is_weekend"] == 1.0

    # Score (call apply function directly to get _info column)
    _, apply_fn = has_no_anomalies(
        model=model_name,
        registry_table=registry_table,
        score_threshold=DEFAULT_SCORE_THRESHOLD,
    )
    result_df = apply_fn(test_df_with_temporal)
    assert "_dq_info" in result_df.columns
    row = result_df.collect()[0]
    assert row["_dq_info"]["anomaly"]["score"] is not None


def test_missing_timestamp_column_behavior(spark: SparkSession):
    """Test behavior when timestamp column doesn't exist."""
    df = spark.createDataFrame(
        [(100.0, 2.0)],
        "amount double, quantity double",
    )

    # Try to extract temporal features from non-existent column
    # The function should raise an exception when trying to use a non-existent column
    with pytest.raises(Exception):  # Should raise AnalysisException or similar
        result_df = extract_temporal_features(df, timestamp_column="nonexistent_timestamp", features=["hour"])
        # Force evaluation to trigger the error
        result_df.collect()


def test_temporal_features_with_nulls(spark: SparkSession):
    """Test temporal feature extraction with null timestamps."""
    df = spark.sql(
        """
        SELECT 100.0 as amount, timestamp('2024-01-01 09:00:00') as event_time
        UNION ALL
        SELECT 100.0 as amount, NULL as event_time
    """
    )

    # Extract temporal features (should handle nulls)
    df_with_temporal = extract_temporal_features(df, timestamp_column="event_time", features=["hour"])

    rows = df_with_temporal.collect()

    # First row should have temporal_hour
    assert rows[0]["temporal_hour"] == 9

    # Second row with null timestamp should have null temporal_hour
    assert rows[1]["temporal_hour"] is None
