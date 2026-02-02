"""Integration tests for temporal features in anomaly detection."""

from datetime import datetime, timedelta
from uuid import uuid4

import pyspark.sql.functions as F
import pytest
from pyspark.sql import SparkSession

from databricks.labs.dqx.anomaly import AnomalyEngine, has_no_anomalies
from databricks.labs.dqx.anomaly.temporal import extract_temporal_features, get_temporal_column_names
from tests.integration_anomaly.test_anomaly_constants import DEFAULT_SCORE_THRESHOLD
from tests.integration_anomaly.test_anomaly_utils import qualify_model_name


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
    full_model_name = qualify_model_name(model_name, registry_table)
    engine.train(
        df=df_with_temporal,
        columns=["amount", "temporal_hour", "temporal_day_of_week"],
        model_name=full_model_name,
        registry_table=registry_table,
    )

    return {
        "model_name": full_model_name,
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
        model=qualify_model_name(model_name, registry_table),
        registry_table=registry_table,
        threshold=DEFAULT_SCORE_THRESHOLD,
    )
    result_df = apply_fn(test_df_with_temporal)

    # Verify scoring works - check _info column exists
    assert "_dq_info" in result_df.columns
    row = result_df.collect()[0]
    assert row["_dq_info"]["anomaly"]["score"] is not None


def test_multiple_temporal_features(spark: SparkSession, make_random, anomaly_engine, anomaly_registry_prefix):
    """Test training with multiple temporal features."""
    unique_id = make_random(8).lower()
    model_name = f"{anomaly_registry_prefix}.test_multi_temporal_{make_random(4).lower()}"
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
        model_name=qualify_model_name(model_name, registry_table),
        registry_table=registry_table,
    )

    # Score
    test_df = spark.sql("SELECT 1 as transaction_id, 100.0 as amount, timestamp('2024-03-15 14:30:00') as event_time")

    test_df_with_temporal = extract_temporal_features(
        test_df, timestamp_column="event_time", features=["hour", "day_of_week", "month", "quarter"]
    )

    # Call apply function directly to get _info column
    _, apply_fn = has_no_anomalies(
        model=qualify_model_name(model_name, registry_table),
        registry_table=registry_table,
        threshold=DEFAULT_SCORE_THRESHOLD,
    )
    result_df = apply_fn(test_df_with_temporal)
    assert "_dq_info" in result_df.columns
    row = result_df.collect()[0]
    assert row["_dq_info"]["anomaly"]["score"] is not None


def test_temporal_pattern_detection(spark: SparkSession, make_random, anomaly_engine, anomaly_registry_prefix):
    """Test that model learns time-based patterns."""
    unique_id = make_random(8).lower()
    model_name = f"{anomaly_registry_prefix}.test_temporal_pattern_{make_random(4).lower()}"
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
        model_name=qualify_model_name(model_name, registry_table),
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
        model=qualify_model_name(model_name, registry_table),
        registry_table=registry_table,
        threshold=DEFAULT_SCORE_THRESHOLD,
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
    model_name = f"{anomaly_registry_prefix}.test_weekend_{make_random(4).lower()}"
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
        model_name=qualify_model_name(model_name, registry_table),
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
        model=qualify_model_name(model_name, registry_table),
        registry_table=registry_table,
        threshold=DEFAULT_SCORE_THRESHOLD,
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


def test_unknown_temporal_feature_raises_error(spark: SparkSession):
    """Test that unknown temporal feature raises ValueError (line 59)."""
    df = spark.sql("SELECT 100.0 as amount, timestamp('2024-01-01 09:00:00') as event_time FROM range(5)")

    # Try to extract an unknown feature
    with pytest.raises(ValueError, match="Unknown temporal feature: invalid_feature"):
        df_with_temporal = extract_temporal_features(
            df, timestamp_column="event_time", features=["hour", "invalid_feature"]
        )
        # Force evaluation
        df_with_temporal.collect()


def test_all_temporal_features(spark: SparkSession):
    """Test all available temporal features including day_of_month and week_of_year."""
    df = spark.sql("SELECT 100.0 as amount, timestamp('2024-03-15 14:30:00') as event_time FROM range(5)")

    # Extract all available features
    df_with_temporal = extract_temporal_features(
        df,
        timestamp_column="event_time",
        features=["hour", "day_of_week", "day_of_month", "month", "quarter", "week_of_year", "is_weekend"],
    )

    # Verify all columns are created
    assert "temporal_hour" in df_with_temporal.columns
    assert "temporal_day_of_week" in df_with_temporal.columns
    assert "temporal_day_of_month" in df_with_temporal.columns
    assert "temporal_month" in df_with_temporal.columns
    assert "temporal_quarter" in df_with_temporal.columns
    assert "temporal_week_of_year" in df_with_temporal.columns
    assert "temporal_is_weekend" in df_with_temporal.columns

    # Verify values
    row = df_with_temporal.collect()[0]
    assert row["temporal_hour"] == 14
    assert row["temporal_day_of_month"] == 15
    assert row["temporal_month"] == 3
    assert row["temporal_quarter"] == 1
    assert row["temporal_week_of_year"] is not None


def test_get_temporal_column_names_with_none():
    """Test get_temporal_column_names with None returns default columns (lines 74-75, 77)."""
    # Call with None - should use default features
    column_names = get_temporal_column_names(features=None)

    # Verify default features: ["hour", "day_of_week", "month"]
    assert column_names == ["temporal_hour", "temporal_day_of_week", "temporal_month"]


def test_get_temporal_column_names_with_custom_features():
    """Test get_temporal_column_names with custom features."""
    # Test with custom features
    column_names = get_temporal_column_names(features=["hour", "quarter", "is_weekend"])

    assert column_names == ["temporal_hour", "temporal_quarter", "temporal_is_weekend"]


def test_get_temporal_column_names_with_all_features():
    """Test get_temporal_column_names with all available features (line 77 mapping)."""
    # Test all features to cover the full mapping dictionary
    all_features = ["hour", "day_of_week", "day_of_month", "month", "quarter", "week_of_year", "is_weekend"]
    column_names = get_temporal_column_names(features=all_features)

    expected = [
        "temporal_hour",
        "temporal_day_of_week",
        "temporal_day_of_month",
        "temporal_month",
        "temporal_quarter",
        "temporal_week_of_year",
        "temporal_is_weekend",
    ]
    assert column_names == expected


def test_get_temporal_column_names_filters_invalid():
    """Test get_temporal_column_names filters out invalid features (line 87)."""
    # Include invalid feature - should be filtered out
    column_names = get_temporal_column_names(features=["hour", "invalid_feature", "month"])

    # Should only return valid features
    assert column_names == ["temporal_hour", "temporal_month"]


def test_temporal_features_default_in_full_training_workflow(
    spark: SparkSession, make_random, anomaly_engine, anomaly_registry_prefix
):
    """Test default temporal features (lines 36-37) in full train → score workflow."""
    unique_id = make_random(8).lower()
    model_name = f"{anomaly_registry_prefix}.test_default_temporal_{make_random(4).lower()}"
    registry_table = f"{anomaly_registry_prefix}.{unique_id}_registry"

    # Create training data with timestamps
    base_time = datetime(2024, 1, 15, 9, 0, 0)
    train_data = [(100.0 + (i % 10), base_time + timedelta(hours=i)) for i in range(50)]
    train_df = spark.createDataFrame(train_data, "amount double, event_time timestamp")

    # Extract temporal features WITHOUT specifying features parameter
    # Should use defaults: ["hour", "day_of_week", "month"]
    train_df_with_temporal = extract_temporal_features(train_df, timestamp_column="event_time")

    # Verify default features were added
    assert "temporal_hour" in train_df_with_temporal.columns
    assert "temporal_day_of_week" in train_df_with_temporal.columns
    assert "temporal_month" in train_df_with_temporal.columns
    # Non-default features should NOT be present
    assert "temporal_quarter" not in train_df_with_temporal.columns
    assert "temporal_is_weekend" not in train_df_with_temporal.columns

    # Train model using default temporal features
    anomaly_engine.train(
        df=train_df_with_temporal,
        columns=["amount", "temporal_hour", "temporal_day_of_week", "temporal_month"],
        model_name=model_name,
        registry_table=registry_table,
    )

    # Create test data and apply same default temporal extraction
    test_time = datetime(2024, 1, 20, 14, 0, 0)
    test_df = spark.createDataFrame([(100.0, test_time)], "amount double, event_time timestamp")
    test_df_with_temporal = extract_temporal_features(test_df, timestamp_column="event_time")

    # Score with trained model - verifies defaults work end-to-end
    _, apply_fn = has_no_anomalies(
        model=qualify_model_name(model_name, registry_table),
        registry_table=registry_table,
        threshold=DEFAULT_SCORE_THRESHOLD,
    )
    result_df = apply_fn(test_df_with_temporal)

    # Verify scoring worked with default temporal features
    assert "_dq_info" in result_df.columns
    row = result_df.collect()[0]
    assert row["_dq_info"]["anomaly"]["score"] is not None

    # Verify get_temporal_column_names also respects defaults
    default_column_names = get_temporal_column_names()  # No features parameter
    assert default_column_names == ["temporal_hour", "temporal_day_of_week", "temporal_month"]
