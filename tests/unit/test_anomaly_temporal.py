"""Unit tests for temporal feature extraction."""

import pytest
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from databricks.labs.dqx.anomaly.temporal import extract_temporal_features, get_temporal_column_names


def test_extract_hour_feature(spark: SparkSession):
    """Test extraction of hour feature."""
    df = spark.createDataFrame(
        [("2024-01-15 14:30:00",), ("2024-01-15 09:15:00",)],
        "timestamp string",
    ).withColumn("timestamp", F.to_timestamp("timestamp"))

    result = extract_temporal_features(df, "timestamp", features=["hour"])

    assert "temporal_hour" in result.columns
    hours = [row["temporal_hour"] for row in result.collect()]
    assert 14 in hours
    assert 9 in hours


def test_extract_day_of_week_feature(spark: SparkSession):
    """Test extraction of day of week feature."""
    df = spark.createDataFrame(
        [("2024-01-15 10:00:00",)],  # Monday
        "timestamp string",
    ).withColumn("timestamp", F.to_timestamp("timestamp"))

    result = extract_temporal_features(df, "timestamp", features=["day_of_week"])

    assert "temporal_day_of_week" in result.columns
    row = result.first()
    assert row is not None
    day_of_week = row["temporal_day_of_week"]
    assert day_of_week == 2  # Monday is 2 in Spark


def test_extract_multiple_features(spark: SparkSession):
    """Test extraction of multiple temporal features."""
    df = spark.createDataFrame(
        [("2024-03-15 14:30:00",)],
        "timestamp string",
    ).withColumn("timestamp", F.to_timestamp("timestamp"))

    result = extract_temporal_features(df, "timestamp", features=["hour", "day_of_week", "month", "quarter"])

    assert "temporal_hour" in result.columns
    assert "temporal_day_of_week" in result.columns
    assert "temporal_month" in result.columns
    assert "temporal_quarter" in result.columns

    row = result.first()
    assert row is not None
    assert row["temporal_hour"] == 14
    assert row["temporal_month"] == 3
    assert row["temporal_quarter"] == 1


def test_extract_is_weekend_feature(spark: SparkSession):
    """Test extraction of is_weekend feature."""
    df = spark.createDataFrame(
        [
            ("2024-01-13 10:00:00",),  # Saturday
            ("2024-01-14 10:00:00",),  # Sunday
            ("2024-01-15 10:00:00",),  # Monday
        ],
        "timestamp string",
    ).withColumn("timestamp", F.to_timestamp("timestamp"))

    result = extract_temporal_features(df, "timestamp", features=["is_weekend"])

    assert "temporal_is_weekend" in result.columns
    weekend_values = [row["temporal_is_weekend"] for row in result.collect()]
    assert weekend_values == [1.0, 1.0, 0.0]


def test_get_temporal_column_names():
    """Test getting temporal column names."""
    names = get_temporal_column_names(["hour", "day_of_week", "month"])

    assert names == ["temporal_hour", "temporal_day_of_week", "temporal_month"]


def test_get_temporal_column_names_default():
    """Test getting default temporal column names."""
    names = get_temporal_column_names(None)

    assert "temporal_hour" in names
    assert "temporal_day_of_week" in names
    assert "temporal_month" in names


def test_invalid_feature_raises_error(spark: SparkSession):
    """Test that invalid feature name raises ValueError."""
    df = spark.createDataFrame(
        [("2024-01-15 10:00:00",)],
        "timestamp string",
    ).withColumn("timestamp", F.to_timestamp("timestamp"))

    with pytest.raises(ValueError, match="Unknown temporal feature"):
        extract_temporal_features(df, "timestamp", features=["invalid_feature"])
