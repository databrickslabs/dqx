"""
Temporal feature extraction for time-series anomaly detection.

Enables detection of anomalies that depend on time-of-day, day-of-week,
or seasonal patterns.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
import pyspark.sql.functions as F


def extract_temporal_features(
    df: DataFrame,
    timestamp_column: str,
    features: list[str] | None = None,
) -> DataFrame:
    """
    Extract temporal features from a timestamp column.

    Args:
        df: DataFrame with timestamp column.
        timestamp_column: Name of timestamp column.
        features: List of features to extract. Options:
            - "hour": Hour of day (0-23)
            - "day_of_week": Day of week (1=Monday, 7=Sunday)
            - "day_of_month": Day of month (1-31)
            - "month": Month (1-12)
            - "quarter": Quarter (1-4)
            - "week_of_year": Week of year (1-53)
            - "is_weekend": Boolean for Saturday/Sunday
            If None, defaults to ["hour", "day_of_week", "month"].

    Returns:
        DataFrame with additional temporal feature columns.
    """
    if features is None:
        features = ["hour", "day_of_week", "month"]

    result = df

    for feature in features:
        if feature == "hour":
            result = result.withColumn("temporal_hour", F.hour(timestamp_column))
        elif feature == "day_of_week":
            result = result.withColumn("temporal_day_of_week", F.dayofweek(timestamp_column))
        elif feature == "day_of_month":
            result = result.withColumn("temporal_day_of_month", F.dayofmonth(timestamp_column))
        elif feature == "month":
            result = result.withColumn("temporal_month", F.month(timestamp_column))
        elif feature == "quarter":
            result = result.withColumn("temporal_quarter", F.quarter(timestamp_column))
        elif feature == "week_of_year":
            result = result.withColumn("temporal_week_of_year", F.weekofyear(timestamp_column))
        elif feature == "is_weekend":
            result = result.withColumn(
                "temporal_is_weekend",
                F.when(F.dayofweek(timestamp_column).isin([1, 7]), 1.0).otherwise(0.0)
            )
        else:
            raise ValueError(f"Unknown temporal feature: {feature}")

    return result


def get_temporal_column_names(features: list[str] | None = None) -> list[str]:
    """
    Get the names of temporal columns that would be created.

    Args:
        features: List of features (same as extract_temporal_features).

    Returns:
        List of column names.
    """
    if features is None:
        features = ["hour", "day_of_week", "month"]

    mapping = {
        "hour": "temporal_hour",
        "day_of_week": "temporal_day_of_week",
        "day_of_month": "temporal_day_of_month",
        "month": "temporal_month",
        "quarter": "temporal_quarter",
        "week_of_year": "temporal_week_of_year",
        "is_weekend": "temporal_is_weekend",
    }

    return [mapping[f] for f in features if f in mapping]

