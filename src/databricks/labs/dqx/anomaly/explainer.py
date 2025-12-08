"""
Explainability utilities for anomaly detection.

Provides feature contribution analysis to understand which columns
contribute most to anomaly scores for individual records.
"""

from __future__ import annotations

from typing import Any
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import MapType, StringType, DoubleType


def compute_feature_contributions(
    model: Any,
    df: DataFrame,
    columns: list[str],
) -> DataFrame:
    """
    Compute per-row feature contributions showing which columns
    contributed most to each anomaly score.

    Uses a heuristic approach based on feature deviations from mean.
    For exact contributions, use SHAP or other model-agnostic methods.

    Args:
        model: Trained scikit-learn IsolationForest model (not used in current heuristic).
        df: DataFrame with anomaly_score already computed.
        columns: Feature columns.

    Returns:
        DataFrame with additional 'anomaly_contributions' map column.
    """
    # Note: DataFrame already has anomaly_score from distributed scoring
    # We compute contributions using a heuristic approach

    # Compute column means (distributed on Spark)
    means = {}
    for col in columns:
        col_mean = df.select(F.mean(col)).first()[0]
        means[col] = col_mean if col_mean is not None else 0.0

    # Compute contributions as deviation from mean weighted by position
    # This is a heuristic approximation for performance
    # For exact contributions, consider using SHAP with the sklearn model
    contributions_expr = F.create_map()
    
    for i, col in columns:
        # Heuristic: contribution proportional to normalized deviation
        deviation = F.abs(F.col(col) - F.lit(means[col]))
        contribution = deviation / (F.lit(len(columns)) + F.lit(1.0))
        contributions_expr = F.expr(
            f"map_concat({contributions_expr._jc.toString()}, "
            f"map('{col}', {contribution._jc.toString()}))"
        )

    # Add contributions to DataFrame
    result = df.withColumn("_raw_contributions", contributions_expr)

    # Normalize contributions to sum to 1.0 per row (distributed on Spark)
    result = result.withColumn(
        "anomaly_contributions",
        F.expr(
            "transform_values(_raw_contributions, "
            "(k, v) -> v / aggregate(map_values(_raw_contributions), 0.0, (acc, x) -> acc + x))"
        ),
    ).drop("_raw_contributions")

    return result


def add_top_contributors_to_message(
    df: DataFrame, threshold: float, top_n: int = 3
) -> DataFrame:
    """
    Enhance error messages with top feature contributors.

    Args:
        df: DataFrame with anomaly_score and anomaly_contributions.
        threshold: Score threshold for anomalies.
        top_n: Number of top contributors to include in message.

    Returns:
        DataFrame with enhanced messages.
    """

    def format_contributions(contributions_map):
        """Format contributions map as string for top N contributors."""
        if not contributions_map:
            return ""
        
        # Sort by contribution value descending
        sorted_items = sorted(contributions_map.items(), key=lambda x: x[1], reverse=True)
        top_items = sorted_items[:top_n]
        
        # Format as "col1=60%, col2=25%, col3=15%"
        formatted = ", ".join([f"{col}={val*100:.0f}%" for col, val in top_items])
        return f" (top contributors: {formatted})"

    # Register UDF
    format_udf = F.udf(format_contributions, StringType())

    # Add formatted contributions to anomalies
    result = df.withColumn(
        "_contrib_suffix",
        F.when(
            F.col("anomaly_score") > F.lit(threshold),
            format_udf(F.col("anomaly_contributions"))
        ).otherwise(F.lit(""))
    )

    return result

