"""
Drift detection for anomaly models.

Compares current data distribution against baseline statistics
to detect significant changes that may indicate model staleness.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
import pyspark.sql.functions as F


@dataclass
class DriftResult:
    """Results from drift detection."""

    drift_detected: bool
    drift_score: float
    drifted_columns: list[str]
    column_scores: dict[str, float]
    recommendation: str


def compute_drift_score(
    df: DataFrame,
    columns: list[str],
    baseline_stats: dict[str, dict[str, float]],
    threshold: float = 3.0,
) -> DriftResult:
    """
    Compute drift score comparing current data to baseline statistics.

    Args:
        df: Current DataFrame to check for drift.
        columns: Columns to check for drift.
        baseline_stats: Baseline statistics from training data.
        threshold: Drift score threshold (default 3.0 = 3 std deviations).

    Returns:
        DriftResult with detection status and details.
    """
    column_scores = {}
    drifted_columns = []

    for col in columns:
        if col not in baseline_stats:
            continue

        baseline = baseline_stats[col]

        # Compute current statistics
        current_stats = df.select(
            F.mean(col).alias("mean"),
            F.stddev(col).alias("std"),
        ).first()

        # Z-score for mean shift
        baseline_mean = baseline["mean"]
        baseline_std = baseline["std"]

        if baseline_std == 0:
            # Avoid division by zero; if baseline std is 0, any change is drift
            z_score = abs(current_stats["mean"] - baseline_mean) if current_stats["mean"] != baseline_mean else 0.0
        else:
            z_score = abs(current_stats["mean"] - baseline_mean) / baseline_std

        # Relative change in standard deviation
        if baseline_std > 0:
            std_change = abs(current_stats["std"] - baseline_std) / baseline_std
        else:
            std_change = abs(current_stats["std"]) if current_stats["std"] > 0 else 0.0

        # Combined drift score (weighted average)
        col_drift_score = (z_score * 0.7) + (std_change * 0.3)

        column_scores[col] = col_drift_score

        if col_drift_score >= threshold:
            drifted_columns.append(col)

    # Overall drift score (max across columns)
    overall_drift_score = max(column_scores.values()) if column_scores else 0.0
    drift_detected = overall_drift_score >= threshold

    recommendation = "retrain" if drift_detected else "ok"

    return DriftResult(
        drift_detected=drift_detected,
        drift_score=overall_drift_score,
        drifted_columns=drifted_columns,
        column_scores=column_scores,
        recommendation=recommendation,
    )


def compute_ks_statistic(df: DataFrame, col: str, baseline_stats: dict[str, float]) -> float:
    """
    Compute Kolmogorov-Smirnov statistic for distribution comparison.

    This is a more rigorous test but requires more computation.
    For now, we use a simplified version based on quantile comparison.

    Args:
        df: Current DataFrame.
        col: Column to check.
        baseline_stats: Baseline statistics for the column.

    Returns:
        KS-like statistic (0 = identical, 1 = completely different).
    """
    # Get current quantiles
    current_quantiles = df.approxQuantile(col, [0.25, 0.5, 0.75], 0.01)

    # Compare to baseline quantiles
    baseline_quantiles = [baseline_stats["p25"], baseline_stats["p50"], baseline_stats["p75"]]

    # Compute max absolute difference (simplified KS statistic)
    differences = [
        abs(current - baseline) for current, baseline in zip(current_quantiles, baseline_quantiles)
    ]

    # Normalize by baseline range to get a 0-1 score
    baseline_range = baseline_stats["max"] - baseline_stats["min"]
    if baseline_range > 0:
        ks_score = max(differences) / baseline_range
    else:
        ks_score = 0.0

    return min(ks_score, 1.0)  # Cap at 1.0

