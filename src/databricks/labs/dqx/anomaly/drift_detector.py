"""
Drift detection for anomaly models.

Compares current data distribution against baseline statistics
to detect significant changes that may indicate model staleness.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

# Minimum sample size for reliable drift detection
# Small batches have high variance and lead to false positives
MIN_SAMPLE_SIZE_FOR_DRIFT = 1000


@dataclass
class DriftResult:
    """Results from drift detection."""

    drift_detected: bool
    drift_score: float
    drifted_columns: list[str]
    column_scores: dict[str, float]
    recommendation: str
    sample_size: int = 0


def _build_aggregation_expressions(
    columns: list[str],
    baseline_stats: dict[str, dict[str, float]],
    col_types: dict[str, str],
) -> tuple[list, list[str]]:
    """Build aggregation expressions for drift detection.

    Returns:
        Tuple of (aggregation expressions, columns to check)
    """
    agg_exprs = []
    columns_to_check = []

    for col in columns:
        if col not in baseline_stats:
            continue

        columns_to_check.append(col)
        col_type = col_types.get(col)
        col_expr = F.col(col).cast("double") if col_type == "boolean" else F.col(col)

        agg_exprs.extend(
            [
                F.mean(col_expr).alias(f"mean_{col}"),
                F.stddev(col_expr).alias(f"std_{col}"),
            ]
        )

    return agg_exprs, columns_to_check


def _compute_column_drift_score(
    baseline_mean: float,
    baseline_std: float,
    current_mean: float | None,
    current_std: float | None,
) -> float:
    """Compute drift score for a single column.

    Args:
        baseline_mean: Mean from training data
        baseline_std: Standard deviation from training data
        current_mean: Mean from current data
        current_std: Standard deviation from current data

    Returns:
        Drift score (weighted average of z-score and std change)
    """
    # Handle None values (can occur with single row or all identical values)
    if current_mean is None:
        current_mean = baseline_mean
    if current_std is None:
        current_std = 0.0

    # Z-score for mean shift
    if baseline_std == 0:
        # Avoid division by zero; if baseline std is 0, any change is drift
        z_score = abs(current_mean - baseline_mean) if current_mean != baseline_mean else 0.0
    else:
        z_score = abs(current_mean - baseline_mean) / baseline_std

    # Relative change in standard deviation
    if baseline_std > 0 and current_std > 0:
        std_change = abs(current_std - baseline_std) / baseline_std
    elif baseline_std > 0:
        # Current std is 0 but baseline is not - this is drift
        std_change = 1.0
    else:
        # Both are 0 - no change
        std_change = 0.0

    # Combined drift score (weighted average)
    return (z_score * 0.7) + (std_change * 0.3)


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
    # Early exit for small batches (high variance, unreliable statistics)
    row_count = df.count()
    if row_count < MIN_SAMPLE_SIZE_FOR_DRIFT:
        return DriftResult(
            drift_detected=False,
            drift_score=0.0,
            drifted_columns=[],
            column_scores={},
            recommendation="skipped_small_batch",
            sample_size=row_count,
        )

    # Build aggregation expressions for all columns (single Spark action)
    col_types = dict(df.dtypes)
    agg_exprs, columns_to_check = _build_aggregation_expressions(columns, baseline_stats, col_types)

    # Early exit if no columns to check
    if not agg_exprs:
        return DriftResult(
            drift_detected=False,
            drift_score=0.0,
            drifted_columns=[],
            column_scores={},
            recommendation="ok",
            sample_size=row_count,
        )

    # Single Spark action for all columns
    stats_row = df.select(*agg_exprs).first()
    assert stats_row is not None, "Failed to compute statistics"

    # Compute drift scores for each column
    column_scores = {}
    drifted_columns = []

    for col in columns_to_check:
        baseline = baseline_stats[col]
        current_mean = stats_row[f"mean_{col}"]
        current_std = stats_row[f"std_{col}"]

        col_drift_score = _compute_column_drift_score(
            baseline["mean"],
            baseline["std"],
            current_mean,
            current_std,
        )

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
        sample_size=row_count,
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
    differences = [abs(current - baseline) for current, baseline in zip(current_quantiles, baseline_quantiles)]

    # Normalize by baseline range to get a 0-1 score
    baseline_range = baseline_stats["max"] - baseline_stats["min"]
    if baseline_range > 0:
        ks_score = max(differences) / baseline_range
    else:
        ks_score = 0.0

    return min(ks_score, 1.0)  # Cap at 1.0
