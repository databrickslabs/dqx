"""
Auto-discovery logic for anomaly detection.

Analyzes DataFrames to recommend columns and segments suitable for
anomaly detection, leveraging existing DQX profiler output when available.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass

from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    DecimalType,
    StringType,
    NumericType,
)

logger = logging.getLogger(__name__)


@dataclass
class AnomalyProfile:
    """Auto-discovery results for anomaly detection."""

    recommended_columns: list[str]
    recommended_segments: list[str]
    segment_count: int
    column_stats: dict[str, dict]
    warnings: list[str]


def auto_discover(
    df: DataFrame,
    profiler_output_table: str | None = None,
) -> AnomalyProfile:
    """
    Auto-discover columns and segments for anomaly detection.

    Uses existing DQX profiler output if available, otherwise computes
    basic statistics on the fly.

    Column selection criteria:
    - Numeric types (int, long, float, double, decimal)
    - stddev > 0 (has variance)
    - null_rate < 50%
    - Exclude: timestamps, IDs (detected by name patterns)

    Segment selection criteria:
    - Categorical types (string, int with low cardinality)
    - Distinct values: 2-50 (inclusive)
    - null_rate < 10%
    - At least 1000 rows per segment (warn if violated)

    Args:
        df: DataFrame to analyze.
        profiler_output_table: Optional table with profiler output.

    Returns:
        AnomalyProfile with recommendations and warnings.
    """
    warnings = []

    if profiler_output_table:
        try:
            # Use existing profiler output
            profile_df = df.sparkSession.table(profiler_output_table)
            return _auto_discover_from_profiler(df, profile_df, warnings)
        except Exception as e:
            logger.warning(f"Failed to read profiler output table {profiler_output_table}: {e}. Falling back to heuristics.")

    # Fallback to on-the-fly heuristics
    return _auto_discover_heuristic(df, warnings)


def _auto_discover_from_profiler(
    df: DataFrame,
    profile_df: DataFrame,
    warnings: list[str],
) -> AnomalyProfile:
    """
    Auto-discover using existing profiler output.

    Args:
        df: Original DataFrame.
        profile_df: Profiler output DataFrame.
        warnings: List to accumulate warnings.

    Returns:
        AnomalyProfile with recommendations.
    """
    # Get input table name from profile if available
    input_table_rows = profile_df.filter(F.col("column_name") == "_metadata").collect()
    if input_table_rows:
        # Extract table name from metadata if present
        pass

    # Select numeric columns suitable for anomaly detection
    numeric_candidates = profile_df.filter(
        (F.col("type").isin(["int", "long", "float", "double", "decimal"]))
        & (F.col("null_rate") < 0.5)
        & (F.col("stddev") > 0)
        & (~F.col("column_name").rlike("(?i)(id|key|timestamp|date|time)$"))
    )

    recommended_columns = [row["column_name"] for row in numeric_candidates.collect()]

    if not recommended_columns:
        warnings.append("No suitable numeric columns found for anomaly detection. Provide columns explicitly.")

    # Select categorical columns suitable for segmentation
    categorical_candidates = profile_df.filter(
        (F.col("type").isin(["string", "int"]))
        & (F.col("distinct_count").between(2, 50))
        & (F.col("null_rate") < 0.1)
    )

    # Validate minimum rows per segment
    recommended_segments = []
    for row in categorical_candidates.collect():
        col = row["column_name"]
        distinct_count = row["distinct_count"]

        # Check minimum segment size
        min_segment_size_row = (
            df.groupBy(col)
            .count()
            .select(F.min("count").alias("min_count"))
            .first()
        )

        if min_segment_size_row:
            min_segment_size = min_segment_size_row["min_count"]
            if min_segment_size < 1000:
                warnings.append(
                    f"Segment column '{col}' has segments with <1000 rows (min: {min_segment_size}), "
                    "models may be unreliable."
                )
            recommended_segments.append(col)

    # Calculate total segment combinations
    segment_count = 1
    if recommended_segments:
        for col in recommended_segments:
            distinct_count = df.select(col).distinct().count()
            segment_count *= distinct_count

        if segment_count > 50:
            warnings.append(
                f"Detected {segment_count} total segments, training may be slow. "
                "Consider filtering or using coarser segmentation."
            )

    # Gather column stats
    column_stats = {}
    for col in recommended_columns:
        stats_row = df.select(
            F.mean(col).alias("mean"),
            F.stddev(col).alias("std"),
            F.min(col).alias("min"),
            F.max(col).alias("max"),
        ).first()
        if stats_row:
            column_stats[col] = stats_row.asDict()

    return AnomalyProfile(
        recommended_columns=recommended_columns,
        recommended_segments=recommended_segments,
        segment_count=segment_count,
        column_stats=column_stats,
        warnings=warnings,
    )


def _auto_discover_heuristic(
    df: DataFrame,
    warnings: list[str],
) -> AnomalyProfile:
    """
    Auto-discover using on-the-fly heuristics.

    Args:
        df: DataFrame to analyze.
        warnings: List to accumulate warnings.

    Returns:
        AnomalyProfile with recommendations.
    """
    recommended_columns = []
    recommended_segments = []
    column_stats = {}

    # Identify numeric columns
    numeric_types = (IntegerType, LongType, FloatType, DoubleType, DecimalType)
    numeric_fields = [f for f in df.schema.fields if isinstance(f.dataType, numeric_types)]

    # Filter out ID/timestamp patterns
    id_pattern = re.compile(r"(?i)(id|key|timestamp|date|time)$")

    for field in numeric_fields:
        col_name = field.name

        # Skip ID/timestamp columns
        if id_pattern.search(col_name):
            continue

        # Compute statistics
        stats_row = df.select(
            F.mean(col_name).alias("mean"),
            F.stddev(col_name).alias("std"),
            F.min(col_name).alias("min"),
            F.max(col_name).alias("max"),
            F.count(F.when(F.col(col_name).isNull(), 1)).alias("null_count"),
        ).first()

        if not stats_row:
            continue

        total_count = df.count()
        null_rate = stats_row["null_count"] / total_count if total_count > 0 else 1.0
        std = stats_row["std"] or 0.0

        # Check criteria
        if null_rate < 0.5 and std > 0:
            recommended_columns.append(col_name)
            column_stats[col_name] = {
                "mean": stats_row["mean"],
                "std": stats_row["std"],
                "min": stats_row["min"],
                "max": stats_row["max"],
            }

    if not recommended_columns:
        warnings.append("No suitable numeric columns found for anomaly detection. Provide columns explicitly.")

    # Identify categorical columns for segmentation
    categorical_types = (StringType, IntegerType)
    categorical_fields = [f for f in df.schema.fields if isinstance(f.dataType, categorical_types)]

    for field in categorical_fields:
        col_name = field.name

        # Skip if already selected as numeric column
        if col_name in recommended_columns:
            continue

        # Compute distinct count and null rate
        stats_row = df.select(
            F.countDistinct(col_name).alias("distinct_count"),
            F.count(F.when(F.col(col_name).isNull(), 1)).alias("null_count"),
        ).first()

        if not stats_row:
            continue

        distinct_count = stats_row["distinct_count"]
        total_count = df.count()
        null_rate = stats_row["null_count"] / total_count if total_count > 0 else 1.0

        # Check segment criteria
        if 2 <= distinct_count <= 50 and null_rate < 0.1:
            # Validate minimum segment size
            min_segment_size_row = (
                df.groupBy(col_name)
                .count()
                .select(F.min("count").alias("min_count"))
                .first()
            )

            if min_segment_size_row:
                min_segment_size = min_segment_size_row["min_count"]
                if min_segment_size < 1000:
                    warnings.append(
                        f"Segment column '{col_name}' has segments with <1000 rows (min: {min_segment_size}), "
                        "models may be unreliable."
                    )
                recommended_segments.append(col_name)
        elif distinct_count > 50:
            # High cardinality
            if isinstance(field.dataType, StringType) and "id" not in col_name.lower():
                warnings.append(
                    f"Column '{col_name}' has {distinct_count} distinct values, "
                    "excluding from auto-selection (too high cardinality for segmentation)."
                )

    # Calculate total segment combinations
    segment_count = 1
    if recommended_segments:
        for col in recommended_segments:
            distinct_count = df.select(col).distinct().count()
            segment_count *= distinct_count

        if segment_count > 50:
            warnings.append(
                f"Detected {segment_count} total segments, training may be slow. "
                "Consider filtering or using coarser segmentation."
            )

    return AnomalyProfile(
        recommended_columns=recommended_columns,
        recommended_segments=recommended_segments,
        segment_count=segment_count,
        column_stats=column_stats,
        warnings=warnings,
    )

