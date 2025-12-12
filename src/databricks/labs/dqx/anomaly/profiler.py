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
    column_types: dict[str, str] = None  # NEW: maps column -> type category
    unsupported_columns: list[str] = None  # NEW: columns that cannot be used


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
        column_types={},  # TODO: Extract from profiler
        unsupported_columns=[],
    )


def _auto_discover_heuristic(
    df: DataFrame,
    warnings: list[str],
) -> AnomalyProfile:
    """
    Auto-discover using on-the-fly heuristics with multi-type support.

    Args:
        df: DataFrame to analyze.
        warnings: List to accumulate warnings.

    Returns:
        AnomalyProfile with recommendations (max 10 columns).
    """
    from pyspark.sql.types import BooleanType, DateType, TimestampType, TimestampNTZType
    
    recommended_columns = []
    recommended_segments = []
    column_stats = {}
    column_types = {}
    unsupported_columns = []
    
    # Priority order: numeric > boolean > low-card categorical > datetime > high-card categorical
    candidates = []  # List of (priority, col_name, col_type, cardinality, null_rate)
    
    # Filter out ID/timestamp patterns
    id_pattern = re.compile(r"(?i)(id|key)$")
    
    total_count = df.count()
    
    for field in df.schema.fields:
        col_name = field.name
        col_type = field.dataType
        
        # Skip ID columns
        if id_pattern.search(col_name):
            continue
        
        # Count nulls
        null_count_row = df.select(F.count(F.when(F.col(col_name).isNull(), 1)).alias("null_count")).first()
        null_rate = null_count_row["null_count"] / total_count if total_count > 0 else 1.0
        
        # Numeric columns
        if isinstance(col_type, (IntegerType, LongType, FloatType, DoubleType, DecimalType)):
            stats_row = df.select(
                F.mean(col_name).alias("mean"),
                F.stddev(col_name).alias("std"),
            ).first()
            
            std = stats_row["std"] or 0.0
            if null_rate < 0.5 and std > 0:
                candidates.append((1, col_name, 'numeric', None, null_rate))  # Priority 1 (highest)
                column_stats[col_name] = {"mean": stats_row["mean"], "std": std}
        
        # Boolean columns
        elif isinstance(col_type, BooleanType):
            if null_rate < 0.5:
                candidates.append((2, col_name, 'boolean', None, null_rate))  # Priority 2
        
        # Datetime columns
        elif isinstance(col_type, (DateType, TimestampType, TimestampNTZType)):
            if null_rate < 0.5:
                candidates.append((4, col_name, 'datetime', None, null_rate))  # Priority 4
        
        # String (categorical) columns
        elif isinstance(col_type, StringType):
            cardinality = df.select(F.countDistinct(col_name)).first()[0]
            
            if cardinality <= 20 and null_rate < 0.5:
                # Low cardinality categorical
                candidates.append((3, col_name, 'categorical', cardinality, null_rate))  # Priority 3
            elif 20 < cardinality <= 100 and null_rate < 0.5:
                # High cardinality categorical (frequency encoding)
                candidates.append((5, col_name, 'categorical', cardinality, null_rate))  # Priority 5
            elif cardinality > 100:
                warnings.append(
                    f"Column '{col_name}' has {cardinality} distinct values (>100), "
                    "excluding from auto-selection (too high cardinality)."
                )
        
        # Unsupported types
        else:
            unsupported_columns.append(col_name)
    
    # Sort by priority (lower number = higher priority)
    candidates.sort(key=lambda x: (x[0], x[1]))  # Sort by priority, then name
    
    # Select top 10 columns
    MAX_COLUMNS = 10
    for priority, col_name, col_type, cardinality, null_rate in candidates[:MAX_COLUMNS]:
        recommended_columns.append(col_name)
        column_types[col_name] = col_type
    
    if len(candidates) > MAX_COLUMNS:
        warnings.append(
            f"Found {len(candidates)} suitable columns, selected top {MAX_COLUMNS} by priority. "
            f"Priority: numeric > boolean > low-card categorical > datetime. "
            f"Manually specify columns to override."
        )
    
    if not recommended_columns:
        warnings.append(
            "No suitable columns found for anomaly detection. "
            "Supported types: numeric, categorical (string), datetime (date/timestamp), boolean. "
            "Provide columns explicitly."
        )

    # Identify categorical columns for segmentation
    categorical_types = (StringType, IntegerType)
    categorical_fields = [f for f in df.schema.fields if isinstance(f.dataType, categorical_types)]

    for field in categorical_fields:
        col_name = field.name

        # Skip if already selected as a numeric column (numbers shouldn't be segments)
        # But allow categorical/string columns even if they're feature columns
        if col_name in recommended_columns and column_types.get(col_name) == 'numeric':
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
    
    # Remove segment columns from feature columns (they would be constant within each segment)
    if recommended_segments:
        recommended_columns = [col for col in recommended_columns if col not in recommended_segments]

    return AnomalyProfile(
        recommended_columns=recommended_columns,
        recommended_segments=recommended_segments,
        segment_count=segment_count,
        column_stats=column_stats,
        warnings=warnings,
        column_types=column_types,
        unsupported_columns=unsupported_columns,
    )

