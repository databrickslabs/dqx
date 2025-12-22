"""
Auto-discovery logic for anomaly detection.

Analyzes DataFrames to recommend columns and segments suitable for
anomaly detection, leveraging existing DQX profiler output when available.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from re import Pattern
from typing import Any

from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    DecimalType,
    StringType,
    BooleanType,
    DateType,
    TimestampType,
    TimestampNTZType,
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
    column_types: dict[str, str] | None = None  # NEW: maps column -> type category
    unsupported_columns: list[str] | None = None  # NEW: columns that cannot be used


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
    warnings: list[str] = []

    if profiler_output_table:
        try:
            # Use existing profiler output
            profile_df = df.sparkSession.table(profiler_output_table)
            return _auto_discover_from_profiler(df, profile_df, warnings)
        except Exception as e:
            logger.warning(
                f"Failed to read profiler output table {profiler_output_table}: {e}. Falling back to heuristics."
            )

    # Fallback to on-the-fly heuristics
    return _auto_discover_heuristic(df, warnings)


def _prioritize_profiler_columns(profile_df: DataFrame, id_pattern: str) -> DataFrame:
    """Add priority column to profiler output and filter suitable candidates."""
    # Filter out ID columns and high-null columns
    # Add temp column to avoid pylint issue with ~ operator on Column type
    filtered_df = (
        profile_df.withColumn("_is_id", F.col("column_name").rlike(id_pattern))
        .filter((F.col("_is_id") == F.lit(False)) & (F.col("null_rate") < 0.5))
        .drop("_is_id")
    )
    return (
        filtered_df.withColumn(
            "priority",
            F.when(
                F.col("type").isin(["int", "long", "float", "double", "decimal"]) & (F.col("stddev") > 0),
                1,  # Numeric with variance
            )
            .when(F.col("type") == "boolean", 2)  # Boolean
            .when((F.col("type") == "string") & (F.col("distinct_count") <= 20), 3)  # Low-cardinality categorical
            .when(F.col("type").isin(["date", "timestamp", "timestampNTZ"]), 4)  # Datetime
            .when(
                (F.col("type") == "string") & (F.col("distinct_count").between(21, 100)),
                5,  # High-cardinality categorical
            )
            .otherwise(999),  # Unsupported/filtered
        )
        .filter(F.col("priority") < 999)
        .orderBy("priority", "column_name")
    )


def _extract_recommended_columns(
    candidates_list: list,
    max_columns: int,
) -> tuple[list[str], dict[str, str]]:
    """Extract recommended columns and their types from candidate rows."""
    recommended_columns = []
    column_types = {}

    for row in candidates_list[:max_columns]:
        col_name = row["column_name"]
        priority = row["priority"]

        # Map profiler type to category
        if priority == 1:
            category = "numeric"
        elif priority == 2:
            category = "boolean"
        elif priority in {3, 5}:
            category = "categorical"
        elif priority == 4:
            category = "datetime"
        else:
            continue

        recommended_columns.append(col_name)
        column_types[col_name] = category

    return recommended_columns, column_types


def _validate_segment_columns_from_profiler(
    df: DataFrame,
    profile_df: DataFrame,
    id_pattern: str,
    recommended_columns: list[str],
    column_types: dict[str, str],
    warnings: list[str],
) -> tuple[list[str], int]:
    """Identify and validate segment columns from profiler output."""
    # Filter categorical candidates, avoiding ~ operator for pylint
    categorical_candidates = (
        profile_df.withColumn("_is_id", F.col("column_name").rlike(id_pattern))
        .filter(
            (F.col("type").isin(["string", "int"]))
            & (F.col("distinct_count").between(2, 50))
            & (F.col("null_rate") < 0.1)
            & (F.col("_is_id") == F.lit(False))
        )
        .drop("_is_id")
    )

    recommended_segments = []
    for row in categorical_candidates.collect():
        col = row["column_name"]

        # Skip if already selected as a numeric column
        if col in recommended_columns and column_types.get(col) == 'numeric':
            continue

        # Check minimum segment size
        min_segment_size_row = df.groupBy(col).count().select(F.min("count").alias("min_count")).first()

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

    return recommended_segments, segment_count


def _gather_column_stats(df: DataFrame, columns: list[str]) -> dict[str, dict]:
    """Gather basic statistics for selected columns."""
    column_stats = {}
    for col in columns:
        stats_row = df.select(
            F.mean(col).alias("mean"),
            F.stddev(col).alias("std"),
            F.min(col).alias("min"),
            F.max(col).alias("max"),
        ).first()
        if stats_row:
            column_stats[col] = stats_row.asDict()
    return column_stats


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
    id_pattern = "(?i)(id|key)$"
    max_columns = 10

    # Prioritize and filter columns
    candidates_df = _prioritize_profiler_columns(profile_df, id_pattern)
    candidates_list = candidates_df.limit(max_columns + 50).collect()  # Get extra for counting

    # Extract recommended columns
    recommended_columns, column_types = _extract_recommended_columns(candidates_list, max_columns)

    # Add warnings about selection
    if len(candidates_list) > max_columns:
        warnings.append(
            f"Found {len(candidates_list)} suitable columns, selected top {max_columns} by priority. "
            f"Priority: numeric > boolean > low-card categorical > datetime. "
            f"Manually specify columns to override."
        )

    if not recommended_columns:
        warnings.append(
            "No suitable columns found for anomaly detection. "
            "Supported types: numeric, categorical (string), datetime, boolean. "
            "Provide columns explicitly."
        )

    # Validate and select segment columns
    recommended_segments, segment_count = _validate_segment_columns_from_profiler(
        df, profile_df, id_pattern, recommended_columns, column_types, warnings
    )

    # Gather column stats
    column_stats = _gather_column_stats(df, recommended_columns)

    return AnomalyProfile(
        recommended_columns=recommended_columns,
        recommended_segments=recommended_segments,
        segment_count=segment_count,
        column_stats=column_stats,
        warnings=warnings,
        column_types=column_types,  # Now populated from profiler
        unsupported_columns=[],  # Profiler doesn't track these explicitly
    )


def _analyze_numeric_column(
    df: DataFrame,
    col_name: str,
    null_rate: float,
) -> tuple[tuple[int, str, str, None, float] | None, dict | None]:
    """Analyze a numeric column and return candidate tuple and stats."""
    if null_rate >= 0.5:
        return None, None

    stats_row = df.select(
        F.mean(col_name).alias("mean"),
        F.stddev(col_name).alias("std"),
    ).first()

    if not stats_row:
        return None, None

    std = stats_row["std"] or 0.0
    if std > 0:
        candidate = (1, col_name, 'numeric', None, null_rate)  # Priority 1
        stats = {"mean": stats_row["mean"], "std": std}
        return candidate, stats

    return None, None


def _analyze_boolean_column(
    col_name: str,
    null_rate: float,
) -> tuple[int, str, str, None, float] | None:
    """Analyze a boolean column and return candidate tuple."""
    if null_rate < 0.5:
        return (2, col_name, 'boolean', None, null_rate)  # Priority 2
    return None


def _analyze_datetime_column(
    col_name: str,
    null_rate: float,
) -> tuple[int, str, str, None, float] | None:
    """Analyze a datetime column and return candidate tuple."""
    if null_rate < 0.5:
        return (4, col_name, 'datetime', None, null_rate)  # Priority 4
    return None


def _analyze_string_column(
    df: DataFrame,
    col_name: str,
    null_rate: float,
    warnings: list[str],
) -> tuple[int, str, str, int, float] | None:
    """Analyze a string (categorical) column and return candidate tuple."""
    cardinality_row = df.select(F.countDistinct(col_name)).first()
    if not cardinality_row:
        return None

    cardinality = cardinality_row[0]

    if cardinality <= 20 and null_rate < 0.5:
        # Low cardinality categorical
        return (3, col_name, 'categorical', cardinality, null_rate)  # Priority 3

    if 20 < cardinality <= 100 and null_rate < 0.5:
        # High cardinality categorical (frequency encoding)
        return (5, col_name, 'categorical', cardinality, null_rate)  # Priority 5

    if cardinality > 100:
        warnings.append(
            f"Column '{col_name}' has {cardinality} distinct values (>100), "
            "excluding from auto-selection (too high cardinality)."
        )

    return None


def _select_top_columns(
    candidates: list[tuple],
    max_columns: int,
    warnings: list[str],
) -> tuple[list[str], dict[str, str]]:
    """Select top N columns from candidates by priority."""
    # Sort by priority (lower number = higher priority)
    candidates.sort(key=lambda x: (x[0], x[1]))  # Sort by priority, then name

    recommended_columns = []
    column_types = {}

    for _priority, col_name, col_type_str, _cardinality, _null_rate in candidates[:max_columns]:
        recommended_columns.append(col_name)
        column_types[col_name] = col_type_str

    if len(candidates) > max_columns:
        warnings.append(
            f"Found {len(candidates)} suitable columns, selected top {max_columns} by priority. "
            f"Priority: numeric > boolean > low-card categorical > datetime. "
            f"Manually specify columns to override."
        )

    if not recommended_columns:
        warnings.append(
            "No suitable columns found for anomaly detection. "
            "Supported types: numeric, categorical (string), datetime (date/timestamp), boolean. "
            "Provide columns explicitly."
        )

    return recommended_columns, column_types


def _validate_and_add_segment_column(
    df: DataFrame,
    col_name: str,
    warnings: list[str],
) -> bool:
    """Validate minimum segment size and add warnings if needed. Returns True if column should be added."""
    min_segment_size_row = df.groupBy(col_name).count().select(F.min("count").alias("min_count")).first()
    if min_segment_size_row:
        min_segment_size = min_segment_size_row["min_count"]
        if min_segment_size < 1000:
            warnings.append(
                f"Segment column '{col_name}' has segments with <1000 rows (min: {min_segment_size}), "
                "models may be unreliable."
            )
        return True
    return False


def _check_high_cardinality_warning(
    field: Any,
    col_name: str,
    distinct_count: int,
    warnings: list[str],
) -> None:
    """Add warning if column has high cardinality."""
    if isinstance(field.dataType, StringType) and "id" not in col_name.lower():
        warnings.append(
            f"Column '{col_name}' has {distinct_count} distinct values, "
            "excluding from auto-selection (too high cardinality for segmentation)."
        )


def _calculate_total_segments(df: DataFrame, recommended_segments: list[str], warnings: list[str]) -> int:
    """Calculate total segment combinations and add warning if too many."""
    if not recommended_segments:
        return 1

    segment_count = 1
    for col in recommended_segments:
        distinct_count = df.select(col).distinct().count()
        segment_count *= distinct_count

    if segment_count > 50:
        warnings.append(
            f"Detected {segment_count} total segments, training may be slow. "
            "Consider filtering or using coarser segmentation."
        )

    return segment_count


def _select_segment_columns(
    df: DataFrame,
    recommended_columns: list[str],
    column_types: dict[str, str],
    id_pattern: "Pattern[str]",
    warnings: list[str],
) -> tuple[list[str], int]:
    """Identify and validate segment columns."""
    recommended_segments = []
    categorical_types = (StringType, IntegerType)
    categorical_fields = [f for f in df.schema.fields if isinstance(f.dataType, categorical_types)]

    for field in categorical_fields:
        col_name = field.name

        # Skip numeric feature columns
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
        is_id_column = id_pattern.search(col_name) is not None

        # Check segment criteria
        meets_segment_criteria = 2 <= distinct_count <= 50 and null_rate < 0.1 and not is_id_column
        is_high_cardinality = distinct_count > 50

        if meets_segment_criteria and _validate_and_add_segment_column(df, col_name, warnings):
            recommended_segments.append(col_name)
        elif is_high_cardinality:
            _check_high_cardinality_warning(field, col_name, distinct_count, warnings)

    # Calculate total segment combinations
    segment_count = _calculate_total_segments(df, recommended_segments, warnings)
    return recommended_segments, segment_count


def _analyze_and_add_numeric_column(
    df: DataFrame,
    col_name: str,
    null_rate: float,
    candidates: list[tuple[int, str, str, int | None, float]],
    column_stats: dict[str, dict[str, float]],
) -> None:
    """Analyze numeric column and add to candidates if suitable."""
    candidate, stats = _analyze_numeric_column(df, col_name, null_rate)
    if candidate:
        candidates.append(candidate)
        if stats:
            column_stats[col_name] = stats


def _analyze_and_add_boolean_column(
    col_name: str,
    null_rate: float,
    candidates: list[tuple[int, str, str, int | None, float]],
) -> None:
    """Analyze boolean column and add to candidates if suitable."""
    candidate = _analyze_boolean_column(col_name, null_rate)
    if candidate:
        candidates.append(candidate)


def _analyze_and_add_datetime_column(
    col_name: str,
    null_rate: float,
    candidates: list[tuple[int, str, str, int | None, float]],
) -> None:
    """Analyze datetime column and add to candidates if suitable."""
    candidate = _analyze_datetime_column(col_name, null_rate)
    if candidate:
        candidates.append(candidate)


def _analyze_and_add_string_column(
    df: DataFrame,
    col_name: str,
    null_rate: float,
    warnings: list[str],
    candidates: list[tuple[int, str, str, int | None, float]],
) -> None:
    """Analyze string column and add to candidates if suitable."""
    candidate = _analyze_string_column(df, col_name, null_rate, warnings)
    if candidate:
        candidates.append(candidate)


def _analyze_column_by_type(
    col_type: Any,
    df: DataFrame,
    col_name: str,
    null_rate: float,
    warnings: list[str],
    candidates: list[tuple[int, str, str, int | None, float]],
    column_stats: dict[str, dict[str, float]],
    unsupported_columns: list[str],
) -> None:
    """Analyze a column based on its type and add to candidates if suitable."""
    if isinstance(col_type, (IntegerType, LongType, FloatType, DoubleType, DecimalType)):
        _analyze_and_add_numeric_column(df, col_name, null_rate, candidates, column_stats)
    elif isinstance(col_type, BooleanType):
        _analyze_and_add_boolean_column(col_name, null_rate, candidates)
    elif isinstance(col_type, (DateType, TimestampType, TimestampNTZType)):
        _analyze_and_add_datetime_column(col_name, null_rate, candidates)
    elif isinstance(col_type, StringType):
        _analyze_and_add_string_column(df, col_name, null_rate, warnings, candidates)
    else:
        unsupported_columns.append(col_name)


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
    column_stats: dict[str, dict] = {}
    unsupported_columns: list[str] = []
    candidates: list[tuple[int, str, str, int | None, float]] = []

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
        assert null_count_row is not None, "Failed to compute null count"
        null_rate = null_count_row["null_count"] / total_count if total_count > 0 else 1.0

        # Analyze by type and collect candidates
        _analyze_column_by_type(
            col_type, df, col_name, null_rate, warnings, candidates, column_stats, unsupported_columns
        )

    # Select top columns
    max_columns = 10
    recommended_columns, column_types = _select_top_columns(candidates, max_columns, warnings)

    # Select segment columns
    recommended_segments, segment_count = _select_segment_columns(
        df, recommended_columns, column_types, id_pattern, warnings
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
