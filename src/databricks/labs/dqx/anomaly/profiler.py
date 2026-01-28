"""
Auto-discovery logic for anomaly detection.

Analyzes DataFrames to recommend columns and segments suitable for
anomaly detection using on-the-fly heuristics.
"""

import logging
import re
from dataclasses import dataclass
from typing import Any

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    TimestampNTZType,
    TimestampType,
)

from databricks.labs.dqx.profiling_utils import compute_null_and_distinct_counts

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


def auto_discover_columns(df: DataFrame) -> AnomalyProfile:
    """
    Auto-discover columns and segments for anomaly detection.

    Analyzes the DataFrame using on-the-fly heuristics to recommend
    suitable columns and segmentation strategy.

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

    Returns:
        AnomalyProfile with recommendations and warnings.
    """
    warnings: list[str] = []
    return _auto_discover_heuristic(df, warnings)


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
    distinct_count: int | None = None,
) -> tuple[int, str, str, int, float] | None:
    """Analyze a string (categorical) column and return candidate tuple."""
    if distinct_count is None:
        cardinality_row = df.select(F.countDistinct(col_name)).first()
        if not cardinality_row:
            return None
        distinct_count = cardinality_row[0]

    if distinct_count <= 20 and null_rate < 0.5:
        # Low cardinality categorical
        return (3, col_name, 'categorical', distinct_count, null_rate)  # Priority 3

    if 20 < distinct_count <= 100 and null_rate < 0.5:
        # High cardinality categorical (frequency encoding)
        return (5, col_name, 'categorical', distinct_count, null_rate)  # Priority 5

    if distinct_count > 100:
        warnings.append(
            f"Column '{col_name}' has {distinct_count} distinct values (>100), "
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


def _calculate_total_segments(
    df: DataFrame,
    recommended_segments: list[str],
    warnings: list[str],
    *,
    total_count: int,
    distinct_counts: dict[str, int] | None = None,
) -> int:
    """Calculate total segment combinations and add warning if too many."""
    if not recommended_segments:
        return 1

    segment_count = 1
    for col in recommended_segments:
        if distinct_counts is not None and col in distinct_counts:
            distinct_count = distinct_counts[col]
        else:
            distinct_count = df.select(col).distinct().count()
        segment_count *= distinct_count

    # Warn if segments are too granular relative to data size
    avg_rows_per_segment = total_count / segment_count if segment_count > 0 else 0

    if segment_count > 50:
        warnings.append(
            f"Detected {segment_count} total segments, training may be slow. "
            "Consider filtering or using coarser segmentation."
        )
    elif avg_rows_per_segment < 100:
        warnings.append(
            f"Detected {segment_count} segments with only ~{int(avg_rows_per_segment)} rows per segment on average. "
            f"Models may be unreliable. Consider reducing segmentation or using more data (total rows: {total_count})."
        )

    return segment_count


def _select_segment_columns(
    df: DataFrame,
    recommended_columns: list[str],
    column_types: dict[str, str],
    id_pattern: "re.Pattern[str]",
    warnings: list[str],
    *,
    total_count: int,
    null_counts: dict[str, int],
    distinct_counts: dict[str, int],
) -> tuple[list[str], int]:
    """Identify and validate segment columns."""
    recommended_segments = []
    candidate_segments = []  # Track all viable candidates for user info
    categorical_types = (StringType, IntegerType)
    categorical_fields = [f for f in df.schema.fields if isinstance(f.dataType, categorical_types)]

    for field in categorical_fields:
        col_name = field.name

        # Skip numeric feature columns
        if col_name in recommended_columns and column_types.get(col_name) == 'numeric':
            continue

        # Compute distinct count and null rate
        distinct_count = distinct_counts.get(col_name)
        null_count = null_counts.get(col_name, 0)
        if distinct_count is None:
            distinct_row = df.select(F.countDistinct(col_name)).first()
            assert distinct_row is not None, "Failed to compute distinct count"
            distinct_count = distinct_row[0]
        null_rate = null_count / total_count if total_count > 0 else 1.0
        is_id_column = id_pattern.search(col_name) is not None

        # Check segment criteria: conservative for auto-discovery
        # Only consider columns with 2-20 distinct values (not 50)
        # Ensure at least 100 rows per segment on average
        meets_segment_criteria = (
            2 <= distinct_count <= 20  # More conservative upper bound
            and null_rate < 0.1
            and not is_id_column
            and (total_count / distinct_count) >= 100  # At least 100 rows per segment
        )
        is_high_cardinality = distinct_count > 50

        if meets_segment_criteria and _validate_and_add_segment_column(df, col_name, warnings):
            candidate_segments.append((col_name, distinct_count, total_count / distinct_count))
        elif is_high_cardinality:
            _check_high_cardinality_warning(field, col_name, distinct_count, warnings)

    # AUTO-DISCOVERY STRATEGY: Be conservative, prefer single segment column
    # Sort candidates by: lowest cardinality first (fewer segments = more reliable)
    candidate_segments.sort(key=lambda x: x[1])  # Sort by distinct_count ascending

    if candidate_segments:
        # For auto-discovery, only select the FIRST (lowest cardinality) candidate
        selected = candidate_segments[0]
        recommended_segments.append(selected[0])

        # Log helpful info about selection and alternatives
        logger.info(
            f"Auto-segmentation selected 1 column: [{selected[0]}] "
            f"({int(selected[1])} segments, ~{int(selected[2])} rows/segment)"
        )

        # Suggest additional segmentation options if available
        if len(candidate_segments) > 1:
            other_options = ", ".join(
                [f"{col} ({int(dc)} segments)" for col, dc, _ in candidate_segments[1:4]]  # Show up to 3 more
            )
            logger.info(
                f"Consider additional segmentation for more granularity: "
                f"segment_by=['{selected[0]}', <column>] where <column> could be: {other_options}"
            )

    # Calculate total segment combinations
    segment_count = _calculate_total_segments(
        df, recommended_segments, warnings, total_count=total_count, distinct_counts=distinct_counts
    )
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
    distinct_count: int | None = None,
) -> None:
    """Analyze string column and add to candidates if suitable."""
    candidate = _analyze_string_column(df, col_name, null_rate, warnings, distinct_count=distinct_count)
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
    distinct_count: int | None = None,
) -> None:
    """Analyze a column based on its type and add to candidates if suitable."""
    if isinstance(col_type, (IntegerType, LongType, FloatType, DoubleType, DecimalType)):
        _analyze_and_add_numeric_column(df, col_name, null_rate, candidates, column_stats)
    elif isinstance(col_type, BooleanType):
        _analyze_and_add_boolean_column(col_name, null_rate, candidates)
    elif isinstance(col_type, (DateType, TimestampType, TimestampNTZType)):
        _analyze_and_add_datetime_column(col_name, null_rate, candidates)
    elif isinstance(col_type, StringType):
        _analyze_and_add_string_column(df, col_name, null_rate, warnings, candidates, distinct_count=distinct_count)
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
    column_names = [f.name for f in df.schema.fields]

    distinct_columns = [f.name for f in df.schema.fields if isinstance(f.dataType, (StringType, IntegerType))]
    null_counts, distinct_counts = compute_null_and_distinct_counts(
        df,
        column_names,
        distinct_columns,
        approx=True,
        rsd=0.05,
    )

    for field in df.schema.fields:
        col_name = field.name
        col_type = field.dataType

        # Skip ID columns
        if id_pattern.search(col_name):
            continue

        null_count = null_counts.get(col_name, 0)
        null_rate = null_count / total_count if total_count > 0 else 1.0

        # Analyze by type and collect candidates
        _analyze_column_by_type(
            col_type,
            df,
            col_name,
            null_rate,
            warnings,
            candidates,
            column_stats,
            unsupported_columns,
            distinct_count=distinct_counts.get(col_name),
        )

    # Select top columns
    max_columns = 10
    recommended_columns, column_types = _select_top_columns(candidates, max_columns, warnings)

    # Select segment columns
    recommended_segments, segment_count = _select_segment_columns(
        df,
        recommended_columns,
        column_types,
        id_pattern,
        warnings,
        total_count=total_count,
        null_counts=null_counts,
        distinct_counts=distinct_counts,
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
