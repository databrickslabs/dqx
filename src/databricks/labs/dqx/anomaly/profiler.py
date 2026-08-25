"""
Auto-discovery logic for row anomaly detection.

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

from databricks.labs.dqx.anomaly.group_config import (
    MAX_AUTO_GROUP_COUNT,
    MAX_BASELINE_COLUMN_CARDINALITY,
    MAX_BASELINE_GROUPS,
    MAX_SEGMENT_MODELS,
    MIN_ROWS_PER_BASELINE_GROUP,
    MIN_ROWS_PER_SEGMENT,
)
from databricks.labs.dqx.profiling_utils import compute_exact_distinct_counts, compute_null_and_distinct_counts

logger = logging.getLogger(__name__)


@dataclass
class AnomalyProfile:
    """Auto-discovery results for row anomaly detection."""

    recommended_columns: list[str]
    recommended_segments: list[str]
    segment_count: int
    column_stats: dict[str, dict]
    warnings: list[str]
    column_types: dict[str, str] | None = None  # NEW: maps column -> type category
    unsupported_columns: list[str] | None = None  # NEW: columns that cannot be used


def auto_discover_columns(df: DataFrame, *, for_baseline: bool = False) -> AnomalyProfile:
    """
    Auto-discover columns and segments for row anomaly detection.

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
        for_baseline: Select the grouping for baseline conditioning rather than for the legacy
            segmented path. Baseline conditioning trains one model whatever the group count, so it
            can afford a finer grouping and only needs enough rows per group to take a median. See
            :func:`select_baseline_columns`.

    Returns:
        AnomalyProfile with recommendations and warnings.
    """
    warnings: list[str] = []
    return _auto_discover_heuristic(df, warnings, for_baseline=for_baseline)


def _compute_numeric_stats_batched(df: DataFrame, column_names: list[str]) -> dict[str, dict[str, float]]:
    """Compute mean and stddev for all numeric columns in a single aggregation."""
    if not column_names:
        return {}
    agg_exprs = []
    for col_name in column_names:
        agg_exprs.append(F.mean(F.col(col_name)).alias(f"{col_name}__mean"))
        agg_exprs.append(F.stddev(F.col(col_name)).alias(f"{col_name}__std"))
    row = df.agg(*agg_exprs).first()
    if row is None:
        return {}
    return {
        col_name: {"mean": row[f"{col_name}__mean"], "std": row[f"{col_name}__std"] or 0.0} for col_name in column_names
    }


def _analyze_numeric_column(
    df: DataFrame,
    col_name: str,
    null_rate: float,
    numeric_stats: dict[str, dict[str, float]] | None = None,
) -> tuple[tuple[int, str, str, None, float] | None, dict | None]:
    """Analyze a numeric column and return candidate tuple and stats. Uses numeric_stats if provided (batched)."""
    if null_rate >= 0.5:
        return None, None

    if numeric_stats and col_name in numeric_stats:
        mean_std = numeric_stats[col_name]
        std = mean_std.get("std") or 0.0
    else:
        stats_row = df.select(
            F.mean(F.col(col_name)).alias("mean"),
            F.stddev(F.col(col_name)).alias("std"),
        ).first()
        if stats_row is None:
            return None, None
        std = stats_row["std"] or 0.0
        mean_std = {"mean": stats_row["mean"], "std": std}

    if std > 0:
        candidate = (1, col_name, 'numeric', None, null_rate)  # Priority 1
        stats = {"mean": mean_std.get("mean"), "std": std}
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
    col_name: str,
    null_rate: float,
    warnings: list[str],
    distinct_count: int,
) -> tuple[int, str, str, int, float] | None:
    """Analyze a string (categorical) column and return candidate tuple. distinct_count is always set by callers."""
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
            "No suitable columns found for row anomaly detection. "
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
    min_segment_size = min_segment_size_row["min_count"] if min_segment_size_row else None
    if min_segment_size is not None and min_segment_size < 1000:
        warnings.append(
            f"Segment column '{col_name}' has segments with <1000 rows (min: {min_segment_size}), "
            "models may be unreliable."
        )
    return True


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
    recommended_segments: list[str],
    warnings: list[str],
    *,
    total_count: int,
    distinct_counts: dict[str, int],
) -> int:
    """Calculate total segment combinations and add warning if too many."""
    if not recommended_segments:
        return 1

    segment_count = 1
    for col in recommended_segments:
        distinct_count = distinct_counts[col]
        segment_count *= distinct_count

    # Warn if segments are too granular relative to data size
    avg_rows_per_segment = total_count / segment_count if segment_count > 0 else 0

    if segment_count > MAX_SEGMENT_MODELS:
        warnings.append(
            f"Detected {segment_count} total segments, training may be slow. "
            "Consider filtering or using coarser segmentation."
        )
    elif avg_rows_per_segment < MIN_ROWS_PER_SEGMENT:
        warnings.append(
            f"Detected {segment_count} segments with only ~{int(avg_rows_per_segment)} rows per segment on average. "
            f"Models may be unreliable. Consider reducing segmentation or using more data (total rows: {total_count})."
        )

    return segment_count


def _is_grouping_candidate(
    distinct_count: int,
    *,
    null_rate: float,
    is_id_column: bool,
    total_count: int,
    for_baseline: bool,
) -> bool:
    """Whether a column may be *considered* as a grouping.

    The bar differs by destination, for the same reason the selection policy does: a segment has to
    support training a forest on its own rows, a baseline group only has to yield a median. Identifier
    -like names and columns that are more than 10% null are rejected either way -- a grouping keyed on
    something nearly unique or frequently missing is not a peer group.
    """
    max_cardinality = MAX_BASELINE_COLUMN_CARDINALITY if for_baseline else MAX_AUTO_GROUP_COUNT
    min_rows = MIN_ROWS_PER_BASELINE_GROUP if for_baseline else MIN_ROWS_PER_SEGMENT
    return (
        2 <= distinct_count <= max_cardinality
        and null_rate < 0.1
        and not is_id_column
        and (total_count / distinct_count) >= min_rows
    )


def select_baseline_columns(candidates: list[tuple[str, int, float]], total_count: int) -> list[str]:
    """Choose a grouping for baseline conditioning, given candidates ordered by cardinality.

    Adds columns while every resulting group still holds enough rows for a representative median and
    the total group count stays within what is sensible to persist and broadcast. That is the whole
    constraint: there is one model regardless of group count, so breadth costs nothing at training
    time and buys a tighter baseline.

    Deliberately *not* the segmented policy of taking a single lowest-cardinality column. On a
    dataset grouped by country x event_type x product, that policy selected 3 groups out of 90 and
    conditioning barely engaged. Cardinality-ascending order makes the choice deterministic and
    spends the row budget on the coarsest dimensions first, so the grouping degrades gracefully on
    smaller tables instead of picking one arbitrary fine dimension.
    """
    selected: list[str] = []
    groups = 1
    for name, distinct_count, _rows_per_group in candidates:
        if distinct_count > MAX_BASELINE_COLUMN_CARDINALITY:
            continue
        prospective = groups * int(distinct_count)
        if prospective > MAX_BASELINE_GROUPS:
            continue
        if total_count / prospective < MIN_ROWS_PER_BASELINE_GROUP:
            continue
        selected.append(name)
        groups = prospective

    if selected:
        logger.info(
            f"Auto-detected baseline grouping {selected}: {groups} groups, "
            f"~{int(total_count / groups)} rows/group (one model regardless of group count)"
        )
        skipped = [c[0] for c in candidates if c[0] not in selected]
        if skipped:
            logger.debug(
                f"Not added to the baseline grouping (would leave under "
                f"{MIN_ROWS_PER_BASELINE_GROUP} rows/group, or exceed {MAX_BASELINE_GROUPS} "
                f"groups): {skipped}"
            )
    return selected


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
    for_baseline: bool = False,
) -> tuple[list[str], int]:
    """Identify and validate segment columns.

    *for_baseline* selects the grouping policy. See :func:`select_baseline_columns` for why the two
    differ: a per-segment model has to train on its group, a baseline group only has to yield a
    median.
    """
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
        if distinct_count is None:
            distinct_row = df.select(F.countDistinct(col_name)).first()
            assert distinct_row is not None  # to satisfy linter
            distinct_count = distinct_row[0]
        null_rate = null_counts.get(col_name, 0) / total_count if total_count > 0 else 1.0

        meets_segment_criteria = _is_grouping_candidate(
            distinct_count,
            null_rate=null_rate,
            is_id_column=id_pattern.search(col_name) is not None,
            total_count=total_count,
            for_baseline=for_baseline,
        )

        if meets_segment_criteria and _validate_and_add_segment_column(df, col_name, warnings):
            candidate_segments.append((col_name, distinct_count, total_count / distinct_count))
        elif distinct_count > 50:
            _check_high_cardinality_warning(field, col_name, distinct_count, warnings)

    # Sort candidates by lowest cardinality first. For the segmented path that means "fewest models";
    # for the baseline path it means the cheapest granularity is added first.
    candidate_segments.sort(key=lambda x: x[1])  # Sort by distinct_count ascending

    if candidate_segments and for_baseline:
        recommended_segments.extend(select_baseline_columns(candidate_segments, total_count))
    elif candidate_segments:
        # LEGACY SEGMENTED PATH: be conservative, take a single column. Every distinct value becomes
        # its own model, so breadth here is paid for in training time.
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
        recommended_segments, warnings, total_count=total_count, distinct_counts=distinct_counts
    )
    return recommended_segments, segment_count


def _analyze_and_add_numeric_column(
    df: DataFrame,
    col_name: str,
    null_rate: float,
    candidates: list[tuple[int, str, str, int | None, float]],
    column_stats: dict[str, dict[str, float]],
    numeric_stats: dict[str, dict[str, float]] | None = None,
) -> None:
    """Analyze numeric column and add to candidates if suitable."""
    candidate, stats = _analyze_numeric_column(df, col_name, null_rate, numeric_stats=numeric_stats)
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
    col_name: str,
    null_rate: float,
    warnings: list[str],
    candidates: list[tuple[int, str, str, int | None, float]],
    distinct_count: int | None = None,
) -> None:
    """Analyze string column and add to candidates if suitable."""
    if distinct_count is None:
        return
    candidate = _analyze_string_column(col_name, null_rate, warnings, distinct_count)
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
    numeric_stats: dict[str, dict[str, float]] | None = None,
) -> None:
    """Analyze a column based on its type and add to candidates if suitable."""
    if isinstance(col_type, (IntegerType, LongType, FloatType, DoubleType, DecimalType)):
        _analyze_and_add_numeric_column(df, col_name, null_rate, candidates, column_stats, numeric_stats=numeric_stats)
    elif isinstance(col_type, BooleanType):
        _analyze_and_add_boolean_column(col_name, null_rate, candidates)
    elif isinstance(col_type, (DateType, TimestampType, TimestampNTZType)):
        _analyze_and_add_datetime_column(col_name, null_rate, candidates)
    elif isinstance(col_type, StringType):
        _analyze_and_add_string_column(col_name, null_rate, warnings, candidates, distinct_count=distinct_count)
    else:
        unsupported_columns.append(col_name)


def _compute_discovery_stats(df: DataFrame) -> tuple[dict[str, int], dict[str, int], dict[str, dict[str, float]], int]:
    """Compute null counts, distinct counts, numeric stats, and total count in one pass."""
    total_count = df.count()
    column_names = [f.name for f in df.schema.fields]
    distinct_columns = [f.name for f in df.schema.fields if isinstance(f.dataType, (StringType, IntegerType))]
    null_counts, distinct_counts = compute_null_and_distinct_counts(
        df, column_names, distinct_columns, approx=True, rsd=0.05
    )
    if distinct_columns:
        distinct_counts.update(compute_exact_distinct_counts(df, distinct_columns))
    numeric_types = (IntegerType, LongType, FloatType, DoubleType, DecimalType)
    numeric_columns = [f.name for f in df.schema.fields if isinstance(f.dataType, numeric_types)]
    numeric_stats = _compute_numeric_stats_batched(df, numeric_columns)
    return null_counts, distinct_counts, numeric_stats, total_count


def _auto_discover_heuristic(df: DataFrame, warnings: list[str], *, for_baseline: bool = False) -> AnomalyProfile:
    """
    Auto-discover using on-the-fly heuristics with multi-type support.

    Args:
        df: DataFrame to analyze.
        warnings: List to accumulate warnings.
        for_baseline: Select the grouping for baseline conditioning rather than the legacy
            segmented path. See :func:`select_baseline_columns`.

    Returns:
        AnomalyProfile with recommendations (max 10 columns).
    """
    column_stats: dict[str, dict] = {}
    unsupported_columns: list[str] = []
    candidates: list[tuple[int, str, str, int | None, float]] = []
    id_pattern = re.compile(r"(?i)(id|key)$")

    null_counts, distinct_counts, numeric_stats, total_count = _compute_discovery_stats(df)

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
            numeric_stats=numeric_stats,
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
        for_baseline=for_baseline,
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
