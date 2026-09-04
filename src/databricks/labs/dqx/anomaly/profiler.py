"""
Auto-discovery logic for row anomaly detection.

Analyzes DataFrames to recommend columns and segments suitable for
anomaly detection using on-the-fly heuristics.
"""

import logging
import math
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

# The sampling defaults live with sampling. profiler does not otherwise depend on core, and core does
# not import profiler, so this direction adds no cycle.
from databricks.labs.dqx.anomaly.core import DEFAULT_SAMPLE_FRACTION, DEFAULT_TRAIN_RATIO
from databricks.labs.dqx.anomaly.group_config import (
    MAX_BASELINE_COLUMN_CARDINALITY,
    MAX_BASELINE_GROUPS,
    MIN_ROWS_PER_BASELINE_GROUP,
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


def auto_discover_columns(df: DataFrame) -> AnomalyProfile:
    """
    Auto-discover feature columns and a baseline grouping for row anomaly detection.

    Analyzes the DataFrame using on-the-fly heuristics to recommend suitable columns and a grouping
    to condition on.

    Column selection criteria:
    - Numeric types (int, long, float, double, decimal)
    - stddev > 0 (has variance)
    - null_rate < 50%
    - Exclude: timestamps, IDs (detected by name patterns)

    Baseline grouping criteria (see :func:`select_baseline_columns`):
    - Categorical types (string, int) with 2-50 distinct values
    - null_rate < 10%
    - At least MIN_ROWS_PER_BASELINE_GROUP rows per resulting group

    Args:
        df: DataFrame to analyze.

    Returns:
        AnomalyProfile with recommendations and warnings.
    """
    warnings: list[str] = []
    return _auto_discover_heuristic(df, warnings)


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
            "excluding from auto-selection (too high cardinality for a baseline grouping)."
        )


def _count_group_combinations(recommended_segments: list[str], distinct_counts: dict[str, int]) -> int:
    """Total groups the chosen grouping produces, as the product of its columns' cardinalities.

    No warnings here any more. This used to caution about too many segments or too few rows each,
    which mattered when every group became its own model. :func:`select_baseline_columns` now enforces
    both bounds while choosing, so neither condition can survive to be warned about.
    """
    if not recommended_segments:
        return 1
    combinations = 1
    for col in recommended_segments:
        combinations *= distinct_counts[col]
    return combinations


def _is_grouping_candidate(
    distinct_count: int,
    *,
    null_rate: float,
    is_id_column: bool,
    total_count: int,
    sample_fraction: float | None = None,
    train_ratio: float | None = None,
) -> bool:
    """Whether a column may be *considered* as a baseline grouping.

    Identifier-like names and columns more than 10% null are rejected: a grouping keyed on something
    nearly unique or frequently missing is not a peer group. The row requirement is only what a
    median needs, since there is one model regardless of how many groups result.
    """
    return (
        2 <= distinct_count <= MAX_BASELINE_COLUMN_CARDINALITY
        and null_rate < 0.1
        and not is_id_column
        and (total_count / distinct_count) >= effective_min_rows_per_group(sample_fraction, train_ratio)
    )


def effective_min_rows_per_group(sample_fraction: float | None = None, train_ratio: float | None = None) -> int:
    """Rows a group needs *on the full table* for the fit to see MIN_ROWS_PER_BASELINE_GROUP of them.

    Discovery runs before sampling, so checking the nominal minimum against the full table overstated what
    the model would get by roughly 4x at the defaults: sampling keeps 0.3 and the train split keeps 0.8 of
    that, leaving about 7 rows from a group that just cleared 30. A per-group median fitted on 7 rows is not
    the representative baseline the constant is there to guarantee.

    Raising the bar on the full table is preferred to re-validating afterwards, which would mean choosing a
    grouping, sampling, finding it too fine, and choosing again.
    """
    retained = (sample_fraction if sample_fraction is not None else DEFAULT_SAMPLE_FRACTION) * (
        train_ratio if train_ratio is not None else DEFAULT_TRAIN_RATIO
    )
    if retained <= 0.0 or retained >= 1.0:
        return MIN_ROWS_PER_BASELINE_GROUP
    return int(math.ceil(MIN_ROWS_PER_BASELINE_GROUP / retained))


def select_baseline_columns(
    candidates: list[tuple[str, int, float]],
    total_count: int,
    sample_fraction: float | None = None,
    train_ratio: float | None = None,
) -> list[str]:
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
        if total_count / prospective < effective_min_rows_per_group(sample_fraction, train_ratio):
            continue
        selected.append(name)
        groups = prospective

    if selected:
        # Neutral wording on purpose: this policy is shared by the path that applies a discovered
        # grouping and the advisory path that only reports one, so it must not claim application.
        logger.info(
            f"Baseline grouping {selected}: {groups} groups, "
            f"~{int(total_count / groups)} rows/group (one model regardless of group count)"
        )
        skipped = [c[0] for c in candidates if c[0] not in selected]
        if skipped:
            logger.debug(
                f"Not added to the baseline grouping (would leave under "
                f"{effective_min_rows_per_group()} rows/group before sampling, or exceed {MAX_BASELINE_GROUPS} "
                f"groups): {skipped}"
            )
    return selected


@dataclass(frozen=True)
class BaselineSuggestion:
    """A grouping the data would support, for advising a caller who did not ask for one."""

    columns: list[str]
    group_count: int
    rows_per_group: int


def suggest_baseline_columns(df: DataFrame, exclude: list[str]) -> BaselineSuggestion | None:
    """Find a grouping the data would support, without selecting feature columns.

    Deliberately narrower than :func:`auto_discover_columns`: the grouping decision needs null rates
    and distinct counts on the categorical columns only, so this skips the numeric mean/stddev
    aggregation a full profile computes and would then throw away.

    Used to *advise*, never to apply. A caller who named *columns* has decided what to measure, and
    DQX does not add engineered features they did not ask for.

    Args:
        df: DataFrame to scan.
        exclude: Columns the caller already named as features. A column cannot be both what is
            measured and what it is measured against.

    Returns:
        The grouping and its shape, or None when the data supports none.
    """
    excluded = set(exclude)
    categorical_types = (StringType, IntegerType)
    categorical = [
        f.name for f in df.schema.fields if isinstance(f.dataType, categorical_types) and f.name not in excluded
    ]
    if not categorical:
        return None

    total_count = df.count()
    if total_count == 0:
        return None
    null_counts, distinct_counts = compute_null_and_distinct_counts(df, categorical, categorical, approx=True, rsd=0.05)
    distinct_counts.update(compute_exact_distinct_counts(df, categorical))

    id_pattern = re.compile(r"(?i)(id|key)$")
    candidates = []
    for name in categorical:
        distinct_count = distinct_counts.get(name)
        if distinct_count is None:
            continue
        if _is_grouping_candidate(
            distinct_count,
            null_rate=null_counts.get(name, 0) / total_count,
            is_id_column=id_pattern.search(name) is not None,
            total_count=total_count,
        ):
            candidates.append((name, distinct_count, total_count / distinct_count))

    candidates.sort(key=lambda candidate: candidate[1])
    selected = select_baseline_columns(candidates, total_count)
    if not selected:
        return None

    group_count = _count_group_combinations(selected, distinct_counts)
    return BaselineSuggestion(
        columns=selected,
        group_count=group_count,
        rows_per_group=int(total_count / group_count) if group_count else total_count,
    )


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
    """Identify baseline grouping columns and count the groups they produce.

    See :func:`select_baseline_columns` for the policy.
    """
    candidate_segments = []
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

        if _is_grouping_candidate(
            distinct_count,
            null_rate=null_rate,
            is_id_column=id_pattern.search(col_name) is not None,
            total_count=total_count,
        ):
            candidate_segments.append((col_name, distinct_count, total_count / distinct_count))
        elif distinct_count > 50:
            _check_high_cardinality_warning(field, col_name, distinct_count, warnings)

    # Cheapest granularity first, so the row budget is spent on coarse dimensions before fine ones
    # and the grouping degrades gracefully on smaller tables.
    candidate_segments.sort(key=lambda x: x[1])  # Sort by distinct_count ascending
    recommended_segments = select_baseline_columns(candidate_segments, total_count)

    segment_count = _count_group_combinations(recommended_segments, distinct_counts)
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


def _auto_discover_heuristic(df: DataFrame, warnings: list[str]) -> AnomalyProfile:
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
    )

    # The grouping columns are the basis of comparison, not features, so drop them from the feature
    # list. A column cannot be both what is measured and what it is measured against.
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
