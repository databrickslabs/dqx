"""
Auto-discovery logic for row anomaly detection.

Analyzes DataFrames to recommend columns and segments suitable for
anomaly detection using on-the-fly heuristics.
"""

import logging
import math
import re
from collections.abc import Callable
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
from databricks.labs.dqx.utils import quote_column_name, sanitize_for_logging

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


def auto_discover_columns(
    df: DataFrame,
    *,
    baseline_by: list[str] | None = None,
    baseline_over_time: str | None = None,
    sample_fraction: float | None = None,
    train_ratio: float | None = None,
    max_rows: int | None = None,
) -> AnomalyProfile:
    """
    Auto-discover feature columns and a baseline grouping for row anomaly detection.

    Analyzes the DataFrame using on-the-fly heuristics to recommend suitable columns and a grouping
    to condition on.

    Column selection criteria:
    - Numeric types (int, long, float, double, decimal)
    - stddev > 0 (has variance)
    - null_rate < 50%
    - Exclude: timestamps, IDs (detected by name patterns)

    Baseline grouping criteria (see *select_baseline_columns*):
    - Categorical types (string, int) with 2-50 distinct values
    - null_rate < 10%
    - Projected support of at least MIN_ROWS_PER_BASELINE_GROUP rows per resulting group

    Args:
        df: DataFrame to analyze.
        baseline_by: Declared grouping, or None to discover a grouping. An empty list keeps the
            pooled comparison and leaves otherwise eligible categorical columns as features.
        baseline_over_time: Declared time axis, retained in the input but excluded from features.
        sample_fraction: Training sample fraction; None uses the training default.
        train_ratio: Fraction of the sample used to fit; None uses the training default.
        max_rows: Optional cap on the sample before the training split.

    Returns:
        AnomalyProfile with recommendations and warnings.
    """
    warnings: list[str] = []
    return _auto_discover_heuristic(
        df, warnings, baseline_by, baseline_over_time, sample_fraction, train_ratio, max_rows
    )


def _compute_numeric_stats_batched(df: DataFrame, column_names: list[str]) -> dict[str, dict[str, float]]:
    """Compute mean and stddev for all numeric columns in a single aggregation."""
    if not column_names:
        return {}
    agg_exprs = []
    for col_name in column_names:
        agg_exprs.append(F.mean(F.col(quote_column_name(col_name))).alias(f"{col_name}__mean"))
        agg_exprs.append(F.stddev(F.col(quote_column_name(col_name))).alias(f"{col_name}__std"))
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
            F.mean(F.col(quote_column_name(col_name))).alias("mean"),
            F.stddev(F.col(quote_column_name(col_name))).alias("std"),
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


def effective_min_rows_per_group(
    sample_fraction: float | None = None,
    train_ratio: float | None = None,
    *,
    total_count: int | None = None,
    max_rows: int | None = None,
) -> int:
    """Full-table support projected to retain MIN_ROWS_PER_BASELINE_GROUP training rows.

    Sampling, splitting, and a cap can retain fewer rows in an individual group than this projection.
    Fitted medians and calibration therefore check their actual usable support independently.
    """
    fraction = sample_fraction if sample_fraction is not None else DEFAULT_SAMPLE_FRACTION
    if max_rows is not None and total_count:
        fraction = min(fraction, max_rows / total_count)
    retained = fraction * (train_ratio if train_ratio is not None else DEFAULT_TRAIN_RATIO)
    if retained <= 0.0 or retained >= 1.0:
        return MIN_ROWS_PER_BASELINE_GROUP
    return int(math.ceil(MIN_ROWS_PER_BASELINE_GROUP / retained))


def select_baseline_columns(
    candidates: list[tuple[str, int, float]],
    total_count: int,
    sample_fraction: float | None = None,
    train_ratio: float | None = None,
    *,
    max_rows: int | None = None,
    group_support: Callable[[list[str]], tuple[int, int]] | None = None,
) -> list[str]:
    """Choose a grouping for baseline conditioning, given candidates ordered by cardinality.

    Uses actual joint group counts and minimum support when *group_support* is supplied; otherwise it
    estimates a balanced Cartesian grouping from the candidate cardinalities. Frame-based discovery
    always supplies measured support. Sampling-adjusted support is an estimate, not a guarantee.
    The total group count stays within what is sensible to persist and broadcast. That is the whole
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
    minimum = effective_min_rows_per_group(sample_fraction, train_ratio, total_count=total_count, max_rows=max_rows)
    for name, distinct_count, _rows_per_group in candidates:
        if distinct_count > MAX_BASELINE_COLUMN_CARDINALITY:
            continue
        prospective, smallest_group = (
            group_support([*selected, name])
            if group_support is not None
            else (groups * int(distinct_count), total_count // (groups * int(distinct_count)))
        )
        if prospective > MAX_BASELINE_GROUPS:
            continue
        if smallest_group < minimum:
            continue
        selected.append(name)
        groups = prospective

    if selected:
        # Neutral wording on purpose: this policy is shared by the path that applies a discovered
        # grouping and the advisory path that only reports one, so it must not claim application.
        logger.info(
            f"Baseline grouping {[sanitize_for_logging(name) for name in selected]}: {groups} groups, "
            f"~{int(total_count / groups)} rows/group (one model regardless of group count)"
        )
        skipped = [c[0] for c in candidates if c[0] not in selected]
        if skipped:
            logger.debug(
                f"Not added to the baseline grouping (would leave under "
                f"{minimum} rows/group before sampling, or exceed {MAX_BASELINE_GROUPS} "
                f"groups): {[sanitize_for_logging(name) for name in skipped]}"
            )
    return selected


def _measure_group_support(df: DataFrame, columns: list[str]) -> tuple[int, int]:
    # Project to safe internal aliases so a source column named 'count' cannot collide with the
    # aggregation output. Only two scalars reach the driver, even for a rejected wide grouping.
    aliases = [f"__dqx_group_{index}" for index in range(len(columns))]
    groups = (
        df.select(*[F.col(quote_column_name(name)).alias(alias) for name, alias in zip(columns, aliases, strict=True)])
        .groupBy(*aliases)
        .count()
    )
    summary = groups.agg(F.count(F.lit(1)).alias("groups"), F.min("count").alias("smallest")).first()
    if summary is None:
        return 0, 0
    return int(summary["groups"]), int(summary["smallest"] or 0)


@dataclass(frozen=True)
class BaselineSuggestion:
    """A grouping the data would support, for advising a caller who did not ask for one."""

    columns: list[str]
    group_count: int
    rows_per_group: int


def suggest_baseline_columns(
    df: DataFrame,
    exclude: list[str],
    *,
    sample_fraction: float | None = None,
    train_ratio: float | None = None,
    max_rows: int | None = None,
) -> BaselineSuggestion | None:
    """Find a grouping the data would support, without selecting feature columns.

    Deliberately narrower than *auto_discover_columns*: the grouping decision needs null rates
    and distinct counts on the categorical columns only, so this skips the numeric mean/stddev
    aggregation a full profile computes and would then throw away.

    Used to *advise*, never to apply. A caller who named *columns* has decided what to measure, and
    DQX does not add engineered features they did not ask for.

    Args:
        df: DataFrame to scan.
        exclude: Columns the caller already named as features. A column cannot be both what is
            measured and what it is measured against.
        sample_fraction: Training sample fraction; None uses the training default.
        train_ratio: Fraction of the sample used to fit; None uses the training default.
        max_rows: Optional cap on the sample before the training split.

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
            sample_fraction=sample_fraction,
            train_ratio=train_ratio,
        ):
            candidates.append((name, distinct_count, total_count / distinct_count))

    candidates.sort(key=lambda candidate: candidate[1])
    selected = select_baseline_columns(
        candidates,
        total_count,
        sample_fraction,
        train_ratio,
        max_rows=max_rows,
        group_support=lambda names: _measure_group_support(df, names),
    )
    if not selected:
        return None

    group_count, _ = _measure_group_support(df, selected)
    return BaselineSuggestion(
        columns=selected,
        group_count=group_count,
        rows_per_group=int(total_count / group_count) if group_count else total_count,
    )


def _select_segment_columns(
    df: DataFrame,
    recommended_columns: list[str],
    column_types: dict[str, str],
    id_pattern: re.Pattern[str],
    warnings: list[str],
    *,
    total_count: int,
    null_counts: dict[str, int],
    distinct_counts: dict[str, int],
    sample_fraction: float | None,
    train_ratio: float | None,
    max_rows: int | None,
) -> tuple[list[str], int]:
    """Identify baseline grouping columns and count the groups they produce.

    See *select_baseline_columns* for the policy.
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
            distinct_row = df.select(F.countDistinct(F.col(quote_column_name(col_name)))).first()
            assert distinct_row is not None  # to satisfy linter
            distinct_count = distinct_row[0]
        null_rate = null_counts.get(col_name, 0) / total_count if total_count > 0 else 1.0

        if _is_grouping_candidate(
            distinct_count,
            null_rate=null_rate,
            is_id_column=id_pattern.search(col_name) is not None,
            total_count=total_count,
            sample_fraction=sample_fraction,
            train_ratio=train_ratio,
        ):
            candidate_segments.append((col_name, distinct_count, total_count / distinct_count))
        elif distinct_count > 50:
            _check_high_cardinality_warning(field, col_name, distinct_count, warnings)

    # Cheapest granularity first, so the row budget is spent on coarse dimensions before fine ones
    # and the grouping degrades gracefully on smaller tables.
    candidate_segments.sort(key=lambda x: x[1])  # Sort by distinct_count ascending
    recommended_segments = select_baseline_columns(
        candidate_segments,
        total_count,
        sample_fraction,
        train_ratio,
        max_rows=max_rows,
        group_support=lambda names: _measure_group_support(df, names),
    )

    segment_count = _measure_group_support(df, recommended_segments)[0] if recommended_segments else 1
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


def _auto_discover_heuristic(
    df: DataFrame,
    warnings: list[str],
    baseline_by: list[str] | None,
    baseline_over_time: str | None,
    sample_fraction: float | None,
    train_ratio: float | None,
    max_rows: int | None,
) -> AnomalyProfile:
    """
    Auto-discover using on-the-fly heuristics with multi-type support.

    Args:
        df: DataFrame to analyze.
        warnings: List to accumulate warnings.
        baseline_by: Declared grouping, or None to discover one. An empty list disables grouping.
        baseline_over_time: Declared time axis to exclude from feature candidates.
        sample_fraction: Training sample fraction; None uses the training default.
        train_ratio: Fraction of the sample used to fit; None uses the training default.
        max_rows: Optional cap on the sample before the training split.

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

        # Skip ID columns
        if id_pattern.search(col_name) or col_name == baseline_over_time:
            continue

        null_count = null_counts.get(col_name, 0)
        null_rate = null_count / total_count if total_count > 0 else 1.0

        # Analyze by type and collect candidates
        _analyze_column_by_type(
            field.dataType,
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

    if baseline_by is None:
        recommended_segments, segment_count = _select_segment_columns(
            df,
            [candidate[1] for candidate in candidates],
            {candidate[1]: candidate[2] for candidate in candidates},
            id_pattern,
            warnings,
            total_count=total_count,
            null_counts=null_counts,
            distinct_counts=distinct_counts,
            sample_fraction=sample_fraction,
            train_ratio=train_ratio,
            max_rows=max_rows,
        )
    else:
        recommended_segments = list(baseline_by)
        segment_count = (
            df.select(*[F.col(quote_column_name(name)) for name in recommended_segments]).distinct().count()
            if recommended_segments
            else 1
        )

    # Resolve the comparison basis before the feature limit so a basis column cannot consume a
    # feature slot. Explicit grouping also prevents unrelated suggested groups from disappearing.
    feature_candidates = [candidate for candidate in candidates if candidate[1] not in recommended_segments]
    recommended_columns, column_types = _select_top_columns(feature_candidates, 10, warnings)
    # Grouping changes a column's role, not its analyzed type. Keep its type without expanding the
    # metadata to unrelated feature candidates omitted by the feature limit.
    column_types.update(
        {candidate[1]: candidate[2] for candidate in candidates if candidate[1] in recommended_segments}
    )

    return AnomalyProfile(
        recommended_columns=recommended_columns,
        recommended_segments=recommended_segments,
        segment_count=segment_count,
        column_stats=column_stats,
        warnings=warnings,
        column_types=column_types,
        unsupported_columns=unsupported_columns,
    )
