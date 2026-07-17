"""
Shared profiling utilities.
"""

import collections.abc

import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def compute_null_and_distinct_counts(
    df: DataFrame,
    column_names: collections.abc.Iterable[str],
    distinct_columns: collections.abc.Iterable[str],
    *,
    approx: bool = True,
    rsd: float = 0.05,
) -> tuple[dict[str, int], dict[str, int]]:
    """Compute null counts and (approx) distinct counts in a single aggregation."""
    column_names = list(column_names)
    distinct_columns = list(distinct_columns)

    null_exprs = [F.count(F.when(F.col(col_name).isNull(), 1)).alias(f"{col_name}__nulls") for col_name in column_names]
    if approx:
        distinct_exprs = [
            F.approx_count_distinct(col_name, rsd=rsd).alias(f"{col_name}__distinct") for col_name in distinct_columns
        ]
    else:
        distinct_exprs = [F.countDistinct(col_name).alias(f"{col_name}__distinct") for col_name in distinct_columns]

    stats_row = df.agg(*null_exprs, *distinct_exprs).first()
    assert stats_row is not None, "Failed to compute column statistics"

    null_counts = {col_name: stats_row[f"{col_name}__nulls"] for col_name in column_names}
    distinct_counts = {col_name: stats_row[f"{col_name}__distinct"] for col_name in distinct_columns}

    return null_counts, distinct_counts


def compute_exact_distinct_counts(
    df: DataFrame,
    columns: collections.abc.Iterable[str],
) -> dict[str, int]:
    """Compute exact distinct counts for provided columns."""
    columns = list(columns)
    if not columns:
        return {}
    distinct_exprs = [F.countDistinct(col_name).alias(col_name) for col_name in columns]
    stats_row = df.agg(*distinct_exprs).first()
    assert stats_row is not None, "Failed to compute distinct counts"
    return {col_name: stats_row[col_name] for col_name in columns}


def calculate_median_absolute_deviation_bounds(
    df: DataFrame, column: str, filter_condition: str | None = None
) -> tuple[float, float] | None:
    """
    Calculates the lower and upper bounds using the median absolute deviation of a numeric column.

    Bounds are defined as *median* ± 3.5 × MAD. Returns None if the filtered DataFrame
    is empty and the median cannot be computed.

    Args:
        df: PySpark DataFrame
        column: Name of the numeric column to calculate MAD for
        filter_condition: SQL filter expression to apply before calculation (optional)

    Returns:
        A (lower_bound, upper_bound) tuple, or None if bounds cannot be calculated.
    """
    median, mad = calculate_median_absolute_deviation(df, column, filter_condition)
    if median is not None and mad is not None:
        median = float(median)
        mad = float(mad)
        lower_bound = median - (3.5 * mad)
        upper_bound = median + (3.5 * mad)
        return lower_bound, upper_bound

    return None


def calculate_median_absolute_deviation(
    df: DataFrame, column: str, filter_condition: str | None
) -> tuple[float | None, float | None]:
    """
    Calculates the Median Absolute Deviation (MAD) for a numeric column.

    MAD is a robust measure of variability: MAD = median(|X_i - median(X)|).
    Computation applies *filter_condition* first, then computes the column median,
    then the median of the absolute deviations from that median.

    Args:
        df: PySpark DataFrame
        column: Name of the numeric column to calculate MAD for
        filter_condition: SQL filter expression to apply before calculation (optional)

    Returns:
        A (median, mad) tuple. Both values are None when the filtered DataFrame is empty.
    """
    if filter_condition is not None:
        df = df.filter(filter_condition)

    median_value = df.agg(F.percentile_approx(column, 0.5)).collect()[0][0]
    df_with_deviations = df.select(F.abs(F.col(column) - F.lit(median_value)).alias("absolute_deviation"))
    mad = df_with_deviations.agg(F.percentile_approx("absolute_deviation", 0.5)).collect()[0][0]

    return median_value, mad
