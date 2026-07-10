"""Common calculations shared between different layers of the framework."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def calculate_median_absolute_deviation_bounds(
    df: DataFrame, column: str, filter_condition: str | None
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
