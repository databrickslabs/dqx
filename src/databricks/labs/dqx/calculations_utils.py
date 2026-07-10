"""
The following package contains common calculation used between different layers of the framework
"""

from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def calculate_median_absolute_deviation_bounds(
    df: DataFrame, column: str, filter_condition: str | None
) -> tuple[float, float] | None:
    """
    Calculates the lower and upper bound of the median absolute deviation of a numeric column.
    Bounds calculated as follows:
    - lower bound: median - 3.5 * MAD
    - upper bound: median + 3.5 * MAD
    Returns `None` if bounds can not be calculated because of `filter_condition`.

    Args:
        df: PySpark DataFrame
        column: Name of the numeric column to calculate MAD for
        filter_condition: Filter to apply before calculation (optional)

    Returns:
        The lower and upper bound of the median absolute deviation.
    """
    median, mad = calculate_median_absolute_deviation(df, column, filter_condition)
    if median is not None and mad is not None:
        median = float(median)
        mad = float(mad)
        lower_bound = median - (3.5 * mad)
        upper_bound = median + (3.5 * mad)
        return lower_bound, upper_bound

    return None


def calculate_median_absolute_deviation(df: DataFrame, column: str, filter_condition: str | None) -> tuple[Any, Any]:
    """
    Calculate the Median Absolute Deviation (MAD) for a numeric column.

    The MAD is a robust measure of variability based on the median, calculated as:
    MAD = median(|X_i - median(X)|)

    This is useful for outlier detection as it is more robust to outliers than
    standard deviation.

    Args:
        df: PySpark DataFrame
        column: Name of the numeric column to calculate MAD for
        filter_condition: Filter to apply before calculation (optional)

    Returns:
        The Median and Absolute Deviation values
    """
    if filter_condition is not None:
        df = df.filter(filter_condition)

    # Step 1: Calculate the median of the column
    median_value = df.agg(F.percentile_approx(column, 0.5)).collect()[0][0]

    # Step 2: Calculate absolute deviations from the median
    df_with_deviations = df.select(F.abs(F.col(column) - F.lit(median_value)).alias("absolute_deviation"))

    # Step 3: Calculate the median of absolute deviations
    mad = df_with_deviations.agg(F.percentile_approx("absolute_deviation", 0.5)).collect()[0][0]

    return median_value, mad
