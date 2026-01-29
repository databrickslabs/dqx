"""Segment utilities for anomaly detection."""

from pyspark.sql import Column
import pyspark.sql.functions as F


def build_segment_filter(segment_values: dict[str, str] | None) -> Column | None:
    """Build Spark filter expression for a segment's values.

    Args:
        segment_values: Dictionary mapping segment column names to values

    Returns:
        Spark Column expression combining all segment filters with AND
        None if segment_values is None or empty

    Example:
        >>> build_segment_filter(dict(region="US", product="A"))
        Column<'((region = US) AND (product = A))'>
        >>> build_segment_filter(None)
        None
    """
    if not segment_values:
        return None

    filter_exprs = [F.col(key) == F.lit(value) for key, value in segment_values.items()]

    segment_filter = filter_exprs[0]
    for expr in filter_exprs[1:]:
        segment_filter = segment_filter & expr

    return segment_filter
