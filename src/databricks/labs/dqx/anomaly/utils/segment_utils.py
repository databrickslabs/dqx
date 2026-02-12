"""Segment utilities for anomaly detection."""

from collections.abc import Mapping
from typing import Any

from pyspark.sql import Column
import pyspark.sql.functions as F


def canonicalize_segment_values(segment_values: Mapping[str, Any] | None) -> dict[str, str]:
    """Canonicalize segment values for deterministic naming and filtering."""
    if not segment_values:
        return {}
    return {str(key): str(value) for key, value in sorted(segment_values.items(), key=lambda item: str(item[0]))}


def build_segment_name(segment_values: Mapping[str, Any] | None) -> str:
    """Build deterministic segment name from segment values."""
    canonical_values = canonicalize_segment_values(segment_values)
    return "_".join(f"{key}={value}" for key, value in canonical_values.items())


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

    filter_exprs = [F.col(key) == F.lit(value) for key, value in canonicalize_segment_values(segment_values).items()]

    segment_filter = filter_exprs[0]
    for expr in filter_exprs[1:]:
        segment_filter = segment_filter & expr

    return segment_filter
