"""
Shared profiling utilities.
"""

from __future__ import annotations

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
