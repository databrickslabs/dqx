from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql.types import DataType


@dataclass(frozen=True)
class DQProfile:
    """Data quality profile class representing a data quality rule candidate."""

    name: str
    column: str
    description: str | None = None
    parameters: dict[str, Any] | None = None
    filter: str | None = None


@dataclass(frozen=True)
class DQProfileType:
    """Data quality profile type: a named builder that may produce a DQProfile for a column.

    Attributes:
        name: Profile type identifier (e.g. "null_or_empty", "is_in", "min_max"). Used to
            look up the builder in the registry and in generated rule metadata.
        builder: Callable that inspects column data and options and returns a DQProfile when
            the column matches the profile criteria, otherwise None. Signature:

            (df, column_name, column_type, profiler_metrics, profiler_options) -> DQProfile | None

            - df: DataFrame for this column (non-null rows only; strings trimmed
              when profiler_options["trim_strings"] is True). Used for distinct/min/max etc.
            - column_name: Name of the column being profiled.
            - column_type: Spark DataType of the column (e.g. StringType(), LongType()).
            - profiler_metrics: Column-level statistics from the profiler (e.g. total_count,
              null_count, empty_count, count_non_null). Same key set as summary_stats[column_name].
            - profiler_options: Profiler options for this run (e.g. max_null_ratio,
              max_empty_ratio, max_in_count, trim_strings, filter).
    """

    name: str
    builder: Callable[[DataFrame, str, DataType, dict[str, Any], dict[str, Any]], DQProfile | None]
