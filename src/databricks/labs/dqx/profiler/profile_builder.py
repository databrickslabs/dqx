import datetime
import decimal
import logging
from collections.abc import Callable
import math
from typing import Any

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame
from pyspark.sql import types as T, functions as F

from databricks.labs.dqx.check_funcs import get_limit_expr
from databricks.labs.dqx.errors import InvalidParameterError
from databricks.labs.dqx.geo.check_funcs import DEFAULT_SRID
from databricks.labs.dqx.profiler.common import TEXT_TYPES, is_geospatial, is_text
from databricks.labs.dqx.profiler.profile import DQProfile, DQProfileBuilder
from databricks.labs.dqx.profiling_utils import calculate_median_absolute_deviation_bounds
from databricks.labs.dqx.profiler.profile_options import (
    PROFILE_OPTION_DISTINCT_RATIO,
    PROFILE_OPTION_FILTER,
    PROFILE_OPTION_GEOSPATIAL_SRID,
    PROFILE_OPTION_MAX_EMPTY_RATIO,
    PROFILE_OPTION_MAX_IN_COUNT,
    PROFILE_OPTION_MAX_NULL_RATIO,
    PROFILE_OPTION_NUM_SIGMAS,
    PROFILE_OPTION_OUTLIER_COLUMNS,
    PROFILE_OPTION_OUTLIERS_RATIO,
    PROFILE_OPTION_PROFILE_GEOSPATIAL,
    PROFILE_OPTION_REMOVE_OUTLIERS,
    PROFILE_OPTION_ROUND,
    PROFILE_OPTION_TRIM_STRINGS,
    PROFILE_OPTION_HAS_NO_OUTLIERS,
    PROFILE_OPTION_HAS_NO_OUTLIERS_ALLOW_COLUMNS,
    PROFILE_OPTION_HAS_NO_OUTLIERS_DENY_COLUMNS,
    DEFAULT_PROFILE_OPTIONS,
)

# Matched pair for serializing timestamp min/max through the Spark fallback: Spark renders with six
# fractional-second digits and Python parses them back. Kept together as constants so the two patterns
# can never drift apart (a mismatch would raise ValueError at parse time).
_TIMESTAMP_SPARK_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSS"
_TIMESTAMP_STRPTIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
_GEO_STAT_MIN_X = "min_x_coordinate"
_GEO_STAT_MAX_X = "max_x_coordinate"
_GEO_STAT_MIN_Y = "min_y_coordinate"
_GEO_STAT_MAX_Y = "max_y_coordinate"
_GEO_STAT_MIN_AREA = "min_area"
_GEO_STAT_MAX_AREA = "max_area"
_GEO_STAT_MIN_NUM_POINTS = "min_num_points"
_GEO_STAT_MAX_NUM_POINTS = "max_num_points"
_GEO_STAT_TYPES = "geometry_types"
_GEO_STAT_EMPTY_COUNT = "empty_geometry_count"
_GEO_STAT_INVALID_COUNT = "invalid_geometry_count"
_GEO_STAT_NULL_ISLAND_COUNT = "null_island_count"

GEOSPATIAL_PROFILE_NAMES: frozenset[str] = frozenset(
    {
        "geometry_type",
        "has_x_coordinate_between",
        "has_y_coordinate_between",
        "is_area_not_less_than",
        "is_area_not_greater_than",
        "is_num_points_not_less_than",
        "is_num_points_not_greater_than",
        "is_non_empty_geometry",
        "is_ogc_valid",
        "is_not_null_island",
    }
)


PROFILE_BUILDER_REGISTRY: dict[str, DQProfileBuilder] = {}
logger = logging.getLogger(__name__)


def register_profile_builder(profile_type: str) -> Callable:
    def wrapper(builder_func: Callable) -> Callable:
        PROFILE_BUILDER_REGISTRY[profile_type] = DQProfileBuilder(name=profile_type, builder=builder_func)
        return builder_func

    return wrapper


def deregister_profile_builder(profile_type: str) -> None:
    """
    Removes a previously registered profile builder from *PROFILE_BUILDER_REGISTRY*.
    No-op if no builder is registered under the given key.

    Args:
        profile_type: Key under which the builder was registered.
    """
    PROFILE_BUILDER_REGISTRY.pop(profile_type, None)


@register_profile_builder("null_or_empty")
def make_null_or_empty_profile(
    _: DataFrame,
    column_name: str,
    column_type: T.DataType,
    profiler_metrics: dict[str, Any],
    profiler_options: dict[str, Any],
) -> DQProfile | None:
    """
    Creates an 'is_not_null_or_empty', 'is_not_null', or 'is_not_empty' profile by checking the input column type,
    profiled metrics, and profiler options.

    Args:
        column_name: Input column name
        column_type: Input column type
        profiler_metrics: Column-level statistics computed by the DQProfiler
        profiler_options: Configuration options for the DQProfiler

    Returns:
        A DQProfile if the correct conditions are met, otherwise None
    """
    if is_text(column_type):
        return _make_null_or_empty_profile(column_name, profiler_metrics, profiler_options)

    return _make_null_profile(column_name, profiler_metrics, profiler_options)


@register_profile_builder("is_in")
def make_is_in_profile(
    df: DataFrame,
    column_name: str,
    column_type: T.DataType,
    profiler_metrics: dict[str, Any],
    profiler_options: dict[str, Any],
) -> DQProfile | None:
    """
    Creates an 'is_in' profile by checking the input column type, profiled metrics, and profiler options.

    Args:
        df: Single-column DataFrame
        column_name: Input column name
        column_type: Input column type
        profiler_metrics: Column-level statistics computed by the DQProfiler
        profiler_options: Configuration options for the DQProfiler

    Returns:
        A DQProfile if the correct conditions are met, otherwise None
    """
    if not _supports_distinct(column_type):
        return None

    total_count = profiler_metrics.get("count", 0)
    if total_count == 0:
        return None

    max_in_count = profiler_options.get(PROFILE_OPTION_MAX_IN_COUNT, 0)
    max_distinct_ratio = profiler_options.get(PROFILE_OPTION_DISTINCT_RATIO, 0.0)

    col = df.columns[0]
    distinct_values = [row[0] for row in df.select(col).distinct().collect()]
    distinct_count = len(distinct_values)
    if distinct_count == 0:
        # The df passed here has nulls already dropped by the caller. If distinct_count is 0,
        # the column is entirely null — no valid values to build an allowlist from.
        return None

    distinct_ratio = (1.0 * distinct_count) / total_count

    if distinct_count < max_in_count and distinct_ratio < max_distinct_ratio:
        return DQProfile(
            name="is_in",
            column=column_name,
            parameters={"in": distinct_values},
            filter=profiler_options.get(PROFILE_OPTION_FILTER, None),
        )

    return None


@register_profile_builder("min_max")
def make_min_max_profile(
    df: DataFrame,
    column_name: str,
    column_type: T.DataType,
    profiler_metrics: dict[str, Any],
    profiler_options: dict[str, Any],
) -> DQProfile | None:
    """
    Creates a 'min_max' profile by checking the input column type, profiled metrics, and profiler options.

    Args:
        df: Single-column DataFrame
        column_name: Input column name (used for DQProfile output)
        column_type: Input column type
        profiler_metrics: Column-level statistics computed by the DQProfiler (includes summary stats)
        profiler_options: Configuration options for the DQProfiler

    Returns:
        A DQProfile if the correct conditions are met, otherwise None
    """
    if profiler_metrics.get("count_non_null", 0) == 0:
        return None

    if not _supports_min_max(column_type):
        return None

    if _remove_outliers(column_name, profiler_options):
        return _make_min_max_profile_with_outlier_removal(
            df, column_name, column_type, profiler_metrics, profiler_options
        )

    return _make_min_max_profile_without_outlier_removal(
        df, column_name, column_type, profiler_metrics, profiler_options
    )


def _make_null_or_empty_profile(
    column_name: str, profiler_metrics: dict[str, Any], profiler_options: dict[str, Any]
) -> DQProfile | None:
    """
    Creates an 'is_not_null_or_empty', 'is_not_null', or 'is_not_empty' profile for text type columns.

    Args:
        column_name: Input column name
        profiler_metrics: Column-level statistics computed by the DQProfiler
        profiler_options: Configuration options for the DQProfiler

    Returns:
        A DQProfile if the correct conditions are met, otherwise None
    """
    total_count = profiler_metrics.get("count", 0)
    if total_count == 0:
        return None

    null_count = profiler_metrics.get("count_null", 0)
    null_ratio = null_count / total_count
    empty_count = profiler_metrics.get("empty_count", 0)
    empty_ratio = empty_count / total_count
    max_null_ratio = profiler_options.get(PROFILE_OPTION_MAX_NULL_RATIO, 0.0)
    max_empty_ratio = profiler_options.get(PROFILE_OPTION_MAX_EMPTY_RATIO, 0.0)
    trim_strings = profiler_options.get(PROFILE_OPTION_TRIM_STRINGS, True)

    not_null = null_ratio <= max_null_ratio
    not_empty = empty_ratio <= max_empty_ratio

    if not_null and not_empty:
        description = (
            f"Column {column_name} has {null_ratio * 100:.1f}% of null values and has {empty_ratio * 100:.1f}% "
            f"of empty values (allowed {max_null_ratio * 100:.1f}% of nulls and {max_empty_ratio * 100:.1f}% of empty values)"
            if null_count > 0 or empty_count > 0
            else None
        )
        return DQProfile(
            name="is_not_null_or_empty",
            column=column_name,
            description=description,
            parameters={"trim_strings": trim_strings},
            filter=profiler_options.get(PROFILE_OPTION_FILTER, None),
        )

    if not_null:
        # Empty ratio exceeds max_empty_ratio, so is_not_null_or_empty is too strict; use is_not_null instead.
        description = (
            f"Column {column_name} has {null_ratio * 100:.1f}% of null values (allowed {max_null_ratio * 100:.1f}%); "
            f"empty value check skipped: {empty_ratio * 100:.1f}% empty (exceeds {max_empty_ratio * 100:.1f}% threshold)"
        )
        return DQProfile(
            name="is_not_null",
            column=column_name,
            description=description,
            filter=profiler_options.get(PROFILE_OPTION_FILTER, None),
        )

    if not_empty:
        return DQProfile(
            name="is_not_empty",
            column=column_name,
            description=(
                f"Column {column_name} has {empty_ratio * 100:.1f}% of empty values (allowed {max_empty_ratio * 100:.1f}%)"
                if empty_count > 0
                else None
            ),
            parameters={"trim_strings": trim_strings},
            filter=profiler_options.get(PROFILE_OPTION_FILTER, None),
        )

    return None


def _make_null_profile(
    column_name: str, profiler_metrics: dict[str, Any], profiler_options: dict[str, Any]
) -> DQProfile | None:
    """
    Builds an 'is_not_null' profile for non-text columns.

    Args:
        column_name: Input column name
        profiler_metrics: Column-level statistics computed by the DQProfiler
        profiler_options: Configuration options for the DQProfiler

    Returns:
        A DQProfile if the correct conditions are met, otherwise None
    """
    null_count = profiler_metrics.get("count_null", 0)
    total_count = profiler_metrics.get("count", 0)
    if total_count == 0:
        return None
    null_ratio = null_count / total_count
    max_null_ratio = profiler_options.get(PROFILE_OPTION_MAX_NULL_RATIO, 0.0)

    if null_ratio <= max_null_ratio:
        return DQProfile(
            name="is_not_null",
            column=column_name,
            description=(
                f"Column {column_name} has {null_ratio * 100:.1f}% of null values (allowed {max_null_ratio * 100:.1f}%)"
                if null_count > 0
                else None
            ),
            filter=profiler_options.get(PROFILE_OPTION_FILTER, None),
        )

    return None


def _supports_distinct(column_type: T.DataType) -> bool:
    """
    Validates that the input column type supports distinct operations.

    Args:
        column_type: Input column type

    Returns:
        True if the column supports distinct operations, otherwise False
    """
    return isinstance(column_type, (T.IntegerType, T.LongType) + TEXT_TYPES)


def _supports_min_max(column_type: T.DataType) -> bool:
    """
    Validates that the input column type supports min and max operations.

    Args:
        column_type: Input column type

    Returns:
        True if the column supports min and max operations, otherwise False
    """
    return isinstance(column_type, (T.DateType, T.NumericType, T.TimestampNTZType, T.TimestampType)) and not isinstance(
        column_type, T.ByteType
    )


def _remove_outliers(column_name: str, profiler_options: dict[str, Any]) -> bool:
    """
    Checks if outliers should be removed when generating 'min_max' profiles.

    Args:
        column_name: Input column name
        profiler_options: Configuration options for the DQProfiler

    Returns:
        True if outlier removal should be applied to this column, otherwise False.
    """
    remove_outliers = profiler_options.get(PROFILE_OPTION_REMOVE_OUTLIERS, True)
    if not remove_outliers:
        return False

    outlier_columns = profiler_options.get(PROFILE_OPTION_OUTLIER_COLUMNS, [])
    if not outlier_columns:
        return True  # empty list means apply to all columns
    return column_name in outlier_columns


def _is_has_no_outliers_enabled(column_name: str, profiler_options: dict[str, Any]) -> bool:
    """
    Checks if *has_no_outliers* profiling is enabled for given column.

    Args:
        column_name: Input column name
        profiler_options: Configuration options for the DQProfiler

    Returns:
        True if *has_no_outliers* profile is enabled to this column, otherwise False.
    """
    return _is_profile_enabled(
        column_name,
        PROFILE_OPTION_HAS_NO_OUTLIERS,
        PROFILE_OPTION_HAS_NO_OUTLIERS_ALLOW_COLUMNS,
        PROFILE_OPTION_HAS_NO_OUTLIERS_DENY_COLUMNS,
        profiler_options,
    )


def _is_profile_enabled(
    column_name: str,
    profile_enabled_option_name: str,
    profile_allow_columns_option_name: str,
    profile_deny_columns_option_name: str,
    profiler_options: dict[str, Any],
) -> bool:
    """
    Checks if a profiler builder is enabled for the given column.

    The builder can be switched on or off for all columns via *profile_enabled_option_name*.
    When enabled globally, *profile_allow_columns_option_name* restricts it to a specific set of
    columns, while *profile_deny_columns_option_name* excludes specific columns and applies the
    builder to all others. The two lists are mutually exclusive.

    Args:
        column_name: Input column name.
        profile_enabled_option_name: Option key whose value is a bool controlling global enablement.
        profile_allow_columns_option_name: Option key whose value is a list of columns to include.
        profile_deny_columns_option_name: Option key whose value is a list of columns to exclude.
        profiler_options: Configuration options for the DQProfiler.

    Returns:
        True if the profiler builder should run for this column, otherwise False.

    Raises:
        InvalidParameterError: if both *profile_allow_columns_option_name* and
            *profile_deny_columns_option_name* are provided at the same time.
    """
    profiler_enabled = profiler_options.get(profile_enabled_option_name, True)
    if not profiler_enabled:
        return False

    allowed_columns = profiler_options.get(profile_allow_columns_option_name, [])
    denied_columns = profiler_options.get(profile_deny_columns_option_name, [])

    if not denied_columns and not allowed_columns:
        return True

    if allowed_columns and denied_columns:
        raise InvalidParameterError(
            f"Values for both '{profile_allow_columns_option_name}' and '{profile_deny_columns_option_name}' are provided in the configuration. Please provide only one of them."
        )

    if allowed_columns and column_name not in allowed_columns:
        return False

    return column_name not in denied_columns


def validate_profile_options(profiler_options: dict[str, Any]) -> None:
    """
    Validates profiler options once, up front, before any profiling work is done.

    Currently checks the allow/deny column-list pairs that are mutually exclusive, so a
    misconfiguration fails fast at option-build time rather than partway through a profiling run
    (which would happen if the check only ran per column inside *_is_profile_enabled*).

    Args:
        profiler_options: Configuration options for the DQProfiler (merged with defaults).

    Raises:
        InvalidParameterError: if both the allow-columns and deny-columns option of a builder are set.
    """
    mutually_exclusive_pairs = [
        (PROFILE_OPTION_HAS_NO_OUTLIERS_ALLOW_COLUMNS, PROFILE_OPTION_HAS_NO_OUTLIERS_DENY_COLUMNS),
    ]
    for allow_option, deny_option in mutually_exclusive_pairs:
        if profiler_options.get(allow_option) and profiler_options.get(deny_option):
            raise InvalidParameterError(
                f"Values for both '{allow_option}' and '{deny_option}' are provided in the configuration. "
                "Please provide only one of them."
            )


def _make_min_max_profile_with_outlier_removal(
    df: DataFrame,
    column_name: str,
    column_type: T.DataType,
    profiler_metrics: dict[str, Any],
    profiler_options: dict[str, Any],
) -> DQProfile | None:
    """
    Creates a 'min_max' profile using outlier-capped values profiled from the input data.

    For numeric types, uses pre-computed metrics from the profiler summary statistics.
    For date/timestamp types, requires a Spark action to cast values to bigint epoch seconds.

    Args:
        df: Single-column DataFrame
        column_name: Input column name (used for DQProfile output)
        column_type: Input column type
        profiler_metrics: Column-level statistics (includes summary stats min/max/mean/stddev for numeric types)
        profiler_options: Configuration options for the DQProfiler

    Returns:
        A 'min_max' DQProfile, or None if min/max limits cannot be determined.
    """
    column_alias = df.columns[0]
    if isinstance(column_type, T.DateType):
        # Convert DateType to timestamp, then to bigint epoch seconds.
        cast_df = df.select(F.col(column_alias).cast("timestamp").cast("bigint").alias(column_alias))
        aggregates = _get_aggregates(cast_df, column_alias)
    elif isinstance(column_type, (T.TimestampType, T.TimestampNTZType)):
        # Cast to timestamp first for TimestampNTZType compatibility (e.g. Spark Connect), then to a
        # double epoch (fractional seconds) rather than bigint so sub-second precision survives the
        # aggregation and is reconstructed below via datetime.fromtimestamp(float(...)).
        cast_df = df.select(F.col(column_alias).cast("timestamp").cast("double").alias(column_alias))
        aggregates = _get_aggregates(cast_df, column_alias)
    else:
        aggregates = {
            "min_value": profiler_metrics.get("min"),
            "max_value": profiler_metrics.get("max"),
            "mean_value": profiler_metrics.get("mean"),
            "stddev_value": profiler_metrics.get("stddev"),
        }

    min_limit, max_limit, description = _get_min_max_limits(column_type, profiler_options, aggregates)
    if min_limit is None or max_limit is None:
        logger.info(f"Can't get min/max for field {column_name}")
        return None

    return DQProfile(
        name="min_max",
        column=column_name,
        description=description,
        parameters={"min": min_limit, "max": max_limit},
        filter=profiler_options.get(PROFILE_OPTION_FILTER, None),
    )


def _get_aggregates(df: DataFrame, column_name: str) -> dict[str, Any]:
    """
    Gets the aggregates for a column.

    Args:
        df: Single-column DataFrame with values cast to a numeric type (e.g. BIGINT)
        column_name: Input column name

    Returns:
        A dictionary containing the aggregates for the column.
    """
    agg_df = df.agg(
        F.min(column_name).alias("min_value"),
        F.max(column_name).alias("max_value"),
        F.mean(column_name).alias("mean_value"),
        F.stddev(column_name).alias("stddev_value"),
    )
    return agg_df.collect()[0].asDict()


def _make_min_max_profile_without_outlier_removal(
    df: DataFrame,
    column_name: str,
    column_type: T.DataType,
    profiler_metrics: dict[str, Any],
    profiler_options: dict[str, Any],
) -> DQProfile | None:
    """
    Creates a 'min_max' profile using real values profiled from the input data.

    Args:
        df: Single-column DataFrame (nulls already dropped)
        column_name: Input column name (used for DQProfile output)
        column_type: Input column type
        profiler_metrics: Column-level statistics (includes summary stats min/max for numeric types)
        profiler_options: Configuration options for the DQProfiler

    Returns:
        A 'min_max' DQProfile
    """
    min_value = profiler_metrics.get("min")
    max_value = profiler_metrics.get("max")

    if min_value is None or max_value is None:
        col = df.columns[0]
        agg_df = df.agg(F.min(col).alias("min_value"), F.max(col).alias("max_value"))
        if isinstance(column_type, (T.TimestampType, T.TimestampNTZType)):
            # Render with six fractional-second digits (always emitted, including .000000) so full
            # microsecond precision is preserved through the string round-trip.
            agg_df = agg_df.select(
                F.date_format("min_value", _TIMESTAMP_SPARK_FORMAT).alias("min_value"),
                F.date_format("max_value", _TIMESTAMP_SPARK_FORMAT).alias("max_value"),
            )
        aggregates = agg_df.collect()[0].asDict()
        if not aggregates or aggregates.get("min_value") is None:
            logger.info(f"Can't get min/max for field {column_name}")
            return None
        if isinstance(column_type, (T.TimestampType, T.TimestampNTZType)):
            min_value = datetime.datetime.strptime(aggregates["min_value"], _TIMESTAMP_STRPTIME_FORMAT).replace(
                tzinfo=datetime.timezone.utc
            )
            max_value = datetime.datetime.strptime(aggregates["max_value"], _TIMESTAMP_STRPTIME_FORMAT).replace(
                tzinfo=datetime.timezone.utc
            )
        else:
            min_value = aggregates["min_value"]
            max_value = aggregates["max_value"]

    # Apply rounding uniformly, regardless of whether values came from summary-stats metrics or
    # the Spark fallback above. This ensures round=True is honoured for all numeric types
    # (float, decimal, int) and for timestamps. _round_value is a no-op when round=False.
    min_value = _round_value(min_value, "down", profiler_options)
    max_value = _round_value(max_value, "up", profiler_options)

    return DQProfile(
        name="min_max",
        column=column_name,
        parameters={"min": min_value, "max": max_value},
        description="Real min/max values were used",
        filter=profiler_options.get(PROFILE_OPTION_FILTER, None),
    )


def _get_min_max_limits(
    column_type: T.DataType, profiler_options: dict[str, Any], aggregates: dict[str, Any]
) -> tuple[Any, Any, str]:
    """
    Calculates the minimum and maximum limits for a column based on the provided aggregates and options.

    Args:
        column_type: The data type of the column.
        profiler_options: Configuration options for the DQProfiler
        aggregates: A dictionary containing the min, max, mean, and stddev values for the column.

    Returns:
        A tuple containing the minimum limit, maximum limit, and description.
    """

    min_value = aggregates.get("min_value")
    max_value = aggregates.get("max_value")
    mean_value = aggregates.get("mean_value")
    stddev_value = aggregates.get("stddev_value")
    num_sigmas = profiler_options.get(PROFILE_OPTION_NUM_SIGMAS, 3)

    if mean_value is None or stddev_value is None:
        adjusted_min_value, adjusted_max_value = _adjust_min_max_limits(
            column_type, min_value, max_value, profiler_options
        )
        return adjusted_min_value, adjusted_max_value, "Real min/max values were used"

    min_limit = mean_value - num_sigmas * stddev_value
    max_limit = mean_value + num_sigmas * stddev_value
    if min_limit < min_value and max_limit > max_value:
        adjusted_min_value, adjusted_max_value = _adjust_min_max_limits(
            column_type, min_value, max_value, profiler_options
        )
        return adjusted_min_value, adjusted_max_value, "Real min/max values were used"
    if min_limit > min_value and max_limit < max_value:
        adjusted_min_value, adjusted_max_value = _adjust_min_max_limits(
            column_type, min_limit, max_limit, profiler_options
        )
        return (
            adjusted_min_value,
            adjusted_max_value,
            f"Range doesn't include outliers, capped by {num_sigmas} sigmas. avg={mean_value}, stddev={stddev_value}, min={min_value}, max={max_value}",
        )
    if min_limit < min_value:
        adjusted_min_value, adjusted_max_value = _adjust_min_max_limits(
            column_type, min_value, max_limit, profiler_options
        )
        return (
            adjusted_min_value,
            adjusted_max_value,
            f"Real min value was used. Max was capped by {num_sigmas} sigmas. avg={mean_value}, stddev={stddev_value}, max={max_value}",
        )
    if max_limit > max_value:
        adjusted_min_value, adjusted_max_value = _adjust_min_max_limits(
            column_type, min_limit, max_value, profiler_options
        )
        return (
            adjusted_min_value,
            adjusted_max_value,
            f"Real max value was used. Min was capped by {num_sigmas} sigmas. avg={mean_value}, stddev={stddev_value}, min={min_value}",
        )
    adjusted_min_value, adjusted_max_value = _adjust_min_max_limits(column_type, min_value, max_value, profiler_options)
    return adjusted_min_value, adjusted_max_value, "Real min/max values were used"


def _adjust_min_max_limits(
    column_type: T.DataType, min_value: Any, max_value: Any, profiler_options: dict[str, Any]
) -> tuple[Any, Any]:
    """
    Adjusts the minimum and maximum limits based on the data type of the column.

    Args:
        column_type: The data type of the column.
        min_value: The minimum value of the column.
        max_value: The maximum value of the column.
        profiler_options: Configuration options for the DQProfiler.

    Returns:
        A tuple containing the adjusted minimum and maximum limits.
    """

    if isinstance(column_type, T.DateType):
        return (
            datetime.datetime.fromtimestamp(int(min_value), tz=datetime.timezone.utc).date(),
            datetime.datetime.fromtimestamp(int(max_value), tz=datetime.timezone.utc).date(),
        )

    if isinstance(column_type, (T.TimestampType, T.TimestampNTZType)):
        # float(), not int(): the epoch is a double carrying fractional seconds (see the double cast in
        # _make_min_max_profile_with_outlier_removal), and fromtimestamp(float) preserves microseconds.
        min_value = datetime.datetime.fromtimestamp(float(min_value), tz=datetime.timezone.utc)
        max_value = datetime.datetime.fromtimestamp(float(max_value), tz=datetime.timezone.utc)
        return _round_value(min_value, "down", profiler_options), _round_value(max_value, "up", profiler_options)

    if isinstance(column_type, T.IntegralType):
        return int(_round_value(min_value, "down", profiler_options)), int(
            _round_value(max_value, "up", profiler_options)
        )

    return min_value, max_value


def _round_value(value: Any, rounding_direction: str, profiler_options: dict[str, Any]) -> Any:
    """
    Rounds a value based on the specified direction and options.

    Args:
        value: The value to round.
        rounding_direction: The direction to round the value ("up" or "down").
        profiler_options: A dictionary of options, including whether to round the value.

    Returns:
        The rounded value, or the original value if rounding is not enabled.
    """
    if value is None or not profiler_options.get(PROFILE_OPTION_ROUND, False):
        return value

    if isinstance(value, datetime.datetime):
        return _round_datetime(value, rounding_direction)

    if isinstance(value, float):
        return _round_float(value, rounding_direction)

    if isinstance(value, int):
        return value  # already rounded

    if isinstance(value, decimal.Decimal):
        return _round_decimal(value, rounding_direction)

    return value


def _round_datetime(value: datetime.datetime, rounding_direction: str) -> datetime.datetime:
    """
    Rounds a datetime value to midnight based on the specified direction.

    There are 2 possible rounding directions:
    * "down" -> truncate to midnight (00:00:00).
    * "up" -> return the next midnight unless value is already midnight.

    Args:
        value: The datetime value to round.
        rounding_direction: The rounding direction ("up" or "down").

    Returns:
        The rounded datetime value.

    Raises:
        InvalidParameterError: If rounding_direction is not 'up' or 'down'.
    """
    midnight = value.replace(hour=0, minute=0, second=0, microsecond=0)

    if rounding_direction == "down":
        return midnight

    if rounding_direction == "up":
        if midnight == value:
            return value
        try:
            return midnight + datetime.timedelta(days=1)
        except OverflowError:
            logger.warning("Rounding datetime up caused overflow; returning datetime.max instead.")
            return datetime.datetime.max
    raise InvalidParameterError(f"Invalid rounding direction: {rounding_direction}. Use 'up' or 'down'.")


def _round_float(value: float, rounding_direction: str) -> float:
    """
    Rounds a float value based on the specified direction.

    Args:
        value: The float value to round.
        rounding_direction: The direction to round the value ('up' or 'down').

    Returns:
        The rounded float value.
    """
    if rounding_direction == "down":
        return math.floor(value)
    if rounding_direction == "up":
        return math.ceil(value)
    return value


def _round_decimal(value: decimal.Decimal, rounding_direction: str) -> decimal.Decimal:
    """
    Rounds a decimal value based on the specified direction.

    Args:
        value: The decimal value to round.
        rounding_direction: The direction to round the value ('up' or 'down').

    Returns:
        The rounded decimal value.
    """
    if rounding_direction == "down":
        return value.to_integral_value(rounding=decimal.ROUND_FLOOR)
    if rounding_direction == "up":
        return value.to_integral_value(rounding=decimal.ROUND_CEILING)
    return value


@register_profile_builder("has_no_outliers")
def make_has_no_outliers_profile(
    df: DataFrame,
    column_name: str,
    column_type: T.DataType,
    profiler_metrics: dict[str, Any],
    profiler_options: dict[str, Any],
) -> DQProfile | None:
    """
    Creates a *has_no_outliers* profile using the same MAD method as the *has_no_outliers* check rule.

    A profile is returned when all the following conditions are met:
    - The column type is child of `pyspark.sql.types.NumericType`.
    - The DataFrame is non-empty.
    - The fraction of outliers (values outside *median* ± 3.5 × MAD) is at or below *outliers_ratio*.
    - Profile generation is enabled at configuration level for all columns or given column.

    Args:
        df: The DataFrame to create the profile for.
        column_name: Input column name
        column_type: Input column type
        profiler_metrics: Column-level statistics computed by the DQProfiler
        profiler_options: Configuration options for the DQProfiler

    Returns:
        A DQProfile if all conditions are met, otherwise None.
    """
    if not isinstance(column_type, T.NumericType):
        return None

    if not _is_has_no_outliers_enabled(column_name, profiler_options):
        return None

    total_non_null_count = profiler_metrics.get("count_non_null", 0)
    if total_non_null_count == 0:
        logger.info(f"Column '{column_name}' has no non-null values. Skipping `has_no_outliers` profile generation")
        return None

    bounds = calculate_median_absolute_deviation_bounds(df, column_name)
    if bounds is None:
        logger.info(
            f"MAD bounds were not calculated for column '{column_name}'. Skipping `has_no_outliers` profile generation"
        )
        return None

    lower_bound, upper_bound = bounds
    # Skip degenerate distributions. Exactly-equal bounds mean MAD == 0 (a constant column). A
    # tiny-but-nonzero MAD collapses the band to a near-zero width relative to the column's scale,
    # which would emit a rule that flags almost every row as an outlier at apply time. Guard both
    # with a scale-relative check so only meaningfully-wide bands produce a rule.
    band_width = upper_bound - lower_bound
    scale = max(abs(lower_bound), abs(upper_bound))
    if band_width <= 0 or (scale > 0 and band_width <= 1e-12 * scale):
        logger.info(
            f"MAD band is degenerate (near-zero width) for column '{column_name}'. "
            "The distribution is (near-)constant. Skipping profile generation."
        )
        return None

    below_lower_bound_expr = F.col(column_name) < get_limit_expr(lower_bound)
    above_upper_bound_expr = F.col(column_name) > get_limit_expr(upper_bound)
    outside_bounds_expr = below_lower_bound_expr | above_upper_bound_expr
    outliers_count = df.filter(outside_bounds_expr).count()

    outliers_ratio = float(outliers_count) / total_non_null_count
    outliers_ratio_threshold = profiler_options.get(
        PROFILE_OPTION_OUTLIERS_RATIO, DEFAULT_PROFILE_OPTIONS[PROFILE_OPTION_OUTLIERS_RATIO]
    )

    safe_column_name = column_name.replace("\n", " ").replace("\r", " ")
    # Inclusive (<=) to match the sibling ratio gates (max_null_ratio / max_empty_ratio), so a
    # column whose outlier fraction exactly equals the configured threshold still emits a rule.
    if outliers_ratio <= outliers_ratio_threshold:
        return DQProfile(
            name="has_no_outliers",
            description=f"Column {safe_column_name} has {outliers_ratio * 100:.1f}% of outliers (allowed: {outliers_ratio_threshold * 100:.1f}%). Lower boundary - {lower_bound}, upper boundary - {upper_bound}.",
            column=column_name,
            filter=profiler_options.get(PROFILE_OPTION_FILTER, None),
        )

    return None


@register_profile_builder("geospatial")
def make_geospatial_profile(
    df: DataFrame,
    column_name: str,
    column_type: T.DataType,
    profiler_metrics: dict[str, Any],
    profiler_options: dict[str, Any],
) -> list[DQProfile] | None:
    """
    Creates geospatial profiles for native GEOMETRY/GEOGRAPHY columns.

    Uses *try_to_geometry* to convert values, aligning with the geospatial check functions.
    the profiled bounds line up with the generated rules. Requires Databricks serverless compute
    or classic compute with Databricks Runtime Version >17.1. If the runtime does not meet these
    requirements, a warning is logged and no geospatial profiles are produced.

    Args:
        df: Single-column DataFrame (nulls already dropped)
        column_name: Input column name
        column_type: Input column type
        profiler_metrics: Column-level statistics computed by the DQProfiler
        profiler_options: Configuration options for the DQProfiler

    Returns:
        A list of DQProfiles, or None when profiling is disabled, the column is not geospatial,
        the column is entirely null, or the spatial functions are unavailable.

    Notes:
        Because spatial aggregations required for profiling can be expensive at scale, geospatial
        profiling must be  enabled via the *profile_geospatial* option (default: *False*).

        When geospatial profiling is enabled and the column is a geometry or geography type, a single
        aggregation computes the bounding box, area range, point-count range, geometry-type distribution
        and quality counts. Summary stats are added to the *profiler_metrics* (and thus the summary statistics)
        and used to emit the matching row-level geospatial profiles.
    """
    if not profiler_options.get(PROFILE_OPTION_PROFILE_GEOSPATIAL, False):
        return None

    if not is_geospatial(column_type):
        return None

    if profiler_metrics.get("count_non_null", 0) == 0:
        return None

    stats = _compute_geospatial_stats(df, column_name, profiler_options)
    if stats is None:
        return None

    profiler_metrics.update(stats)
    profiles = _build_geospatial_profiles(column_name, stats, profiler_metrics, profiler_options)
    return profiles or None


def _compute_geospatial_stats(
    df: DataFrame, column_name: str, profiler_options: dict[str, Any]
) -> dict[str, Any] | None:
    """
    Runs a single spatial aggregation over a geometry column and returns the raw stats.

    Requires Databricks serverless compute or classic compute with Databricks Runtime
    Version >17.1. If the runtime does not meet these requirements, a warning is logged
    and no geospatial profiles are produced.

    Args:
        df: Single-column DataFrame with nulls dropped
        column_name: Input column name
        profiler_options: Configuration options for the DQProfiler

    Returns:
        A dictionary of statistics about the geometry values in the profiled column.
    """
    column_label = df.columns[0]
    geom = f"try_to_geometry(`{column_label}`)"
    srid = profiler_options.get(PROFILE_OPTION_GEOSPATIAL_SRID, None)
    # Match the area expression used by the geospatial area checks so profiled areas and generated
    # rules are computed in the same units of measure (see geo/check_funcs.py).
    area = f"st_area(st_transform(st_setsrid({geom}, {DEFAULT_SRID}), {srid}))" if srid else f"st_area({geom})"
    # Null island: a POINT at the origin with zero (or absent) Z/M ordinates. Mirrors is_not_null_island.
    null_island = (
        f"{geom} IS NOT NULL AND st_geometrytype({geom}) = 'ST_Point' "
        f"AND st_x({geom}) = 0.0 AND st_y({geom}) = 0.0 "
        f"AND (st_z({geom}) IS NULL OR st_z({geom}) = 0.0) "
        f"AND (st_m({geom}) IS NULL OR st_m({geom}) = 0.0)"
    )
    aggregations = [
        F.expr(f"min(st_xmin({geom}))").alias(_GEO_STAT_MIN_X),
        F.expr(f"max(st_xmax({geom}))").alias(_GEO_STAT_MAX_X),
        F.expr(f"min(st_ymin({geom}))").alias(_GEO_STAT_MIN_Y),
        F.expr(f"max(st_ymax({geom}))").alias(_GEO_STAT_MAX_Y),
        F.expr(f"min({area})").alias(_GEO_STAT_MIN_AREA),
        F.expr(f"max({area})").alias(_GEO_STAT_MAX_AREA),
        F.expr(f"min(st_npoints({geom}))").alias(_GEO_STAT_MIN_NUM_POINTS),
        F.expr(f"max(st_npoints({geom}))").alias(_GEO_STAT_MAX_NUM_POINTS),
        F.expr(f"array_sort(collect_set(st_geometrytype({geom})))").alias(_GEO_STAT_TYPES),
        F.expr(f"count_if({geom} IS NULL OR st_isempty({geom}))").alias(_GEO_STAT_EMPTY_COUNT),
        F.expr(f"count_if({geom} IS NULL OR NOT st_isvalid({geom}))").alias(_GEO_STAT_INVALID_COUNT),
        F.expr(f"count_if({null_island})").alias(_GEO_STAT_NULL_ISLAND_COUNT),
    ]
    try:
        row = df.agg(*aggregations).first()
    except AnalysisException as exc:
        safe_column_name = column_name.replace("\n", " ").replace("\r", " ")
        logger.warning(
            f"Skipping geospatial profiling for column '{safe_column_name}': the spatial SQL functions "
            f"are unavailable on this runtime (requires Databricks serverless or DBR 17.1+). Details: {exc}"
        )
        return None

    return row.asDict() if row else None


def _build_geospatial_profiles(
    column_name: str,
    stats: dict[str, Any],
    profiler_metrics: dict[str, Any],
    profiler_options: dict[str, Any],
) -> list[DQProfile]:
    """
    Builds a list of geospatial DQProfiles from the aggregated profiler stats.

    Args:
        column_name: Input column name.
        stats: Dictionary of stats from profiling values in the input column.
        profiler_metrics: Profiler metrics from non-geospatial profiling.
        profiler_options: Profiler options.

    Returns:
        A list of geospatial DQProfiles.
    """
    dq_filter = profiler_options.get(PROFILE_OPTION_FILTER, None)
    profiles = []

    geometry_types = stats.get(_GEO_STAT_TYPES) or []
    if len(geometry_types) == 1:
        profiles.append(
            DQProfile(
                name="geometry_type",
                column=column_name,
                parameters={"type": geometry_types[0]},
                description=f"All profiled geometries are of type {geometry_types[0]}",
                filter=dq_filter,
            )
        )

    profiles.extend(_build_geospatial_range_profiles(column_name, stats, profiler_options, dq_filter))
    profiles.extend(
        _build_geospatial_quality_profiles(column_name, stats, profiler_metrics, profiler_options, dq_filter)
    )
    return profiles


def _build_geospatial_range_profiles(
    column_name: str,
    stats: dict[str, Any],
    profiler_options: dict[str, Any],
    dq_filter: str | None,
) -> list[DQProfile]:
    """
    Builds value range profiles for the bounding-box, area and number of points in the input
    geometry column.

    Args:
        column_name: Input column name.
        stats: Dictionary of stats from profiling values in the input column.
        profiler_options: Profiler options.
        dq_filter: Row filter applied to the input column before profiling column values.

    Returns:
        A list of geospatial DQProfiles.
    """
    profiles = []

    min_x, max_x = stats.get(_GEO_STAT_MIN_X), stats.get(_GEO_STAT_MAX_X)
    if min_x is not None and max_x is not None:
        profiles.append(
            DQProfile(
                name="has_x_coordinate_between",
                column=column_name,
                parameters={
                    "min_value": _round_value(float(min_x), "down", profiler_options),
                    "max_value": _round_value(float(max_x), "up", profiler_options),
                },
                filter=dq_filter,
            )
        )

    min_y, max_y = stats.get(_GEO_STAT_MIN_Y), stats.get(_GEO_STAT_MAX_Y)
    if min_y is not None and max_y is not None:
        profiles.append(
            DQProfile(
                name="has_y_coordinate_between",
                column=column_name,
                parameters={
                    "min_value": _round_value(float(min_y), "down", profiler_options),
                    "max_value": _round_value(float(max_y), "up", profiler_options),
                },
                filter=dq_filter,
            )
        )

    srid = profiler_options.get(PROFILE_OPTION_GEOSPATIAL_SRID, None)
    min_area, max_area = stats.get(_GEO_STAT_MIN_AREA), stats.get(_GEO_STAT_MAX_AREA)
    if min_area is not None:
        profiles.append(
            DQProfile(
                name="is_area_not_less_than",
                column=column_name,
                parameters={"value": _round_value(float(min_area), "down", profiler_options), "srid": srid},
                filter=dq_filter,
            )
        )
    if max_area is not None:
        profiles.append(
            DQProfile(
                name="is_area_not_greater_than",
                column=column_name,
                parameters={"value": _round_value(float(max_area), "up", profiler_options), "srid": srid},
                filter=dq_filter,
            )
        )

    min_num_points, max_num_points = stats.get(_GEO_STAT_MIN_NUM_POINTS), stats.get(_GEO_STAT_MAX_NUM_POINTS)
    if min_num_points is not None:
        profiles.append(
            DQProfile(
                name="is_num_points_not_less_than",
                column=column_name,
                parameters={"value": int(min_num_points)},
                filter=dq_filter,
            )
        )
    if max_num_points is not None:
        profiles.append(
            DQProfile(
                name="is_num_points_not_greater_than",
                column=column_name,
                parameters={"value": int(max_num_points)},
                filter=dq_filter,
            )
        )

    return profiles


def _build_geospatial_quality_profiles(
    column_name: str,
    stats: dict[str, Any],
    profiler_metrics: dict[str, Any],
    profiler_options: dict[str, Any],
    dq_filter: str | None,
) -> list[DQProfile]:
    """
    Builds profiles for empty, OGC-invalid, or null-island values in geometry columns.

    Profiles are emitted only when the sampled violation ratio is within *max_null_ratio*.
    Columns with some empty/invalid/null-island values are still profiled.

    Args:
        column_name: Input column name.
        stats: Dictionary of stats from profiling values in the input column.
        profiler_metrics: Dictionary of profiling metrics from profiling values in the input column.
        profiler_options: Profiler options.
        dq_filter: Row filter applied to the input column before profiling column values.

    Returns:
        A list of geospatial DQProfiles.
    """
    total = profiler_metrics.get("count_non_null", 0)
    if total <= 0:
        return []

    max_null_ratio = profiler_options.get(PROFILE_OPTION_MAX_NULL_RATIO, 0.0)
    profiles = []
    property_profiles = [
        (_GEO_STAT_EMPTY_COUNT, "is_non_empty_geometry"),
        (_GEO_STAT_INVALID_COUNT, "is_ogc_valid"),
        (_GEO_STAT_NULL_ISLAND_COUNT, "is_not_null_island"),
    ]
    for stat_key, profile_name in property_profiles:
        violation_ratio = stats.get(stat_key, 0) / total
        if violation_ratio <= max_null_ratio:
            profiles.append(DQProfile(name=profile_name, column=column_name, filter=dq_filter))

    return profiles
