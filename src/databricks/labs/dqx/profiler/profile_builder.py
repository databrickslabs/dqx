import datetime
import decimal
import logging
from collections.abc import Callable
import math
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import types as T, functions as F
from databricks.labs.dqx.errors import InvalidParameterError
from databricks.labs.dqx.profiler.profile import DQProfile, DQProfileType


PROFILE_BUILDER_REGISTRY: dict[str, DQProfileType] = {}
logger = logging.getLogger(__name__)


def register_profile_builder(profile_type: str) -> Callable:
    def wrapper(builder_func: Callable) -> Callable:
        PROFILE_BUILDER_REGISTRY[profile_type] = DQProfileType(name=profile_type, builder=builder_func)
        return builder_func

    return wrapper


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
    if _is_text(column_type):
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

    total_count = profiler_metrics.get("total_count", 0)
    if total_count == 0:
        return None

    max_in_count = profiler_options.get("max_in_count", 0)
    max_distinct_ratio = profiler_options.get("distinct_ratio", 0.0)

    col = df.columns[0]
    distinct_df = df.select(col).distinct()
    distinct_count = distinct_df.count()
    if distinct_count == 0:
        # The df passed here has nulls already dropped by the caller. If distinct_count is 0,
        # the column is entirely null — no valid values to build an allowlist from.
        return None

    distinct_ratio = (1.0 * distinct_count) / total_count

    if distinct_count <= max_in_count and distinct_ratio <= max_distinct_ratio:
        distinct_values = [row[0] for row in distinct_df.collect()]
        return DQProfile(
            name="is_in",
            column=column_name,
            parameters={"in": distinct_values},
            filter=profiler_options.get("filter", None),
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


def _is_text(column_type: T.DataType) -> bool:
    """
    Validates that the input column type is a Spark text type.

    Args:
        column_type: Input column type

    Returns:
        True if the column is a Spark text type, otherwise False
    """
    return isinstance(column_type, (T.CharType, T.StringType, T.VarcharType))


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
    total_count = profiler_metrics.get("total_count", 0)
    if total_count == 0:
        return None

    null_count = profiler_metrics.get("null_count", 0)
    null_ratio = null_count / total_count
    empty_count = profiler_metrics.get("empty_count", 0)
    empty_ratio = empty_count / total_count
    max_null_ratio = profiler_options.get("max_null_ratio", 0.0)
    max_empty_ratio = profiler_options.get("max_empty_ratio", 0.0)
    trim_strings = profiler_options.get("trim_strings", True)

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
            filter=profiler_options.get("filter", None),
        )

    if not_null:
        return DQProfile(
            name="is_not_null",
            column=column_name,
            description=(
                f"Column {column_name} has {null_ratio * 100:.1f}% of null values (allowed {max_null_ratio * 100:.1f}%)"
                if null_count > 0
                else None
            ),
            filter=profiler_options.get("filter", None),
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
            filter=profiler_options.get("filter", None),
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
    null_count = profiler_metrics.get("null_count", 0)
    total_count = profiler_metrics.get("total_count", 0)
    if total_count == 0:
        return None
    null_ratio = null_count / total_count
    max_null_ratio = profiler_options.get("max_null_ratio", 0.0)

    if null_ratio <= max_null_ratio:
        return DQProfile(
            name="is_not_null",
            column=column_name,
            description=(
                f"Column {column_name} has {null_ratio * 100:.1f}% of null values (allowed {max_null_ratio * 100:.1f}%)"
                if null_count > 0
                else None
            ),
            filter=profiler_options.get("filter", None),
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
    return isinstance(column_type, (T.IntegerType, T.LongType, T.StringType))


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
    """
    remove_outliers = profiler_options.get("remove_outliers", True)
    if not remove_outliers:
        return False

    outlier_columns = profiler_options.get("outlier_columns", [])
    if not outlier_columns:
        return False
    return column_name in outlier_columns


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
        A 'min_max' DQProfile
    """
    column_alias = df.columns[0]
    if isinstance(column_type, T.DateType):
        # Convert DateType to timestamp, then to bigint epoch seconds.
        cast_df = df.select(F.col(column_alias).cast("timestamp").cast("bigint").alias(column_alias))
        aggregates = _get_aggregates(cast_df, column_alias)
    elif isinstance(column_type, T.TimestampType):
        cast_df = df.select(F.col(column_alias).cast("bigint").alias(column_alias))
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
        filter=profiler_options.get("filter", None),
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
        if isinstance(column_type, T.TimestampType):
            agg_df = agg_df.select(
                F.date_format("min_value", "yyyy-MM-dd HH:mm:ss").alias("min_value"),
                F.date_format("max_value", "yyyy-MM-dd HH:mm:ss").alias("max_value"),
            )
        aggregates = agg_df.collect()[0].asDict()
        if not aggregates or aggregates.get("min_value") is None:
            logger.info(f"Can't get min/max for field {column_name}")
            return None
        if isinstance(column_type, T.TimestampType):
            min_value = _round_value(
                datetime.datetime.strptime(aggregates["min_value"], "%Y-%m-%d %H:%M:%S").replace(
                    tzinfo=datetime.timezone.utc
                ),
                "down",
                profiler_options,
            )
            max_value = _round_value(
                datetime.datetime.strptime(aggregates["max_value"], "%Y-%m-%d %H:%M:%S").replace(
                    tzinfo=datetime.timezone.utc
                ),
                "up",
                profiler_options,
            )
        elif isinstance(column_type, T.IntegralType):
            min_value = int(_round_value(aggregates["min_value"], "down", profiler_options))
            max_value = int(_round_value(aggregates["max_value"], "up", profiler_options))
        else:
            min_value = aggregates["min_value"]
            max_value = aggregates["max_value"]

    return DQProfile(
        name="min_max",
        column=column_name,
        parameters={"min": min_value, "max": max_value},
        description="Real min/max values were used",
        filter=profiler_options.get("filter", None),
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
    num_sigmas = profiler_options.get("num_sigmas", 3)

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

    if isinstance(column_type, T.TimestampType):
        min_value = datetime.datetime.fromtimestamp(int(min_value), tz=datetime.timezone.utc)
        max_value = datetime.datetime.fromtimestamp(int(max_value), tz=datetime.timezone.utc)
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
    if not value or not profiler_options.get("round", False):
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
