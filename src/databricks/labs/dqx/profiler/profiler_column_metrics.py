import logging
from collections.abc import Callable

from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql import types as T

from databricks.labs.dqx.errors import InvalidParameterError
from databricks.labs.dqx.profiler.common import is_text


DQProfileColumnMetricFunc = Callable[[T.StructField, str], Column | None]
PROFILE_COLUMN_METRIC_REGISTRY: dict[str, DQProfileColumnMetricFunc] = {}
# Reserved keys that always-on profiler internals own. They are computed inline in
# `_build_column_metrics` (count_non_null) or derived from it (count_null, count), and downstream
# profile builders read them by string key. Allowing registry entries to shadow these keys would
# let a user metric overwrite the inline aggregation via Row.asDict() (last-value-wins on duplicate
# aliases), corrupting count_null/count and every builder that depends on them.
RESERVED_PROFILE_COLUMN_METRIC_KEYS: frozenset[str] = frozenset({"count", "count_non_null", "count_null"})
logger = logging.getLogger(__name__)


def register_profile_column_metric(
    profile_column_metric_type: str,
) -> Callable[[DQProfileColumnMetricFunc], DQProfileColumnMetricFunc]:
    """
    Registers data quality profile column metric function. The function that may create a column metric depending on
    the column type of the input column or other internal logic. Result column is used in an aggregation function
    resulting in a single value for a given column and data frame. The aggregation value will be used further to at the profiling
    stage to supply common column level metrics to construct corresponding builders.

    Expected signature of the function is as follows:
    (field,column_label) -> Column | None
    where:
        - field: struct field of the profiling column
        - column_label: name of the column that is present in the dataframe to be aggregated
    The function may return *None* if aggregation is not applicable.

    The following keys are reserved for always-on profiler internals and cannot be used as a
    registration key: *count*, *count_non_null*, *count_null*. Registering under any of these keys
    raises *InvalidParameterError*.

    Args:
        profile_column_metric_type: Key under which the metric is registered and exposed to profile builders.

    Raises:
        InvalidParameterError: If *profile_column_metric_type* is one of the reserved keys.
    """

    def wrapper(metric_func: DQProfileColumnMetricFunc) -> DQProfileColumnMetricFunc:
        if profile_column_metric_type in RESERVED_PROFILE_COLUMN_METRIC_KEYS:
            raise InvalidParameterError(
                f"'{profile_column_metric_type}' is a reserved profile column metric key and cannot be registered. "
                f"Reserved keys: {sorted(RESERVED_PROFILE_COLUMN_METRIC_KEYS)}."
            )
        if profile_column_metric_type in PROFILE_COLUMN_METRIC_REGISTRY:
            logger.warning(f"Overwriting profile column metric registered as '{profile_column_metric_type}'")
        PROFILE_COLUMN_METRIC_REGISTRY[profile_column_metric_type] = metric_func
        return metric_func

    return wrapper


def deregister_profile_column_metric(profile_column_metric_type: str) -> None:
    """
    Removes a previously registered profile column metric from *PROFILE_COLUMN_METRIC_REGISTRY*.
    No-op if no metric is registered under the given key.

    The reserved keys *count*, *count_non_null*, *count_null* are always-on and cannot be
    deregistered; passing any of them raises *InvalidParameterError* so callers do not silently
    assume a reserved metric has been disabled.

    Args:
        profile_column_metric_type: Key under which the metric was registered.

    Raises:
        InvalidParameterError: If *profile_column_metric_type* is one of the reserved keys.
    """
    if profile_column_metric_type in RESERVED_PROFILE_COLUMN_METRIC_KEYS:
        raise InvalidParameterError(
            f"'{profile_column_metric_type}' is a reserved profile column metric key and cannot be deregistered. "
            f"Reserved keys: {sorted(RESERVED_PROFILE_COLUMN_METRIC_KEYS)}."
        )
    PROFILE_COLUMN_METRIC_REGISTRY.pop(profile_column_metric_type, None)


def build_registered_metric_aggregations(field: T.StructField, column_label: str) -> list[Column]:
    """
    Return aliased aggregation columns from *PROFILE_COLUMN_METRIC_REGISTRY*, skipping reserved keys
    and any metric function that returns *None* for the given field.

    Reserved keys (*count*, *count_non_null*, *count_null*) are computed inline by the profiler and
    must never be shadowed by a registry entry — see *RESERVED_PROFILE_COLUMN_METRIC_KEYS*. The
    reserved-key filter here is defence-in-depth for entries injected directly into the registry
    (bypassing the *register_profile_column_metric* guard).

    Args:
        field: Struct field of the profiling column.
        column_label: Name of the column present in the dataframe to be aggregated.
    """
    aggregations: list[Column] = []
    for metric_name, metric_function in PROFILE_COLUMN_METRIC_REGISTRY.items():
        if metric_name in RESERVED_PROFILE_COLUMN_METRIC_KEYS:
            continue
        metric_col = metric_function(field, column_label)
        if metric_col is not None:
            aggregations.append(metric_col.alias(metric_name))
    return aggregations


@register_profile_column_metric("empty_count")
def empty_count(field: T.StructField, column_label: str) -> Column:
    """
    Profiling column metric for empty count. Applicable for text columns only, otherwise returns literal *0* for
    backward compatibility.
    """
    return F.count_if(F.col(column_label) == "") if is_text(field.dataType) else F.lit(0)


@register_profile_column_metric("count_distinct")
def count_distinct(_field: T.StructField, column_label: str) -> Column:
    """
    Profiling column metric for count distinct. Applicable for all columns.
    """
    return F.countDistinct(column_label)
