import logging
from collections.abc import Callable

from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql import types as T

from databricks.labs.dqx.profiler.common import is_text


DQProfileColumnMetricFunc = Callable[[T.StructField, str], Column | None]
PROFILE_COLUMN_METRIC_REGISTRY: dict[str, DQProfileColumnMetricFunc] = {}
logger = logging.getLogger(__name__)


def register_profile_column_metric() -> Callable:
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
    The function may return `None` if aggregation is not applicable.
    """

    def wrapper(metric_func: Callable) -> Callable:
        PROFILE_COLUMN_METRIC_REGISTRY[metric_func.__name__] = metric_func
        return metric_func

    return wrapper


@register_profile_column_metric()
def empty_count(field: T.StructField, column_label: str) -> Column | None:
    """
    Profiling column metric for empty count. Applicable for text columns only, otherwise returns literal `0` for
    backward compatibility.
    """
    return F.count_if(F.col(column_label) == "") if is_text(field.dataType) else F.lit(0)


@register_profile_column_metric()
def count_distinct(_field: T.StructField, column_label: str) -> Column | None:
    """
    Profiling column metric for count distinct. Applicable for all columns.
    """
    return F.countDistinct(column_label)


@register_profile_column_metric()
def count_non_null(_field: T.StructField, column_label: str) -> Column | None:
    """
    Profiling column metric for count not null values. Applicable for all columns.
    """
    return F.count(column_label)
