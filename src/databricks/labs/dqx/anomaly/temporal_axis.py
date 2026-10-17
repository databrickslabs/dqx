"""Consistent time coordinates for fitting and replaying temporal baselines."""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from databricks.labs.dqx.utils import quote_column_name


def temporal_axis_seconds(df: DataFrame, time_column: str) -> Column:
    """Return whole seconds on the input column's time axis.

    Timestamps with a time zone represent absolute instants. Dates and timestamps
    without a time zone represent calendar values: interpreting them in a session
    time zone would move a persisted model's axis when another session scores it.
    Casting dates to timestamp_ntz gives midnight on that same calendar axis.
    Nulls remain null, and whole-second precision matches temporal model fitting.
    """
    value = F.col(quote_column_name(time_column))
    if isinstance(df.schema[time_column].dataType, T.DateType):
        value = value.cast(T.TimestampNTZType())
    return F.unix_timestamp(value).cast(T.DoubleType())
