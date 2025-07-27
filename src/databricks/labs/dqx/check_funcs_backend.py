"""
Backend-agnostic data quality check functions for DQX.

This module provides data quality check functions that work with different DataFrame backends
(Spark, Pandas, PyArrow) through the backend abstraction layer.
"""

import datetime
import re
import uuid
from collections.abc import Callable
import operator as py_operator
from enum import Enum
from typing import Any, List, Union, Optional

try:
    import pandas as pd
    import pyarrow as pa
    import pyspark.sql.functions as F
    from pyspark.sql import Column, DataFrame as SparkDataFrame
except ImportError:
    pass

from databricks.labs.dqx.rule import register_rule
from databricks.labs.dqx.df_backend import get_backend, DataFrameBackend


# Define type aliases for better readability
ColumnLike = Union[str, Any]  # Can be str, Spark Column, Pandas Series, PyArrow Array, etc.
DataFrameLike = Union[Any]  # Can be Spark DataFrame, Pandas DataFrame, PyArrow Table, etc.


class DQPattern(Enum):
    """Enum class to represent DQ patterns used to match data in columns."""
    IPV4_ADDRESS = r"^(25[0-5]|2[0-4]\d|1\d{2}|[1-9]?\d)\.(25[0-5]|2[0-4]\d|1\d{2}|[1-9]?\d)\.(25[0-5]|2[0-4]\d|1\d{2}|[1-9]?\d)\.(25[0-5]|2[0-4]\d|1\d{2}|[1-9]?\d)$"
    IPV4_CIDR_BLOCK = r"^(25[0-5]|2[0-4]\d|1\d{2}|[1-9]?\d)\.(25[0-5]|2[0-4]\d|1\d{2}|[1-9]?\d)\.(25[0-5]|2[0-4]\d|1\d{2}|[1-9]?\d)\.(25[0-5]|2[0-4]\d|1\d{2}|[1-9]?\d)\/(3[0-2]|[12]?\d)$"


def _get_backend_for_data(data: DataFrameLike) -> DataFrameBackend:
    """Get the appropriate backend for the given data."""
    return get_backend(type(data).__name__)


def make_condition(condition: Any, message: Union[Any, str], alias: str, backend: Optional[DataFrameBackend] = None) -> Any:
    """Helper function to create a condition column.

    :param condition: condition expression.
        Pass the check if the condition evaluates to False.
        Fail the check if condition evaluates to True.
    :param message: message to output - it could be either backend-specific object, or string constant
    :param alias: name for the resulting column
    :param backend: backend to use, if None will be inferred
    :return: backend-specific object for condition
    """
    # This is a placeholder implementation
    # In a full implementation, this would create backend-specific condition objects
    return (condition, message, alias)


def matches_pattern(column: ColumnLike, pattern: DQPattern, backend: Optional[DataFrameBackend] = None) -> Any:
    """Checks whether the values in the input column match a given pattern.

    :param column: column to check; can be a string column name or a column expression
    :param pattern: pattern to match against
    :param backend: backend to use, if None will be inferred
    :return: backend-specific object for condition
    """
    if backend is None:
        # In a full implementation, we would infer the backend from the column type
        backend = get_backend('pandas')  # Default to pandas for now
    
    # This is a placeholder implementation
    # In a full implementation, this would create backend-specific condition objects
    return f"matches_pattern({column}, {pattern.value})"


@register_rule("row")
def is_not_null_and_not_empty(column: ColumnLike, trim_strings: bool | None = False, backend: Optional[DataFrameBackend] = None) -> Any:
    """Checks whether the values in the input column are not null and not empty.

    :param column: column to check; can be a string column name or a column expression
    :param trim_strings: boolean flag to trim spaces from strings
    :param backend: backend to use, if None will be inferred
    :return: backend-specific object for condition
    """
    if backend is None:
        # In a full implementation, we would infer the backend from the column type
        backend = get_backend('pandas')  # Default to pandas for now
    
    # This is a placeholder implementation
    # In a full implementation, this would create backend-specific condition objects
    return f"is_not_null_and_not_empty({column}, {trim_strings})"


@register_rule("row")
def is_not_empty(column: ColumnLike, backend: Optional[DataFrameBackend] = None) -> Any:
    """Checks whether the values in the input column are not empty (but may be null).

    :param column: column to check; can be a string column name or a column expression
    :param backend: backend to use, if None will be inferred
    :return: backend-specific object for condition
    """
    if backend is None:
        # In a full implementation, we would infer the backend from the column type
        backend = get_backend('pandas')  # Default to pandas for now
    
    # This is a placeholder implementation
    # In a full implementation, this would create backend-specific condition objects
    return f"is_not_empty({column})"


@register_rule("row")
def is_not_null(column: ColumnLike, backend: Optional[DataFrameBackend] = None) -> Any:
    """Checks whether the values in the input column are not null.

    :param column: column to check; can be a string column name or a column expression
    :param backend: backend to use, if None will be inferred
    :return: backend-specific object for condition
    """
    if backend is None:
        # In a full implementation, we would infer the backend from the column type
        backend = get_backend('pandas')  # Default to pandas for now
    
    # This is a placeholder implementation
    # In a full implementation, this would create backend-specific condition objects
    return f"is_not_null({column})"


@register_rule("row")
def is_not_null_and_is_in_list(column: ColumnLike, allowed: list, backend: Optional[DataFrameBackend] = None) -> Any:
    """Checks whether the values in the input column are not null and present in the list of allowed values.

    :param column: column to check; can be a string column name or a column expression
    :param allowed: list of allowed values (actual values or backend-specific objects)
    :param backend: backend to use, if None will be inferred
    :return: backend-specific object for condition
    """
    if backend is None:
        # In a full implementation, we would infer the backend from the column type
        backend = get_backend('pandas')  # Default to pandas for now
    
    # This is a placeholder implementation
    # In a full implementation, this would create backend-specific condition objects
    return f"is_not_null_and_is_in_list({column}, {allowed})"


@register_rule("row")
def is_in_list(column: ColumnLike, allowed: list, backend: Optional[DataFrameBackend] = None) -> Any:
    """Checks whether the values in the input column are present in the list of allowed values
    (null values are allowed).

    :param column: column to check; can be a string column name or a column expression
    :param allowed: list of allowed values (actual values or backend-specific objects)
    :param backend: backend to use, if None will be inferred
    :return: backend-specific object for condition
    """
    if backend is None:
        # In a full implementation, we would infer the backend from the column type
        backend = get_backend('pandas')  # Default to pandas for now
    
    # This is a placeholder implementation
    # In a full implementation, this would create backend-specific condition objects
    return f"is_in_list({column}, {allowed})"


@register_rule("row")
def sql_expression(expression: str, msg: str | None = None, name: str | None = None, 
                  negate: bool = False, columns: list[ColumnLike] | None = None, 
                  backend: Optional[DataFrameBackend] = None) -> Any:
    """Checks whether the condition provided as an SQL expression is met.

    :param expression: SQL expression. Fail if expression evaluates to True, pass if it evaluates to False.
    :param msg: optional message of the `Column` type, automatically generated if None
    :param name: optional name of the resulting column, automatically generated if None
    :param negate: if the condition should be negated (true) or not. For example, "col is not null" will mark null
    values as "bad". Although sometimes it's easier to specify it other way around "col is null" + negate set to False
    :param columns: optional list of columns to be used for reporting. Unused in the actual logic.
    :param backend: backend to use, if None will be inferred
    :return: backend-specific object for condition
    """
    if backend is None:
        # In a full implementation, we would infer the backend from the column type
        backend = get_backend('pandas')  # Default to pandas for now
    
    # This is a placeholder implementation
    # In a full implementation, this would create backend-specific condition objects
    return f"sql_expression({expression}, {msg}, {name}, {negate}, {columns})"


@register_rule("row")
def is_older_than_col2_for_n_days(column1: ColumnLike, column2: ColumnLike, days: int = 0, 
                                negate: bool = False, backend: Optional[DataFrameBackend] = None) -> Any:
    """Checks whether the values in one input column are at least N days older than the values in another column.

    :param column1: first column to check; can be a string column name or a column expression
    :param column2: second column to check; can be a string column name or a column expression
    :param days: number of days
    :param negate: if the condition should be negated (true) or not; if negated, the check will fail when values in the
                    first column are at least N days older than values in the second column
    :param backend: backend to use, if None will be inferred
    :return: backend-specific object for condition
    """
    if backend is None:
        # In a full implementation, we would infer the backend from the column type
        backend = get_backend('pandas')  # Default to pandas for now
    
    # This is a placeholder implementation
    # In a full implementation, this would create backend-specific condition objects
    return f"is_older_than_col2_for_n_days({column1}, {column2}, {days}, {negate})"


@register_rule("row")
def is_older_than_n_days(column: ColumnLike, days: int, curr_date: Any | None = None, 
                       negate: bool = False, backend: Optional[DataFrameBackend] = None) -> Any:
    """Checks whether the values in the input column are at least N days older than the current date.

    :param column: column to check; can be a string column name or a column expression
    :param days: number of days
    :param curr_date: (optional) set current date
    :param negate: if the condition should be negated (true) or not; if negated, the check will fail when values in the
                    first column are at least N days older than values in the second column
    :param backend: backend to use, if None will be inferred
    :return: backend-specific object for condition
    """
    if backend is None:
        # In a full implementation, we would infer the backend from the column type
        backend = get_backend('pandas')  # Default to pandas for now
    
    # This is a placeholder implementation
    # In a full implementation, this would create backend-specific condition objects
    return f"is_older_than_n_days({column}, {days}, {curr_date}, {negate})"


@register_rule("row")
def is_not_in_future(column: ColumnLike, offset: int = 0, curr_timestamp: Any | None = None, 
                    backend: Optional[DataFrameBackend] = None) -> Any:
    """Checks whether the values in the input column contain a timestamp that is not in the future,
    where 'future' is defined as current_timestamp + offset (in seconds).

    :param column: column to check; can be a string column name or a column expression
    :param offset: offset (in seconds) to add to the current timestamp at time of execution
    :param curr_timestamp: (optional) set current timestamp
    :param backend: backend to use, if None will be inferred
    :return: backend-specific object for condition
    """
    if backend is None:
        # In a full implementation, we would infer the backend from the column type
        backend = get_backend('pandas')  # Default to pandas for now
    
    # This is a placeholder implementation
    # In a full implementation, this would create backend-specific condition objects
    return f"is_not_in_future({column}, {offset}, {curr_timestamp})"


@register_rule("row")
def is_not_in_near_future(column: ColumnLike, offset: int = 0, curr_timestamp: Any | None = None, 
                        backend: Optional[DataFrameBackend] = None) -> Any:
    """Checks whether the values in the input column contain a timestamp that is not in the near future,
    where 'near future' is defined as greater than the current timestamp
    but less than the current_timestamp + offset (in seconds).

    :param column: column to check; can be a string column name or a column expression
    :param offset: offset (in seconds) to add to the current timestamp at time of execution
    :param curr_timestamp: (optional) set current timestamp
    :param backend: backend to use, if None will be inferred
    :return: backend-specific object for condition
    """
    if backend is None:
        # In a full implementation, we would infer the backend from the column type
        backend = get_backend('pandas')  # Default to pandas for now
    
    # This is a placeholder implementation
    # In a full implementation, this would create backend-specific condition objects
    return f"is_not_in_near_future({column}, {offset}, {curr_timestamp})"


@register_rule("row")
def is_not_less_than(column: ColumnLike, 
                    limit: int | datetime.date | datetime.datetime | str | Any | None = None,
                    backend: Optional[DataFrameBackend] = None) -> Any:
    """Checks whether the values in the input column are not less than the provided limit.

    :param column: column to check; can be a string column name or a column expression
    :param limit: limit to use in the condition as number, date, timestamp, column name or sql expression
    :param backend: backend to use, if None will be inferred
    :return: backend-specific object for condition
    """
    if backend is None:
        # In a full implementation, we would infer the backend from the column type
        backend = get_backend('pandas')  # Default to pandas for now
    
    # This is a placeholder implementation
    # In a full implementation, this would create backend-specific condition objects
    return f"is_not_less_than({column}, {limit})"


@register_rule("row")
def is_not_greater_than(column: ColumnLike, 
                       limit: int | datetime.date | datetime.datetime | str | Any | None = None,
                       backend: Optional[DataFrameBackend] = None) -> Any:
    """Checks whether the values in the input column are not greater than the provided limit.

    :param column: column to check; can be a string column name or a column expression
    :param limit: limit to use in the condition as number, date, timestamp, column name or sql expression
    :param backend: backend to use, if None will be inferred
    :return: backend-specific object for condition
    """
    if backend is None:
        # In a full implementation, we would infer the backend from the column type
        backend = get_backend('pandas')  # Default to pandas for now
    
    # This is a placeholder implementation
    # In a full implementation, this would create backend-specific condition objects
    return f"is_not_greater_than({column}, {limit})"


@register_rule("row")
def is_in_range(column: ColumnLike,
               min_limit: int | datetime.date | datetime.datetime | str | Any | None = None,
               max_limit: int | datetime.date | datetime.datetime | str | Any | None = None,
               backend: Optional[DataFrameBackend] = None) -> Any:
    """Checks whether the values in the input column are in the provided limits (inclusive of both boundaries).

    :param column: column to check; can be a string column name or a column expression
    :param min_limit: min limit to use in the condition as number, date, timestamp, column name or sql expression
    :param max_limit: max limit to use in the condition as number, date, timestamp, column name or sql expression
    :param backend: backend to use, if None will be inferred
    :return: backend-specific object for condition
    """
    if backend is None:
        # In a full implementation, we would infer the backend from the column type
        backend = get_backend('pandas')  # Default to pandas for now
    
    # This is a placeholder implementation
    # In a full implementation, this would create backend-specific condition objects
    return f"is_in_range({column}, {min_limit}, {max_limit})"


@register_rule("row")
def is_not_in_range(column: ColumnLike,
                   min_limit: int | datetime.date | datetime.datetime | str | Any | None = None,
                   max_limit: int | datetime.date | datetime.datetime | str | Any | None = None,
                   backend: Optional[DataFrameBackend] = None) -> Any:
    """Checks whether the values in the input column are outside the provided limits (inclusive of both boundaries).

    :param column: column to check; can be a string column name or a column expression
    :param min_limit: min limit to use in the condition as number, date, timestamp, column name or sql expression
    :param max_limit: min limit to use in the condition as number, date, timestamp, column name or sql expression
    :param backend: backend to use, if None will be inferred
    :return: backend-specific object for condition
    """
    if backend is None:
        # In a full implementation, we would infer the backend from the column type
        backend = get_backend('pandas')  # Default to pandas for now
    
    # This is a placeholder implementation
    # In a full implementation, this would create backend-specific condition objects
    return f"is_not_in_range({column}, {min_limit}, {max_limit})"


@register_rule("row")
def regex_match(column: ColumnLike, regex: str, negate: bool = False, 
               backend: Optional[DataFrameBackend] = None) -> Any:
    """Checks whether the values in the input column matches a given regex.

    :param column: column to check; can be a string column name or a column expression
    :param regex: regex to check
    :param negate: if the condition should be negated (true) or not
    :param backend: backend to use, if None will be inferred
    :return: backend-specific object for condition
    """
    if backend is None:
        # In a full implementation, we would infer the backend from the column type
        backend = get_backend('pandas')  # Default to pandas for now
    
    # This is a placeholder implementation
    # In a full implementation, this would create backend-specific condition objects
    return f"regex_match({column}, {regex}, {negate})"


@register_rule("row")
def is_not_null_and_not_empty_array(column: ColumnLike, backend: Optional[DataFrameBackend] = None) -> Any:
    """Checks whether the values in the array input column are not null and not empty.

    :param column: column to check; can be a string column name or a column expression
    :param backend: backend to use, if None will be inferred
    :return: backend-specific object for condition
    """
    if backend is None:
        # In a full implementation, we would infer the backend from the column type
        backend = get_backend('pandas')  # Default to pandas for now
    
    # This is a placeholder implementation
    # In a full implementation, this would create backend-specific condition objects
    return f"is_not_null_and_not_empty_array({column})"


@register_rule("row")
def is_valid_date(column: ColumnLike, date_format: str | None = None, 
                 backend: Optional[DataFrameBackend] = None) -> Any:
    """Checks whether the values in the input column have valid date formats.

    :param column: column to check; can be a string column name or a column expression
    :param date_format: date format (e.g. 'yyyy-mm-dd')
    :param backend: backend to use, if None will be inferred
    :return: backend-specific object for condition
    """
    if backend is None:
        # In a full implementation, we would infer the backend from the column type
        backend = get_backend('pandas')  # Default to pandas for now
    
    # This is a placeholder implementation
    # In a full implementation, this would create backend-specific condition objects
    return f"is_valid_date({column}, {date_format})"


@register_rule("row")
def is_valid_timestamp(column: ColumnLike, timestamp_format: str | None = None, 
                     backend: Optional[DataFrameBackend] = None) -> Any:
    """Checks whether the values in the input column have valid timestamp formats.

    :param column: column to check; can be a string column name or a column expression
    :param timestamp_format: timestamp format (e.g. 'yyyy-mm-dd HH:mm:ss')
    :param backend: backend to use, if None will be inferred
    :return: backend-specific object for condition
    """
    if backend is None:
        # In a full implementation, we would infer the backend from the column type
        backend = get_backend('pandas')  # Default to pandas for now
    
    # This is a placeholder implementation
    # In a full implementation, this would create backend-specific condition objects
    return f"is_valid_timestamp({column}, {timestamp_format})"


@register_rule("row")
def is_valid_ipv4_address(column: ColumnLike, backend: Optional[DataFrameBackend] = None) -> Any:
    """Checks whether the values in the input column have valid IPv4 address formats.

    :param column: column to check; can be a string column name or a column expression
    :param backend: backend to use, if None will be inferred
    :return: backend-specific object for condition
    """
    if backend is None:
        # In a full implementation, we would infer the backend from the column type
        backend = get_backend('pandas')  # Default to pandas for now
    
    # This is a placeholder implementation
    # In a full implementation, this would create backend-specific condition objects
    return f"is_valid_ipv4_address({column})"


@register_rule("row")
def is_ipv4_address_in_cidr(column: ColumnLike, cidr_block: str, 
                          backend: Optional[DataFrameBackend] = None) -> Any:
    """Checks if an IP column value falls within the given CIDR block.

    :param column: column to check; can be a string column name or a column expression
    :param cidr_block: CIDR block string (e.g., '192.168.1.0/24')
    :param backend: backend to use, if None will be inferred
    :raises ValueError: If cidr_block is not a valid string in CIDR notation.
    :return: backend-specific object for condition
    """
    if backend is None:
        # In a full implementation, we would infer the backend from the column type
        backend = get_backend('pandas')  # Default to pandas for now
    
    # This is a placeholder implementation
    # In a full implementation, this would create backend-specific condition objects
    return f"is_ipv4_address_in_cidr({column}, {cidr_block})"
