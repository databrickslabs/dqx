import datetime
import re
import warnings
import ipaddress
import uuid
from collections.abc import Callable, Sequence
from enum import Enum
from itertools import zip_longest
import operator as py_operator
from typing import Any
import pandas as pd  # type: ignore[import-untyped]
import pyspark.sql.functions as F
from pyspark.sql import types
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.window import Window
from databricks.labs.dqx.rule import register_rule
from databricks.labs.dqx.utils import (
    get_column_name_or_alias,
    is_sql_query_safe,
    normalize_col_str,
    get_columns_as_strings,
    to_lowercase,
)
from databricks.labs.dqx.errors import MissingParameterError, InvalidParameterError, UnsafeSqlQueryError

_IPV4_OCTET = r"(25[0-5]|2[0-4]\d|1\d{2}|[1-9]?\d)"
_IPV4_CIDR_SUFFIX = r"(3[0-2]|[12]?\d)"
IPV4_MAX_OCTET_COUNT = 4
IPV4_BIT_LENGTH = 32

# Curated aggregate functions for data quality checks
# These are univariate (single-column) aggregate functions suitable for DQ monitoring
# Maps function names to human-readable display names for error messages
CURATED_AGGR_FUNCTIONS = {
    "count": "Count",
    "sum": "Sum",
    "avg": "Average",
    "min": "Min",
    "max": "Max",
    "count_distinct": "Distinct count",
    "approx_count_distinct": "Approximate distinct count",
    "count_if": "Conditional count",
    "stddev": "Standard deviation",
    "stddev_pop": "Population standard deviation",
    "stddev_samp": "Sample standard deviation",
    "variance": "Variance",
    "var_pop": "Population variance",
    "var_samp": "Sample variance",
    "median": "Median",
    "mode": "Mode",
    "skewness": "Skewness",
    "kurtosis": "Kurtosis",
    "percentile": "Percentile",
    "approx_percentile": "Approximate percentile",
}

# Aggregate functions incompatible with Spark window functions
# These require two-stage aggregation (groupBy + join) instead of window functions when used with group_by
# Spark limitation: DISTINCT operations are not supported in window functions
WINDOW_INCOMPATIBLE_AGGREGATES = {
    "count_distinct",  # DISTINCT_WINDOW_FUNCTION_UNSUPPORTED error
    # Future: Add other aggregates that don't work with windows (e.g., collect_set with DISTINCT)
}


class DQPattern(Enum):
    """Enum class to represent DQ patterns used to match data in columns."""

    IPV4_ADDRESS = rf"^{_IPV4_OCTET}\.{_IPV4_OCTET}\.{_IPV4_OCTET}\.{_IPV4_OCTET}$"
    IPV4_CIDR_BLOCK = rf"{IPV4_ADDRESS[:-1]}/{_IPV4_CIDR_SUFFIX}$"


def make_condition(condition: Column, message: Column | str, alias: str) -> Column:
    """Helper function to create a condition column.

    Args:
        condition: condition expression.
            - Pass the check if the condition evaluates to False
            - Fail the check if condition evaluates to True
        message: message to output - it could be either *Column* object, or string constant
        alias: name for the resulting column

    Returns:
        an instance of *Column* type, that either returns string if condition is evaluated to *true*,
        or *null* if condition is evaluated to *false*
    """
    if isinstance(message, str):
        msg_col = F.lit(message)
    else:
        msg_col = message

    return (F.when(condition, msg_col).otherwise(F.lit(None).cast("string"))).alias(_cleanup_alias_name(alias))


def _matches_pattern(column: str | Column, pattern: DQPattern) -> Column:
    """Checks whether the values in the input column match a given pattern.

    Args:
        column: column to check; can be a string column name or a column expression
        pattern: pattern to match against

    Returns:
        Column object for condition
    """
    col_str_norm, col_expr_str, col_expr = get_normalized_column_and_expr(column)
    condition = _does_not_match_pattern(col_expr, pattern)
    final_condition = F.when(col_expr.isNotNull(), condition).otherwise(F.lit(None))

    condition_str = f"' in Column '{col_expr_str}' does not match pattern '{pattern.name}'"

    return make_condition(
        final_condition,
        F.concat_ws("", F.lit("Value '"), col_expr.cast("string"), F.lit(condition_str)),
        f"{col_str_norm}_does_not_match_pattern_{pattern.name.lower()}",
    )


@register_rule("row")
def is_not_null_and_not_empty(column: str | Column, trim_strings: bool | None = False) -> Column:
    """Checks whether the values in the input column are not null and not empty.

    Args:
        column: column to check; can be a string column name or a column expression
        trim_strings: boolean flag to trim spaces from strings

    Returns:
        Column object for condition
    """
    col_str_norm, col_expr_str, col_expr = get_normalized_column_and_expr(column)
    if trim_strings:
        col_expr = F.trim(col_expr).alias(col_str_norm)
    condition = col_expr.isNull() | (col_expr.cast("string").isNull() | (col_expr.cast("string") == F.lit("")))
    return make_condition(
        condition, f"Column '{col_expr_str}' value is null or empty", f"{col_str_norm}_is_null_or_empty"
    )


@register_rule("row")
def is_not_empty(column: str | Column) -> Column:
    """Checks whether the values in the input column are not empty (but may be null).

    Args:
        column: column to check; can be a string column name or a column expression

    Returns:
        Column object for condition
    """
    col_str_norm, col_expr_str, col_expr = get_normalized_column_and_expr(column)
    condition = col_expr.cast("string") == F.lit("")
    return make_condition(condition, f"Column '{col_expr_str}' value is empty", f"{col_str_norm}_is_empty")


@register_rule("row")
def is_not_null(column: str | Column) -> Column:
    """Checks whether the values in the input column are not null.

    Args:
        column: column to check; can be a string column name or a column expression

    Returns:
        Column object for condition
    """
    col_str_norm, col_expr_str, col_expr = get_normalized_column_and_expr(column)
    return make_condition(col_expr.isNull(), f"Column '{col_expr_str}' value is null", f"{col_str_norm}_is_null")


@register_rule("row")
def is_not_null_and_is_in_list(column: str | Column, allowed: list, case_sensitive: bool = True) -> Column:
    """Checks whether the values in the input column are not null and present in the list of allowed values.
    Can optionally perform a case-insensitive comparison.
    This check is not suited for `MapType` or `StructType` columns.

    Args:
        column: column to check; can be a string column name or a column expression
        allowed: list of allowed values (actual values or Column objects)
        case_sensitive: whether to perform a case-sensitive comparison (default: True)

    Returns:
        Column object for condition

    Raises:
        MissingParameterError: If the allowed list is not provided.
        InvalidParameterError: If the allowed parameter is not a list, or if the list is empty.
    """
    if allowed is None:
        raise MissingParameterError("allowed list is not provided.")
    if not isinstance(allowed, list):
        raise InvalidParameterError(f"allowed parameter must be a list, got {str(type(allowed))} instead.")
    if not allowed:
        raise InvalidParameterError("allowed list must not be empty.")

    allowed_cols = [item if isinstance(item, Column) else F.lit(item) for item in allowed]
    col_str_norm, col_expr_str, col_expr = get_normalized_column_and_expr(column)

    # Apply case-insensitive transformation if needed
    if not case_sensitive:
        has_arrays = any(isinstance(item, (list, tuple)) for item in allowed if not isinstance(item, Column))
        col_expr_compare = to_lowercase(col_expr, is_array=has_arrays)
        allowed_cols_compare = [
            to_lowercase(c, is_array=isinstance(allowed[i], (list, tuple))) for i, c in enumerate(allowed_cols)
        ]
    else:
        col_expr_compare, allowed_cols_compare = col_expr, allowed_cols

    condition = col_expr.isNull() | ~col_expr_compare.isin(*allowed_cols_compare)

    return make_condition(
        condition,
        F.concat_ws(
            "",
            F.lit("Value '"),
            F.when(col_expr.isNull(), F.lit("null")).otherwise(col_expr.cast("string")),
            F.lit(f"' in Column '{col_expr_str}' is null or not in the allowed list: ["),
            F.concat_ws(", ", *[c.cast("string") for c in allowed_cols]),
            F.lit("]"),
        ),
        f"{col_str_norm}_is_null_or_is_not_in_the_list",
    )


@register_rule("row")
def is_in_list(column: str | Column, allowed: list, case_sensitive: bool = True) -> Column:
    """Checks whether the values in the input column are present in the list of allowed values
    (null values are allowed). Can optionally perform a case-insensitive comparison.
    This check is not suited for `MapType` or `StructType` columns.

    Note:
        This check is not suited for `MapType` or `StructType` columns. For best performance with large
        lists, use the `foreign_key` check function.

    Args:
        column: column to check; can be a string column name or a column expression
        allowed: list of allowed values (actual values or Column objects)
        case_sensitive: whether to perform a case-sensitive comparison (default: True)

    Returns:
        Column object for condition

    Raises:
        MissingParameterError: If the allowed list is not provided.
        InvalidParameterError: If the allowed parameter is not a list.
    """
    if allowed is None:
        raise MissingParameterError("allowed list is not provided.")
    if not isinstance(allowed, list):
        raise InvalidParameterError(f"allowed parameter must be a list, got {str(type(allowed))} instead.")
    if not allowed:
        raise InvalidParameterError("allowed list must not be empty.")

    allowed_cols = [item if isinstance(item, Column) else F.lit(item) for item in allowed]
    col_str_norm, col_expr_str, col_expr = get_normalized_column_and_expr(column)

    # Apply case-insensitive transformation if needed
    if not case_sensitive:
        has_arrays = any(isinstance(item, (list, tuple)) for item in allowed if not isinstance(item, Column))
        col_expr_compare = to_lowercase(col_expr, is_array=has_arrays)
        allowed_cols_compare = [
            to_lowercase(c, is_array=isinstance(allowed[i], (list, tuple))) for i, c in enumerate(allowed_cols)
        ]
    else:
        col_expr_compare, allowed_cols_compare = col_expr, allowed_cols

    condition = ~col_expr_compare.isin(*allowed_cols_compare)

    return make_condition(
        condition,
        F.concat_ws(
            "",
            F.lit("Value '"),
            F.when(col_expr.isNull(), F.lit("null")).otherwise(col_expr.cast("string")),
            F.lit(f"' in Column '{col_expr_str}' is not in the allowed list: ["),
            F.concat_ws(", ", *[c.cast("string") for c in allowed_cols]),
            F.lit("]"),
        ),
        f"{col_str_norm}_is_not_in_the_list",
    )


@register_rule("row")
def is_not_in_list(column: str | Column, forbidden: list, case_sensitive: bool = True) -> Column:
    """Checks whether the values in the input column are NOT present in the list of forbidden values
    (null values are allowed). Can optionally perform a case-insensitive comparison.

    Note:
        This check is not suited for `MapType` or `StructType` columns. For best performance with large
        lists, use the `foreign_key` check function with the `negate` parameter set to `True`.

    Args:
        column: column to check; can be a string column name or a column expression
        forbidden: list of forbidden values (actual values or Column objects)
        case_sensitive: whether to perform a case-sensitive comparison (default: True)

    Returns:
        Column object for condition

    Raises:
        MissingParameterError: If the forbidden list is not provided.
        InvalidParameterError: If the forbidden parameter is not a list.
    """
    if forbidden is None:
        raise MissingParameterError("forbidden list is not provided.")
    if not isinstance(forbidden, list):
        raise InvalidParameterError(f"forbidden parameter must be a list, got {str(type(forbidden))} instead.")
    if not forbidden:
        raise InvalidParameterError("forbidden list must not be empty.")

    forbidden_cols = [item if isinstance(item, Column) else F.lit(item) for item in forbidden]
    col_str_norm, col_expr_str, col_expr = get_normalized_column_and_expr(column)

    # Apply case-insensitive transformation if needed
    if not case_sensitive:
        has_arrays = any(isinstance(item, (list, tuple)) for item in forbidden if not isinstance(item, Column))
        col_expr_compare = to_lowercase(col_expr, is_array=has_arrays)
        forbidden_cols_compare = [
            to_lowercase(c, is_array=isinstance(forbidden[i], (list, tuple))) for i, c in enumerate(forbidden_cols)
        ]
    else:
        col_expr_compare, forbidden_cols_compare = col_expr, forbidden_cols

    condition = col_expr_compare.isin(*forbidden_cols_compare)

    return make_condition(
        condition,
        F.concat_ws(
            "",
            F.lit("Value '"),
            F.when(col_expr.isNull(), F.lit("null")).otherwise(col_expr.cast("string")),
            F.lit(f"' in Column '{col_expr_str}' is in the forbidden list: ["),
            F.concat_ws(", ", *[c.cast("string") for c in forbidden_cols]),
            F.lit("]"),
        ),
        f"{col_str_norm}_is_in_the_forbidden_list",
    )


@register_rule("row")
def sql_expression(
    expression: str,
    msg: str | None = None,
    name: str | None = None,
    negate: bool = False,
    columns: list[str | Column] | None = None,
) -> Column:
    """Checks whether the condition provided as an SQL expression is met.

    Args:
        expression: SQL expression. Fail if expression evaluates to False, pass if it evaluates to True.
        msg: optional message of the *Column* type, automatically generated if None
        name: optional name of the resulting column, automatically generated if None
        negate: if the condition should be negated (true) or not. For example, "col is not null" will mark null
            values as "bad". Although sometimes it's easier to specify it other way around "col is null" + negate set to True
        columns: optional list of columns to be used for validation against the actual input DataFrame,
            reporting and for constructing name prefix if check name is not provided.

    Returns:
        new Column
    """
    expr_col = F.expr(expression)
    expr_msg = expression

    if negate:
        expr_msg = "~(" + expression + ")"
        message = F.concat_ws("", F.lit(f"Value is matching expression: {expr_msg}"))
    else:
        expr_col = ~expr_col
        message = F.concat_ws("", F.lit(f"Value is not matching expression: {expr_msg}"))

    if not name:
        name = get_column_name_or_alias(expr_col, normalize=True)
        if columns:
            name = normalize_col_str(
                "_".join([get_column_name_or_alias(col, normalize=True) for col in columns]) + "_" + name
            )

    return make_condition(expr_col, msg or message, name)


@register_rule("row")
def is_older_than_col2_for_n_days(
    column1: str | Column, column2: str | Column, days: int = 0, negate: bool = False
) -> Column:
    """Checks whether the values in one input column are at least N days older than the values in another column.

    Args:
        column1: first column to check; can be a string column name or a column expression
        column2: second column to check; can be a string column name or a column expression
        days: number of days
        negate: if the condition should be negated (true) or not; if negated, the check will fail when values in the
            first column are at least N days older than values in the second column

    Returns:
        new Column
    """
    col_str_norm1, col_expr_str1, col_expr1 = get_normalized_column_and_expr(column1)
    col_str_norm2, col_expr_str2, col_expr2 = get_normalized_column_and_expr(column2)

    col1_date = F.to_date(col_expr1)
    col2_date = F.to_date(col_expr2)
    condition = col1_date >= F.date_sub(col2_date, days)
    if negate:
        return make_condition(
            ~condition,
            F.concat_ws(
                "",
                F.lit("Value '"),
                col1_date.cast("string"),
                F.lit(f"' in Column '{col_expr_str1}' is less than Value '"),
                col2_date.cast("string"),
                F.lit(f"' in Column '{col_expr_str2}' for {days} or more days"),
            ),
            f"is_col_{col_str_norm1}_not_older_than_{col_str_norm2}_for_n_days",
        )

    return make_condition(
        condition,
        F.concat_ws(
            "",
            F.lit("Value '"),
            col1_date.cast("string"),
            F.lit(f"' in Column '{col_expr_str1}' is not less than Value '"),
            col2_date.cast("string"),
            F.lit(f"' in Column '{col_expr_str2}' for more than {days} days"),
        ),
        f"is_col_{col_str_norm1}_older_than_{col_str_norm2}_for_n_days",
    )


@register_rule("row")
def is_older_than_n_days(
    column: str | Column, days: int, curr_date: Column | None = None, negate: bool = False
) -> Column:
    """Checks whether the values in the input column are at least N days older than the current date.

    Args:
        column: column to check; can be a string column name or a column expression
        days: number of days
        curr_date: (optional) set current date
        negate: if the condition should be negated (true) or not; if negated, the check will fail when values in the
            first column are at least N days older than values in the second column

    Returns:
        new Column
    """
    col_str_norm, col_expr_str, col_expr = get_normalized_column_and_expr(column)
    if curr_date is None:
        curr_date = F.current_date()

    col_date = F.to_date(col_expr)
    condition = col_date >= F.date_sub(curr_date, days)

    if negate:
        return make_condition(
            ~condition,
            F.concat_ws(
                "",
                F.lit("Value '"),
                col_date.cast("string"),
                F.lit(f"' in Column '{col_expr_str}' is less than current date '"),
                curr_date.cast("string"),
                F.lit(f"' for {days} or more days"),
            ),
            f"is_col_{col_str_norm}_not_older_than_n_days",
        )

    return make_condition(
        condition,
        F.concat_ws(
            "",
            F.lit("Value '"),
            col_date.cast("string"),
            F.lit(f"' in Column '{col_expr_str}' is not less than current date '"),
            curr_date.cast("string"),
            F.lit(f"' for more than {days} days"),
        ),
        f"is_col_{col_str_norm}_older_than_n_days",
    )


@register_rule("row")
def is_not_in_future(column: str | Column, offset: int = 0, curr_timestamp: Column | None = None) -> Column:
    """Checks whether the values in the input column contain a timestamp that is not in the future,
    where 'future' is defined as current_timestamp + offset (in seconds).

    Args:
        column: column to check; can be a string column name or a column expression
        offset: offset (in seconds) to add to the current timestamp at time of execution
        curr_timestamp: (optional) set current timestamp

    Returns:
        new Column
    """
    col_str_norm, col_expr_str, col_expr = get_normalized_column_and_expr(column)
    if curr_timestamp is None:
        curr_timestamp = F.current_timestamp()

    timestamp_offset = F.from_unixtime(F.unix_timestamp(curr_timestamp) + offset)
    condition = col_expr > timestamp_offset

    return make_condition(
        condition,
        F.concat_ws(
            "",
            F.lit("Value '"),
            col_expr.cast("string"),
            F.lit(f"' in Column '{col_expr_str}' is greater than time '"),
            timestamp_offset,
            F.lit("'"),
        ),
        f"{col_str_norm}_in_future",
    )


@register_rule("row")
def is_not_in_near_future(column: str | Column, offset: int = 0, curr_timestamp: Column | None = None) -> Column:
    """Checks whether the values in the input column contain a timestamp that is not in the near future,
    where 'near future' is defined as greater than the current timestamp
    but less than the current_timestamp + offset (in seconds).

    Args:
        column: column to check; can be a string column name or a column expression
        offset: offset (in seconds) to add to the current timestamp at time of execution
        curr_timestamp: (optional) set current timestamp

    Returns:
        new Column
    """
    col_str_norm, col_expr_str, col_expr = get_normalized_column_and_expr(column)
    if curr_timestamp is None:
        curr_timestamp = F.current_timestamp()

    near_future = F.from_unixtime(F.unix_timestamp(curr_timestamp) + offset)
    condition = (col_expr > curr_timestamp) & (col_expr < near_future)

    return make_condition(
        condition,
        F.concat_ws(
            "",
            F.lit("Value '"),
            col_expr.cast("string"),
            F.lit(f"' in Column '{col_expr_str}' is greater than '"),
            curr_timestamp.cast("string"),
            F.lit(" and smaller than '"),
            near_future.cast("string"),
            F.lit("'"),
        ),
        f"{col_str_norm}_in_near_future",
    )


@register_rule("row")
def is_equal_to(
    column: str | Column, value: int | float | str | datetime.date | datetime.datetime | Column | None = None
) -> Column:
    """Check whether the values in the input column are equal to the given value.

    Args:
        column (str | Column): Column to check. Can be a string column name or a column expression.
        value (int | float | str | datetime.date | datetime.datetime | Column | None, optional):
            The value to compare with. Can be a literal or a Spark Column. Defaults to None.

    Returns:
        Column: A Spark Column condition that fails if the column value is not equal to the given value.
    """
    col_str_norm, col_expr_str, col_expr = get_normalized_column_and_expr(column)
    value_expr = get_limit_expr(value)
    condition = col_expr != value_expr

    return make_condition(
        condition,
        F.concat_ws(
            "",
            F.lit("Value '"),
            col_expr.cast("string"),
            F.lit(f"' in Column '{col_expr_str}' is not equal to value: "),
            value_expr.cast("string"),
        ),
        f"{col_str_norm}_not_equal_to_value",
    )


@register_rule("row")
def is_not_equal_to(
    column: str | Column, value: int | float | str | datetime.date | datetime.datetime | Column | None = None
) -> Column:
    """Check whether the values in the input column are not equal to the given value.

    Args:
        column (str | Column): Column to check. Can be a string column name or a column expression.
        value (int | float | str | datetime.date | datetime.datetime | Column | None, optional):
            The value to compare with. Can be a literal or a Spark Column. Defaults to None.

    Returns:
        Column: A Spark Column condition that fails if the column value is equal to the given value.
    """
    col_str_norm, col_expr_str, col_expr = get_normalized_column_and_expr(column)
    value_expr = get_limit_expr(value)
    condition = col_expr == value_expr

    return make_condition(
        condition,
        F.concat_ws(
            "",
            F.lit("Value '"),
            col_expr.cast("string"),
            F.lit(f"' in Column '{col_expr_str}' is equal to value: "),
            value_expr.cast("string"),
        ),
        f"{col_str_norm}_equal_to_value",
    )


@register_rule("row")
def is_not_less_than(
    column: str | Column, limit: int | float | datetime.date | datetime.datetime | str | Column | None = None
) -> Column:
    """Checks whether the values in the input column are not less than the provided limit.

    Args:
        column: column to check; can be a string column name or a column expression
        limit: limit to use in the condition as number, date, timestamp, column name or sql expression

    Returns:
        new Column
    """
    col_str_norm, col_expr_str, col_expr = get_normalized_column_and_expr(column)
    limit_expr = get_limit_expr(limit)
    condition = col_expr < limit_expr

    return make_condition(
        condition,
        F.concat_ws(
            "",
            F.lit("Value '"),
            col_expr.cast("string"),
            F.lit(f"' in Column '{col_expr_str}' is less than limit: "),
            limit_expr.cast("string"),
        ),
        f"{col_str_norm}_less_than_limit",
    )


@register_rule("row")
def is_not_greater_than(
    column: str | Column, limit: int | float | datetime.date | datetime.datetime | str | Column | None = None
) -> Column:
    """Checks whether the values in the input column are not greater than the provided limit.

    Args:
        column: column to check; can be a string column name or a column expression
        limit: limit to use in the condition as number, date, timestamp, column name or sql expression

    Returns:
        new Column
    """
    col_str_norm, col_expr_str, col_expr = get_normalized_column_and_expr(column)
    limit_expr = get_limit_expr(limit)
    condition = col_expr > limit_expr

    return make_condition(
        condition,
        F.concat_ws(
            "",
            F.lit("Value '"),
            col_expr.cast("string"),
            F.lit(f"' in Column '{col_expr_str}' is greater than limit: "),
            limit_expr.cast("string"),
        ),
        f"{col_str_norm}_greater_than_limit",
    )


@register_rule("row")
def is_in_range(
    column: str | Column,
    min_limit: int | float | datetime.date | datetime.datetime | str | Column | None = None,
    max_limit: int | float | datetime.date | datetime.datetime | str | Column | None = None,
) -> Column:
    """Checks whether the values in the input column are in the provided limits (inclusive of both boundaries).

    Args:
        column: column to check; can be a string column name or a column expression
        min_limit: min limit to use in the condition as number, date, timestamp, column name or sql expression
        max_limit: max limit to use in the condition as number, date, timestamp, column name or sql expression

    Returns:
        new Column
    """
    col_str_norm, col_expr_str, col_expr = get_normalized_column_and_expr(column)
    min_limit_expr = get_limit_expr(min_limit)
    max_limit_expr = get_limit_expr(max_limit)

    condition = (col_expr < min_limit_expr) | (col_expr > max_limit_expr)

    return make_condition(
        condition,
        F.concat_ws(
            "",
            F.lit("Value '"),
            col_expr.cast("string"),
            F.lit(f"' in Column '{col_expr_str}' not in range: ["),
            min_limit_expr.cast("string"),
            F.lit(", "),
            max_limit_expr.cast("string"),
            F.lit("]"),
        ),
        f"{col_str_norm}_not_in_range",
    )


@register_rule("row")
def is_not_in_range(
    column: str | Column,
    min_limit: int | float | datetime.date | datetime.datetime | str | Column | None = None,
    max_limit: int | float | datetime.date | datetime.datetime | str | Column | None = None,
) -> Column:
    """Checks whether the values in the input column are outside the provided limits (inclusive of both boundaries).

    Args:
        column: column to check; can be a string column name or a column expression
        min_limit: min limit to use in the condition as number, date, timestamp, column name or sql expression
        max_limit: max limit to use in the condition as number, date, timestamp, column name or sql expression

    Returns:
        new Column
    """
    col_str_norm, col_expr_str, col_expr = get_normalized_column_and_expr(column)
    min_limit_expr = get_limit_expr(min_limit)
    max_limit_expr = get_limit_expr(max_limit)

    condition = (col_expr >= min_limit_expr) & (col_expr <= max_limit_expr)

    return make_condition(
        condition,
        F.concat_ws(
            "",
            F.lit("Value '"),
            col_expr.cast("string"),
            F.lit(f"' in Column '{col_expr_str}' in range: ["),
            min_limit_expr.cast("string"),
            F.lit(", "),
            max_limit_expr.cast("string"),
            F.lit("]"),
        ),
        f"{col_str_norm}_in_range",
    )


@register_rule("row")
def regex_match(column: str | Column, regex: str, negate: bool = False) -> Column:
    """Checks whether the values in the input column matches a given regex.

    Args:
        column: column to check; can be a string column name or a column expression
        regex: regex to check
        negate: if the condition should be negated (true) or not

    Returns:
        Column object for condition
    """
    col_str_norm, col_expr_str, col_expr = get_normalized_column_and_expr(column)
    if negate:
        condition = col_expr.rlike(regex)
        return make_condition(condition, f"Column '{col_expr_str}' is matching regex", f"{col_str_norm}_matching_regex")

    condition = ~col_expr.rlike(regex)
    return make_condition(
        condition, f"Column '{col_expr_str}' is not matching regex", f"{col_str_norm}_not_matching_regex"
    )


@register_rule("row")
def is_not_null_and_not_empty_array(column: str | Column) -> Column:
    """Checks whether the values in the array input column are not null and not empty.

    Args:
        column: column to check; can be a string column name or a column expression

    Returns:
        Column object for condition
    """
    col_str_norm, col_expr_str, col_expr = get_normalized_column_and_expr(column)
    condition = col_expr.isNull() | (F.size(col_expr) == 0)
    return make_condition(
        condition, f"Column '{col_expr_str}' is null or empty array", f"{col_str_norm}_is_null_or_empty_array"
    )


@register_rule("row")
def is_valid_date(column: str | Column, date_format: str | None = None) -> Column:
    """Checks whether the values in the input column have valid date formats.

    Args:
        column: column to check; can be a string column name or a column expression
        date_format: date format (e.g. 'yyyy-mm-dd')

    Returns:
        Column object for condition
    """
    col_str_norm, col_expr_str, col_expr = get_normalized_column_and_expr(column)
    date_col = F.try_to_timestamp(col_expr) if date_format is None else F.try_to_timestamp(col_expr, F.lit(date_format))
    condition = F.when(col_expr.isNull(), F.lit(None)).otherwise(date_col.isNull())
    condition_str = f"' in Column '{col_expr_str}' is not a valid date"
    if date_format is not None:
        condition_str += f" with format '{date_format}'"
    return make_condition(
        condition,
        F.concat_ws("", F.lit("Value '"), col_expr.cast("string"), F.lit(condition_str)),
        f"{col_str_norm}_is_not_valid_date",
    )


@register_rule("row")
def is_valid_timestamp(column: str | Column, timestamp_format: str | None = None) -> Column:
    """Checks whether the values in the input column have valid timestamp formats.

    Args:
        column: column to check; can be a string column name or a column expression
        timestamp_format: timestamp format (e.g. 'yyyy-mm-dd HH:mm:ss')

    Returns:
        Column object for condition
    """
    col_str_norm, col_expr_str, col_expr = get_normalized_column_and_expr(column)
    ts_col = (
        F.try_to_timestamp(col_expr)
        if timestamp_format is None
        else F.try_to_timestamp(col_expr, F.lit(timestamp_format))
    )
    condition = F.when(col_expr.isNull(), F.lit(None)).otherwise(ts_col.isNull())
    condition_str = f"' in Column '{col_expr_str}' is not a valid timestamp"
    if timestamp_format is not None:
        condition_str += f" with format '{timestamp_format}'"
    return make_condition(
        condition,
        F.concat_ws("", F.lit("Value '"), col_expr.cast("string"), F.lit(condition_str)),
        f"{col_str_norm}_is_not_valid_timestamp",
    )


@register_rule("row")
def is_valid_ipv4_address(column: str | Column) -> Column:
    """Checks whether the values in the input column have valid IPv4 address formats.

    Args:
        column: column to check; can be a string column name or a column expression

    Returns:
        Column object for condition
    """
    return _matches_pattern(column, DQPattern.IPV4_ADDRESS)


@register_rule("row")
def is_ipv4_address_in_cidr(column: str | Column, cidr_block: str) -> Column:
    """
    Checks if an IPv4 column value falls within the given CIDR block.

    Args:
        column: column to check; can be a string column name or a column expression
        cidr_block: CIDR block string (e.g., '192.168.1.0/24')

    Returns:
        Column object for condition

    Raises:
        MissingParameterError: if *cidr_block* is None.
        InvalidParameterError: if *cidr_block* is an empty string.
        InvalidParameterError: if *cidr_block* is provided but not in valid IPv4 CIDR notation.
    """
    if cidr_block is None:
        raise MissingParameterError("'cidr_block' is not provided.")

    if not isinstance(cidr_block, str):
        raise InvalidParameterError(f"'cidr_block' must be a string, got {type(cidr_block)} instead.")

    if not cidr_block:
        raise InvalidParameterError("'cidr_block' must be a non-empty string.")

    if not re.match(DQPattern.IPV4_CIDR_BLOCK.value, cidr_block):
        raise InvalidParameterError(f"CIDR block '{cidr_block}' is not a valid IPv4 CIDR block.")

    col_str_norm, col_expr_str, col_expr = get_normalized_column_and_expr(column)
    cidr_col_expr = F.lit(cidr_block)
    ipv4_msg_col = is_valid_ipv4_address(column)

    ip_bits_col = _convert_ipv4_to_bits(col_expr)
    cidr_ip_bits_col, cidr_prefix_length_col = _convert_ipv4_cidr_to_bits_and_prefix(cidr_col_expr)
    ip_net = _get_network_address(ip_bits_col, cidr_prefix_length_col, IPV4_BIT_LENGTH)
    cidr_net = _get_network_address(cidr_ip_bits_col, cidr_prefix_length_col, IPV4_BIT_LENGTH)

    cidr_msg = F.concat_ws(
        "",
        F.lit("Value '"),
        col_expr.cast("string"),
        F.lit(f"' in Column '{col_expr_str}' is not in the CIDR block '{cidr_block}'"),
    )
    return make_condition(
        condition=ipv4_msg_col.isNotNull() | (ip_net != cidr_net),
        message=F.when(ipv4_msg_col.isNotNull(), ipv4_msg_col).otherwise(cidr_msg),
        alias=f"{col_str_norm}_is_not_ipv4_in_cidr",
    )


@register_rule("row")
def is_valid_ipv6_address(column: str | Column) -> Column:
    """
    Validate if the column contains properly formatted IPv6 addresses.

    Args:
        column: The column to check; can be a string column name or a Column expression.

    Returns:
        Column object for condition indicating whether a value is a valid IPv6 address.
    """
    warnings.warn(
        "IPv6 Address validation uses pandas user-defined functions which may degrade performance. "
        "Sample or limit large datasets when running IPV6 address validation.",
        UserWarning,
    )

    col_str_norm, col_expr_str, col_expr = get_normalized_column_and_expr(column)

    is_valid_ipv6_address_udf = _build_is_valid_ipv6_address_udf()
    ipv6_match_condition = is_valid_ipv6_address_udf(col_expr)
    final_condition = F.when(col_expr.isNotNull(), ~ipv6_match_condition).otherwise(F.lit(None))
    condition_str = f"' in Column '{col_expr_str}' does not match pattern 'IPV6_ADDRESS'"

    return make_condition(
        final_condition,
        F.concat_ws("", F.lit("Value '"), col_expr.cast("string"), F.lit(condition_str)),
        f"{col_str_norm}_does_not_match_pattern_ipv6_address",
    )


@register_rule("row")
def is_ipv6_address_in_cidr(column: str | Column, cidr_block: str) -> Column:
    """
    Fail if IPv6 is invalid OR (valid AND not in CIDR). Null for null inputs.

    Args:
        column: The column to check; can be a string column name or a Column expression.
        cidr_block: The CIDR block to check against.

    Returns:
        Column: A Column expression indicating whether each value is not a valid IPv6 address or not in the CIDR block.

    Raises:
        MissingParameterError: If *cidr_block* is None.
        InvalidParameterError: If *cidr_block* is an empty string.
        InvalidParameterError: if *cidr_block* is provided but not in valid IPv6 CIDR notation.
    """
    warnings.warn(
        "Checking if an IPv6 Address is in CIDR block uses pandas user-defined functions "
        "which may degrade performance. Sample or limit large datasets when running IPv6 validation.",
        UserWarning,
    )

    if cidr_block is None:
        raise MissingParameterError("'cidr_block' is not provided.")

    if not isinstance(cidr_block, str):
        raise InvalidParameterError(f"'cidr_block' must be a string, got {type(cidr_block)} instead.")

    if not cidr_block:
        raise InvalidParameterError("'cidr_block' must be a non-empty string.")

    if not _is_valid_ipv6_cidr_block(cidr_block):
        raise InvalidParameterError(f"CIDR block '{cidr_block}' is not a valid IPv6 CIDR block.")

    col_str_norm, col_expr_str, col_expr = get_normalized_column_and_expr(column)
    cidr_lit = F.lit(cidr_block)
    ipv6_msg_col = is_valid_ipv6_address(column)
    is_valid_ipv6 = ipv6_msg_col.isNull()
    in_cidr = _build_is_ipv6_address_in_cidr_udf()(col_expr, cidr_lit)
    condition = F.when(col_expr.isNull(), F.lit(None)).otherwise(
        F.when(~is_valid_ipv6, F.lit(True)).otherwise(~in_cidr)
    )
    cidr_msg = F.concat_ws(
        "",
        F.lit("Value '"),
        col_expr.cast("string"),
        F.lit("' in Column '"),
        F.lit(col_expr_str),
        F.lit(f"' is not in the CIDR block '{cidr_block}'"),
    )
    message = F.when(~is_valid_ipv6, ipv6_msg_col).otherwise(cidr_msg)

    return make_condition(
        condition=condition,
        message=message,
        alias=f"{col_str_norm}_is_not_ipv6_in_cidr",
    )


@register_rule("row")
def is_data_fresh(
    column: str | Column,
    max_age_minutes: int,
    base_timestamp: str | datetime.date | datetime.datetime | Column | None = None,
) -> Column:
    """Checks whether the values in the timestamp column are not older than the specified number of minutes from the base timestamp column.

    This is useful for identifying stale data due to delayed pipelines and helps catch upstream issues early.

    Args:
        column: column to check; can be a string column name or a column expression containing timestamp values
        max_age_minutes: maximum age in minutes before data is considered stale
        base_timestamp: (optional) set base timestamp column from which the stale check is calculated, if not provided uses current_timestamp()

    Returns:
        Column object for condition
    """
    col_str_norm, col_expr_str, col_expr = get_normalized_column_and_expr(column)
    if base_timestamp is None:
        base_timestamp = F.current_timestamp()
    base_timestamp_col_expr = get_limit_expr(base_timestamp)
    # Calculate the threshold timestamp (base time - max_age_minutes)
    threshold_timestamp = base_timestamp_col_expr - F.expr(f"INTERVAL {max_age_minutes} MINUTES")

    # Check if the timestamp is older than the threshold (stale)
    condition = col_expr < threshold_timestamp

    return make_condition(
        condition,
        F.concat_ws(
            "",
            F.lit("Value '"),
            col_expr.cast("string"),
            F.lit(f"' in Column '{col_expr_str}' is older than {max_age_minutes} minutes from base timestamp '"),
            base_timestamp_col_expr.cast("string"),
            F.lit("'"),
        ),
        f"{col_str_norm}_is_data_fresh",
    )


@register_rule("dataset")
def has_no_outliers(column: str | Column, row_filter: str | None = None) -> tuple[Column, Callable]:
    """
    Build an outlier check condition and closure for dataset-level validation.

    This function uses a statistical method called MAD (Median Absolute Deviation) to check whether
    the specified column's values are within the calculated limits. The lower limit is calculated as
    median - 3.5 * MAD and the upper limit as median + 3.5 * MAD. Values outside these limits are considered outliers.


    Args:
        column: column to check; can be a string column name or a column expression
        row_filter: Optional SQL expression for filtering rows before checking for outliers.


    Returns:
        A tuple of:
            - A Spark Column representing the condition for outliers violations.
            - A closure that applies the outliers check and adds the necessary condition/count columns.
    """
    column = F.col(column) if isinstance(column, str) else column

    col_str_norm, col_expr_str, col_expr = get_normalized_column_and_expr(column)

    unique_str = uuid.uuid4().hex  # make sure any column added to the dataframe is unique
    condition_col = f"__condition_{col_str_norm}_{unique_str}"

    def apply(df: DataFrame) -> DataFrame:
        """
        Apply the outlier detection logic to the DataFrame.

        Adds columns indicating the median and MAD for the column.

        Args:
            df: The input DataFrame to validate for outliers.

        Returns:
            The DataFrame with additional median and MAD columns for outlier detection.
        """
        column_type = df.schema[col_expr_str].dataType
        if not isinstance(column_type, (types.NumericType)):
            raise InvalidParameterError(
                f"Column '{col_expr_str}' must be of numeric type to perform outlier detection using MAD method, "
                f"but got type '{column_type.simpleString()}' instead."
            )
        filter_condition = F.expr(row_filter) if row_filter else F.lit(True)
        median, mad = _calculate_median_absolute_deviation(df, col_expr_str, row_filter)
        if median is not None and mad is not None:
            median = float(median)
            mad = float(mad)
            # Create outlier condition
            lower_bound = median - (3.5 * mad)
            upper_bound = median + (3.5 * mad)
            lower_bound_expr = get_limit_expr(lower_bound)
            upper_bound_expr = get_limit_expr(upper_bound)

            condition = (col_expr < (lower_bound_expr)) | (col_expr > (upper_bound_expr))

            # Add outlier detection columns
            result_df = df.withColumn(condition_col, F.when(filter_condition & condition, True).otherwise(False))
        else:
            # If median or mad could not be calculated, no outliers can be detected
            result_df = df.withColumn(condition_col, F.lit(False))

        return result_df

    condition = make_condition(
        condition=F.col(condition_col),
        message=F.concat_ws(
            "",
            F.lit("Value '"),
            col_expr.cast("string"),
            F.lit(f"' in Column '{col_expr_str}' is an outlier as per MAD."),
        ),
        alias=f"{col_str_norm}_has_outliers",
    )
    return condition, apply


@register_rule("dataset")
def is_unique(
    columns: list[str | Column],
    nulls_distinct: bool = True,
    row_filter: str | None = None,
) -> tuple[Column, Callable]:
    """
    Build a uniqueness check condition and closure for dataset-level validation.

    This function checks whether the specified columns contain unique values within the dataset
    and reports rows with duplicate combinations. When *nulls_distinct*
    is True (default), rows with NULLs are treated as distinct (SQL ANSI behavior); otherwise,
    NULLs are treated as equal when checking for duplicates.

    In streaming, uniqueness is validated within individual micro-batches only.

    Args:
        columns: List of column names (str) or Spark Column expressions to validate for uniqueness.
        nulls_distinct: Whether NULLs are treated as distinct (default: True).
        row_filter: Optional SQL expression for filtering rows before checking uniqueness.
            Auto-injected from the check filter.

    Returns:
        A tuple of:
            - A Spark Column representing the condition for uniqueness violations.
            - A closure that applies the uniqueness check and adds the necessary condition/count columns.
    """
    if len(columns) == 1:
        single_key = columns[0]
        column = F.col(single_key) if isinstance(single_key, str) else single_key
    else:  # composite key
        column = F.struct(*[F.col(col) if isinstance(col, str) else col for col in columns])

    col_str_norm, col_expr_str, col_expr = get_normalized_column_and_expr(column)

    unique_str = uuid.uuid4().hex  # make sure any column added to the dataframe is unique
    condition_col = f"__condition_{col_str_norm}_{unique_str}"
    count_col = f"__count_{col_str_norm}_{unique_str}"

    def apply(df: DataFrame) -> DataFrame:
        """
        Apply the uniqueness check logic to the DataFrame.

        Adds columns indicating whether the row violates uniqueness, and how many duplicates exist.
        The condition is applied during check evaluation to flag duplicates.

        Args:
            df: The input DataFrame to validate for uniqueness.

        Returns:
            The DataFrame with additional condition and count columns for uniqueness validation.
        """
        window_count_col = f"__window_count_{col_str_norm}_{unique_str}"

        w = Window.partitionBy(col_expr)

        filter_condition = F.lit(True)
        if row_filter:
            filter_condition = filter_condition & F.expr(row_filter)

        if nulls_distinct:
            # All columns must be non-null
            for col in columns:
                col_ref = F.col(col) if isinstance(col, str) else col
                filter_condition = filter_condition & col_ref.isNotNull()

        # Conditionally count only matching rows within the window
        df = df.withColumn(window_count_col, F.sum(F.when(filter_condition, F.lit(1)).otherwise(F.lit(0))).over(w))

        df = (
            # Add condition column used in make_condition
            df.withColumn(condition_col, F.col(window_count_col) > 1)
            .withColumn(count_col, F.coalesce(F.col(window_count_col), F.lit(0)))
            .drop(window_count_col)
        )

        return df

    condition = make_condition(
        condition=F.col(condition_col),
        message=F.concat_ws(
            "",
            F.lit("Value '"),
            (
                col_expr.cast("string")
                if nulls_distinct
                else F.when(col_expr.isNull(), F.lit("null")).otherwise(col_expr.cast("string"))
            ),
            F.lit(f"' in column '{col_expr_str}' is not unique, found "),
            F.col(count_col).cast("string"),
            F.lit(" duplicates"),
        ),
        alias=f"{col_str_norm}_is_not_unique",
    )

    return condition, apply


@register_rule("dataset")
def foreign_key(
    columns: list[str | Column],
    ref_columns: list[str | Column],
    ref_df_name: str | None = None,  # must provide reference DataFrame name
    ref_table: str | None = None,  # or reference table name
    negate: bool = False,
    row_filter: str | None = None,
) -> tuple[Column, Callable]:
    """
    Build a foreign key check condition and closure for dataset-level validation.

    This function verifies that values in the specified foreign key columns exist (or don't exist, if *negate=True*) in
    the corresponding reference columns of another DataFrame or table. Rows where
    foreign key values do not match the reference are reported as violations.

    NULL values in the foreign key columns are ignored (SQL ANSI behavior).

    Args:
        columns: List of column names (str) or Column expressions in the dataset (foreign key).
        ref_columns: List of column names (str) or Column expressions in the reference dataset.
        ref_df_name: Name of the reference DataFrame (used when passing DataFrames directly).
        ref_table: Name of the reference table (used when reading from catalog).
        row_filter: Optional SQL expression for filtering rows before checking the foreign key.
            Auto-injected from the check filter.
        negate: If True, the condition is negated (i.e., the check fails when the foreign key values exist in the
            reference DataFrame/Table). If False, the check fails when the foreign key values do not exist in the reference.

    Returns:
        A tuple of:
            - A Spark Column representing the condition for foreign key violations.
            - A closure that applies the foreign key validation by joining against the reference.

    Raises:
        MissingParameterError:
            - if neither *ref_df_name* nor *ref_table* is provided.
        InvalidParameterError:
            - if both *ref_df_name* and *ref_table* are provided.
            - if the number of *columns* and *ref_columns* do not match.
            - if *ref_df_name* is not found in the provided *ref_dfs* dictionary.
    """
    _validate_ref_params(columns, ref_columns, ref_df_name, ref_table)

    not_null_condition = F.lit(True)
    if len(columns) == 1:
        column = columns[0]
        ref_column = ref_columns[0]
    else:
        column, ref_column, not_null_condition = _handle_fk_composite_keys(columns, ref_columns, not_null_condition)

    col_str_norm, col_expr_str, col_expr = get_normalized_column_and_expr(column)
    ref_col_str_norm, ref_col_expr_str, ref_col_expr = get_normalized_column_and_expr(ref_column)
    unique_str = uuid.uuid4().hex  # make sure any column added to the dataframe is unique
    condition_col = f"__{col_str_norm}_{unique_str}"

    def apply(df: DataFrame, spark: SparkSession, ref_dfs: dict[str, DataFrame]) -> DataFrame:
        """
        Apply the foreign key check logic to the DataFrame.

        Joins the dataset with the reference DataFrame or table to verify the foreign key values.
        Adds a condition column indicating whether each row violates the foreign key constraint.

        Args:
            df: The input DataFrame to validate.
            spark: SparkSession used if reading a reference table.
            ref_dfs: Dictionary of reference DataFrames (by name), used for joins.

        Returns:
            The DataFrame with an additional condition column for foreign key validation.
        """
        ref_df = _get_ref_df(ref_df_name, ref_table, ref_dfs, spark)

        ref_alias = f"__ref_{col_str_norm}_{unique_str}"
        ref_df_distinct = ref_df.select(ref_col_expr.alias(ref_alias)).distinct()

        filter_expr = F.expr(row_filter) if row_filter else F.lit(True)

        joined = df.join(
            ref_df_distinct, on=(col_expr == F.col(ref_alias)) & col_expr.isNotNull() & filter_expr, how="left"
        )

        base_condition = not_null_condition & col_expr.isNotNull()
        match_failed = F.col(ref_alias).isNull()
        match_succeeded = F.col(ref_alias).isNotNull()
        violation_condition = base_condition & (match_succeeded if negate else match_failed)

        # FK violation: no match found for non-null FK values if negate=False, opposite if negate=True
        # Add condition column used in make_condition
        result_df = joined.withColumn(condition_col, violation_condition)

        return result_df

    op_name = "exists_in" if negate else "not_exists_in"

    condition = make_condition(
        condition=F.col(condition_col),
        message=F.concat_ws(
            "",
            F.lit("Value '"),
            col_expr.cast("string"),
            F.lit("' in column '"),
            F.lit(col_expr_str),
            F.lit(f"' {'' if negate else 'not '}found in reference column '"),
            F.lit(ref_col_expr_str),
            F.lit("'"),
        ),
        alias=f"{col_str_norm}_{op_name}_ref_{ref_col_str_norm}",
    )

    return condition, apply


@register_rule("dataset")
def sql_query(
    query: str,
    merge_columns: list[str] | None = None,
    msg: str | None = None,
    name: str | None = None,
    negate: bool = False,
    condition_column: str = "condition",
    input_placeholder: str = "input_view",
    row_filter: str | None = None,
) -> tuple[Column, Callable]:
    """
    Checks whether the condition column generated by SQL query is met.

    Supports two modes:
    - Row-level validation (merge_columns provided): Query results are joined back to specific rows
    - Dataset-level validation (merge_columns omitted or None): All rows get the same check result

    Use dataset-level for aggregate validations like "total count > 100" or "avg(amount) < 1000".
    Use row-level when you need to identify specific problematic rows.

    Args:
        query: SQL query that must return as a minimum a condition column and
            all merge columns (if provided). When merge_columns are provided, the resulting DataFrame is
            automatically joined back to the input DataFrame. When merge_columns are not provided, the check
            applies to all rows (either all pass or all fail), making it useful for dataset-level validation
            with custom_metrics. Reference DataFrames when provided in the ref_dfs parameter are registered as temp view.
        merge_columns: Optional (can be None or omitted). List of columns to join results back to input DataFrame.
            - If provided: Row-level validation - different rows can have different results
            - If None/omitted: Dataset-level validation - all rows get same result
            When provided, columns must form a unique key to avoid duplicate records.
        condition_column: Column name indicating violation (boolean). Fail the check if True, pass it if False
        msg: Optional custom message or Column expression.
        name: Optional name for the result.
        negate: If True, the condition is negated (i.e., the check fails when the condition is False).
        input_placeholder: Name to be used in the sql query as `{{ input_placeholder }}` to refer to the
            input DataFrame on which the checks are applied.
        row_filter: Optional SQL expression for filtering rows before checking the foreign key.
            Auto-injected from the check filter.

    Returns:
        Tuple (condition column, apply function).

    Raises:
        UnsafeSqlQueryError: if the SQL query fails the safety check (e.g., contains disallowed operations).
    """
    _validate_sql_query_params(query, merge_columns)

    # Normalize empty list to None (both mean "no merge columns" / dataset-level check)
    if merge_columns is not None and not merge_columns:
        merge_columns = None

    alias_name = (
        name
        if name
        else (
            "_".join(merge_columns) + f"_query_{condition_column}_violation"
            if merge_columns
            else f"query_{condition_column}_violation"
        )
    )

    unique_str = uuid.uuid4().hex  # make sure any column added to the dataframe is unique
    unique_condition_column = f"{alias_name}_{condition_column}_{unique_str}"
    unique_input_view = f"{alias_name}_{input_placeholder}_{unique_str}"

    def _replace_template(sql: str, replacements: dict[str, str]) -> str:
        """
        Replace {{ template }} placeholders in sql with actual names, allowing for whitespace between braces.
        """
        for key, val in replacements.items():
            pattern = r"\{\{\s*" + re.escape(key) + r"\s*\}\}"
            sql = re.sub(pattern, val, sql)
        return sql

    def apply(df: DataFrame, spark: SparkSession, ref_dfs: dict[str, DataFrame]) -> DataFrame:
        filtered_df = df
        if row_filter:
            filtered_df = df.filter(F.expr(row_filter))

        # since the check could be applied multiple times, the views created here must be unique
        filtered_df.createOrReplaceTempView(unique_input_view)
        replacements = {input_placeholder: unique_input_view}

        for ref_name, ref_df in (ref_dfs or {}).items():
            ref_name_unique = f"{ref_name}_{unique_str}"
            ref_df.createOrReplaceTempView(ref_name_unique)
            replacements[ref_name] = ref_name_unique

        query_resolved = _replace_template(query, replacements)
        if not is_sql_query_safe(query_resolved):
            # we only replace dict keys so there is no risk of SQL injection here,
            # but we still want to ensure the query is safe to execute
            raise UnsafeSqlQueryError(
                "Resolved SQL query is not safe for execution. Please ensure it does not contain any unsafe operations."
            )

        # When merge_columns is None, the check applies to all rows (dataset-level check)
        if merge_columns is None:
            return _apply_dataset_level_sql_check(
                df, spark, query_resolved, condition_column, unique_condition_column, row_filter
            )

        # Resolve the SQL query against the input DataFrame and any reference DataFrames
        user_query_df = spark.sql(query_resolved).select(
            *merge_columns, F.col(condition_column).alias(unique_condition_column)
        )

        # If merge columns aren't unique, multiple query rows can attach to a single input row,
        # potentially causing false positives!
        # Take distinct rows so that we don't multiply records in the output.
        user_query_df_unique = user_query_df.groupBy(*merge_columns).agg(
            F.max(F.col(unique_condition_column)).alias(unique_condition_column)
        )

        # To retain the original records we need to join back to the input DataFrame.
        # Therefore, applying this check multiple times at once can potentially lead to long spark plans.
        # When applying large number of sql query checks, it may be beneficial to split it into separate runs.
        joined_df = df.join(user_query_df_unique, on=merge_columns, how="left")

        # we only care about original columns + condition
        result_df = joined_df.select(*[joined_df[col] for col in df.columns], joined_df[unique_condition_column])

        return result_df

    if negate:
        message_expr = F.lit(msg) if msg else F.lit(f"Value is matching query: '{query}'")
        condition_col_expr = ~F.col(unique_condition_column)
    else:
        message_expr = F.lit(msg) if msg else F.lit(f"Value is not matching query: '{query}'")
        condition_col_expr = F.col(unique_condition_column)

    condition = make_condition(condition=condition_col_expr, message=message_expr, alias=alias_name)

    return condition, apply


@register_rule("dataset")
def is_aggr_not_greater_than(
    column: str | Column,
    limit: int | float | str | Column,
    aggr_type: str = "count",
    group_by: list[str | Column] | None = None,
    row_filter: str | None = None,
    aggr_params: dict[str, Any] | None = None,
) -> tuple[Column, Callable]:
    """
    Build an aggregation check condition and closure for dataset-level validation.

    This function verifies that an aggregation on a column or group of columns does not exceed
    a specified limit. Supports curated aggregate functions (count, sum, avg, stddev, percentile, etc.)
    and any Databricks built-in aggregate. Rows where the aggregation result exceeds the limit are flagged.

    Args:
        column: Column name (str) or Column expression to aggregate.
        limit: Numeric value, column name, or SQL expression for the limit. String literals must be single quoted, e.g. 'string_value'.
        aggr_type: Aggregation type (default: 'count'). Curated types include count, sum, avg, min, max,
            count_distinct, stddev, percentile, and more. Any Databricks built-in aggregate is supported.
        group_by: Optional list of column names or Column expressions to group by.
        row_filter: Optional SQL expression to filter rows before aggregation. Auto-injected from the check filter.
        aggr_params: Optional dict of parameters for aggregates requiring them (e.g., percentile value for
            percentile functions, accuracy for approximate aggregates). Parameters are passed as keyword
            arguments to the Spark function.

    Returns:
        A tuple of:
            - A Spark Column representing the condition for aggregation limit violations.
            - A closure that applies the aggregation check and adds the necessary condition/metric columns.
    """
    return _is_aggr_compare(
        column,
        limit,
        aggr_type,
        aggr_params,
        group_by,
        row_filter,
        compare_op=py_operator.gt,
        compare_op_label="greater than",
        compare_op_name="greater_than",
    )


@register_rule("dataset")
def is_aggr_not_less_than(
    column: str | Column,
    limit: int | float | str | Column,
    aggr_type: str = "count",
    group_by: list[str | Column] | None = None,
    row_filter: str | None = None,
    aggr_params: dict[str, Any] | None = None,
) -> tuple[Column, Callable]:
    """
    Build an aggregation check condition and closure for dataset-level validation.

    This function verifies that an aggregation on a column or group of columns is not below
    a specified limit. Supports curated aggregate functions (count, sum, avg, stddev, percentile, etc.)
    and any Databricks built-in aggregate. Rows where the aggregation result is below the limit are flagged.

    Args:
        column: Column name (str) or Column expression to aggregate.
        limit: Numeric value, column name, or SQL expression for the limit. String literals must be single quoted, e.g. 'string_value'.
        aggr_type: Aggregation type (default: 'count'). Curated types include count, sum, avg, min, max,
            count_distinct, stddev, percentile, and more. Any Databricks built-in aggregate is supported.
        group_by: Optional list of column names or Column expressions to group by.
        row_filter: Optional SQL expression to filter rows before aggregation. Auto-injected from the check filter.
        aggr_params: Optional dict of parameters for aggregates requiring them (e.g., percentile value for
            percentile functions, accuracy for approximate aggregates). Parameters are passed as keyword
            arguments to the Spark function.

    Returns:
        A tuple of:
            - A Spark Column representing the condition for aggregation limit violations.
            - A closure that applies the aggregation check and adds the necessary condition/metric columns.
    """
    return _is_aggr_compare(
        column,
        limit,
        aggr_type,
        aggr_params,
        group_by,
        row_filter,
        compare_op=py_operator.lt,
        compare_op_label="less than",
        compare_op_name="less_than",
    )


@register_rule("dataset")
def is_aggr_equal(
    column: str | Column,
    limit: int | float | str | Column,
    aggr_type: str = "count",
    group_by: list[str | Column] | None = None,
    row_filter: str | None = None,
    aggr_params: dict[str, Any] | None = None,
) -> tuple[Column, Callable]:
    """
    Build an aggregation check condition and closure for dataset-level validation.

    This function verifies that an aggregation on a column or group of columns is equal to
    a specified limit. Supports curated aggregate functions (count, sum, avg, stddev, percentile, etc.)
    and any Databricks built-in aggregate. Rows where the aggregation result is not equal to the limit are flagged.

    Args:
        column: Column name (str) or Column expression to aggregate.
        limit: Numeric value, column name, or SQL expression for the limit. String literals must be single quoted, e.g. 'string_value'.
        aggr_type: Aggregation type (default: 'count'). Curated types include count, sum, avg, min, max,
            count_distinct, stddev, percentile, and more. Any Databricks built-in aggregate is supported.
        group_by: Optional list of column names or Column expressions to group by.
        row_filter: Optional SQL expression to filter rows before aggregation. Auto-injected from the check filter.
        aggr_params: Optional dict of parameters for aggregates requiring them (e.g., percentile value for
            percentile functions, accuracy for approximate aggregates). Parameters are passed as keyword
            arguments to the Spark function.

    Returns:
        A tuple of:
            - A Spark Column representing the condition for aggregation limit violations.
            - A closure that applies the aggregation check and adds the necessary condition/metric columns.
    """
    return _is_aggr_compare(
        column,
        limit,
        aggr_type,
        aggr_params,
        group_by,
        row_filter,
        compare_op=py_operator.ne,
        compare_op_label="not equal to",
        compare_op_name="not_equal_to",
    )


@register_rule("dataset")
def is_aggr_not_equal(
    column: str | Column,
    limit: int | float | str | Column,
    aggr_type: str = "count",
    group_by: list[str | Column] | None = None,
    row_filter: str | None = None,
    aggr_params: dict[str, Any] | None = None,
) -> tuple[Column, Callable]:
    """
    Build an aggregation check condition and closure for dataset-level validation.

    This function verifies that an aggregation on a column or group of columns is not equal to
    a specified limit. Supports curated aggregate functions (count, sum, avg, stddev, percentile, etc.)
    and any Databricks built-in aggregate. Rows where the aggregation result is equal to the limit are flagged.

    Args:
        column: Column name (str) or Column expression to aggregate.
        limit: Numeric value, column name, or SQL expression for the limit. String literals must be single quoted, e.g. 'string_value'.
        aggr_type: Aggregation type (default: 'count'). Curated types include count, sum, avg, min, max,
            count_distinct, stddev, percentile, and more. Any Databricks built-in aggregate is supported.
        group_by: Optional list of column names or Column expressions to group by.
        row_filter: Optional SQL expression to filter rows before aggregation. Auto-injected from the check filter.
        aggr_params: Optional dict of parameters for aggregates requiring them (e.g., percentile value for
            percentile functions, accuracy for approximate aggregates). Parameters are passed as keyword
            arguments to the Spark function.

    Returns:
        A tuple of:
            - A Spark Column representing the condition for aggregation limit violations.
            - A closure that applies the aggregation check and adds the necessary condition/metric columns.
    """
    return _is_aggr_compare(
        column,
        limit,
        aggr_type,
        aggr_params,
        group_by,
        row_filter,
        compare_op=py_operator.eq,
        compare_op_label="equal to",
        compare_op_name="equal_to",
    )


@register_rule("dataset")
def compare_datasets(
    columns: list[str | Column],
    ref_columns: list[str | Column],
    ref_df_name: str | None = None,
    ref_table: str | None = None,
    check_missing_records: bool | None = False,
    exclude_columns: list[str | Column] | None = None,
    null_safe_row_matching: bool | None = True,
    null_safe_column_value_matching: bool | None = True,
    row_filter: str | None = None,
    abs_tolerance: float | None = None,
    rel_tolerance: float | None = None,
) -> tuple[Column, Callable]:
    """
    Dataset-level check that compares two datasets and returns a condition for changed rows,
    with details on row and column-level differences.

    Only columns that are common across both datasets will be compared. Mismatched columns are ignored.
    Detailed information about the differences is provided in the condition column.
    The comparison does not support Map types (any column comparison on map type is skipped automatically).

    The log containing detailed differences is written to the message field of the check result as a JSON string.

    Examples:
    ```json
    {
      "row_missing": false,
      "row_extra": true,
      "changed": {
        "val": {
          "df": "val1"
        }
      }
    }
    ```

    Args:
      columns: List of columns to use for row matching with the reference DataFrame
        (can be a list of string column names or column expressions). Only simple column
        expressions are supported, e.g. F.col("col_name").
      ref_columns: List of columns in the reference DataFrame or Table to row match against
        the source DataFrame (can be a list of string column names or column expressions).
        The *columns* parameter is matched with *ref_columns* by position, so the order of
        the provided columns in both lists must be exactly aligned. Only simple column
        expressions are supported, e.g. F.col("col_name").
      ref_df_name: Name of the reference DataFrame (used when passing DataFrames directly).
      ref_table: Name of the reference table (used when reading from catalog).
      check_missing_records: Perform FULL OUTER JOIN between the DataFrames to also find
        records that could be missing from the DataFrame. Use with caution as it may produce
        output with more rows than in the original DataFrame.
      exclude_columns: List of columns to exclude from the value comparison but not from row
        matching (can be a list of string column names or column expressions). Only simple
        column expressions are supported, e.g. F.col("col_name"). This parameter does not alter
        the list of columns used to determine row matches; it only controls which columns are
        skipped during the column value comparison.
      null_safe_row_matching: If True, treats nulls as equal when matching rows.
      null_safe_column_value_matching: If True, treats nulls as equal when matching column values.
        If enabled, (NULL, NULL) column values are equal and matching.
      row_filter: Optional SQL expression to filter rows in the input DataFrame. Auto-injected
        from the check filter.
      abs_tolerance: Values are considered equal if the absolute difference is less than or equal to the tolerance. This is applicable to numeric columns.
            Example: abs(a - b) <= tolerance
            With tolerance=0.01:
            2.001 and 2.0099 → equal (diff = 0.0089)
            2.001 and 2.02 → not equal (diff = 0.019)
      rel_tolerance: Relative tolerance for numeric comparisons. Differences within this relative tolerance are ignored. Useful if numbers vary in scale.
            Example: abs(a - b) <= rel_tolerance * max(abs(a), abs(b))
            With tolerance=0.01 (1%):
            100 vs 101 → equal (diff = 1, tolerance = 1)
            2.001 vs 2.0099 → equal


    Returns:
      Tuple[Column, Callable]:
        - A Spark Column representing the condition for comparison violations.
        - A closure that applies the comparison validation.

    Raises:
        MissingParameterError:
            - if neither *ref_df_name* nor *ref_table* is provided.
        InvalidParameterError:
            - if both *ref_df_name* and *ref_table* are provided.
            - if the number of *columns* and *ref_columns* do not match.
            - if *abs_tolerance* or *rel_tolerance* is negative.
    """
    _validate_ref_params(columns, ref_columns, ref_df_name, ref_table)

    abs_tolerance = 0.0 if abs_tolerance is None else abs_tolerance
    rel_tolerance = 0.0 if rel_tolerance is None else rel_tolerance
    if abs_tolerance < 0 or rel_tolerance < 0:
        raise InvalidParameterError("Absolute and/or relative tolerances if provided must be non-negative")

    # convert all input columns to strings
    pk_column_names = get_columns_as_strings(columns, allow_simple_expressions_only=True)
    ref_pk_column_names = get_columns_as_strings(ref_columns, allow_simple_expressions_only=True)
    exclude_column_names = (
        get_columns_as_strings(exclude_columns, allow_simple_expressions_only=True) if exclude_columns else []
    )
    check_alias = normalize_col_str(f"datasets_diff_pk_{'_'.join(pk_column_names)}_ref_{'_'.join(ref_pk_column_names)}")

    unique_id = uuid.uuid4().hex
    condition_col = f"__compare_status_{unique_id}"
    row_missing_col = f"__row_missing_{unique_id}"
    row_extra_col = f"__row_extra_{unique_id}"
    columns_changed_col = f"__columns_changed_{unique_id}"
    filter_col = f"__filter_{uuid.uuid4().hex}"

    def apply(df: DataFrame, spark: SparkSession, ref_dfs: dict[str, DataFrame]) -> DataFrame:
        ref_df = _get_ref_df(ref_df_name, ref_table, ref_dfs, spark)

        # map type columns must be skipped as they cannot be compared with eqNullSafe
        map_type_columns = {field.name for field in df.schema.fields if isinstance(field.dataType, types.MapType)}

        # columns to compare: present in both df and ref_df, not in PK, not excluded, not map type
        compare_columns = [
            col
            for col in df.columns
            if (
                col in ref_df.columns
                and col not in pk_column_names
                and col not in exclude_column_names
                and col not in map_type_columns
            )
        ]

        # determine skipped columns: present in df, not compared, and not PK
        skipped_columns = [col for col in df.columns if col not in compare_columns and col not in pk_column_names]

        # apply filter before aliasing to avoid ambiguity
        df = df.withColumn(filter_col, F.expr(row_filter) if row_filter else F.lit(True))

        df = df.alias("df")
        ref_df = ref_df.alias("ref_df")

        results = _match_rows(
            df, ref_df, pk_column_names, ref_pk_column_names, check_missing_records, null_safe_row_matching
        )
        results = _add_row_diffs(results, pk_column_names, ref_pk_column_names, row_missing_col, row_extra_col)
        results = _add_column_diffs(
            results, compare_columns, columns_changed_col, null_safe_column_value_matching, abs_tolerance, rel_tolerance
        )
        results = _add_compare_condition(
            results, condition_col, row_missing_col, row_extra_col, columns_changed_col, filter_col
        )

        # in a full outer join, rows may be missing from either side, we take the first non-null value
        coalesced_pk_columns = [
            F.coalesce(F.col(f"df.{col}"), F.col(f"ref_df.{ref_col}")).alias(col)
            for col, ref_col in zip(pk_column_names, ref_pk_column_names)
        ]

        # make sure original columns + condition column are present in the output
        return results.select(
            *coalesced_pk_columns,
            *[F.col(f"df.{col}").alias(col) for col in compare_columns],
            *[F.col(f"df.{col}").alias(col) for col in skipped_columns],
            F.col(condition_col),
        )

    condition = F.col(condition_col).isNotNull()

    return (
        make_condition(
            condition=condition, message=F.when(condition, F.to_json(F.col(condition_col))), alias=check_alias
        ),
        apply,
    )


@register_rule("dataset")
def is_data_fresh_per_time_window(
    column: str | Column,
    window_minutes: int,
    min_records_per_window: int,
    lookback_windows: int | None = None,
    row_filter: str | None = None,
    curr_timestamp: Column | None = None,
) -> tuple[Column, Callable]:
    """
    Build a completeness freshness check that validates records arrive at least every X minutes
    with a threshold for the expected number of rows per time window.

    If *lookback_windows* is provided, only data within that lookback period will be validated.
    If omitted, the entire dataset will be checked.

    Args:
        column: Column name (str) or Column expression containing timestamps to check.
        window_minutes: Time window in minutes to check for data arrival.
        min_records_per_window: Minimum number of records expected per time window.
        lookback_windows: Optional number of time windows to look back from *curr_timestamp*.
            This filters records to include only those within the specified number of time windows from *curr_timestamp*.
            If no lookback is provided, the check is applied to the entire dataset.
        row_filter: Optional SQL expression to filter rows before checking.
        curr_timestamp: Optional current timestamp column. If not provided, current_timestamp() function is used.

    Returns:
        A tuple of:
            - A Spark Column representing the condition for missing data within a time window.
            - A closure that applies the completeness check and adds the necessary condition columns.

    Raises:
        InvalidParameterError: If min_records_per_window or window_minutes are not positive integers,
            or if lookback_windows is provided and is not a positive integer.
    """
    col_str_norm, _, col_expr = get_normalized_column_and_expr(column)

    unique_str = uuid.uuid4().hex
    condition_col = f"__data_volume_condition_completeness_{col_str_norm}_{unique_str}"
    interval_col = f"__interval_{col_str_norm}_{unique_str}"
    count_col = f"__count_{col_str_norm}_{unique_str}"

    if lookback_windows is not None and lookback_windows <= 0:
        raise InvalidParameterError("lookback_windows must be a positive integer if provided")
    if min_records_per_window is None or min_records_per_window <= 0:
        raise InvalidParameterError("min_records_per_window must be a positive integer")
    if window_minutes is None or window_minutes <= 0:
        raise InvalidParameterError("window_minutes must be a positive integer")

    if curr_timestamp is None:
        curr_timestamp = F.current_timestamp()

    def apply(df: DataFrame) -> DataFrame:
        """
        Apply the data arrival completeness check logic to the DataFrame.

        Creates time windows and checks if each window has the minimum required records.

        Args:
            df: The input DataFrame to validate.

        Returns:
            The DataFrame with additional condition columns for completeness validation.
        """
        # Build filter condition
        filter_condition = F.lit(True)
        if row_filter:
            filter_condition = filter_condition & F.expr(row_filter)

        # Limit checking to be within the lookback window if needed
        if lookback_windows is not None:
            lookback_minutes = window_minutes * lookback_windows
            cutoff_timestamp = F.from_unixtime(F.unix_timestamp(curr_timestamp) - F.lit(lookback_minutes * 60))
            filter_condition = filter_condition & (col_expr >= cutoff_timestamp)

        # Always filter by current time upper bound
        filter_condition = filter_condition & (col_expr <= curr_timestamp)

        # Window in Spark only returns non-null windows for rows where the timestamp column is non-null
        # so we have to make sure to coalesce it to a safe default value
        safe_col_expr = F.coalesce(col_expr, F.lit("1900-01-01 00:00:00").cast("timestamp"))

        # Create time windows
        df = df.withColumn(interval_col, F.window(safe_col_expr, f"INTERVAL {window_minutes} MINUTES"))

        window_spec = Window.partitionBy(F.col(interval_col))
        df = df.withColumn(count_col, F.count_if(filter_condition).over(window_spec))

        # Check if count is below minimum threshold
        df = df.withColumn(
            condition_col,
            F.when((F.col(count_col) < min_records_per_window) & filter_condition, True).otherwise(False),
        )

        return df

    condition = make_condition(
        condition=F.col(condition_col),
        message=F.concat_ws(
            "",
            F.lit("Data arrival completeness check failed: only "),
            F.col(count_col).cast("string"),
            F.lit(f" records found in {window_minutes}-minute interval starting at "),
            F.col(interval_col).start.cast("string"),
            F.lit(" and ending at "),
            F.col(interval_col).end.cast("string"),
            F.lit(f", expected at least {min_records_per_window} records"),
        ),
        alias=f"{col_str_norm}_is_data_fresh_per_time_window",
    )

    return condition, apply


@register_rule("dataset")
def has_valid_schema(
    expected_schema: str | types.StructType | None = None,
    ref_df_name: str | None = None,
    ref_table: str | None = None,
    columns: list[str | Column] | None = None,
    strict: bool = False,
) -> tuple[Column, Callable]:
    """
    Build a schema compatibility check condition and closure for dataset-level validation.

    This function checks whether the DataFrame schema is compatible with the expected schema.
    In non-strict mode, validates that all expected columns exist with compatible types.
    In strict mode, validates that the schema matches exactly (same columns, same order, same types)
    for the columns specified in columns or for all columns if columns is not specified.

    Args:
        expected_schema: Expected schema as a DDL string (e.g., "id INT, name STRING") or StructType object.
        ref_df_name: Name of the reference DataFrame (used when passing DataFrames directly).
        ref_table: Name of the reference table to load the schema from (e.g. "catalog.schema.table")
        columns: Optional list of columns to validate (default: all columns are considered)
        strict: Whether to perform strict schema validation (default: False).
            - False: Validates that all expected columns exist with compatible types (allows extra columns)
            - True: Validates exact schema match (same columns, same order, same types)

    Returns:
        A tuple of:
            - A Spark Column representing the condition for schema compatibility violations.
            - A closure that applies the schema check and adds the necessary condition columns.

    Raises:
        InvalidParameterError:
            - If the *expected_schema* string is invalid or cannot be parsed
            - If *expected_schema* is neither a string nor a StructType
            - If more than one of *expected_schema*, *ref_df_name*, or *ref_table* are specified
            - If none of *expected_schema*, *ref_df_name*, or *ref_table* are specified

    Note:
        Exactly one of *expected_schema*, *ref_df_name*, or *ref_table* must be specified.
    """
    expected_params = ["expected_schema", "ref_df_name", "ref_table"]
    non_null_params = dict(filter(lambda param: param[0] in expected_params and param[1] is not None, locals().items()))

    if len(non_null_params) != 1:
        raise InvalidParameterError(
            "Must specify one of 'expected_schema', 'ref_df_name', or 'ref_table' when using 'has_valid_schema'"
        )

    column_names: list[str] | None = None
    if columns:
        column_names = [get_column_name_or_alias(col) if not isinstance(col, str) else col for col in columns]

    expected_schema = _get_schema(expected_schema or types.StructType(), column_names)

    unique_str = uuid.uuid4().hex  # make sure any column added to the dataframe is unique
    condition_col = f"__schema_condition_{unique_str}"
    message_col = f"__schema_message_{unique_str}"

    def apply(df: DataFrame, spark: SparkSession, ref_dfs: dict[str, DataFrame]) -> DataFrame:
        """
        Apply the schema compatibility check logic to the DataFrame.

        Adds columns indicating whether the DataFrame schema is incompatible with the expected schema.

        Args:
            df: The input DataFrame to validate for schema compatibility.
            spark: SparkSession used to get the reference table schema
            ref_dfs: A dictionary mapping reference DataFrame names to DataFrame objects.

        Returns:
            The DataFrame with additional condition and message columns for schema validation.
        """

        if ref_df_name or ref_table:
            ref_df = _get_ref_df(ref_df_name, ref_table, ref_dfs, spark)
            _expected_schema = _get_schema(ref_df.schema, column_names)
        else:
            _expected_schema = expected_schema

        actual_schema = df.select(*columns).schema if columns else df.schema

        if strict:
            errors = _get_strict_schema_comparison(actual_schema, _expected_schema)
        else:
            errors = _get_permissive_schema_comparison(actual_schema, _expected_schema)

        has_errors = len(errors) > 0
        error_message = "; ".join(errors) if errors else None

        df = df.withColumn(condition_col, F.lit(has_errors))
        df = df.withColumn(message_col, F.lit(error_message))

        return df

    condition = make_condition(
        condition=F.col(condition_col),
        message=F.concat_ws("", F.lit("Schema validation failed: "), F.col(message_col)),
        alias="has_invalid_schema",
    )

    return condition, apply


@register_rule("row")
def is_valid_json(column: str | Column) -> Column:
    """
    Checks whether the values in the input column are valid JSON strings.

    Args:
        column: Column name (str) or Column expression to check for valid JSON.

    Returns:
        A Spark Column representing the condition for invalid JSON strings.
    """
    col_str_norm, col_expr_str, col_expr = get_normalized_column_and_expr(column)
    return make_condition(
        ~F.when(col_expr.isNotNull(), F.try_parse_json(col_expr_str).isNotNull()),
        F.concat_ws(
            "",
            F.lit("Value '"),
            col_expr.cast("string"),
            F.lit(f"' in Column '{col_expr_str}' is not a valid JSON string"),
        ),
        f"{col_str_norm}_is_not_valid_json",
    )


@register_rule("row")
def has_json_keys(column: str | Column, keys: list[str], require_all: bool = True) -> Column:
    """
    Checks whether the values in the input column contain specific keys in the outermost JSON object.

    Args:
        column: The name of the column or the column expression to check for JSON keys.
        keys: A list of JSON keys to verify within the outermost JSON object.
        require_all: If True, all specified keys must be present. If False, at least one key must be present.

    Returns:
        A Spark Column representing the condition for missing JSON keys.
    """
    if not keys:
        raise InvalidParameterError("The 'keys' parameter must be a non-empty list of strings.")
    if any(not isinstance(k, str) for k in keys):
        raise InvalidParameterError("All keys must be of type string.")

    col_str_norm, col_expr_str, col_expr = get_normalized_column_and_expr(column)
    json_keys_array = F.json_object_keys(col_expr)
    required_keys = F.array_distinct(F.array(*[F.lit(k) for k in keys]))

    json_validation_error = is_valid_json(col_expr_str)
    is_invalid_json = json_validation_error.isNotNull()

    has_json_keys_msg = F.concat_ws(
        "",
        F.lit("Value '"),
        F.when(col_expr.isNull(), F.lit("null")).otherwise(col_expr.cast("string")),
        F.lit(f"' in Column '{col_expr_str}' is missing keys in the list: ["),
        F.concat_ws(", ", F.lit(keys)),
        F.lit("]"),
    )
    message = F.when(is_invalid_json, json_validation_error).otherwise(has_json_keys_msg)

    if require_all:
        missing = F.array_except(required_keys, json_keys_array)
        condition_when_valid = F.size(missing) == 0
    else:
        condition_when_valid = F.arrays_overlap(json_keys_array, required_keys)

    condition = F.when(~is_invalid_json, condition_when_valid).otherwise(F.lit(False))
    # Treat NULL values as valid (no violation) to ensure consistent behavior across ANSI/non-ANSI modes
    condition = condition | col_expr.isNull()

    return make_condition(
        ~condition,
        message,
        f"{col_str_norm}_does_not_have_json_keys",
    )


@register_rule("row")
def has_valid_json_schema(column: str | Column, schema: str | types.StructType) -> Column:
    """
    Validates that JSON strings in the specified column conform to an expected schema.

    The validation utilizes standard Spark JSON parsing rules, specifically:
    * **Type Coercion is Permitted:** Values that can be successfully cast to the target schema type
    (e.g. a JSON number like 0.12 parsing into a field defined as STRING) are considered valid.
    * **Extra Fields are Ignored:** Fields present in the JSON, but missing from the schema are ignored.
    * **Missing keys imply null:** If a key is missing from the JSON object, Spark treats it as a null value.
    * **Strictness:** If a schema field is defined as NOT NULL, validation will fail if the key is missing (implicit null) or explicitly set to null.
    * **Nested JSON behavior:** If a nullable parent field is explicitly null (e.g. `{"parent": null}`), its children are **not** validated.
    However, if the parent exists (e.g. `{"parent": {}}`) but a required child is missing, validation fails.
    * **Nested Depth Limit:** The validation logic supports a maximum nested depth of 10 levels.

    Args:
        column: Column name or Column expression containing JSON strings.
        schema: Expected schema as a DDL string (e.g. "struct<id:string NOT NULL>", "id INT, name STRING")
            or a generic StructType. To enforce strict presence of a field, you must explicitly set it to nullable=False
            or use NOT NULL in the DDL string.

    Returns:
        A string Column containing the error message if the JSON does not conform to the schema,
            or null if validation passes.

    Raises:
        InvalidParameterError: If the schema string is invalid/unparsable, or if the input schema is neither a string nor a StructType.
    """

    _expected_schema = _get_schema(schema)
    schema_str = _expected_schema.simpleString()
    col_str_norm, col_expr_str, col_expr = get_normalized_column_and_expr(column)

    json_validation_error = is_valid_json(col_str_norm)

    is_invalid_json = json_validation_error.isNotNull()

    # Add unique corrupt-record field to isolate parse errors
    corrupt_record_name = f"{uuid.uuid4().hex[:8]}_dqx_corrupt_record"

    extended_schema = types.StructType(
        _expected_schema.fields + [types.StructField(corrupt_record_name, types.StringType(), True)]
    )

    # Attempt to parse JSON using the extended schema
    parsed_struct = F.from_json(
        col_expr,
        extended_schema,
        options={"columnNameOfCorruptRecord": corrupt_record_name},
    )

    # Core conformity: must be valid JSON and not corrupt
    is_not_corrupt = parsed_struct[corrupt_record_name].isNull()
    base_conformity = ~is_invalid_json & is_not_corrupt

    # Field presence checks (non-null + exists)
    field_presence_checks = _generate_field_presence_checks(_expected_schema, parsed_struct)
    has_missing_or_null_fields = F.array_contains(
        F.array(*[F.coalesce(expr, F.lit(False)) for expr in field_presence_checks]),
        False,
    )

    is_conforming = base_conformity & ~has_missing_or_null_fields
    condition = is_conforming | col_expr.isNull()

    error_msg = F.concat_ws(
        "",
        F.lit("Value '"),
        F.when(col_expr.isNull(), F.lit("null")).otherwise(col_expr.cast("string")),
        F.lit(f"' in Column '{col_expr_str}' does not conform to expected JSON schema: "),
        F.lit(schema_str),
    )

    final_error_msg = F.when(is_invalid_json, json_validation_error).otherwise(error_msg)

    return make_condition(
        ~condition,
        final_error_msg,
        f"{col_str_norm}_has_invalid_json_schema",
    )


def _get_schema(input_schema: str | types.StructType, columns: list[str] | None = None) -> types.StructType:
    """
    Normalize the input schema into a Spark StructType schema.

    Args:
        input_schema: Schema definition, either as a DDL string or a StructType.
        columns: Optional list of columns to keep (default: all).

    Returns:
        StructType schema.

    Raises:
        InvalidParameterError: If the schema string is invalid or cannot be parsed, or if
             the input schema is neither a string nor a StructType.
    """
    if isinstance(input_schema, types.StructType):
        expected_schema = input_schema
    elif isinstance(input_schema, str):
        try:
            parsed_schema = types.StructType.fromDDL(input_schema)
        except Exception as e:  # Catch schema parsing errors from Spark
            raise InvalidParameterError(f"Invalid schema string '{input_schema}'. Error: {e}") from e

        if not isinstance(parsed_schema, types.StructType):  # Handles cases like input_schema="STRING"
            raise InvalidParameterError(f"Invalid schema string '{input_schema}' (not a StructType)")

        expected_schema = parsed_schema
    else:
        raise InvalidParameterError(f"'input_schema' must be str or StructType, got {type(input_schema).__name__}")

    if columns:
        return types.StructType([f for f in expected_schema.fields if f.name in columns])

    return expected_schema


def _get_strict_schema_comparison(actual_schema: types.StructType, expected_schema: types.StructType) -> list[str]:
    """
    Performs a strict schema comparison between actual and expected DataFrame schemas.

    Args:
        actual_schema: Actual DataFrame Schema as a Spark `StructType`
        expected_schema: Expected DataFrame Schema as a Spark `StructType`

    Return:
        List of differences between the actual and expected schemas
    """

    errors = []

    if actual_schema == expected_schema:
        return []

    for i, (actual_field, expected_field) in enumerate(zip_longest(actual_schema.fields, expected_schema.fields)):
        if not actual_field:
            errors.append(f"Column '{expected_field.name}' in expected schema not present in checked data")
            continue

        if not expected_field:
            errors.append(f"Column '{actual_field.name}' in checked data not present in expected schema")
            continue

        if actual_field.name != expected_field.name:
            errors.append(
                f"Column with index {i} has incorrect name, expected '{expected_field.name}', got '{actual_field.name}'"
            )

        if actual_field.dataType != expected_field.dataType:
            errors.append(
                f"Column '{actual_field.name}' has incorrect type, "
                f"expected '{expected_field.dataType.typeName()}', got '{actual_field.dataType.typeName()}'"
            )

        if actual_field.nullable != expected_field.nullable:
            errors.append(
                f"Column '{actual_field.name}' has incorrect nullability, "
                f"expected '{expected_field.nullable}', got '{actual_field.nullable}'"
            )

    return errors


def _get_permissive_schema_comparison(actual_schema: types.StructType, expected_schema: types.StructType) -> list[str]:
    """
    Performs a permissive schema comparison between actual and expected DataFrame schemas.
    Checks that all expected columns exist with compatible data types. Allows for differences
    in the exact column type, nullability, and order.

    Args:
        actual_schema: Actual DataFrame Schema as a Spark `StructType`
        expected_schema: Expected DataFrame Schema as a Spark `StructType`

    Return:
        List of differences between the actual and expected schemas
    """

    errors = []
    actual_fields_map = {field.name: field for field in actual_schema.fields}

    for expected_field in expected_schema.fields:
        if expected_field.name not in actual_fields_map:
            errors.append(f"Column '{expected_field.name}' in expected schema not present in checked data")
            continue

        actual_field = actual_fields_map[expected_field.name]
        if not _is_compatible_type(actual_field.dataType, expected_field.dataType):
            errors.append(
                f"Column '{expected_field.name}' has incompatible type, "
                f"expected '{expected_field.dataType.typeName()}', got '{actual_field.dataType.typeName()}'"
            )

    return errors


def _is_compatible_type(actual_type: types.DataType, expected_type: types.DataType) -> bool:
    """
    Checks if two Spark `DataTypes` are compatible. Allows for type-widening.

    Args:
        actual_type: The actual data type
        expected_type: The expected data type

    Returns:
        True if types are compatible, False otherwise
    """

    if actual_type == expected_type:
        return True

    if isinstance(actual_type, types.AtomicType) and isinstance(expected_type, types.AtomicType):
        return _is_compatible_atomic_type(actual_type, expected_type)

    if isinstance(actual_type, types.ArrayType) and isinstance(expected_type, types.ArrayType):
        return _is_compatible_type(actual_type.elementType, expected_type.elementType)

    if isinstance(actual_type, types.MapType) and isinstance(expected_type, types.MapType):
        has_compatible_keys = _is_compatible_type(actual_type.keyType, expected_type.keyType)
        has_compatible_values = _is_compatible_type(actual_type.valueType, expected_type.valueType)
        return has_compatible_keys and has_compatible_values

    if isinstance(actual_type, types.StructType) and isinstance(expected_type, types.StructType):
        return _is_compatible_struct_type(actual_type, expected_type)

    if isinstance(actual_type, types.VariantType):
        # NOTE: `VariantType` can be parsed to any `AtomicType`, `StructType`, `MapType`, or `ArrayType`
        return True

    return False


def _is_compatible_struct_type(actual_type: types.StructType, expected_type: types.StructType) -> bool:
    """
    Check if two Spark `StructTypes` are compatible. Allows for type-widening.

    Args:
        actual_type: The actual data type
        expected_type: The expected data type

    Returns:
        True if types are compatible, False otherwise
    """

    if len(actual_type.fields) != len(expected_type.fields):
        return False

    for actual_field, expected_field in zip(actual_type.fields, expected_type.fields):
        if actual_field.name != expected_field.name:
            return False
        if not _is_compatible_type(actual_field.dataType, expected_field.dataType):
            return False
    return True


def _is_compatible_atomic_type(actual_type: types.AtomicType, expected_type: types.AtomicType) -> bool:
    """
    Check if two Spark `AtomicTypes` are compatible. Allows for type-widening.

    Args:
        actual_type: The actual data type
        expected_type: The expected data type

    Returns:
        True if types are compatible, False otherwise
    """

    numeric_types = [
        types.ByteType,
        types.ShortType,
        types.IntegerType,
        types.LongType,
        types.DecimalType,
        types.FloatType,
        types.DoubleType,
    ]

    string_types = [
        types.CharType,
        types.VarcharType,
        types.StringType,
    ]

    datetime_types = [
        types.DateType,
        types.TimestampNTZType,
        types.TimestampType,
    ]

    if actual_type == expected_type:
        return True

    if type(actual_type) in numeric_types and type(expected_type) in numeric_types:
        # Allow widening conversions for numeric types
        return True

    if type(actual_type) in string_types and type(expected_type) in string_types:
        # Allow widening conversions for string types
        return True

    if type(actual_type) in datetime_types and type(expected_type) in datetime_types:
        # Allow widening conversions for datetime types
        return True

    return False


def _generate_field_presence_checks(
    expected_schema: types.StructType, parsed_struct_col: Column, max_depth: int = 10, current_depth: int = 0
) -> list[Column]:
    """
    Recursively generate Spark Column expressions that verify each field defined in the expected
    schema is present and non-null within a parsed struct column.

    Args:
        expected_schema: The StructType defining the expected JSON schema.
        parsed_struct_col: The parsed struct column (e.g., from from_json) to validate.
        max_depth: Maximum recursion depth to prevent excessive nesting. Default is 10.
        current_depth: Current recursion depth.

    Returns:
        A list of Column expressions, one per field in the expected schema, that evaluate to True
        if the corresponding field is non-null.
    """
    if current_depth > max_depth:
        return []

    validations = []
    for field in expected_schema.fields:
        field_ref = parsed_struct_col[field.name]
        if not field.nullable:
            validations.append(field_ref.isNotNull())
        if isinstance(field.dataType, types.StructType):
            child_checks = _generate_field_presence_checks(
                field.dataType, field_ref, max_depth=max_depth, current_depth=current_depth + 1
            )
            if field.nullable:
                child_checks = [(field_ref.isNull() | check) for check in child_checks]
            validations.extend(child_checks)

    return validations


def _match_rows(
    df: DataFrame,
    ref_df: DataFrame,
    pk_column_names: list[str],
    ref_pk_column_names: list[str],
    check_missing_records: bool | None,
    null_safe_row_matching: bool | None = True,
) -> DataFrame:
    """
    Perform a null-safe join between two DataFrames based on primary key columns.
    Ensure that corresponding pk columns are compared together, match by position in pk and ref pk cols.
    Use eq null safe join to ensure that:
        - 1 == 1 matches
        - NULL <=> NULL matches
        - 1 <=> NULL does not match

    Args:
        df: The input DataFrame to join.
        ref_df: The reference DataFrame to join against.
        pk_column_names: List of primary key column names in the input DataFrame.
        ref_pk_column_names: List of primary key column names in the reference DataFrame.
        check_missing_records: If True, perform a full outer join to find missing records in both DataFrames.
        null_safe_row_matching: If True, treats nulls as equal when matching rows.

    Returns:
        A DataFrame with the results of the join.
    """
    join_condition = F.lit(True)
    for column, ref_column in zip(pk_column_names, ref_pk_column_names):
        if null_safe_row_matching:
            join_condition = join_condition & F.col(f"df.{column}").eqNullSafe(F.col(f"ref_df.{ref_column}"))
        else:
            join_condition = join_condition & (F.col(f"df.{column}") == F.col(f"ref_df.{ref_column}"))

    results = df.join(
        ref_df,
        on=join_condition,
        # full outer join allows us to find missing records in both DataFrames
        how="full_outer" if check_missing_records else "left_outer",
    )
    return results


def _add_row_diffs(
    df: DataFrame, pk_column_names: list[str], ref_pk_column_names: list[str], row_missing_col: str, row_extra_col: str
) -> DataFrame:
    """
    Adds flags to the DataFrame indicating missing or extra rows during comparison.

    A row is considered missing if it exists in the reference DataFrame but not in the source DataFrame.
    This is determined by checking if all primary key columns in the source DataFrame (df) are null.
    A row is extra if it exists in the source DataFrame but not in the reference DataFrame.
    This is determined by checking if all primary key columns in the reference DataFrame (ref_df) are null.
    """
    row_missing_condition = F.lit(True)
    row_extra_condition = F.lit(True)

    # check for existence against all pk columns
    for df_col_name, ref_col_name in zip(pk_column_names, ref_pk_column_names):
        row_missing_condition = row_missing_condition & F.col(f"df.{df_col_name}").isNull()
        row_extra_condition = row_extra_condition & F.col(f"ref_df.{ref_col_name}").isNull()

    df = df.withColumn(row_missing_col, row_missing_condition)
    df = df.withColumn(row_extra_col, row_extra_condition)

    return df


def _add_numeric_tolerance_condition(
    col_name: str, abs_tolerance: float, rel_tolerance: float, null_safe_column_value_matching: bool | None = None
) -> Column:
    df_col = F.col(f"df.{col_name}")
    ref_col = F.col(f"ref_df.{col_name}")

    # Handle NULL cases explicitly based on null_safe_column_value_matching
    if null_safe_column_value_matching:
        # NULL safety: (NULL, NULL) should be considered equal
        both_null = df_col.isNull() & ref_col.isNull()
        either_null = df_col.isNull() | ref_col.isNull()

        # For non-NULL values, apply tolerance logic
        tolerance_match = _match_values_with_tolerance(df_col, ref_col, abs_tolerance, rel_tolerance)

        # Values are considered equal if:
        # 1. Both are NULL (null safety), OR
        # 2. Neither is NULL AND they're within tolerance
        values_match = both_null | (~either_null & tolerance_match)
    else:
        # Null safety disabled: if either value is NULL, consider them matching
        either_null = df_col.isNull() | ref_col.isNull()

        tolerance_match = _match_values_with_tolerance(df_col, ref_col, abs_tolerance, rel_tolerance)

        # Values are considered equal if: either is NULL OR both non-NULL and within tolerance
        values_match = either_null | tolerance_match

    # Return True if values are NOT within tolerance (indicating a difference)
    return ~values_match


def _match_values_with_tolerance(df_col: Column, ref_col: Column, abs_tolerance: float, rel_tolerance: float) -> Column:
    abs_diff = F.abs(df_col - ref_col)
    tolerance_val_relative = rel_tolerance * F.greatest(F.abs(df_col), F.abs(ref_col))
    return (abs_diff <= F.lit(abs_tolerance)) | (abs_diff <= tolerance_val_relative)


def _add_column_diffs(
    df: DataFrame,
    compare_columns: list[str],
    columns_changed_col: str,
    null_safe_column_value_matching: bool | None = True,
    abs_tolerance: float = 0.0,
    rel_tolerance: float = 0.0,
) -> DataFrame:
    """
    Adds a column to the DataFrame that contains a map of changed columns and their differences.

    This function compares specified columns between two datasets (*df* and *ref_df*) and identifies differences.
    For each column in *compare_columns*, it checks if the values in *df* and *ref_df* are equal.
    If a difference is found, it adds the column name and the differing values to a map stored in *columns_changed_col*.

    Args:
        df: The input DataFrame containing columns to compare.
        compare_columns: List of column names to compare between *df* and *ref_df*.
        columns_changed_col: Name of the column to store the map of changed columns and their differences.
        null_safe_column_value_matching: If True, treats nulls as equal when matching column values.
            If enabled (NULL, NULL) column values are equal and matching.
            If False, uses a standard inequality comparison (`!=`), where (NULL, NULL) values are not considered equal.
        abs_tolerance: Absolute tolerance for numeric comparisons. Differences within this absolute tolerance are ignored.
            Example: abs(a - b) <= abs_tolerance
        rel_tolerance: Relative tolerance for numeric comparisons. Differences within this relative tolerance are ignored.
            Example: abs(a - b) <= rel_tolerance * max(abs(a), abs(b))
    Returns:
        A DataFrame with the added *columns_changed_col* containing the map of changed columns and differences.
    """
    columns_changed = []
    if compare_columns:

        for col_name in compare_columns:
            is_numeric = isinstance(df.schema[col_name].dataType, types.NumericType)

            if (abs_tolerance > 0.0 or rel_tolerance > 0.0) and is_numeric:
                # Absolute and relative difference
                condition = _add_numeric_tolerance_condition(
                    col_name, abs_tolerance, rel_tolerance, null_safe_column_value_matching
                )
            else:
                condition = (
                    ~F.col(f"df.{col_name}").eqNullSafe(F.col(f"ref_df.{col_name}"))
                    if null_safe_column_value_matching
                    else F.col(f"df.{col_name}") != F.col(f"ref_df.{col_name}")
                )

            columns_changed.append(
                F.when(
                    condition,
                    F.struct(
                        F.lit(col_name).alias("col_changed"),
                        F.struct(
                            F.col(f"df.{col_name}").cast("string").alias("df"),
                            F.col(f"ref_df.{col_name}").cast("string").alias("ref"),
                        ).alias("diff"),
                    ),
                ).otherwise(None)
            )

        df = df.withColumn(columns_changed_col, F.array_compact(F.array(*columns_changed)))

        df = df.withColumn(
            columns_changed_col,
            F.map_from_arrays(
                F.col(columns_changed_col).getField("col_changed"),
                F.col(columns_changed_col).getField("diff"),
            ),
        )
    else:
        # No columns to compare, inject empty map
        df = df.withColumn(columns_changed_col, F.create_map())

    return df


def _add_compare_condition(
    df: DataFrame,
    condition_col: str,
    row_missing_col: str,
    row_extra_col: str,
    columns_changed_col: str,
    filter_col: str,
) -> DataFrame:
    """
    Add the condition column only for mismatched records based on filter and differences.
    This function adds a new column (*condition_col*) to the DataFrame, which contains structured information
    about mismatched records. The mismatches are determined based on the presence of missing rows, extra rows,
    and differences in specified columns.

    Args:
        df: The input DataFrame containing the comparison results.
        condition_col: The name of the column to add, which will store mismatch information.
        row_missing_col: The name of the column indicating missing rows.
        row_extra_col: The name of the column indicating extra rows.
        columns_changed_col: The name of the column containing differences in compared columns.
        filter_col: The name of the column used to filter records for comparison.

    Returns:
        The input DataFrame with the added *condition_col* containing mismatch information.
    """
    all_is_ok = ~F.col(row_missing_col) & ~F.col(row_extra_col) & (F.size(F.col(columns_changed_col)) == 0)
    return df.withColumn(
        condition_col,
        F.when(
            # apply filter but skip it for missing rows (null filter col)
            (F.col(f"df.{filter_col}").isNull() | F.col(f"df.{filter_col}")) & ~all_is_ok,
            F.struct(
                F.col(row_missing_col).alias("row_missing"),
                F.col(row_extra_col).alias("row_extra"),
                F.col(columns_changed_col).alias("changed"),
            ),
        ),
    )


def _build_aggregate_expression(
    aggr_type: str,
    filtered_expr: Column,
    aggr_params: dict[str, Any] | None,
) -> Column:
    """
    Build the appropriate Spark aggregate expression based on function type and parameters.

    Args:
        aggr_type: Name of the aggregate function.
        filtered_expr: Column expression with filters applied.
        aggr_params: Optional parameters for the aggregate function.

    Returns:
        Spark Column expression for the aggregate.

    Raises:
        MissingParameterError: If required parameters are missing for specific aggregates.
        InvalidParameterError: If the aggregate function is not found or parameters are invalid.
    """
    if aggr_type == "count_distinct":
        return F.countDistinct(filtered_expr)

    if aggr_type in {"percentile", "approx_percentile"}:
        if not aggr_params or "percentile" not in aggr_params:
            raise MissingParameterError(
                f"'{aggr_type}' requires aggr_params with 'percentile' key (e.g., {{'percentile': 0.95}})"
            )
        pct = aggr_params["percentile"]
        # Pass through any additional parameters to Spark (e.g., accuracy, frequency)
        # Spark will validate parameter names and types at runtime
        other_params = {k: v for k, v in aggr_params.items() if k != "percentile"}

        try:
            aggr_func = getattr(F, aggr_type)
            return aggr_func(filtered_expr, pct, **other_params)
        except Exception as exc:
            raise InvalidParameterError(f"Failed to build '{aggr_type}' expression: {exc}") from exc

    try:
        aggr_func = getattr(F, aggr_type)
        if aggr_params:
            return aggr_func(filtered_expr, **aggr_params)
        return aggr_func(filtered_expr)
    except AttributeError as exc:
        raise InvalidParameterError(
            f"Aggregate function '{aggr_type}' not found in pyspark.sql.functions. "
            f"Verify the function name is correct, or check if your Databricks Runtime version supports this function. "
            f"Some newer aggregate functions (e.g., mode, median) require DBR 15.4+ (Spark 3.5+). "
            f"See: https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-functions-builtin-alpha"
        ) from exc
    except Exception as exc:
        raise InvalidParameterError(f"Failed to build '{aggr_type}' expression: {exc}") from exc


def _validate_aggregate_return_type(
    df: DataFrame,
    aggr_type: str,
    metric_col: str,
) -> None:
    """
    Validate aggregate returns a numeric type that can be compared to limits.

    This is a schema-only validation (no data scanning) that checks whether the aggregate
    function returns a type compatible with numeric comparisons.

    Args:
        df: DataFrame containing the aggregate result column.
        aggr_type: Name of the aggregate function being validated.
        metric_col: Column name containing the aggregate result.

    Raises:
        InvalidParameterError: If the aggregate returns a non-numeric type (Array, Map, Struct)
            that cannot be compared to limits.
    """
    result_type = df.schema[metric_col].dataType
    if isinstance(result_type, (types.ArrayType, types.MapType, types.StructType)):
        raise InvalidParameterError(
            f"Aggregate function '{aggr_type}' returned {result_type.typeName()} "
            f"which cannot be compared to numeric limits. "
            f"Use aggregate functions that return numeric values (e.g., count, sum, avg)."
        )


def _is_aggr_compare(
    column: str | Column,
    limit: int | float | str | Column,
    aggr_type: str,
    aggr_params: dict[str, Any] | None,
    group_by: list[str | Column] | None,
    row_filter: str | None,
    compare_op: Callable[[Column, Column], Column],
    compare_op_label: str,
    compare_op_name: str,
) -> tuple[Column, Callable]:
    """
    Helper to build aggregation comparison checks with a given operator.

    Constructs a condition and closure that verify whether an aggregation on a column
    (or groups of columns) satisfies a comparison against a limit (e.g., greater than).

    Args:
        column: Column name (str) or Column expression to aggregate.
        limit: Numeric value, column name, or SQL expression for the limit. String literals must be single quoted, e.g. 'string_value'.
        aggr_type: Aggregation type. Curated functions include 'count', 'sum', 'avg', 'min', 'max',
            'count_distinct', 'stddev', 'percentile', and more. Any Databricks built-in aggregate
            function is supported (will trigger a warning for non-curated functions).
        aggr_params: Optional dictionary of parameters for aggregate functions that require them
            (e.g., percentile functions need {"percentile": 0.95}).
        group_by: Optional list of columns or Column expressions to group by.
        row_filter: Optional SQL expression to filter rows before aggregation.
        compare_op: Comparison operator (e.g., operator.gt, operator.lt).
        compare_op_label: Human-readable label for the comparison (e.g., 'greater than').
        compare_op_name: Name identifier for the comparison (e.g., 'greater_than').

    Returns:
        A tuple of:
            - A Spark Column representing the condition for the aggregation check.
            - A closure that applies the aggregation check logic.

    Raises:
        InvalidParameterError: If an aggregate returns non-numeric types or is not found.
        MissingParameterError: If required parameters for specific aggregates are not provided.
    """
    # Warn if using non-curated aggregate function
    is_curated = aggr_type in CURATED_AGGR_FUNCTIONS
    if not is_curated:
        warnings.warn(
            f"Using non-curated aggregate function '{aggr_type}'. "
            f"Curated functions: {', '.join(sorted(CURATED_AGGR_FUNCTIONS))}. "
            f"Non-curated aggregates must return a single numeric value per group.",
            UserWarning,
            stacklevel=3,
        )

    aggr_col_str_norm, aggr_col_str, aggr_col_expr = get_normalized_column_and_expr(column)

    group_by_list_str = (
        ", ".join(col if isinstance(col, str) else get_column_name_or_alias(col) for col in group_by)
        if group_by
        else None
    )
    group_by_str = (
        "_".join(col if isinstance(col, str) else get_column_name_or_alias(col) for col in group_by)
        if group_by
        else None
    )

    name = (
        f"{aggr_col_str_norm}_{aggr_type.lower()}_group_by_{group_by_str}_{compare_op_name}_limit".lstrip("_")
        if group_by_str
        else f"{aggr_col_str_norm}_{aggr_type.lower()}_{compare_op_name}_limit".lstrip("_")
    )

    limit_expr = get_limit_expr(limit)

    unique_str = uuid.uuid4().hex  # make sure any column added to the dataframe is unique
    condition_col = f"__condition_{aggr_col_str_norm}_{aggr_type}_{compare_op_name}_{unique_str}"
    metric_col = f"__metric_{aggr_col_str_norm}_{aggr_type}_{compare_op_name}_{unique_str}"

    def apply(df: DataFrame) -> DataFrame:
        """
        Apply the aggregation comparison check logic to the DataFrame.

        Computes the specified aggregation over the dataset (or groups if provided)
        and compares the result against the limit. Adds condition and metric columns
        used for check evaluation.

        Args:
            df: The input DataFrame to validate.

        Returns:
            The DataFrame with additional condition and metric columns for aggregation validation.
        """
        filter_col = F.expr(row_filter) if row_filter else F.lit(True)
        filtered_expr = F.when(filter_col, aggr_col_expr) if row_filter else aggr_col_expr

        # Build aggregation expression
        aggr_expr = _build_aggregate_expression(aggr_type, filtered_expr, aggr_params)

        if group_by:
            # Convert group_by to Column expressions (reused for both window and groupBy approaches)
            group_cols = [F.col(col) if isinstance(col, str) else col for col in group_by]

            # Check if aggregate is incompatible with window functions (e.g., count_distinct with DISTINCT)
            if aggr_type in WINDOW_INCOMPATIBLE_AGGREGATES:
                # Use two-stage aggregation: groupBy + join (instead of window functions)
                # This is required for aggregates like count_distinct that don't support window DISTINCT operations
                agg_df = df.groupBy(*group_cols).agg(aggr_expr.alias(metric_col))

                # Join aggregated metrics back to original DataFrame to maintain row-level granularity
                # Note: Aliased Column expressions in group_by are not supported for window-incompatible
                # aggregates (e.g., count_distinct). Use string column names or simple F.col() expressions.
                join_cols = [col if isinstance(col, str) else get_column_name_or_alias(col) for col in group_by]
                df = df.join(agg_df, on=join_cols, how="left")
            else:
                # Use standard window function approach for window-compatible aggregates
                window_spec = Window.partitionBy(*group_cols)
                df = df.withColumn(metric_col, aggr_expr.over(window_spec))

                # Validate non-curated aggregates (type check only - window functions return same row count)
                if not is_curated:
                    _validate_aggregate_return_type(df, aggr_type, metric_col)
        else:
            # When no group-by columns are provided, using partitionBy would move all rows into a single partition,
            # forcing the window function to process the entire dataset in one task.
            # To avoid this performance issue, we compute a global aggregation instead.
            # Note: The aggregation naturally returns a single row without a groupBy clause,
            # so no explicit limit is required (informational only).
            agg_df = df.select(aggr_expr.alias(metric_col)).limit(1)

            # Validate non-curated aggregates (type check only - we already limited to 1 row)
            if not is_curated:
                _validate_aggregate_return_type(agg_df, aggr_type, metric_col)

            df = df.crossJoin(agg_df)  # bring the metric across all rows

        df = df.withColumn(condition_col, compare_op(F.col(metric_col), limit_expr))

        return df

    # Get human-readable display name for aggregate function (including params if present)
    aggr_display_name = _get_aggregate_display_name(aggr_type, aggr_params)

    condition = make_condition(
        condition=F.col(condition_col),
        message=F.concat_ws(
            "",
            F.lit(f"{aggr_display_name} value "),
            F.col(metric_col).cast("string"),
            F.lit(f" in column '{aggr_col_str}'"),
            F.lit(f"{' per group of columns ' if group_by_list_str else ''}"),
            F.lit(f"'{group_by_list_str}'" if group_by_list_str else ""),
            F.lit(f" is {compare_op_label} limit: "),
            limit_expr.cast("string"),
        ),
        alias=name,
    )

    return condition, apply


def _get_ref_df(
    ref_df_name: str | None, ref_table: str | None, ref_dfs: dict[str, DataFrame] | None, spark: SparkSession
) -> DataFrame:
    """
    Retrieve the reference DataFrame based on the provided parameters.

    This helper fetches the reference DataFrame either from the supplied dictionary of DataFrames
    (using *ref_df_name* as the key) or by reading a table from the Unity Catalog (using *ref_table*).
    It raises an error if the necessary reference source is not properly specified or cannot be found.

    Args:
        ref_df_name: The key name of the reference DataFrame in the provided dictionary (optional).
        ref_table: The name of the reference table to read from the Spark catalog (optional).
        ref_dfs: A dictionary mapping reference DataFrame names to DataFrame objects.
        spark: The active SparkSession used to read the reference table if needed.

    Returns:
        A Spark DataFrame representing the reference dataset.

    Raises:
        MissingParameterError: If neither or both of *ref_df_name* and *ref_table* are provided,
            or if the specified reference DataFrame is not found, or ref_table is not provided.
        InvalidParameterError: If *ref_table* is provided but is an empty string.
    """
    if ref_df_name:
        if ref_dfs is None:
            raise MissingParameterError(
                "Reference DataFrames dictionary not provided. "
                f"Provide '{ref_df_name}' reference DataFrame when applying the checks."
            )

        if ref_df_name not in ref_dfs:
            raise MissingParameterError(
                f"Reference DataFrame with key '{ref_df_name}' not found. "
                f"Provide reference '{ref_df_name}' DataFrame when applying the checks."
            )

        return ref_dfs[ref_df_name]

    if not ref_table:
        raise InvalidParameterError("'ref_table' must be a non-empty string.")

    return spark.table(ref_table)


def _cleanup_alias_name(column: str) -> str:
    """
    Sanitize a column name for use as an alias by replacing dots with underscores.

    This helper avoids issues when using struct field names as aliases,
    since dots in column names can cause ambiguity in Spark SQL.

    Args:
        column: The column name as a string.

    Returns:
        A sanitized column name with dots replaced by underscores.
    """
    # avoid issues with structs
    return column.replace(".", "_")


def get_limit_expr(
    limit: int | float | datetime.date | datetime.datetime | str | Column | None = None,
) -> Column:
    """
    Generate a Spark Column expression for a limit value.

    This helper converts the provided limit (literal, string expression, or Column)
    into a Spark Column expression suitable for use in conditions.

    Args:
        limit: The limit to use in the condition. Can be a literal (int, float, date, datetime),
            a string SQL expression, or a Spark Column.

    Returns:
        A Spark Column expression representing the limit.

    Raises:
        MissingParameterError: If the limit is not provided (None).
    """
    if limit is None:
        raise MissingParameterError("Limit is not provided.")

    if isinstance(limit, str):
        return F.expr(limit)
    if isinstance(limit, Column):
        return limit
    return F.lit(limit)


def get_normalized_column_and_expr(column: str | Column) -> tuple[str, str, Column]:
    """
    Extract the normalized column name, original column name as string, and column expression.

    This helper ensures that both a normalized string representation and a raw string representation
    of the column are available, along with the corresponding Spark Column expression.
    Useful for generating aliases, conditions, and consistent messaging.

    Args:
        column: The input column, provided as either a string column name or a Spark Column expression.

    Returns:
        A tuple containing:
            - Normalized column name as a string (suitable for use in aliases or metadata).
            - Original column name as a string.
            - Spark Column expression corresponding to the input.
    """
    col_expr = _get_column_expr(column)
    column_str = get_column_name_or_alias(col_expr)
    col_str_norm = get_column_name_or_alias(col_expr, normalize=True)

    return col_str_norm, column_str, col_expr


def _get_aggregate_display_name(aggr_type: str, aggr_params: dict[str, Any] | None = None) -> str:
    """
    Get a human-readable display name for an aggregate function.

    This helper provides user-friendly names for aggregate functions in error messages,
    transforming technical function names (e.g., 'count_distinct') into readable text
    (e.g., 'Distinct value count').

    Args:
        aggr_type: The aggregate function name (e.g., 'count_distinct', 'max', 'avg').
        aggr_params: Optional parameters passed to the aggregate function.

    Returns:
        A human-readable display name for the aggregate function, including parameters
        if provided. For non-curated functions, returns the function name in quotes
        with 'value' suffix.
    """
    # Get base display name (curated functions have friendly names, others show function name in quotes)
    base_name = CURATED_AGGR_FUNCTIONS.get(aggr_type, f"'{aggr_type}'")

    # Add parameters if present
    if aggr_params:
        # Format parameters as key=value pairs
        param_str = ", ".join(f"{k}={v}" for k, v in aggr_params.items())
        return f"{base_name} ({param_str})"

    return base_name


def _get_column_expr(column: Column | str) -> Column:
    """
    Convert a column input (string or Column) into a Spark Column expression.

    Args:
        column: The input column, provided as either a string column name or a Spark Column expression.

    Returns:
        A Spark Column expression corresponding to the input.
    """
    return F.expr(column) if isinstance(column, str) else column


def _handle_fk_composite_keys(columns: list[str | Column], ref_columns: list[str | Column], not_null_condition: Column):
    """
    Construct composite key expressions and not-null condition for foreign key validation.

    This helper function builds structured column expressions for composite foreign keys
    in both the main and reference datasets. It also updates the not-null condition to skip any rows where
    any of the composite key columns are NULL, in line with SQL ANSI foreign key semantics.

    Args:
        columns: List of columns (names or expressions) from the input DataFrame forming the composite key.
        ref_columns: List of columns (names or expressions) from the reference DataFrame forming the composite key.
        not_null_condition: Existing condition Column to be combined with not-null checks for the composite key.

    Returns:
        A tuple containing:
            - Column expression representing the composite key in the input DataFrame.
            - Column expression representing the composite key in the reference DataFrame.
            - Updated not-null condition Column ensuring no NULLs in any composite key field.
    """
    # Extract column names from columns for consistent aliasing
    columns_names = [get_column_name_or_alias(col) if not isinstance(col, str) else col for col in columns]

    # skip nulls from comparison for ANSI standard compliance
    # if any column is Null, skip the row from the check
    for col_name in columns_names:
        not_null_condition = not_null_condition & F.col(col_name).isNotNull()

    column = _build_fk_composite_key_struct(columns, columns_names)
    ref_column = _build_fk_composite_key_struct(ref_columns, columns_names)

    return column, ref_column, not_null_condition


def _build_fk_composite_key_struct(columns: list[str | Column], columns_names: list[str]):
    """
    Build a Spark struct expression for composite foreign key validation with consistent field aliases.

    This helper constructs a Spark expression from the provided list of columns (names or Column expressions),
    ensuring each field in the struct has a consistent alias based on the provided column names.
    This is used for comparing composite foreign keys as a single struct value.

    Args:
        columns: List of columns (names as str or Spark Column expressions) to include in the struct.
        columns_names: List of normalized column names (str) to use as aliases for the struct fields.

    Returns:
        A Spark Column representing a struct with the specified columns and aliases.
    """
    struct_fields = []
    for alias, col in zip(columns_names, columns):
        if isinstance(col, str):
            struct_fields.append(F.col(col).alias(alias))
        else:
            struct_fields.append(col.alias(alias))
    return F.struct(*struct_fields)


def _validate_ref_params(
    columns: list[str | Column], ref_columns: list[str | Column], ref_df_name: str | None, ref_table: str | None
):
    """
    Validate reference parameters to ensure correctness and prevent ambiguity.

    This helper verifies that:
    - Exactly one of *ref_df_name* or *ref_table* is provided (not both, not neither).
    - The number of columns in the input DataFrame matches the number of reference columns.

    Args:
        columns: List of columns from the input DataFrame.
        ref_columns: List of columns from the reference DataFrame or table.
        ref_df_name: Optional name of the reference DataFrame.
        ref_table: Optional name of the reference table.

    Raises:
        MissingParameterError:
            - if neither *ref_df_name* nor *ref_table* is provided.
        InvalidParameterError:
            - if both *ref_df_name* and *ref_table* are provided.
            - if the number of *columns* and *ref_columns* do not match.
    """
    if ref_df_name is not None and ref_table is not None:
        raise InvalidParameterError(
            "Both 'ref_df_name' and 'ref_table' were provided. Please provide only one to avoid ambiguity."
        )

    if not ref_df_name and not ref_table:
        raise MissingParameterError("Either 'ref_df_name' or 'ref_table' is required but neither was provided.")

    if not isinstance(columns, list) or not isinstance(ref_columns, list):
        raise InvalidParameterError("'columns' and 'ref_columns' must be lists.")

    if len(columns) != len(ref_columns):
        raise InvalidParameterError(
            f"'columns' has {len(columns)} entries but 'ref_columns' has {len(ref_columns)}. "
            "Both must have the same length to allow comparison."
        )


def _does_not_match_pattern(column: Column, pattern: DQPattern) -> Column:
    """
    Internal function that returns a Boolean Column indicating if values
    in the column do NOT match the given pattern.
    """
    col_expr = _get_column_expr(column)
    return ~col_expr.rlike(pattern.value)


def _extract_octets_to_bits(column: Column, pattern: str) -> Column:
    """Extracts 4 octets from an IP column and returns the binary string."""
    ip_match = F.regexp_extract(column, pattern, 0)
    octets = F.split(ip_match, r"\.")
    octets_bin = [F.lpad(F.conv(octets[i], 10, 2), 8, "0") for i in range(IPV4_MAX_OCTET_COUNT)]
    return F.concat(*octets_bin).alias("ip_bits")


def _convert_ipv4_to_bits(ip_col: Column) -> Column:
    """Returns 32-bit binary string from IPv4 address (no CIDR). (e.g., '11000000101010000000000100000001')."""
    return _extract_octets_to_bits(ip_col, DQPattern.IPV4_ADDRESS.value)


def _convert_ipv4_cidr_to_bits_and_prefix(cidr_col: Column) -> tuple[Column, Column]:
    """Returns binary IP and prefix length from CIDR (e.g., '192.168.1.0/24')."""
    ip_bits = _extract_octets_to_bits(cidr_col, DQPattern.IPV4_CIDR_BLOCK.value)
    # The 5th capture group in the regex pattern corresponds to the CIDR prefix length.
    prefix_length = F.regexp_extract(cidr_col, DQPattern.IPV4_CIDR_BLOCK.value, 5).cast("int").alias("prefix_length")
    return ip_bits, prefix_length


def _get_network_address(ip_bits: Column, prefix_length: Column, total_bits: int) -> Column:
    """
    Returns the network address from IP bits using the CIDR prefix length.

    Args:
        ip_bits: Binary string representation of the IP address (32 or 128 bits)
        prefix_length: Prefix length for CIDR notation
        total_bits: Total number of bits in the IP address (32 for IPv4)

    Returns:
        Network address as a binary string of the same total length
    """
    return F.rpad(F.substring(ip_bits, 1, prefix_length), total_bits, "0")


def _build_is_valid_ipv6_address_udf() -> Callable:
    """
    Build a user-defined function (UDF) to check if a string is a valid IPv6 address.

    Returns:
        Callable: A UDF that checks if a string is a valid IPv6 address
    """

    @F.pandas_udf("boolean")  # type: ignore[call-overload]
    def _is_valid_ipv6_address_udf(column: pd.Series) -> pd.Series:
        # Self-contained validation logic to avoid serialization issues
        def is_valid_ipv6_local(ip_str):
            if pd.isna(ip_str) or ip_str is None:
                return False

            try:
                # Import inside UDF to avoid serialization issues
                ipaddress.IPv6Address(str(ip_str))
                return True
            except (ipaddress.AddressValueError, TypeError):
                return False

        return column.apply(is_valid_ipv6_local)

    return _is_valid_ipv6_address_udf


def _build_is_ipv6_address_in_cidr_udf() -> Callable:
    """
    Build a user-defined function (UDF) to check if an IPv6 address is in a CIDR block.

    Returns:
        Callable: A UDF that checks if an IPv6 address is in a CIDR block
    """

    @F.pandas_udf("boolean")  # type: ignore[call-overload]
    def handler(ipv6_column: pd.Series, cidr_column: pd.Series) -> pd.Series:
        # Self-contained CIDR checking logic to avoid serialization issues
        def ipv6_in_cidr_local(ip_str, cidr_str):
            if pd.isna(ip_str) or pd.isna(cidr_str) or ip_str is None or cidr_str is None:
                return False

            try:
                # Import inside UDF to avoid serialization issues
                ip_obj = ipaddress.IPv6Address(str(ip_str))
                network = ipaddress.IPv6Network(str(cidr_str), strict=False)
                return ip_obj in network
            except (ipaddress.AddressValueError, ipaddress.NetmaskValueError, TypeError):
                return False

        return ipv6_column.combine(cidr_column, ipv6_in_cidr_local)

    return handler


def _is_valid_ipv6_cidr_block(cidr: str) -> bool:
    """Validate if the string is a valid IPv6 CIDR block.

    Args:
        cidr: The CIDR block string to validate.
    Returns:
        True if the string is a valid CIDR block, False otherwise.
    """

    try:
        if "/" not in cidr:
            return False
        ipaddress.IPv6Network(cidr, strict=False)
        return True
    except (ipaddress.AddressValueError, ipaddress.NetmaskValueError):
        return False


def _apply_dataset_level_sql_check(
    df: DataFrame,
    spark: SparkSession,
    query_resolved: str,
    condition_column: str,
    unique_condition_column: str,
    row_filter: str | None,
) -> DataFrame:
    """
    Apply a dataset-level SQL check where all rows get the same validation result.

    Args:
        df: Input DataFrame to apply the check to.
        spark: SparkSession for executing SQL.
        query_resolved: The resolved SQL query (with placeholders replaced).
        condition_column: Name of the condition column in the query result.
        unique_condition_column: Unique name for the condition column in the output.
        row_filter: Optional SQL expression for filtering which rows receive the check result.

    Returns:
        DataFrame with the condition column added.
    """
    # Query should only return the condition column
    user_query_df = spark.sql(query_resolved).select(F.col(condition_column).alias(unique_condition_column))

    # Capture up to two rows to detect accidental multi-row outputs
    condition_rows = user_query_df.take(2)
    if not condition_rows:
        # No rows returned: treat as condition not met (False)
        condition_value = False
    elif len(condition_rows) > 1:
        raise InvalidParameterError(
            "Dataset-level sql_query without merge_columns must return exactly one row. "
            "Provide merge_columns for row-level checks or aggregate the query to a single row."
        )
    else:
        condition_value = condition_rows[0][unique_condition_column]

    # Apply the condition consistently with row_filter behavior:
    # - If row_filter is provided, only rows matching the filter get the condition value
    # - Rows not matching the filter get None (consistent with row-level checks)
    if row_filter:
        filter_expr = F.expr(row_filter)
        result_df = df.withColumn(
            unique_condition_column, F.when(filter_expr, F.lit(condition_value)).otherwise(F.lit(None))
        )
    else:
        # No filter: apply condition to all rows
        result_df = df.withColumn(unique_condition_column, F.lit(condition_value))

    return result_df


def _validate_sql_query_params(query: str, merge_columns: list[str] | None) -> None:
    """
    Validate SQL query parameters to ensure correctness and safety.
    This helper verifies that the SQL query is safe for execution.

    Args:
        query: The SQL query string to validate.
        merge_columns: Optional list of column names (validated when provided).

    Raises:
        UnsafeSqlQueryError: If the SQL query is unsafe.
        InvalidParameterError: If merge_columns is provided but not a sequence of strings.
    """
    if not is_sql_query_safe(query):
        raise UnsafeSqlQueryError(
            "Provided SQL query is not safe for execution. Please ensure it does not contain any unsafe operations."
        )

    if merge_columns is None:
        return

    if not isinstance(merge_columns, Sequence) or isinstance(merge_columns, str):
        raise InvalidParameterError(
            "'merge_columns' must be a sequence of column names (e.g., list or tuple) when provided."
        )

    invalid_columns = [col for col in merge_columns if not isinstance(col, str) or not col]
    if invalid_columns:
        raise InvalidParameterError("'merge_columns' entries must be non-empty strings.")


def _calculate_median_absolute_deviation(df: DataFrame, column: str, filter_condition: str | None) -> tuple[Any, Any]:
    """
    Calculate the Median Absolute Deviation (MAD) for a numeric column.

    The MAD is a robust measure of variability based on the median, calculated as:
    MAD = median(|X_i - median(X)|)

    This is useful for outlier detection as it is more robust to outliers than
    standard deviation.

    Args:
        df: PySpark DataFrame
        column: Name of the numeric column to calculate MAD for
        filter_condition: Filter to apply before calculation (optional)

    Returns:
        The Median and Absolute Deviation values
    """
    if filter_condition is not None:
        df = df.filter(filter_condition)

    # Step 1: Calculate the median of the column
    median_value = df.agg(F.percentile_approx(column, 0.5)).collect()[0][0]

    # Step 2: Calculate absolute deviations from the median
    df_with_deviations = df.select(F.abs(F.col(column) - F.lit(median_value)).alias("absolute_deviation"))

    # Step 3: Calculate the median of absolute deviations
    mad = df_with_deviations.agg(F.percentile_approx("absolute_deviation", 0.5)).collect()[0][0]

    return median_value, mad
