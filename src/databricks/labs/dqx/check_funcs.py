import datetime
import re
import uuid
from collections.abc import Callable
import operator as py_operator
from enum import Enum

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
)

_IPV4_OCTET = r"(25[0-5]|2[0-4]\d|1\d{2}|[1-9]?\d)"


class DQPattern(Enum):
    """Enum class to represent DQ patterns used to match data in columns."""

    IPV4_ADDRESS = rf"^{_IPV4_OCTET}\.{_IPV4_OCTET}\.{_IPV4_OCTET}\.{_IPV4_OCTET}$"
    IPV4_CIDR_BLOCK = rf"^{_IPV4_OCTET}\.{_IPV4_OCTET}\.{_IPV4_OCTET}\.{_IPV4_OCTET}\/(3[0-2]|[12]?\d)$"


def make_condition(condition: Column, message: Column | str, alias: str) -> Column:
    """Helper function to create a condition column.

    :param condition: condition expression.
        Pass the check if the condition evaluates to False.
        Fail the check if condition evaluates to True.
    :param message: message to output - it could be either `Column` object, or string constant
    :param alias: name for the resulting column
    :return: an instance of `Column` type, that either returns string if condition is evaluated to `true`,
             or `null` if condition is evaluated to `false`
    """
    if isinstance(message, str):
        msg_col = F.lit(message)
    else:
        msg_col = message

    return (F.when(condition, msg_col).otherwise(F.lit(None).cast("string"))).alias(_cleanup_alias_name(alias))


def matches_pattern(column: str | Column, pattern: DQPattern) -> Column:
    """Checks whether the values in the input column match a given pattern.

    :param column: column to check; can be a string column name or a column expression
    :param pattern: pattern to match against
    :return: Column object for condition
    """
    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
    condition = ~col_expr.rlike(pattern.value)
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

    :param column: column to check; can be a string column name or a column expression
    :param trim_strings: boolean flag to trim spaces from strings
    :return: Column object for condition
    """
    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
    if trim_strings:
        col_expr = F.trim(col_expr).alias(col_str_norm)
    condition = col_expr.isNull() | (col_expr.cast("string").isNull() | (col_expr.cast("string") == F.lit("")))
    return make_condition(
        condition, f"Column '{col_expr_str}' value is null or empty", f"{col_str_norm}_is_null_or_empty"
    )


@register_rule("row")
def is_not_empty(column: str | Column) -> Column:
    """Checks whether the values in the input column are not empty (but may be null).

    :param column: column to check; can be a string column name or a column expression
    :return: Column object for condition
    """
    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
    condition = col_expr.cast("string") == F.lit("")
    return make_condition(condition, f"Column '{col_expr_str}' value is empty", f"{col_str_norm}_is_empty")


@register_rule("row")
def is_not_null(column: str | Column) -> Column:
    """Checks whether the values in the input column are not null.

    :param column: column to check; can be a string column name or a column expression
    :return: Column object for condition
    """
    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
    return make_condition(col_expr.isNull(), f"Column '{col_expr_str}' value is null", f"{col_str_norm}_is_null")


@register_rule("row")
def is_not_null_and_is_in_list(column: str | Column, allowed: list) -> Column:
    """Checks whether the values in the input column are not null and present in the list of allowed values.

    :param column: column to check; can be a string column name or a column expression
    :param allowed: list of allowed values (actual values or Column objects)
    :return: Column object for condition
    """
    if not allowed:
        raise ValueError("allowed list is not provided.")

    allowed_cols = [item if isinstance(item, Column) else F.lit(item) for item in allowed]
    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
    condition = col_expr.isNull() | ~col_expr.isin(*allowed_cols)
    return make_condition(
        condition,
        F.concat_ws(
            "",
            F.lit("Value '"),
            F.when(col_expr.isNull(), F.lit("null")).otherwise(col_expr.cast("string")),
            F.lit(f"' in Column '{col_expr_str}' is null or not in the allowed list: ["),
            F.concat_ws(", ", *allowed_cols),
            F.lit("]"),
        ),
        f"{col_str_norm}_is_null_or_is_not_in_the_list",
    )


@register_rule("row")
def is_in_list(column: str | Column, allowed: list) -> Column:
    """Checks whether the values in the input column are present in the list of allowed values
    (null values are allowed).

    :param column: column to check; can be a string column name or a column expression
    :param allowed: list of allowed values (actual values or Column objects)
    :return: Column object for condition
    """
    if not allowed:
        raise ValueError("allowed list is not provided.")

    allowed_cols = [item if isinstance(item, Column) else F.lit(item) for item in allowed]
    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
    condition = ~col_expr.isin(*allowed_cols)
    return make_condition(
        condition,
        F.concat_ws(
            "",
            F.lit("Value '"),
            F.when(col_expr.isNull(), F.lit("null")).otherwise(col_expr.cast("string")),
            F.lit(f"' in Column '{col_expr_str}' is not in the allowed list: ["),
            F.concat_ws(", ", *allowed_cols),
            F.lit("]"),
        ),
        f"{col_str_norm}_is_not_in_the_list",
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

    :param expression: SQL expression. Fail if expression evaluates to True, pass if it evaluates to False.
    :param msg: optional message of the `Column` type, automatically generated if None
    :param name: optional name of the resulting column, automatically generated if None
    :param negate: if the condition should be negated (true) or not. For example, "col is not null" will mark null
    values as "bad". Although sometimes it's easier to specify it other way around "col is null" + negate set to False
    :param columns: optional list of columns to be used for reporting. Unused in the actual logic.
    :return: new Column
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

    :param column1: first column to check; can be a string column name or a column expression
    :param column2: second column to check; can be a string column name or a column expression
    :param days: number of days
    :param negate: if the condition should be negated (true) or not; if negated, the check will fail when values in the
                    first column are at least N days older than values in the second column
    :return: new Column
    """
    col_str_norm1, col_expr_str1, col_expr1 = _get_normalized_column_and_expr(column1)
    col_str_norm2, col_expr_str2, col_expr2 = _get_normalized_column_and_expr(column2)

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

    :param column: column to check; can be a string column name or a column expression
    :param days: number of days
    :param curr_date: (optional) set current date
    :param negate: if the condition should be negated (true) or not; if negated, the check will fail when values in the
                    first column are at least N days older than values in the second column
    :return: new Column
    """
    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
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

    :param column: column to check; can be a string column name or a column expression
    :param offset: offset (in seconds) to add to the current timestamp at time of execution
    :param curr_timestamp: (optional) set current timestamp
    :return: new Column
    """
    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
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

    :param column: column to check; can be a string column name or a column expression
    :param offset: offset (in seconds) to add to the current timestamp at time of execution
    :param curr_timestamp: (optional) set current timestamp
    :return: new Column
    """
    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
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
def is_not_less_than(
    column: str | Column, limit: int | datetime.date | datetime.datetime | str | Column | None = None
) -> Column:
    """Checks whether the values in the input column are not less than the provided limit.

    :param column: column to check; can be a string column name or a column expression
    :param limit: limit to use in the condition as number, date, timestamp, column name or sql expression
    :return: new Column
    """
    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
    limit_expr = _get_limit_expr(limit)
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
    column: str | Column, limit: int | datetime.date | datetime.datetime | str | Column | None = None
) -> Column:
    """Checks whether the values in the input column are not greater than the provided limit.

    :param column: column to check; can be a string column name or a column expression
    :param limit: limit to use in the condition as number, date, timestamp, column name or sql expression
    :return: new Column
    """
    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
    limit_expr = _get_limit_expr(limit)
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
    min_limit: int | datetime.date | datetime.datetime | str | Column | None = None,
    max_limit: int | datetime.date | datetime.datetime | str | Column | None = None,
) -> Column:
    """Checks whether the values in the input column are in the provided limits (inclusive of both boundaries).

    :param column: column to check; can be a string column name or a column expression
    :param min_limit: min limit to use in the condition as number, date, timestamp, column name or sql expression
    :param max_limit: max limit to use in the condition as number, date, timestamp, column name or sql expression
    :return: new Column
    """
    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
    min_limit_expr = _get_limit_expr(min_limit)
    max_limit_expr = _get_limit_expr(max_limit)

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
    min_limit: int | datetime.date | datetime.datetime | str | Column | None = None,
    max_limit: int | datetime.date | datetime.datetime | str | Column | None = None,
) -> Column:
    """Checks whether the values in the input column are outside the provided limits (inclusive of both boundaries).

    :param column: column to check; can be a string column name or a column expression
    :param min_limit: min limit to use in the condition as number, date, timestamp, column name or sql expression
    :param max_limit: min limit to use in the condition as number, date, timestamp, column name or sql expression
    :return: new Column
    """
    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
    min_limit_expr = _get_limit_expr(min_limit)
    max_limit_expr = _get_limit_expr(max_limit)

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

    :param column: column to check; can be a string column name or a column expression
    :param regex: regex to check
    :param negate: if the condition should be negated (true) or not
    :return: Column object for condition
    """
    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
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

    :param column: column to check; can be a string column name or a column expression
    :return: Column object for condition
    """
    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
    condition = col_expr.isNull() | (F.size(col_expr) == 0)
    return make_condition(
        condition, f"Column '{col_expr_str}' is null or empty array", f"{col_str_norm}_is_null_or_empty_array"
    )


@register_rule("row")
def is_valid_date(column: str | Column, date_format: str | None = None) -> Column:
    """Checks whether the values in the input column have valid date formats.

    :param column: column to check; can be a string column name or a column expression
    :param date_format: date format (e.g. 'yyyy-mm-dd')
    :return: Column object for condition
    """
    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
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

    :param column: column to check; can be a string column name or a column expression
    :param timestamp_format: timestamp format (e.g. 'yyyy-mm-dd HH:mm:ss')
    :return: Column object for condition
    """
    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
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

    :param column: column to check; can be a string column name or a column expression
    :return: Column object for condition
    """
    return matches_pattern(column, DQPattern.IPV4_ADDRESS)


@register_rule("row")
def is_ipv4_address_in_cidr(column: str | Column, cidr_block: str) -> Column:
    """
    Checks if an IP column value falls within the given CIDR block.

    :param column: column to check; can be a string column name or a column expression
    :param cidr_block: CIDR block string (e.g., '192.168.1.0/24')
    :raises ValueError: If cidr_block is not a valid string in CIDR notation.

    :return: Column object for condition
    """

    if not cidr_block:
        raise ValueError("'cidr_block' must be a non-empty string.")

    if not re.match(DQPattern.IPV4_CIDR_BLOCK.value, cidr_block):
        raise ValueError(f"CIDR block '{cidr_block}' is not a valid IPv4 CIDR block.")

    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
    cidr_col_expr = F.lit(cidr_block)
    ipv4_msg_col = is_valid_ipv4_address(column)

    ip_bits_col = _convert_ipv4_to_bits(col_expr)
    cidr_ip_bits_col, cidr_prefix_length_col = _convert_cidr_to_bits_and_prefix(cidr_col_expr)
    ip_net = _get_network_address(ip_bits_col, cidr_prefix_length_col)
    cidr_net = _get_network_address(cidr_ip_bits_col, cidr_prefix_length_col)

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


@register_rule("dataset")
def is_unique(
    columns: list[str | Column],
    nulls_distinct: bool = True,
    row_filter: str | None = None,
) -> tuple[Column, Callable]:
    """
    Build a uniqueness check condition and closure for dataset-level validation.

    This function checks whether the specified columns contain unique values within the dataset
    and reports rows with duplicate combinations. When `nulls_distinct`
    is True (default), rows with NULLs are treated as distinct (SQL ANSI behavior); otherwise,
    NULLs are treated as equal when checking for duplicates.

    In streaming, uniqueness is validated within individual micro-batches only.

    :param columns: List of column names (str) or Spark Column expressions to validate for uniqueness.
    :param nulls_distinct: Whether NULLs are treated as distinct (default: True).
    :param row_filter: Optional SQL expression for filtering rows before checking uniqueness.
    Auto-injected from the check filter.
    :return: A tuple of:
        - A Spark Column representing the condition for uniqueness violations.
        - A closure that applies the uniqueness check and adds the necessary condition/count columns.
    """
    if len(columns) == 1:
        single_key = columns[0]
        column = F.col(single_key) if isinstance(single_key, str) else single_key
    else:  # composite key
        column = F.struct(*[F.col(col) if isinstance(col, str) else col for col in columns])

    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)

    unique_str = uuid.uuid4().hex  # make sure any column added to the dataframe is unique
    condition_col = f"__condition_{col_str_norm}_{unique_str}"
    count_col = f"__count_{col_str_norm}_{unique_str}"

    def apply(df: DataFrame) -> DataFrame:
        """
        Apply the uniqueness check logic to the DataFrame.

        Adds columns indicating whether the row violates uniqueness, and how many duplicates exist.
        The condition is applied during check evaluation to flag duplicates.

        :param df: The input DataFrame to validate for uniqueness.
        :return: The DataFrame with additional condition and count columns for uniqueness validation.
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

    This function verifies that values in the specified foreign key columns exist (or don't exist, if `negate=True`) in
    the corresponding reference columns of another DataFrame or table. Rows where
    foreign key values do not match the reference are reported as violations.

    NULL values in the foreign key columns are ignored (SQL ANSI behavior).

    :param columns: List of column names (str) or Column expressions in the dataset (foreign key).
    :param ref_columns: List of column names (str) or Column expressions in the reference dataset.
    :param ref_df_name: Name of the reference DataFrame (used when passing DataFrames directly).
    :param ref_table: Name of the reference table (used when reading from catalog).
    :param row_filter: Optional SQL expression for filtering rows before checking the foreign key.
    Auto-injected from the check filter.
    :param negate: If True, the condition is negated (i.e., the check fails when the foreign key values exist in the
        reference DataFrame/Table). If False, the check fails when the foreign key values do not exist in the reference.
    :return: A tuple of:
        - A Spark Column representing the condition for foreign key violations.
        - A closure that applies the foreign key validation by joining against the reference.
    """
    _validate_ref_params(columns, ref_columns, ref_df_name, ref_table)

    not_null_condition = F.lit(True)
    if len(columns) == 1:
        column = columns[0]
        ref_column = ref_columns[0]
    else:
        column, ref_column, not_null_condition = _handle_fk_composite_keys(columns, ref_columns, not_null_condition)

    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
    ref_col_str_norm, ref_col_expr_str, ref_col_expr = _get_normalized_column_and_expr(ref_column)
    unique_str = uuid.uuid4().hex  # make sure any column added to the dataframe is unique
    condition_col = f"__{col_str_norm}_{unique_str}"

    def apply(df: DataFrame, spark: SparkSession, ref_dfs: dict[str, DataFrame]) -> DataFrame:
        """
        Apply the foreign key check logic to the DataFrame.

        Joins the dataset with the reference DataFrame or table to verify the foreign key values.
        Adds a condition column indicating whether each row violates the foreign key constraint.

        :param df: The input DataFrame to validate.
        :param spark: SparkSession used if reading a reference table.
        :param ref_dfs: Dictionary of reference DataFrames (by name), used for joins.
        :return: The DataFrame with an additional condition column for foreign key validation.
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
    merge_columns: list[str],
    msg: str | None = None,
    name: str | None = None,
    negate: bool = False,
    condition_column: str = "condition",
    input_placeholder: str = "input_view",
    row_filter: str | None = None,
) -> tuple[Column, Callable]:
    """
    Checks whether the condition column generated by SQL query is met.

    :param query: SQL query that must return as a minimum a condition column and
    all merge columns. The resulting DataFrame is automatically joined back to the input DataFrame.
    using the merge_columns. Reference DataFrames when provided in the ref_dfs parameter are registered as temp view.
    :param condition_column: Column name indicating violation (boolean). Fail the check if True, pass it if False
    :param merge_columns: List of columns for join back to the input DataFrame.
        They must provide a unique key for the join, otherwise a duplicate records may be produced.
    :param msg: Optional custom message or Column expression.
    :param name: Optional name for the result.
    :param negate: If True, the condition is negated (i.e., the check fails when the condition is False).
    :param input_placeholder: Name to be used in the sql query as {{ input_placeholder }} to refer to the
     input DataFrame on which the checks are applied.
    :param row_filter: Optional SQL expression for filtering rows before checking the foreign key.
    Auto-injected from the check filter.
    :return: Tuple (condition column, apply function).
    """
    if not merge_columns:
        raise ValueError("merge_columns must contain at least one column.")

    if not is_sql_query_safe(query):
        raise ValueError(
            "Provided SQL query is not safe for execution. Please ensure it does not contain any unsafe operations."
        )

    alias_name = name if name else "_".join(merge_columns) + f"_query_{condition_column}_violation"

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
            raise ValueError(
                "Resolved SQL query is not safe for execution. Please ensure it does not contain any unsafe operations."
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
) -> tuple[Column, Callable]:
    """
    Build an aggregation check condition and closure for dataset-level validation.

    This function verifies that an aggregation (count, sum, avg, min, max) on a column
    or group of columns does not exceed a specified limit. Rows where the aggregation
    result exceeds the limit are flagged.

    :param column: Column name (str) or Column expression to aggregate.
    :param limit: Numeric value, column name, or SQL expression for the limit.
    :param aggr_type: Aggregation type: 'count', 'sum', 'avg', 'min', or 'max' (default: 'count').
    :param group_by: Optional list of column names or Column expressions to group by.
    :param row_filter: Optional SQL expression to filter rows before aggregation. Auto-injected from the check filter.
    :return: A tuple of:
        - A Spark Column representing the condition for aggregation limit violations.
        - A closure that applies the aggregation check and adds the necessary condition/metric columns.
    """
    return _is_aggr_compare(
        column,
        limit,
        aggr_type,
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
) -> tuple[Column, Callable]:
    """
    Build an aggregation check condition and closure for dataset-level validation.

    This function verifies that an aggregation (count, sum, avg, min, max) on a column
    or group of columns is not below a specified limit. Rows where the aggregation
    result is below the limit are flagged.

    :param column: Column name (str) or Column expression to aggregate.
    :param limit: Numeric value, column name, or SQL expression for the limit.
    :param aggr_type: Aggregation type: 'count', 'sum', 'avg', 'min', or 'max' (default: 'count').
    :param group_by: Optional list of column names or Column expressions to group by.
    :param row_filter: Optional SQL expression to filter rows before aggregation. Auto-injected from the check filter.
    :return: A tuple of:
        - A Spark Column representing the condition for aggregation limit violations.
        - A closure that applies the aggregation check and adds the necessary condition/metric columns.
    """
    return _is_aggr_compare(
        column,
        limit,
        aggr_type,
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
) -> tuple[Column, Callable]:
    """
    Build an aggregation check condition and closure for dataset-level validation.

    This function verifies that an aggregation (count, sum, avg, min, max) on a column
    or group of columns is equal to a specified limit. Rows where the aggregation
    result is not equal to the limit are flagged.

    :param column: Column name (str) or Column expression to aggregate.
    :param limit: Numeric value, column name, or SQL expression for the limit.
    :param aggr_type: Aggregation type: 'count', 'sum', 'avg', 'min', or 'max' (default: 'count').
    :param group_by: Optional list of column names or Column expressions to group by.
    :param row_filter: Optional SQL expression to filter rows before aggregation. Auto-injected from the check filter.
    :return: A tuple of:
        - A Spark Column representing the condition for aggregation limit violations.
        - A closure that applies the aggregation check and adds the necessary condition/metric columns.
    """
    return _is_aggr_compare(
        column,
        limit,
        aggr_type,
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
) -> tuple[Column, Callable]:
    """
    Build an aggregation check condition and closure for dataset-level validation.

    This function verifies that an aggregation (count, sum, avg, min, max) on a column
    or group of columns is not equal to a specified limit. Rows where the aggregation
    result is equal to the limit are flagged.

    :param column: Column name (str) or Column expression to aggregate.
    :param limit: Numeric value, column name, or SQL expression for the limit.
    :param aggr_type: Aggregation type: 'count', 'sum', 'avg', 'min', or 'max' (default: 'count').
    :param group_by: Optional list of column names or Column expressions to group by.
    :param row_filter: Optional SQL expression to filter rows before aggregation. Auto-injected from the check filter.
    :return: A tuple of:
        - A Spark Column representing the condition for aggregation limit violations.
        - A closure that applies the aggregation check and adds the necessary condition/metric columns.
    """
    return _is_aggr_compare(
        column,
        limit,
        aggr_type,
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
) -> tuple[Column, Callable]:
    """
    Dataset-level check that compares two datasets and returns a condition for changed rows,
    with details on a row and column-level differences.
    Only columns that are common across both datasets will be compared. Mismatched columns are ignored.
    Detailed information about the differences is provided in the condition column.
    The comparison does not support Map types (any column comparison on map type is skipped automatically).

    The log containing detailed differences is written to the message field of the check result as JSON string.
    Example: {\"row_missing\":false,\"row_extra\":true,\"changed\":{\"val\":{\"df\":\"val1\"}}}

    :param columns: List of columns to use for row matching with the reference DataFrame
    (can be a list of string column names or column expressions).
    Only simple column expressions are supported, e.g. F.col("col_name")
    :param ref_columns: List of columns in the reference DataFrame or Table to row match against the source DataFrame
    (can be a list of string column names or column expressions). The `columns` parameter is matched  with `ref_columns`
    by position, so the order of the provided columns in both lists must be exactly aligned.
    Only simple column expressions are supported, e.g. F.col("col_name")
    :param ref_df_name: Name of the reference DataFrame (used when passing DataFrames directly).
    :param ref_table: Name of the reference table (used when reading from catalog).
    :param check_missing_records: Perform FULL OUTER JOIN between the DataFrames to find also records
    that could be missing from the DataFrame. Use this with caution as it may produce output with more rows
    than in the original DataFrame.
    :param exclude_columns: List of columns to exclude from the value comparison but not from row matching
    (can be a list of string column names or column expressions).
    Only simple column expressions are supported, e.g. F.col("col_name")
    The parameter does not alter the list of columns used to determine row matches,
    it only controls which columns are skipped during the column value comparison.
    :param null_safe_row_matching: If True, treats nulls as equal when matching rows.
    :param null_safe_column_value_matching: If True, treats nulls as equal when matching column values.
    If enabled (NULL, NULL) column values are equal and matching.
    :param row_filter: Optional SQL expression to filter rows in the input DataFrame.
    Auto-injected from the check filter.
    :return: A tuple of:
        - A Spark Column representing the condition for comparison violations.
        - A closure that applies the comparison validation.
    """
    _validate_ref_params(columns, ref_columns, ref_df_name, ref_table)

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
        results = _add_column_diffs(results, compare_columns, columns_changed_col, null_safe_column_value_matching)
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
    Ensure that corresponding pk columns are compared together, match by position in pk and ref pk cols
    Use eq null safe join to ensure that: 1 == 1 matches; NULL <=> NULL matches; 1 <=> NULL does not match

    :param df: The input DataFrame to join.
    :param ref_df: The reference DataFrame to join against.
    :param pk_column_names: List of primary key column names in the input DataFrame.
    :param ref_pk_column_names: List of primary key column names in the reference DataFrame.
    :param check_missing_records: If True, perform a full outer join to find missing records in both DataFrames.
    :param null_safe_row_matching: If True, treats nulls as equal when matching rows.
    :return: A DataFrame with the results of the join.
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


def _add_column_diffs(
    df: DataFrame,
    compare_columns: list[str],
    columns_changed_col: str,
    null_safe_column_value_matching: bool | None = True,
) -> DataFrame:
    """
    Adds a column to the DataFrame that contains a map of changed columns and their differences.

    This function compares specified columns between two datasets (`df` and `ref_df`) and identifies differences.
    For each column in `compare_columns`, it checks if the values in `df` and `ref_df` are equal.
    If a difference is found, it adds the column name and the differing values to a map stored in `columns_changed_col`.

    :param df: The input DataFrame containing columns to compare.
    :param compare_columns: List of column names to compare between `df` and `ref_df`.
    :param columns_changed_col: Name of the column to store the map of changed columns and their differences.
    :param null_safe_column_value_matching: If True, treats nulls as equal when matching column values.
    If enabled (NULL, NULL) column values are equal and matching.
    If False, uses a standard inequality comparison (`!=`), where (NULL, NULL) values are not considered equal.
    :return: A DataFrame with the added `columns_changed_col` containing the map of changed columns and differences.
    """
    if compare_columns:
        columns_changed = [
            F.when(
                # with null-safe comparison values are matching if they are equal or both are NULL
                (
                    ~F.col(f"df.{col}").eqNullSafe(F.col(f"ref_df.{col}"))
                    if null_safe_column_value_matching
                    else F.col(f"df.{col}") != F.col(f"ref_df.{col}")
                ),
                F.struct(
                    F.lit(col).alias("col_changed"),
                    F.struct(
                        F.col(f"df.{col}").cast("string").alias("df"),
                        F.col(f"ref_df.{col}").cast("string").alias("ref"),
                    ).alias("diff"),
                ),
            ).otherwise(None)
            for col in compare_columns
        ]

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
    This function adds a new column (`condition_col`) to the DataFrame, which contains structured information
    about mismatched records. The mismatches are determined based on the presence of missing rows, extra rows,
    and differences in specified columns.
    :param df: The input DataFrame containing the comparison results.
    :param condition_col: The name of the column to add, which will store mismatch information.
    :param row_missing_col: The name of the column indicating missing rows.
    :param row_extra_col: The name of the column indicating extra rows.
    :param columns_changed_col: The name of the column containing differences in compared columns.
    :param filter_col: The name of the column used to filter records for comparison.
    :return: The input DataFrame with the added `condition_col` containing mismatch information.
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


def _is_aggr_compare(
    column: str | Column,
    limit: int | float | str | Column,
    aggr_type: str,
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

    :param column: Column name (str) or Column expression to aggregate.
    :param limit: Numeric value, column name, or SQL expression for the limit.
    :param aggr_type: Aggregation type: 'count', 'sum', 'avg', 'min', or 'max'.
    :param group_by: Optional list of columns or Column expressions to group by.
    :param row_filter: Optional SQL expression to filter rows before aggregation.
    :param compare_op: Comparison operator (e.g., operator.gt, operator.lt).
    :param compare_op_label: Human-readable label for the comparison (e.g., 'greater than').
    :param compare_op_name: Name identifier for the comparison (e.g., 'greater_than').
    :return: A tuple of:
        - A Spark Column representing the condition for the aggregation check.
        - A closure that applies the aggregation check logic.
    """
    supported_aggr_types = {"count", "sum", "avg", "min", "max"}
    if aggr_type not in supported_aggr_types:
        raise ValueError(f"Unsupported aggregation type: {aggr_type}. Supported: {supported_aggr_types}")

    aggr_col_str_norm, aggr_col_str, aggr_col_expr = _get_normalized_column_and_expr(column)

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

    limit_expr = _get_limit_expr(limit)

    unique_str = uuid.uuid4().hex  # make sure any column added to the dataframe is unique
    condition_col = f"__condition_{aggr_col_str_norm}_{aggr_type}_{compare_op_name}_{unique_str}"
    metric_col = f"__metric_{aggr_col_str_norm}_{aggr_type}_{compare_op_name}_{unique_str}"

    def apply(df: DataFrame) -> DataFrame:
        """
        Apply the aggregation comparison check logic to the DataFrame.

        Computes the specified aggregation over the dataset (or groups if provided)
        and compares the result against the limit. Adds condition and metric columns
        used for check evaluation.

        :param df: The input DataFrame to validate.
        :return: The DataFrame with additional condition and metric columns for aggregation validation.
        """
        filter_col = F.expr(row_filter) if row_filter else F.lit(True)

        window_spec = Window.partitionBy(
            *[F.col(col) if isinstance(col, str) else col for col in group_by] if group_by else []
        )

        filtered_expr = F.when(filter_col, aggr_col_expr) if row_filter else aggr_col_expr
        aggr_expr = getattr(F, aggr_type)(filtered_expr)

        # Add condition and metric columns used in make_condition
        df = df.withColumn(metric_col, aggr_expr.over(window_spec))
        df = df.withColumn(condition_col, compare_op(F.col(metric_col), limit_expr))

        return df

    condition = make_condition(
        condition=F.col(condition_col),
        message=F.concat_ws(
            "",
            F.lit(f"{aggr_type.capitalize()} "),
            F.col(metric_col).cast("string"),
            F.lit(f"{' per group of columns ' if group_by_list_str else ''}"),
            F.lit(f"'{group_by_list_str}'" if group_by_list_str else ""),
            F.lit(f" in column '{aggr_col_str}' is {compare_op_label} limit: "),
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
    (using `ref_df_name` as the key) or by reading a table from the Unity Catalog (using `ref_table`).
    It raises an error if the necessary reference source is not properly specified or cannot be found.

    :param ref_df_name: The key name of the reference DataFrame in the provided dictionary (optional).
    :param ref_table: The name of the reference table to read from the Spark catalog (optional).
    :param ref_dfs: A dictionary mapping reference DataFrame names to DataFrame objects.
    :param spark: The active SparkSession used to read the reference table if needed.
    :return: A Spark DataFrame representing the reference dataset.
    :raises ValueError: If neither or both of `ref_df_name` and `ref_table` are provided,
                        or if the specified reference DataFrame is not found.
    """
    if ref_df_name:
        if not ref_dfs:
            raise ValueError(
                "Reference DataFrames dictionary not provided. "
                f"Provide '{ref_df_name}' reference DataFrame when applying the checks."
            )

        if ref_df_name not in ref_dfs:
            raise ValueError(
                f"Reference DataFrame with key '{ref_df_name}' not found. "
                f"Provide reference '{ref_df_name}' DataFrame when applying the checks."
            )

        return ref_dfs[ref_df_name]

    if not ref_table:
        raise ValueError("The 'ref_table' must be provided.")

    return spark.table(ref_table)


def _cleanup_alias_name(column: str) -> str:
    """
    Sanitize a column name for use as an alias by replacing dots with underscores.

    This helper avoids issues when using struct field names as aliases,
    since dots in column names can cause ambiguity in Spark SQL.

    :param column: The column name as a string.
    :return: A sanitized column name with dots replaced by underscores.
    """
    # avoid issues with structs
    return column.replace(".", "_")


def _get_limit_expr(
    limit: int | float | datetime.date | datetime.datetime | str | Column | None = None,
) -> Column:
    """
    Generate a Spark Column expression for a limit value.

    This helper converts the provided limit (literal, string expression, or Column)
    into a Spark Column expression suitable for use in conditions.

    :param limit: The limit to use in the condition. Can be a literal (int, float, date, datetime),
                  a string SQL expression, or a Spark Column.
    :return: A Spark Column expression representing the limit.
    :raises ValueError: If the limit is not provided (None).
    """
    if limit is None:
        raise ValueError("Limit is not provided.")

    if isinstance(limit, str):
        return F.expr(limit)
    if isinstance(limit, Column):
        return limit
    return F.lit(limit)


def _get_normalized_column_and_expr(column: str | Column) -> tuple[str, str, Column]:
    """
    Extract the normalized column name, original column name as string, and column expression.

    This helper ensures that both a normalized string representation and a raw string representation
    of the column are available, along with the corresponding Spark Column expression.
    Useful for generating aliases, conditions, and consistent messaging.

    :param column: The input column, provided as either a string column name or a Spark Column expression.
    :return: A tuple containing:
             - Normalized column name as a string (suitable for use in aliases or metadata).
             - Original column name as a string.
             - Spark Column expression corresponding to the input.
    """
    col_expr = F.expr(column) if isinstance(column, str) else column
    column_str = get_column_name_or_alias(col_expr)
    col_str_norm = get_column_name_or_alias(col_expr, normalize=True)

    return col_str_norm, column_str, col_expr


def _handle_fk_composite_keys(columns: list[str | Column], ref_columns: list[str | Column], not_null_condition: Column):
    """
    Construct composite key expressions and not-null condition for foreign key validation.

    This helper function builds structured column expressions for composite foreign keys
    in both the main and reference datasets. It also updates the not-null condition to skip any rows where
    any of the composite key columns are NULL, in line with SQL ANSI foreign key semantics.

    :param columns: List of columns (names or expressions) from the input DataFrame forming the composite key.
    :param ref_columns: List of columns (names or expressions) from the reference DataFrame forming the composite key.
    :param not_null_condition: Existing condition Column to be combined with not-null checks for the composite key.
    :return: A tuple containing:
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

    :param columns: List of columns (names as str or Spark Column expressions) to include in the struct.
    :param columns_names: List of normalized column names (str) to use as aliases for the struct fields.
    :return: A Spark Column representing a struct with the specified columns and aliases.
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
    - Exactly one of `ref_df_name` or `ref_table` is provided (not both, not neither).
    - The number of columns in the input DataFrame matches the number of reference columns.

    :param columns: List of columns from the input DataFrame.
    :param ref_columns: List of columns from the reference DataFrame or table.
    :param ref_df_name: Optional name of the reference DataFrame.
    :param ref_table: Optional name of the reference table.
    :raises ValueError: If both or neither of `ref_df_name` and `ref_table` are provided,
                        or if the lengths of `columns` and `ref_columns` do not match.
    """
    if ref_df_name and ref_table:
        raise ValueError(
            "Both 'ref_df_name' and 'ref_table' are provided. Please provide only one of them to avoid ambiguity."
        )

    if not ref_df_name and not ref_table:
        raise ValueError("Either 'ref_df_name' or 'ref_table' must be provided to specify the reference DataFrame.")

    if len(columns) != len(ref_columns):
        raise ValueError("The number of columns to check against the reference columns must be equal.")


def _extract_octets_to_bits(column: Column, pattern: str) -> Column:
    """Extracts 4 octets from an IP column and returns the binary string."""
    ip_match = F.regexp_extract(column, pattern, 0)
    octets = F.split(ip_match, r"\.")
    octets_bin = [F.lpad(F.conv(octets[i], 10, 2), 8, "0") for i in range(4)]
    return F.concat(*octets_bin).alias("ip_bits")


def _convert_ipv4_to_bits(ip_col: Column) -> Column:
    """Returns 32-bit binary string from IPv4 address (no CIDR). (e.g., '11000000101010000000000100000001')."""
    return _extract_octets_to_bits(ip_col, DQPattern.IPV4_ADDRESS.value)


def _convert_cidr_to_bits_and_prefix(cidr_col: Column) -> tuple[Column, Column]:
    """Returns binary IP and prefix length from CIDR (e.g., '192.168.1.0/24')."""
    ip_bits = _extract_octets_to_bits(cidr_col, DQPattern.IPV4_CIDR_BLOCK.value)
    # The 5th capture group in the regex pattern corresponds to the CIDR prefix length.
    prefix_length = F.regexp_extract(cidr_col, DQPattern.IPV4_CIDR_BLOCK.value, 5).cast("int").alias("prefix_length")
    return ip_bits, prefix_length


def _get_network_address(ip_bits: Column, prefix_length: Column) -> Column:
    """
    Returns the network address from IP bits using the CIDR prefix length.

    :param ip_bits: 32-bit binary string representation of the IPv4 address
    :param prefix_length: Prefix length for CIDR notation
    :return: Network address as a 32-bit binary string
    """
    return F.rpad(F.substring(ip_bits, 1, prefix_length), 32, "0")
