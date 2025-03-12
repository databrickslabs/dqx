import datetime
import re

import pyspark.sql.functions as F
from pyspark.sql import Column
from pyspark.sql.window import Window


def make_condition(condition: Column, message: Column | str, alias: str) -> Column:
    """Helper function to create a condition column.

    :param condition: condition expression
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


def is_not_null_and_not_empty(col_name: str | Column, trim_strings: bool | None = False) -> Column:
    """Checks whether the values in the input column are not null and not empty.

    :param col_name: column to check; can be a string column name or Column
    :param trim_strings: boolean flag to trim spaces from strings
    :return: Column object for condition
    """
    column_alias, column_expr = _get_column_expr(col_name)
    if trim_strings:
        column_expr = F.trim(column_expr).alias(column_alias)
    condition = column_expr.isNull() | (column_expr.cast("string").isNull() | (column_expr.cast("string") == F.lit("")))
    return make_condition(condition, f"Column {column_alias} is null or empty", f"{column_alias}_is_null_or_empty")


def is_not_empty(col_name: str | Column) -> Column:
    """Checks whether the values in the input column are not empty (but may be null).

    :param col_name: column to check; can be a string column name or Column
    :return: Column object for condition
    """
    column_alias, column_expr = _get_column_expr(col_name)
    condition = column_expr.cast("string") == F.lit("")
    return make_condition(condition, f"Column {column_alias} is empty", f"{column_alias}_is_empty")


def is_not_null(col_name: str | Column) -> Column:
    """Checks whether the values in the input column are not null.

    :param col_name: column to check; can be a string column name or Column
    :return: Column object for condition
    """
    column_alias, column_expr = _get_column_expr(col_name)
    return make_condition(column_expr.isNull(), f"Column {column_alias} is null", f"{column_alias}_is_null")


def is_not_null_and_is_in_list(col_name: str | Column, allowed: list) -> Column:
    """Checks whether the values in the input column are not null and present in the list of allowed values.

    :param col_name: column to check; can be a string column name or Column
    :param allowed: list of allowed values (actual values or Column objects)
    :return: Column object for condition
    """
    if not allowed:
        raise ValueError("allowed list is not provided.")

    allowed_cols = [item if isinstance(item, Column) else F.lit(item) for item in allowed]
    column_alias, column_expr = _get_column_expr(col_name)
    condition = column_expr.isNull() | ~column_expr.isin(*allowed_cols)
    return make_condition(
        condition,
        F.concat_ws(
            "",
            F.lit("Value "),
            F.when(column_expr.isNull(), F.lit("null")).otherwise(column_expr.cast("string")),
            F.lit(" is null or not in the allowed list: ["),
            F.concat_ws(", ", *allowed_cols),
            F.lit("]"),
        ),
        f"{column_alias}_is_null_or_is_not_in_the_list",
    )


def is_in_list(col_name: str | Column, allowed: list) -> Column:
    """Checks whether the values in the input column are present in the list of allowed values
    (null values are allowed).

    :param col_name: column to check; can be a string column name or Column
    :param allowed: list of allowed values (actual values or Column objects)
    :return: Column object for condition
    """
    if not allowed:
        raise ValueError("allowed list is not provided.")

    allowed_cols = [item if isinstance(item, Column) else F.lit(item) for item in allowed]
    column_alias, column_expr = _get_column_expr(col_name)
    condition = ~column_expr.isin(*allowed_cols)
    return make_condition(
        condition,
        F.concat_ws(
            "",
            F.lit("Value "),
            F.when(column_expr.isNull(), F.lit("null")).otherwise(column_expr),
            F.lit(" is not in the allowed list: ["),
            F.concat_ws(", ", *allowed_cols),
            F.lit("]"),
        ),
        f"{column_alias}_is_not_in_the_list",
    )


normalize_regex = re.compile("[^a-zA-Z-0-9]+")


def sql_expression(expression: str, msg: str | None = None, name: str | None = None, negate: bool = False) -> Column:
    """Checks whether the condition provided as an SQL expression is met.

    :param expression: SQL expression
    :param msg: optional message of the `Column` type, automatically generated if None
    :param name: optional name of the resulting column, automatically generated if None
    :param negate: if the condition should be negated (true) or not. For example, "col is not null" will mark null
    values as "bad". Although sometimes it's easier to specify it other way around "col is null" + negate set to False
    :return: new Column
    """
    expr_col = F.expr(expression)
    expression_msg = expression

    if negate:
        expression_msg = "~(" + expression + ")"
        message = F.concat_ws("", F.lit(f"Value is matching expression: {expression_msg}"))
    else:
        expr_col = ~expr_col
        message = F.concat_ws("", F.lit(f"Value is not matching expression: {expression_msg}"))

    name = name if name else re.sub(normalize_regex, "_", expression)

    return make_condition(expr_col, msg or message, name)


def is_older_than_col2_for_n_days(col_name1: str | Column, col_name2: str, days: int = 0) -> Column:
    """Checks whether the values in one input column are at least N days older than the values in another column.

    :param col_name1: first column to check; can be a string column name or Column
    :param col_name2: second column to check; can be a string column name or Column
    :param days: number of days
    :return: new Column
    """
    column_alias1, column_expr1 = _get_column_expr(col_name1)
    column_alias2, column_expr2 = _get_column_expr(col_name2)
    col1_date = F.to_date(column_expr1)
    col2_date = F.to_date(column_expr2)
    condition = col1_date < F.date_sub(col2_date, days)

    return make_condition(
        condition,
        F.concat_ws(
            "",
            F.lit(f"Value of {column_alias1}: '"),
            col1_date,
            F.lit(f"' less than value of {column_alias2}: '"),
            col2_date,
            F.lit(f"' for more than {days} days"),
        ),
        f"is_col_{column_alias1}_older_than_{column_alias2}_for_n_days",
    )


def is_older_than_n_days(col_name: str | Column, days: int, curr_date: Column | None = None) -> Column:
    """Checks whether the values in the input column are at least N days older than the current date.

    :param col_name: column to check; can be a string column name or Column
    :param days: number of days
    :param curr_date: (optional) set current date
    :return: new Column
    """
    column_alias, column_expr = _get_column_expr(col_name)
    if curr_date is None:
        curr_date = F.current_date()

    col_date = F.to_date(column_expr)
    condition = col_date < F.date_sub(curr_date, days)

    return make_condition(
        condition,
        F.concat_ws(
            "",
            F.lit(f"Value of {col_name}: '"),
            col_date,
            F.lit("' less than current date: '"),
            curr_date,
            F.lit(f"' for more than {days} days"),
        ),
        f"is_col_{column_alias}_older_than_n_days",
    )


def is_not_in_future(col_name: str | Column, offset: int = 0, curr_timestamp: Column | None = None) -> Column:
    """Checks whether the values in the input column contain a timestamp that is not in the future,
    where 'future' is defined as current_timestamp + offset (in seconds).

    :param col_name: column to check; can be a string column name or Column
    :param offset: offset (in seconds) to add to the current timestamp at time of execution
    :param curr_timestamp: (optional) set current timestamp
    :return: new Column
    """
    column_alias, column_expr = _get_column_expr(col_name)
    if curr_timestamp is None:
        curr_timestamp = F.current_timestamp()

    timestamp_offset = F.from_unixtime(F.unix_timestamp(curr_timestamp) + offset)
    condition = column_expr > timestamp_offset

    return make_condition(
        condition,
        F.concat_ws("", F.lit("Value '"), column_expr, F.lit("' is greater than time '"), timestamp_offset, F.lit("'")),
        f"{column_alias}_in_future",
    )


def is_not_in_near_future(col_name: str | Column, offset: int = 0, curr_timestamp: Column | None = None) -> Column:
    """Checks whether the values in the input column contain a timestamp that is not in the near future,
    where 'near future' is defined as greater than the current timestamp
    but less than the current_timestamp + offset (in seconds).

    :param col_name: column to check; can be a string column name or Column
    :param offset: offset (in seconds) to add to the current timestamp at time of execution
    :param curr_timestamp: (optional) set current timestamp
    :return: new Column
    """
    column_alias, column_expr = _get_column_expr(col_name)
    if curr_timestamp is None:
        curr_timestamp = F.current_timestamp()

    near_future = F.from_unixtime(F.unix_timestamp(curr_timestamp) + offset)
    condition = (column_expr > curr_timestamp) & (column_expr < near_future)

    return make_condition(
        condition,
        F.concat_ws(
            "",
            F.lit("Value '"),
            column_expr,
            F.lit("' is greater than '"),
            curr_timestamp,
            F.lit(" and smaller than '"),
            near_future,
            F.lit("'"),
        ),
        f"{column_alias}_in_near_future",
    )


def is_not_less_than(
    col_name: str | Column, limit: int | datetime.date | datetime.datetime | str | Column | None = None
) -> Column:
    """Checks whether the values in the input column are not less than the provided limit.

    :param col_name: column to check; can be a string column name or Column
    :param limit: limit to use in the condition as number, date, timestamp, column name or sql expression
    :return: new Column
    """
    column_alias, column_expr = _get_column_expr(col_name)
    limit_expr = _get_column_expr_limit(limit)
    condition = column_expr < limit_expr

    return make_condition(
        condition,
        F.concat_ws(" ", F.lit("Value"), column_expr, F.lit("is less than limit:"), limit_expr.cast("string")),
        f"{column_alias}_less_than_limit",
    )


def is_not_greater_than(
    col_name: str | Column, limit: int | datetime.date | datetime.datetime | str | Column | None = None
) -> Column:
    """Checks whether the values in the input column are not greater than the provided limit.

    :param col_name: column to check; can be a string column name or Column
    :param limit: limit to use in the condition as number, date, timestamp, column name or sql expression
    :return: new Column
    """
    column_alias, column_expr = _get_column_expr(col_name)
    limit_expr = _get_column_expr_limit(limit)
    condition = column_expr > limit_expr

    return make_condition(
        condition,
        F.concat_ws(" ", F.lit("Value"), column_expr, F.lit("is greater than limit:"), limit_expr.cast("string")),
        f"{column_alias}_greater_than_limit",
    )


def is_in_range(
    col_name: str | Column,
    min_limit: int | datetime.date | datetime.datetime | str | Column | None = None,
    max_limit: int | datetime.date | datetime.datetime | str | Column | None = None,
) -> Column:
    """Checks whether the values in the input column are in the provided limits (inclusive of both boundaries).

    :param col_name: column to check; can be a string column name or Column
    :param min_limit: min limit to use in the condition as number, date, timestamp, column name or sql expression
    :param max_limit: max limit to use in the condition as number, date, timestamp, column name or sql expression
    :return: new Column
    """
    column_alias, column_expr = _get_column_expr(col_name)
    min_limit_expr = _get_column_expr_limit(min_limit)
    max_limit_expr = _get_column_expr_limit(max_limit)

    condition = (column_expr < min_limit_expr) | (column_expr > max_limit_expr)

    return make_condition(
        condition,
        F.concat_ws(
            " ",
            F.lit("Value"),
            column_expr,
            F.lit("not in range: ["),
            min_limit_expr.cast("string"),
            F.lit(","),
            max_limit_expr.cast("string"),
            F.lit("]"),
        ),
        f"{column_alias}_not_in_range",
    )


def is_not_in_range(
    col_name: str | Column,
    min_limit: int | datetime.date | datetime.datetime | str | Column | None = None,
    max_limit: int | datetime.date | datetime.datetime | str | Column | None = None,
) -> Column:
    """Checks whether the values in the input column are outside the provided limits (inclusive of both boundaries).

    :param col_name: column to check; can be a string column name or Column
    :param min_limit: min limit to use in the condition as number, date, timestamp, column name or sql expression
    :param max_limit: min limit to use in the condition as number, date, timestamp, column name or sql expression
    :return: new Column
    """
    column_alias, column_expr = _get_column_expr(col_name)
    min_limit_expr = _get_column_expr_limit(min_limit)
    max_limit_expr = _get_column_expr_limit(max_limit)

    condition = (column_expr >= min_limit_expr) & (column_expr <= max_limit_expr)

    return make_condition(
        condition,
        F.concat_ws(
            " ",
            F.lit("Value"),
            column_expr,
            F.lit("in range: ["),
            min_limit_expr.cast("string"),
            F.lit(","),
            max_limit_expr.cast("string"),
            F.lit("]"),
        ),
        f"{column_alias}_in_range",
    )


def regex_match(col_name: str | Column, regex: str, negate: bool = False) -> Column:
    """Checks whether the values in the input column matches a given regex.

    :param col_name: column to check; can be a string column name or Column
    :param regex: regex to check
    :param negate: if the condition should be negated (true) or not
    :return: Column object for condition
    """
    column_alias, column_expr = _get_column_expr(col_name)
    if negate:
        condition = column_expr.rlike(regex)
        return make_condition(condition, f"Column {column_alias} is matching regex", f"{column_alias}_matching_regex")

    condition = ~column_expr.rlike(regex)
    return make_condition(
        condition, f"Column {column_alias} is not matching regex", f"{column_alias}_not_matching_regex"
    )


def is_not_null_and_not_empty_array(col_name: str | Column) -> Column:
    """Checks whether the values in the array input column are not null and not empty.

    :param col_name: column to check; can be a string column name or Column
    :return: Column object for condition
    """
    column_alias, column_expr = _get_column_expr(col_name)
    condition = column_expr.isNull() | (F.size(column_expr) == 0)
    return make_condition(
        condition, f"Column {column_alias} is null or empty array", f"{column_alias}_is_null_or_empty_array"
    )


def is_valid_date(col_name: str | Column, date_format: str | None = None) -> Column:
    """Checks whether the values in the input column have valid date formats.

    :param col_name: column to check; can be a string column name or Column
    :param date_format: date format (e.g. 'yyyy-mm-dd')
    :return: Column object for condition
    """
    column_alias, column_expr = _get_column_expr(col_name)
    date_col = (
        F.try_to_timestamp(column_expr) if date_format is None else F.try_to_timestamp(column_expr, F.lit(date_format))
    )
    condition = F.when(column_expr.isNull(), F.lit(None)).otherwise(date_col.isNull())
    condition_str = "' is not a valid date"
    if date_format is not None:
        condition_str += f" with format '{date_format}'"
    return make_condition(
        condition,
        F.concat_ws("", F.lit("Value '"), column_expr, F.lit(condition_str)),
        f"{column_alias}_is_not_valid_date",
    )


def is_valid_timestamp(col_name: str | Column, timestamp_format: str | None = None) -> Column:
    """Checks whether the values in the input column have valid timestamp formats.

    :param col_name: column to check; can be a string column name or Column
    :param timestamp_format: timestamp format (e.g. 'yyyy-mm-dd HH:mm:ss')
    :return: Column object for condition
    """
    column_alias, column_expr = _get_column_expr(col_name)
    ts_col = (
        F.try_to_timestamp(column_expr)
        if timestamp_format is None
        else F.try_to_timestamp(column_expr, F.lit(timestamp_format))
    )
    condition = F.when(column_expr.isNull(), F.lit(None)).otherwise(ts_col.isNull())
    condition_str = "' is not a valid timestamp"
    if timestamp_format is not None:
        condition_str += f" with format '{timestamp_format}'"
    return make_condition(
        condition,
        F.concat_ws("", F.lit("Value '"), column_expr, F.lit(condition_str)),
        f"{column_alias}_is_not_valid_timestamp",
    )


def is_unique(col_name: str | Column, window_spec: str | Column | None = None) -> Column:
    """Checks whether the values in the input column are unique
    and reports an issue for each row that contains a duplicate value.
    Null values are not considered duplicates, following the ANSI SQL standard.
    It should be used carefully in the streaming context,
    as uniqueness check will only be performed on individual micro-batches.

    :param col_name: column to check; can be a string column name or Column
    :param window_spec: window specification for the partition by clause. Default value for NULL in the time column
    of the window spec must be provided using coalesce() to prevent rows exclusion!
    e.g. "window(coalesce(b, '1970-01-01'), '2 hours')"
    :return: Column object for condition
    """
    column_alias, column_expr = _get_column_expr(col_name)
    if window_spec is None:
        partition_by_spec = Window.partitionBy(column_expr)
    else:
        if isinstance(window_spec, str):
            window_spec = F.expr(window_spec)
        partition_by_spec = Window.partitionBy(window_spec)

    condition = F.when(column_expr.isNotNull(), F.count(column_expr).over(partition_by_spec) == 1)
    return make_condition(~condition, f"Column {column_alias} has duplicate values", f"{column_alias}_is_not_unique")


def _cleanup_alias_name(col_name: str) -> str:
    # avoid issues with structs
    return col_name.replace(".", "_")


def _get_column_expr_limit(
    limit: int | datetime.date | datetime.datetime | str | Column | None = None,
) -> Column:
    """Helper function to generate a column expression limit based on the provided limit value.

    :param limit: limit to use in the condition (literal value or column expression)
    :return: column expression.
    :raises ValueError: if limit is not provided.
    """
    if limit is None:
        raise ValueError("Limit is not provided.")

    if isinstance(limit, str):
        return F.expr(limit)
    if isinstance(limit, Column):
        return limit
    return F.lit(limit)


def _get_column_expr(column: str | Column) -> tuple[str, Column]:
    if isinstance(column, str):
        return column, F.col(column)
    return _get_column_expr_alias(column), column


def _get_column_expr_alias(column: Column) -> str:
    match = re.search(r"Column<'(.*)'>", str(column))
    if match is None:
        raise ValueError("Invalid column expression string")
    raw_alias = match.group(1)
    return re.sub(normalize_regex, "_", raw_alias.lower()).rstrip("_")
