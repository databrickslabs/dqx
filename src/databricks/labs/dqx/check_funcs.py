import datetime
from collections.abc import Callable
import operator as py_operator
import pyspark.sql.functions as F
from pyspark.sql import Column
from pyspark.sql.window import Window

from databricks.labs.dqx.rule import register_rule
from databricks.labs.dqx.utils import get_column_as_string


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


@register_rule("single_column")
def is_not_null_and_not_empty(column: str | Column, trim_strings: bool | None = False) -> Column:
    """Checks whether the values in the input column are not null and not empty.

    :param column: column to check; can be a string column name or a column expression
    :param trim_strings: boolean flag to trim spaces from strings
    :return: Column object for condition
    """
    col_str_norm, col_expr_str, col_expr = _get_norm_column_and_expr(column)
    if trim_strings:
        col_expr = F.trim(col_expr).alias(col_str_norm)
    condition = col_expr.isNull() | (col_expr.cast("string").isNull() | (col_expr.cast("string") == F.lit("")))
    return make_condition(
        condition, f"Column '{col_expr_str}' value is null or empty", f"{col_str_norm}_is_null_or_empty"
    )


@register_rule("single_column")
def is_not_empty(column: str | Column) -> Column:
    """Checks whether the values in the input column are not empty (but may be null).

    :param column: column to check; can be a string column name or a column expression
    :return: Column object for condition
    """
    col_str_norm, col_expr_str, col_expr = _get_norm_column_and_expr(column)
    condition = col_expr.cast("string") == F.lit("")
    return make_condition(condition, f"Column '{col_expr_str}' value is empty", f"{col_str_norm}_is_empty")


@register_rule("single_column")
def is_not_null(column: str | Column) -> Column:
    """Checks whether the values in the input column are not null.

    :param column: column to check; can be a string column name or a column expression
    :return: Column object for condition
    """
    col_str_norm, col_expr_str, col_expr = _get_norm_column_and_expr(column)
    return make_condition(col_expr.isNull(), f"Column '{col_expr_str}' value is null", f"{col_str_norm}_is_null")


@register_rule("single_column")
def is_not_null_and_is_in_list(column: str | Column, allowed: list) -> Column:
    """Checks whether the values in the input column are not null and present in the list of allowed values.

    :param column: column to check; can be a string column name or a column expression
    :param allowed: list of allowed values (actual values or Column objects)
    :return: Column object for condition
    """
    if not allowed:
        raise ValueError("allowed list is not provided.")

    allowed_cols = [item if isinstance(item, Column) else F.lit(item) for item in allowed]
    col_str_norm, col_expr_str, col_expr = _get_norm_column_and_expr(column)
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


@register_rule("single_column")
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
    col_str_norm, col_expr_str, col_expr = _get_norm_column_and_expr(column)
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


@register_rule("no_column")
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
    expr_msg = expression

    if negate:
        expr_msg = "~(" + expression + ")"
        message = F.concat_ws("", F.lit(f"Value is matching expression: {expr_msg}"))
    else:
        expr_col = ~expr_col
        message = F.concat_ws("", F.lit(f"Value is not matching expression: {expr_msg}"))

    name = name if name else get_column_as_string(expression, normalize=True)

    return make_condition(expr_col, msg or message, name)


@register_rule("single_column")
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
    col_str_norm1, col_expr_str1, col_expr1 = _get_norm_column_and_expr(column1)
    col_str_norm2, col_expr_str2, col_expr2 = _get_norm_column_and_expr(column2)

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


@register_rule("single_column")
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
    col_str_norm, col_expr_str, col_expr = _get_norm_column_and_expr(column)
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


@register_rule("single_column")
def is_not_in_future(column: str | Column, offset: int = 0, curr_timestamp: Column | None = None) -> Column:
    """Checks whether the values in the input column contain a timestamp that is not in the future,
    where 'future' is defined as current_timestamp + offset (in seconds).

    :param column: column to check; can be a string column name or a column expression
    :param offset: offset (in seconds) to add to the current timestamp at time of execution
    :param curr_timestamp: (optional) set current timestamp
    :return: new Column
    """
    col_str_norm, col_expr_str, col_expr = _get_norm_column_and_expr(column)
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


@register_rule("single_column")
def is_not_in_near_future(column: str | Column, offset: int = 0, curr_timestamp: Column | None = None) -> Column:
    """Checks whether the values in the input column contain a timestamp that is not in the near future,
    where 'near future' is defined as greater than the current timestamp
    but less than the current_timestamp + offset (in seconds).

    :param column: column to check; can be a string column name or a column expression
    :param offset: offset (in seconds) to add to the current timestamp at time of execution
    :param curr_timestamp: (optional) set current timestamp
    :return: new Column
    """
    col_str_norm, col_expr_str, col_expr = _get_norm_column_and_expr(column)
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


@register_rule("single_column")
def is_not_less_than(
    column: str | Column, limit: int | datetime.date | datetime.datetime | str | Column | None = None
) -> Column:
    """Checks whether the values in the input column are not less than the provided limit.

    :param column: column to check; can be a string column name or a column expression
    :param limit: limit to use in the condition as number, date, timestamp, column name or sql expression
    :return: new Column
    """
    col_str_norm, col_expr_str, col_expr = _get_norm_column_and_expr(column)
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


@register_rule("single_column")
def is_not_greater_than(
    column: str | Column, limit: int | datetime.date | datetime.datetime | str | Column | None = None
) -> Column:
    """Checks whether the values in the input column are not greater than the provided limit.

    :param column: column to check; can be a string column name or a column expression
    :param limit: limit to use in the condition as number, date, timestamp, column name or sql expression
    :return: new Column
    """
    col_str_norm, col_expr_str, col_expr = _get_norm_column_and_expr(column)
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


@register_rule("single_column")
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
    col_str_norm, col_expr_str, col_expr = _get_norm_column_and_expr(column)
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


@register_rule("single_column")
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
    col_str_norm, col_expr_str, col_expr = _get_norm_column_and_expr(column)
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


@register_rule("single_column")
def regex_match(column: str | Column, regex: str, negate: bool = False) -> Column:
    """Checks whether the values in the input column matches a given regex.

    :param column: column to check; can be a string column name or a column expression
    :param regex: regex to check
    :param negate: if the condition should be negated (true) or not
    :return: Column object for condition
    """
    col_str_norm, col_expr_str, col_expr = _get_norm_column_and_expr(column)
    if negate:
        condition = col_expr.rlike(regex)
        return make_condition(condition, f"Column '{col_expr_str}' is matching regex", f"{col_str_norm}_matching_regex")

    condition = ~col_expr.rlike(regex)
    return make_condition(
        condition, f"Column '{col_expr_str}' is not matching regex", f"{col_str_norm}_not_matching_regex"
    )


@register_rule("single_column")
def is_not_null_and_not_empty_array(column: str | Column) -> Column:
    """Checks whether the values in the array input column are not null and not empty.

    :param column: column to check; can be a string column name or a column expression
    :return: Column object for condition
    """
    col_str_norm, col_expr_str, col_expr = _get_norm_column_and_expr(column)
    condition = col_expr.isNull() | (F.size(col_expr) == 0)
    return make_condition(
        condition, f"Column '{col_expr_str}' is null or empty array", f"{col_str_norm}_is_null_or_empty_array"
    )


@register_rule("single_column")
def is_valid_date(column: str | Column, date_format: str | None = None) -> Column:
    """Checks whether the values in the input column have valid date formats.

    :param column: column to check; can be a string column name or a column expression
    :param date_format: date format (e.g. 'yyyy-mm-dd')
    :return: Column object for condition
    """
    col_str_norm, col_expr_str, col_expr = _get_norm_column_and_expr(column)
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


@register_rule("single_column")
def is_valid_timestamp(column: str | Column, timestamp_format: str | None = None) -> Column:
    """Checks whether the values in the input column have valid timestamp formats.

    :param column: column to check; can be a string column name or a column expression
    :param timestamp_format: timestamp format (e.g. 'yyyy-mm-dd HH:mm:ss')
    :return: Column object for condition
    """
    col_str_norm, col_expr_str, col_expr = _get_norm_column_and_expr(column)
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


@register_rule("multi_column")
def is_unique(
    columns: list[str | Column],
    row_filter: str | None = None,  # auto-injected when applying checks
    window_spec: str | Column | None = None,
    nulls_distinct: bool | None = True,
) -> Column:
    """Checks whether the values in the input column(s) are unique
    and reports an issue for each row that contains a duplicate value.
    Note: This check should be used cautiously in a streaming context,
    as uniqueness validation is only applied within individual spark micro-batches.

    :param columns: columns to check; can be a list of column names or column expressions
    :param row_filter: SQL filter expression to apply for aggregation; auto-injected using check filter
    :param window_spec: window specification for the partition by clause. Default value for NULL in the time column
    of the window spec must be provided using coalesce() to prevent rows exclusion!
    e.g. "window(coalesce(b, '1970-01-01'), '2 hours')"
    :param nulls_distinct: If True (default - conform with the SQL ANSI Standard), null values are treated as unknown,
    thus not duplicates, e.g. "(NULL, NULL) not equals (NULL, NULL); (1, NULL) not equals (1, NULL)
    If False, null values are treated as duplicates,
    e.g. eg. (1, NULL) equals (1, NULL) and (NULL, NULL) equals (NULL, NULL)
    :return: Column object for condition
    """
    col_expr = F.struct(*[F.col(col) if isinstance(col, str) else col for col in columns])
    col_str_norm, col_expr_str, col_expr = _get_norm_column_and_expr(col_expr)

    if nulls_distinct:
        # skip evaluation if any column is null
        any_null = F.lit(False)
        for column in columns:
            column = F.col(column) if isinstance(column, str) else column
            any_null = any_null | column.isNull()
        col_expr = F.when(~any_null, col_expr)

    if window_spec is None:
        partition_by_spec = Window.partitionBy(col_expr)
    else:
        if isinstance(window_spec, str):
            window_spec = F.expr(window_spec)
        partition_by_spec = Window.partitionBy(window_spec)

    count_expr = F.count(col_expr).over(partition_by_spec)

    if row_filter:
        filter_condition = F.expr(row_filter)
        condition = F.when(filter_condition & col_expr.isNotNull(), count_expr == 1)
    else:
        condition = F.when(col_expr.isNotNull(), count_expr == 1)

    return make_condition(
        ~condition,
        F.concat_ws(
            "", F.lit("Value '"), col_expr.cast("string"), F.lit(f"' in Column '{col_expr_str}' is not unique")
        ),
        f"{col_str_norm}_is_not_unique",
    )


@register_rule("single_column")
def is_aggr_not_greater_than(
    column: str | Column,
    limit: int | float | str | Column,
    row_filter: str | None = None,  # auto-injected when applying checks
    aggr_type: str = "count",
    group_by: list[str | Column] | None = None,
) -> Column:
    """
    Returns a Column expression indicating whether an aggregation over all or group of rows is greater than the limit.
    Nulls are excluded from aggregations. To include rows with nulls for count aggregation, pass "*" for the column.

    :param column: column to apply the aggregation on; can be a list of column names or column expressions
    :param row_filter: SQL filter expression to apply for aggregation; auto-injected using check filter
    :param limit: Limit to use in the condition as number, column name or sql expression
    :param aggr_type: Aggregation type - "count", "sum", "avg", "max", or "min"
    :param group_by: Optional list of columns or column expressions to group by
    before counting rows to check row count per group of columns.
    :return: Column expression (same for every row) indicating if count is less than limit
    """
    return _is_aggr_compare(
        column,
        limit,
        aggr_type,
        row_filter,
        group_by,
        compare_op=py_operator.gt,
        compare_op_label="greater than",
        compare_op_name="greater_than",
    )


@register_rule("single_column")
def is_aggr_not_less_than(
    column: str | Column,
    limit: int | float | str | Column,
    row_filter: str | None = None,
    aggr_type: str = "count",
    group_by: list[str | Column] | None = None,
) -> Column:
    """
    Returns a Column expression indicating whether an aggregation over all or group of rows is less than the limit.
    Nulls are excluded from aggregations. To include rows with nulls for count aggregation, pass "*" for the column.

    :param column: column to apply the aggregation on; can be a list of column names or column expressions
    :param row_filter: SQL filter expression to apply for aggregation; auto-injected using check filter
    :param limit: Limit to use in the condition as number, column name or sql expression
    :param aggr_type: Aggregation type - "count", "sum", "avg", "max", or "min"
    :param group_by: Optional list of columns or column expressions to group by
    before counting rows to check row count per group of columns.
    :return: Column expression (same for every row) indicating if count is less than limit
    """
    return _is_aggr_compare(
        column,
        limit,
        aggr_type,
        row_filter,
        group_by,
        compare_op=py_operator.lt,
        compare_op_label="less than",
        compare_op_name="less_than",
    )


def _is_aggr_compare(
    column: str | Column,
    limit: int | float | str | Column,
    aggr_type: str,
    row_filter: str | None,
    group_by: list[str | Column] | None,
    compare_op: Callable[[Column, Column], Column],
    compare_op_label: str,
    compare_op_name: str,
) -> Column:
    supported_aggr_types = {"count", "sum", "avg", "min", "max"}
    if aggr_type not in supported_aggr_types:
        raise ValueError(f"Unsupported aggregation type: {aggr_type}. Supported types: {supported_aggr_types}")

    limit_expr = _get_limit_expr(limit)
    filter_col = F.expr(row_filter) if row_filter else F.lit(True)
    window_spec = Window.partitionBy(
        *[F.col(col) if isinstance(col, str) else col for col in group_by] if group_by else []
    )

    aggr_col = F.col(column) if isinstance(column, str) else column
    aggr_expr = getattr(F, aggr_type)(F.when(filter_col, aggr_col) if row_filter else aggr_col)
    metric = aggr_expr.over(window_spec)
    condition = compare_op(metric, limit_expr)

    group_by_list_str = (
        ", ".join(col if isinstance(col, str) else get_column_as_string(col) for col in group_by) if group_by else None
    )
    group_by_str = (
        "_".join(col if isinstance(col, str) else get_column_as_string(col) for col in group_by) if group_by else None
    )
    aggr_col_str_norm = get_column_as_string(column, normalize=True)
    aggr_col_str = column if isinstance(column, str) else get_column_as_string(column)

    name = (
        f"{aggr_col_str_norm}_{aggr_type.lower()}_group_by_{group_by_str}_{compare_op_name}_limit".lstrip("_")
        if group_by_str
        else f"{aggr_col_str_norm}_{aggr_type.lower()}_{compare_op_name}_limit".lstrip("_")
    )

    return make_condition(
        condition,
        F.concat_ws(
            "",
            F.lit(f"{aggr_type.capitalize()} "),
            metric.cast("string"),
            F.lit(f"{' per group of columns ' if group_by_list_str else ''}"),
            F.lit(f"'{group_by_list_str}'" if group_by_list_str else ""),
            F.lit(f" in column '{aggr_col_str}' is {compare_op_label} limit: "),
            limit_expr.cast("string"),
        ),
        name,
    )


def _cleanup_alias_name(column: str) -> str:
    # avoid issues with structs
    return column.replace(".", "_")


def _get_limit_expr(
    limit: int | float | datetime.date | datetime.datetime | str | Column | None = None,
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


def _get_norm_column_and_expr(column: str | Column) -> tuple[str, str, Column]:
    """
    Helper function to extract the normalized column name as string, column name as string, and column expression.

    :param column: Column to check; can be a string column name or a column expression.
    :return: Tuple containing the normalized column name as string, column name as string, and column expression.
    """
    col_expr = F.expr(column) if isinstance(column, str) else column
    column_str = get_column_as_string(col_expr)
    col_str_norm = get_column_as_string(col_expr, normalize=True)

    return col_str_norm, column_str, col_expr
