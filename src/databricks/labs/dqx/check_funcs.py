import datetime
import uuid
from collections.abc import Callable
import operator as py_operator
import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame
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


@register_rule("row")
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


@register_rule("row")
def is_not_empty(column: str | Column) -> Column:
    """Checks whether the values in the input column are not empty (but may be null).

    :param column: column to check; can be a string column name or a column expression
    :return: Column object for condition
    """
    col_str_norm, col_expr_str, col_expr = _get_norm_column_and_expr(column)
    condition = col_expr.cast("string") == F.lit("")
    return make_condition(condition, f"Column '{col_expr_str}' value is empty", f"{col_str_norm}_is_empty")


@register_rule("row")
def is_not_null(column: str | Column) -> Column:
    """Checks whether the values in the input column are not null.

    :param column: column to check; can be a string column name or a column expression
    :return: Column object for condition
    """
    col_str_norm, col_expr_str, col_expr = _get_norm_column_and_expr(column)
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


@register_rule("row")
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


@register_rule("row")
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


@register_rule("row")
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


@register_rule("row")
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


@register_rule("row")
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


@register_rule("row")
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


@register_rule("row")
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


@register_rule("row")
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


@register_rule("dataset")
def is_unique(
    columns: list[str | Column],  # auto-injected from check columns
    row_filter: str | None = None,  # auto-injected from check filter
    nulls_distinct: bool = True,
) -> tuple[Column, Callable]:
    """
    Build a uniqueness check condition and closure for dataset-level validation.

    This function checks whether the specified columns contain unique values
    and reports an issue for each row that contains a duplicate combination.

    Note: Use this check cautiously in a streaming context.
    The uniqueness validation is applied only within individual Spark micro-batches
    and does not guarantee global uniqueness across all data processed over time.

    :param columns: List of columns to check for uniqueness. Each element can be a column name (str)
                    or a Spark Column expression.
    :param row_filter: Optional filter condition pushed down from the check filter.
    :param nulls_distinct: Whether NULL values should be treated as distinct (default: True).
                           - If True (SQL ANSI standard behavior): NULLs are treated as unknown,
                             so rows like (NULL, NULL) are not considered duplicates.
                           - If False: NULLs are treated as equal, so rows like (NULL, NULL)
                             are considered duplicates.
    :return: A tuple containing:
             - A Column representing the condition to apply on the result DataFrame's condition column.
             - A closure that applies the uniqueness check to a DataFrame, producing a DataFrame
               with a boolean column indicating uniqueness violations.
    """
    # Compose the column expression (struct for multiple columns)
    col_expr = F.struct(*[F.col(c) if isinstance(c, str) else c for c in columns])
    col_str_norm, col_expr_str, col_expr = _get_norm_column_and_expr(col_expr)

    unique_str = uuid.uuid4().hex
    condition_col = f"__condition_{col_str_norm}_{unique_str}"
    count_col = f"__count_{col_str_norm}_{unique_str}"

    def apply(df: DataFrame, _ref_dfs: dict[str, DataFrame] | None = None) -> DataFrame:
        """
        Apply the uniqueness check to the provided DataFrame.
        This closure adds a condition and count column indicating if the column value is duplicated and by how many.
        The condition and count columns are applied on the DataFrame when evaluating the check.

        :param df: Input DataFrame to validate for uniqueness.
        """
        window_count_col = f"__window_count_{col_str_norm}_{unique_str}"

        filter_col = F.expr(row_filter) if row_filter else F.lit(True)
        w = Window.partitionBy(F.when(filter_col, col_expr))

        filter_condition = F.lit(True)
        if nulls_distinct:
            for column in columns:
                col_ref = F.col(column) if isinstance(column, str) else column
                filter_condition = filter_condition & col_ref.isNotNull()

        # Conditionally count only matching rows within the window
        df = df.withColumn(window_count_col, F.sum(F.when(filter_condition, F.lit(1)).otherwise(F.lit(0))).over(w))

        # Build output
        df = (
            # Add any columns used in make_condition
            df.withColumn(condition_col, F.col(window_count_col) > 1)
            .withColumn(count_col, F.coalesce(F.col(window_count_col), F.lit(0)))
            .drop(window_count_col)
        )

        return df

    condition = make_condition(
        condition=F.col(condition_col) == F.lit(True),
        message=F.concat_ws(
            "",
            F.lit("Value '"),
            col_expr.cast("string"),
            F.lit(f"' in column '{col_expr_str}' is not unique, found "),
            F.col(count_col).cast("string"),
            F.lit(" duplicates"),
        ),
        alias=f"{col_str_norm}_is_not_unique",
    )

    return condition, apply


@register_rule("dataset")
def foreign_key(
    column: str | Column,  # auto-injected from check column
    row_filter: str | None = None,  # auto-injected from check filter
    ref_column: str | Column,
    ref_df_name: str,
) -> tuple[Column, Callable]:
    """
    Build a foreign key check condition and closure for dataset-level validation.

    This function returns:
    - A Spark Column representing the condition used to evaluate foreign key violations.
    - A closure that applies the foreign key check to a DataFrame. The closure performs
      the necessary join against the reference DataFrame and generates a result DataFrame
      with a boolean column indicating whether each row violates the foreign key constraint.
      When executed the Column expression is applied on the resulting DataFrame.

    The check ignores NULL values in the foreign key column, following the SQL ANSI standard.

    :param column: The column in the main DataFrame to validate as a foreign key.
    :param row_filter: Optional filter condition pushed down from the check filter.
    :param ref_column: The column in the reference DataFrame to validate against.
    :param ref_df_name: The name of the reference DataFrame; must be provided at check execution.
    :return: A tuple containing:
        - The condition Column to apply on the condition column of the DataFrame produced by the closure.
        - A closure that accepts a DataFrame and applies the foreign key validation logic.
    """
    col_str_norm, col_expr_str, col_expr = _get_norm_column_and_expr(column)
    ref_col_str_norm, ref_col_expr_str, ref_col_expr = _get_norm_column_and_expr(ref_column)
    unique_str = uuid.uuid4().hex
    condition_col = f"__{col_str_norm}_{unique_str}"

    def apply(df: DataFrame, ref_dfs: dict[str, DataFrame]) -> DataFrame:
        """
        Apply the foreign key check to the provided DataFrame.
        This closure adds a condition column to the DataFrame.
        whether the key exists in the reference DataFrame. Condition from the make condition is applied to
        the condition column when the check is evaluated.

        :param df: Input DataFrame to validate for uniqueness.
        :param ref_dfs: Dictionary of reference DataFrames,
        must contain the reference DataFrame with name provided by `ref_df_name`.
        """
        if ref_df_name not in ref_dfs:
            raise ValueError(
                f"Reference DataFrame '{ref_df_name}' not found in provided reference DataFrames."
                f"Provide '{ref_df_name}' DataFrame when applying the checks."
            )

        ref_alias = f"__ref_{col_str_norm}_{unique_str}"

        filter_expr = F.expr(row_filter) if row_filter else F.lit(True)

        ref_df = ref_dfs[ref_df_name].select(ref_col_expr.alias(ref_alias)).distinct()
        joined = df.join(ref_df, (col_expr == F.col(ref_alias)) & col_expr.isNotNull() & filter_expr, how="left")

        # FK violation: no match found for non-null FK values
        # Add any columns used in make_condition
        result_df = joined.withColumn(condition_col, (col_expr.isNotNull()) & F.col(ref_alias).isNull())

        return result_df

    condition = make_condition(
        condition=F.col(condition_col) == F.lit(True),
        message=F.concat_ws(
            "",
            F.lit("FK violation: Value '"),
            col_expr.cast("string"),
            F.lit("' in column '"),
            F.lit(col_expr_str),
            F.lit(f"' not found in reference column '{ref_col_expr_str}'"),
        ),
        alias=f"{col_str_norm}_{ref_col_str_norm}_foreign_key_violation",
    )

    return condition, apply


@register_rule("row")
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
    :param limit: Limit to use in the condition as number, column name or sql expression
    :param row_filter: Optional filter condition pushed down from the check filter.
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


@register_rule("row")
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
    :param row_filter: Optional filter condition pushed down from the check filter.
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
