---
sidebar_label: check_funcs
title: databricks.labs.dqx.check_funcs
---

## DQPattern Objects

```python
class DQPattern(Enum)
```

Enum class to represent DQ patterns used to match data in columns.

#### make\_condition

```python
def make_condition(condition: Column, message: Column | str,
                   alias: str) -> Column
```

Helper function to create a condition column.

**Arguments**:

- `condition`: condition expression.
Pass the check if the condition evaluates to False.
Fail the check if condition evaluates to True.
- `message`: message to output - it could be either `Column` object, or string constant
- `alias`: name for the resulting column

**Returns**:

an instance of `Column` type, that either returns string if condition is evaluated to `true`,
or `null` if condition is evaluated to `false`

#### matches\_pattern

```python
def matches_pattern(column: str | Column, pattern: DQPattern) -> Column
```

Checks whether the values in the input column match a given pattern.

**Arguments**:

- `column`: column to check; can be a string column name or a column expression
- `pattern`: pattern to match against

**Returns**:

Column object for condition

#### is\_not\_null\_and\_not\_empty

```python
@register_rule("row")
def is_not_null_and_not_empty(column: str | Column,
                              trim_strings: bool | None = False) -> Column
```

Checks whether the values in the input column are not null and not empty.

**Arguments**:

- `column`: column to check; can be a string column name or a column expression
- `trim_strings`: boolean flag to trim spaces from strings

**Returns**:

Column object for condition

#### is\_not\_empty

```python
@register_rule("row")
def is_not_empty(column: str | Column) -> Column
```

Checks whether the values in the input column are not empty (but may be null).

**Arguments**:

- `column`: column to check; can be a string column name or a column expression

**Returns**:

Column object for condition

#### is\_not\_null

```python
@register_rule("row")
def is_not_null(column: str | Column) -> Column
```

Checks whether the values in the input column are not null.

**Arguments**:

- `column`: column to check; can be a string column name or a column expression

**Returns**:

Column object for condition

#### is\_not\_null\_and\_is\_in\_list

```python
@register_rule("row")
def is_not_null_and_is_in_list(column: str | Column, allowed: list) -> Column
```

Checks whether the values in the input column are not null and present in the list of allowed values.

**Arguments**:

- `column`: column to check; can be a string column name or a column expression
- `allowed`: list of allowed values (actual values or Column objects)

**Returns**:

Column object for condition

#### is\_in\_list

```python
@register_rule("row")
def is_in_list(column: str | Column, allowed: list) -> Column
```

Checks whether the values in the input column are present in the list of allowed values

(null values are allowed).

**Arguments**:

- `column`: column to check; can be a string column name or a column expression
- `allowed`: list of allowed values (actual values or Column objects)

**Returns**:

Column object for condition

#### sql\_expression

```python
@register_rule("row")
def sql_expression(expression: str,
                   msg: str | None = None,
                   name: str | None = None,
                   negate: bool = False,
                   columns: list[str | Column] | None = None) -> Column
```

Checks whether the condition provided as an SQL expression is met.

**Arguments**:

- `expression`: SQL expression. Fail if expression evaluates to True, pass if it evaluates to False.
- `msg`: optional message of the `Column` type, automatically generated if None
- `name`: optional name of the resulting column, automatically generated if None
- `negate`: if the condition should be negated (true) or not. For example, &quot;col is not null&quot; will mark null
values as &quot;bad&quot;. Although sometimes it&#x27;s easier to specify it other way around &quot;col is null&quot; + negate set to False
- `columns`: optional list of columns to be used for reporting. Unused in the actual logic.

**Returns**:

new Column

#### is\_older\_than\_col2\_for\_n\_days

```python
@register_rule("row")
def is_older_than_col2_for_n_days(column1: str | Column,
                                  column2: str | Column,
                                  days: int = 0,
                                  negate: bool = False) -> Column
```

Checks whether the values in one input column are at least N days older than the values in another column.

**Arguments**:

- `column1`: first column to check; can be a string column name or a column expression
- `column2`: second column to check; can be a string column name or a column expression
- `days`: number of days
- `negate`: if the condition should be negated (true) or not; if negated, the check will fail when values in the
first column are at least N days older than values in the second column

**Returns**:

new Column

#### is\_older\_than\_n\_days

```python
@register_rule("row")
def is_older_than_n_days(column: str | Column,
                         days: int,
                         curr_date: Column | None = None,
                         negate: bool = False) -> Column
```

Checks whether the values in the input column are at least N days older than the current date.

**Arguments**:

- `column`: column to check; can be a string column name or a column expression
- `days`: number of days
- `curr_date`: (optional) set current date
- `negate`: if the condition should be negated (true) or not; if negated, the check will fail when values in the
first column are at least N days older than values in the second column

**Returns**:

new Column

#### is\_not\_in\_future

```python
@register_rule("row")
def is_not_in_future(column: str | Column,
                     offset: int = 0,
                     curr_timestamp: Column | None = None) -> Column
```

Checks whether the values in the input column contain a timestamp that is not in the future,

where &#x27;future&#x27; is defined as current_timestamp + offset (in seconds).

**Arguments**:

- `column`: column to check; can be a string column name or a column expression
- `offset`: offset (in seconds) to add to the current timestamp at time of execution
- `curr_timestamp`: (optional) set current timestamp

**Returns**:

new Column

#### is\_not\_in\_near\_future

```python
@register_rule("row")
def is_not_in_near_future(column: str | Column,
                          offset: int = 0,
                          curr_timestamp: Column | None = None) -> Column
```

Checks whether the values in the input column contain a timestamp that is not in the near future,

where &#x27;near future&#x27; is defined as greater than the current timestamp
but less than the current_timestamp + offset (in seconds).

**Arguments**:

- `column`: column to check; can be a string column name or a column expression
- `offset`: offset (in seconds) to add to the current timestamp at time of execution
- `curr_timestamp`: (optional) set current timestamp

**Returns**:

new Column

#### is\_not\_less\_than

```python
@register_rule("row")
def is_not_less_than(
    column: str | Column,
    limit: int | datetime.date | datetime.datetime | str | Column | None = None
) -> Column
```

Checks whether the values in the input column are not less than the provided limit.

**Arguments**:

- `column`: column to check; can be a string column name or a column expression
- `limit`: limit to use in the condition as number, date, timestamp, column name or sql expression

**Returns**:

new Column

#### is\_not\_greater\_than

```python
@register_rule("row")
def is_not_greater_than(
    column: str | Column,
    limit: int | datetime.date | datetime.datetime | str | Column | None = None
) -> Column
```

Checks whether the values in the input column are not greater than the provided limit.

**Arguments**:

- `column`: column to check; can be a string column name or a column expression
- `limit`: limit to use in the condition as number, date, timestamp, column name or sql expression

**Returns**:

new Column

#### is\_in\_range

```python
@register_rule("row")
def is_in_range(
    column: str | Column,
    min_limit: int | datetime.date | datetime.datetime | str | Column
    | None = None,
    max_limit: int | datetime.date | datetime.datetime | str | Column
    | None = None
) -> Column
```

Checks whether the values in the input column are in the provided limits (inclusive of both boundaries).

**Arguments**:

- `column`: column to check; can be a string column name or a column expression
- `min_limit`: min limit to use in the condition as number, date, timestamp, column name or sql expression
- `max_limit`: max limit to use in the condition as number, date, timestamp, column name or sql expression

**Returns**:

new Column

#### is\_not\_in\_range

```python
@register_rule("row")
def is_not_in_range(
    column: str | Column,
    min_limit: int | datetime.date | datetime.datetime | str | Column
    | None = None,
    max_limit: int | datetime.date | datetime.datetime | str | Column
    | None = None
) -> Column
```

Checks whether the values in the input column are outside the provided limits (inclusive of both boundaries).

**Arguments**:

- `column`: column to check; can be a string column name or a column expression
- `min_limit`: min limit to use in the condition as number, date, timestamp, column name or sql expression
- `max_limit`: min limit to use in the condition as number, date, timestamp, column name or sql expression

**Returns**:

new Column

#### regex\_match

```python
@register_rule("row")
def regex_match(column: str | Column,
                regex: str,
                negate: bool = False) -> Column
```

Checks whether the values in the input column matches a given regex.

**Arguments**:

- `column`: column to check; can be a string column name or a column expression
- `regex`: regex to check
- `negate`: if the condition should be negated (true) or not

**Returns**:

Column object for condition

#### is\_not\_null\_and\_not\_empty\_array

```python
@register_rule("row")
def is_not_null_and_not_empty_array(column: str | Column) -> Column
```

Checks whether the values in the array input column are not null and not empty.

**Arguments**:

- `column`: column to check; can be a string column name or a column expression

**Returns**:

Column object for condition

#### is\_valid\_date

```python
@register_rule("row")
def is_valid_date(column: str | Column,
                  date_format: str | None = None) -> Column
```

Checks whether the values in the input column have valid date formats.

**Arguments**:

- `column`: column to check; can be a string column name or a column expression
- `date_format`: date format (e.g. &#x27;yyyy-mm-dd&#x27;)

**Returns**:

Column object for condition

#### is\_valid\_timestamp

```python
@register_rule("row")
def is_valid_timestamp(column: str | Column,
                       timestamp_format: str | None = None) -> Column
```

Checks whether the values in the input column have valid timestamp formats.

**Arguments**:

- `column`: column to check; can be a string column name or a column expression
- `timestamp_format`: timestamp format (e.g. &#x27;yyyy-mm-dd HH:mm:ss&#x27;)

**Returns**:

Column object for condition

#### is\_valid\_ipv4\_address

```python
@register_rule("row")
def is_valid_ipv4_address(column: str | Column) -> Column
```

Checks whether the values in the input column have valid IPv4 address formats.

**Arguments**:

- `column`: column to check; can be a string column name or a column expression

**Returns**:

Column object for condition

#### is\_ipv4\_address\_in\_cidr

```python
@register_rule("row")
def is_ipv4_address_in_cidr(column: str | Column, cidr_block: str) -> Column
```

Checks if an IP column value falls within the given CIDR block.

**Arguments**:

- `column`: column to check; can be a string column name or a column expression
- `cidr_block`: CIDR block string (e.g., &#x27;192.168.1.0/24&#x27;)

**Raises**:

- `ValueError`: If cidr_block is not a valid string in CIDR notation.

**Returns**:

Column object for condition

#### is\_data\_fresh

```python
@register_rule("row")
def is_data_fresh(
    column: str | Column,
    max_age_minutes: int,
    base_timestamp: str | datetime.date | datetime.datetime | Column
    | None = None
) -> Column
```

Checks whether the values in the timestamp column are not older than the specified number of minutes from the base timestamp column.

This is useful for identifying stale data due to delayed pipelines and helps catch upstream issues early.

**Arguments**:

- `column`: column to check; can be a string column name or a column expression containing timestamp values
- `max_age_minutes`: maximum age in minutes before data is considered stale
- `base_timestamp`: (optional) set base timestamp column from which the stale check is calculated, if not provided uses current_timestamp()

**Returns**:

Column object for condition

#### is\_unique

```python
@register_rule("dataset")
def is_unique(columns: list[str | Column],
              nulls_distinct: bool = True,
              row_filter: str | None = None) -> tuple[Column, Callable]
```

Build a uniqueness check condition and closure for dataset-level validation.

This function checks whether the specified columns contain unique values within the dataset
and reports rows with duplicate combinations. When `nulls_distinct`
is True (default), rows with NULLs are treated as distinct (SQL ANSI behavior); otherwise,
NULLs are treated as equal when checking for duplicates.

In streaming, uniqueness is validated within individual micro-batches only.

**Arguments**:

- `columns`: List of column names (str) or Spark Column expressions to validate for uniqueness.
- `nulls_distinct`: Whether NULLs are treated as distinct (default: True).
- `row_filter`: Optional SQL expression for filtering rows before checking uniqueness.
Auto-injected from the check filter.

**Returns**:

A tuple of:
- A Spark Column representing the condition for uniqueness violations.
- A closure that applies the uniqueness check and adds the necessary condition/count columns.

#### foreign\_key

```python
@register_rule("dataset")
def foreign_key(columns: list[str | Column],
                ref_columns: list[str | Column],
                ref_df_name: str | None = None,
                ref_table: str | None = None,
                negate: bool = False,
                row_filter: str | None = None) -> tuple[Column, Callable]
```

Build a foreign key check condition and closure for dataset-level validation.

This function verifies that values in the specified foreign key columns exist (or don&#x27;t exist, if `negate=True`) in
the corresponding reference columns of another DataFrame or table. Rows where
foreign key values do not match the reference are reported as violations.

NULL values in the foreign key columns are ignored (SQL ANSI behavior).

**Arguments**:

- `columns`: List of column names (str) or Column expressions in the dataset (foreign key).
- `ref_columns`: List of column names (str) or Column expressions in the reference dataset.
- `ref_df_name`: Name of the reference DataFrame (used when passing DataFrames directly).
- `ref_table`: Name of the reference table (used when reading from catalog).
- `row_filter`: Optional SQL expression for filtering rows before checking the foreign key.
Auto-injected from the check filter.
- `negate`: If True, the condition is negated (i.e., the check fails when the foreign key values exist in the
reference DataFrame/Table). If False, the check fails when the foreign key values do not exist in the reference.

**Returns**:

A tuple of:
- A Spark Column representing the condition for foreign key violations.
- A closure that applies the foreign key validation by joining against the reference.

#### sql\_query

```python
@register_rule("dataset")
def sql_query(query: str,
              merge_columns: list[str],
              msg: str | None = None,
              name: str | None = None,
              negate: bool = False,
              condition_column: str = "condition",
              input_placeholder: str = "input_view",
              row_filter: str | None = None) -> tuple[Column, Callable]
```

Checks whether the condition column generated by SQL query is met.

**Arguments**:

- `query`: SQL query that must return as a minimum a condition column and
all merge columns. The resulting DataFrame is automatically joined back to the input DataFrame.
using the merge_columns. Reference DataFrames when provided in the ref_dfs parameter are registered as temp view.
- `condition_column`: Column name indicating violation (boolean). Fail the check if True, pass it if False
- `merge_columns`: List of columns for join back to the input DataFrame.
They must provide a unique key for the join, otherwise a duplicate records may be produced.
- `msg`: Optional custom message or Column expression.
- `name`: Optional name for the result.
- `negate`: If True, the condition is negated (i.e., the check fails when the condition is False).
- `input_placeholder`: Name to be used in the sql query as `{{ input_placeholder }}` to refer to the
input DataFrame on which the checks are applied.
- `row_filter`: Optional SQL expression for filtering rows before checking the foreign key.
Auto-injected from the check filter.

**Returns**:

Tuple (condition column, apply function).

#### is\_aggr\_not\_greater\_than

```python
@register_rule("dataset")
def is_aggr_not_greater_than(
        column: str | Column,
        limit: int | float | str | Column,
        aggr_type: str = "count",
        group_by: list[str | Column] | None = None,
        row_filter: str | None = None) -> tuple[Column, Callable]
```

Build an aggregation check condition and closure for dataset-level validation.

This function verifies that an aggregation (count, sum, avg, min, max) on a column
or group of columns does not exceed a specified limit. Rows where the aggregation
result exceeds the limit are flagged.

**Arguments**:

- `column`: Column name (str) or Column expression to aggregate.
- `limit`: Numeric value, column name, or SQL expression for the limit.
- `aggr_type`: Aggregation type: &#x27;count&#x27;, &#x27;sum&#x27;, &#x27;avg&#x27;, &#x27;min&#x27;, or &#x27;max&#x27; (default: &#x27;count&#x27;).
- `group_by`: Optional list of column names or Column expressions to group by.
- `row_filter`: Optional SQL expression to filter rows before aggregation. Auto-injected from the check filter.

**Returns**:

A tuple of:
- A Spark Column representing the condition for aggregation limit violations.
- A closure that applies the aggregation check and adds the necessary condition/metric columns.

#### is\_aggr\_not\_less\_than

```python
@register_rule("dataset")
def is_aggr_not_less_than(
        column: str | Column,
        limit: int | float | str | Column,
        aggr_type: str = "count",
        group_by: list[str | Column] | None = None,
        row_filter: str | None = None) -> tuple[Column, Callable]
```

Build an aggregation check condition and closure for dataset-level validation.

This function verifies that an aggregation (count, sum, avg, min, max) on a column
or group of columns is not below a specified limit. Rows where the aggregation
result is below the limit are flagged.

**Arguments**:

- `column`: Column name (str) or Column expression to aggregate.
- `limit`: Numeric value, column name, or SQL expression for the limit.
- `aggr_type`: Aggregation type: &#x27;count&#x27;, &#x27;sum&#x27;, &#x27;avg&#x27;, &#x27;min&#x27;, or &#x27;max&#x27; (default: &#x27;count&#x27;).
- `group_by`: Optional list of column names or Column expressions to group by.
- `row_filter`: Optional SQL expression to filter rows before aggregation. Auto-injected from the check filter.

**Returns**:

A tuple of:
- A Spark Column representing the condition for aggregation limit violations.
- A closure that applies the aggregation check and adds the necessary condition/metric columns.

#### is\_aggr\_equal

```python
@register_rule("dataset")
def is_aggr_equal(column: str | Column,
                  limit: int | float | str | Column,
                  aggr_type: str = "count",
                  group_by: list[str | Column] | None = None,
                  row_filter: str | None = None) -> tuple[Column, Callable]
```

Build an aggregation check condition and closure for dataset-level validation.

This function verifies that an aggregation (count, sum, avg, min, max) on a column
or group of columns is equal to a specified limit. Rows where the aggregation
result is not equal to the limit are flagged.

**Arguments**:

- `column`: Column name (str) or Column expression to aggregate.
- `limit`: Numeric value, column name, or SQL expression for the limit.
- `aggr_type`: Aggregation type: &#x27;count&#x27;, &#x27;sum&#x27;, &#x27;avg&#x27;, &#x27;min&#x27;, or &#x27;max&#x27; (default: &#x27;count&#x27;).
- `group_by`: Optional list of column names or Column expressions to group by.
- `row_filter`: Optional SQL expression to filter rows before aggregation. Auto-injected from the check filter.

**Returns**:

A tuple of:
- A Spark Column representing the condition for aggregation limit violations.
- A closure that applies the aggregation check and adds the necessary condition/metric columns.

#### is\_aggr\_not\_equal

```python
@register_rule("dataset")
def is_aggr_not_equal(
        column: str | Column,
        limit: int | float | str | Column,
        aggr_type: str = "count",
        group_by: list[str | Column] | None = None,
        row_filter: str | None = None) -> tuple[Column, Callable]
```

Build an aggregation check condition and closure for dataset-level validation.

This function verifies that an aggregation (count, sum, avg, min, max) on a column
or group of columns is not equal to a specified limit. Rows where the aggregation
result is equal to the limit are flagged.

**Arguments**:

- `column`: Column name (str) or Column expression to aggregate.
- `limit`: Numeric value, column name, or SQL expression for the limit.
- `aggr_type`: Aggregation type: &#x27;count&#x27;, &#x27;sum&#x27;, &#x27;avg&#x27;, &#x27;min&#x27;, or &#x27;max&#x27; (default: &#x27;count&#x27;).
- `group_by`: Optional list of column names or Column expressions to group by.
- `row_filter`: Optional SQL expression to filter rows before aggregation. Auto-injected from the check filter.

**Returns**:

A tuple of:
- A Spark Column representing the condition for aggregation limit violations.
- A closure that applies the aggregation check and adds the necessary condition/metric columns.

#### compare\_datasets

```python
@register_rule("dataset")
def compare_datasets(columns: list[str | Column],
                     ref_columns: list[str | Column],
                     ref_df_name: str | None = None,
                     ref_table: str | None = None,
                     check_missing_records: bool | None = False,
                     exclude_columns: list[str | Column] | None = None,
                     null_safe_row_matching: bool | None = True,
                     null_safe_column_value_matching: bool | None = True,
                     row_filter: str | None = None) -> tuple[Column, Callable]
```

Dataset-level check that compares two datasets and returns a condition for changed rows,

with details on a row and column-level differences.
Only columns that are common across both datasets will be compared. Mismatched columns are ignored.
Detailed information about the differences is provided in the condition column.
The comparison does not support Map types (any column comparison on map type is skipped automatically).

The log containing detailed differences is written to the message field of the check result as JSON string.
Example: `{"row_missing":false,"row_extra":true,"changed":{"val":{"df":"val1"}}}`

**Arguments**:

- `columns`: List of columns to use for row matching with the reference DataFrame
(can be a list of string column names or column expressions).
Only simple column expressions are supported, e.g. F.col(&quot;col_name&quot;)
- `ref_columns`: List of columns in the reference DataFrame or Table to row match against the source DataFrame
(can be a list of string column names or column expressions). The `columns` parameter is matched  with `ref_columns`
by position, so the order of the provided columns in both lists must be exactly aligned.
Only simple column expressions are supported, e.g. F.col(&quot;col_name&quot;)
- `ref_df_name`: Name of the reference DataFrame (used when passing DataFrames directly).
- `ref_table`: Name of the reference table (used when reading from catalog).
- `check_missing_records`: Perform FULL OUTER JOIN between the DataFrames to find also records
that could be missing from the DataFrame. Use this with caution as it may produce output with more rows
than in the original DataFrame.
- `exclude_columns`: List of columns to exclude from the value comparison but not from row matching
(can be a list of string column names or column expressions).
Only simple column expressions are supported, e.g. F.col(&quot;col_name&quot;)
The parameter does not alter the list of columns used to determine row matches,
it only controls which columns are skipped during the column value comparison.
- `null_safe_row_matching`: If True, treats nulls as equal when matching rows.
- `columns`0: If True, treats nulls as equal when matching column values.
If enabled (NULL, NULL) column values are equal and matching.
- `columns`1: Optional SQL expression to filter rows in the input DataFrame.
Auto-injected from the check filter.

**Returns**:

A tuple of:
- A Spark Column representing the condition for comparison violations.
- A closure that applies the comparison validation.

#### is\_data\_fresh\_per\_time\_window

```python
@register_rule("dataset")
def is_data_fresh_per_time_window(
        column: str | Column,
        window_minutes: int,
        min_records_per_window: int,
        lookback_windows: int | None = None,
        row_filter: str | None = None,
        curr_timestamp: Column | None = None) -> tuple[Column, Callable]
```

Build a completeness freshness check that validates records arrive at least every X minutes

with a threshold for the expected number of rows per time window.

If `lookback_windows` is provided, only data within that lookback period will be validated.
If omitted, the entire dataset will be checked.

**Arguments**:

- `column`: Column name (str) or Column expression containing timestamps to check.
- `window_minutes`: Time window in minutes to check for data arrival.
- `min_records_per_window`: Minimum number of records expected per time window.
- `lookback_windows`: Optional number of time windows to look back from `curr_timestamp`.
This filters records to include only those within the specified number of time windows from `curr_timestamp`.
If no lookback is provided, the check is applied to the entire dataset.
- `row_filter`: Optional SQL expression to filter rows before checking.
- `curr_timestamp`: Optional current timestamp column. If not provided, current_timestamp() function is used.

**Returns**:

A tuple of:
- A Spark Column representing the condition for missing data within a time window.
- A closure that applies the completeness check and adds the necessary condition columns.

