# databricks.labs.dqx.check\_funcs

## DQPattern Objects[​](#dqpattern-objects "Direct link to DQPattern Objects")

```python
class DQPattern(Enum)

```

Enum class to represent DQ patterns used to match data in columns.

### make\_condition[​](#make_condition "Direct link to make_condition")

```python
def make_condition(condition: Column, message: Column | str,
                   alias: str) -> Column

```

Helper function to create a condition column.

**Arguments**:

* `condition` - condition expression.

  <!-- -->

  * Pass the check if the condition evaluates to False
  * Fail the check if condition evaluates to True

* `message` - message to output - it could be either *Column* object, or string constant

* `alias` - name for the resulting column

**Returns**:

an instance of *Column* type, that either returns string if condition is evaluated to *true*, or *null* if condition is evaluated to *false*

### is\_not\_null\_and\_not\_empty[​](#is_not_null_and_not_empty "Direct link to is_not_null_and_not_empty")

```python
@register_rule("row")
def is_not_null_and_not_empty(column: str | Column,
                              trim_strings: bool | None = False) -> Column

```

Checks whether the values in the input column are not null and not empty.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression
* `trim_strings` - boolean flag to trim spaces from strings

**Returns**:

Column object for condition

### is\_not\_empty[​](#is_not_empty "Direct link to is_not_empty")

```python
@register_rule("row")
def is_not_empty(column: str | Column,
                 trim_strings: bool | None = False) -> Column

```

Checks whether the values in the input column are not empty (but may be null).

**Arguments**:

* `column` - column to check; can be a string column name or a column expression
* `trim_strings` - boolean flag to trim spaces from strings

**Returns**:

Column object for condition

### is\_not\_null[​](#is_not_null "Direct link to is_not_null")

```python
@register_rule("row")
def is_not_null(column: str | Column) -> Column

```

Checks whether the values in the input column are not null.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression

**Returns**:

Column object for condition

### is\_null[​](#is_null "Direct link to is_null")

```python
@register_rule("row")
def is_null(column: str | Column) -> Column

```

Checks whether the values in the input column are null.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression

**Returns**:

Column object for condition

### is\_empty[​](#is_empty "Direct link to is_empty")

```python
@register_rule("row")
def is_empty(column: str | Column,
             trim_strings: bool | None = False) -> Column

```

Checks whether the values in the input column are empty (but may be null).

**Arguments**:

* `column` - column to check; can be a string column name or a column expression
* `trim_strings` - boolean flag to trim spaces from strings

**Returns**:

Column object for condition

### is\_null\_or\_empty[​](#is_null_or_empty "Direct link to is_null_or_empty")

```python
@register_rule("row")
def is_null_or_empty(column: str | Column,
                     trim_strings: bool | None = False) -> Column

```

Checks whether the values in the input column are either null or empty.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression
* `trim_strings` - boolean flag to trim spaces from strings

**Returns**:

Column object for condition

### is\_not\_null\_and\_is\_in\_list[​](#is_not_null_and_is_in_list "Direct link to is_not_null_and_is_in_list")

```python
@register_rule("row")
def is_not_null_and_is_in_list(column: str | Column,
                               allowed: list,
                               case_sensitive: bool = True) -> Column

```

Checks whether the values in the input column are not null and present in the list of allowed values. Can optionally perform a case-insensitive comparison. This check is not suited for `MapType` or `StructType` columns.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression
* `allowed` - list of allowed values (actual values or Column objects)
* `case_sensitive` - whether to perform a case-sensitive comparison (default: True)

**Returns**:

Column object for condition

**Raises**:

* `MissingParameterError` - If the allowed list is not provided.
* `InvalidParameterError` - If the allowed parameter is not a list, or if the list is empty.

### is\_in\_list[​](#is_in_list "Direct link to is_in_list")

```python
@register_rule("row")
def is_in_list(column: str | Column,
               allowed: list,
               case_sensitive: bool = True) -> Column

```

Checks whether the values in the input column are present in the list of allowed values (null values are allowed). Can optionally perform a case-insensitive comparison. This check is not suited for `MapType` or `StructType` columns.

**Notes**:

This check is not suited for `MapType` or `StructType` columns. For best performance with large lists, use the `foreign_key` check function.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression
* `allowed` - list of allowed values (actual values or Column objects)
* `case_sensitive` - whether to perform a case-sensitive comparison (default: True)

**Returns**:

Column object for condition

**Raises**:

* `MissingParameterError` - If the allowed list is not provided.
* `InvalidParameterError` - If the allowed parameter is not a list.

### is\_not\_in\_list[​](#is_not_in_list "Direct link to is_not_in_list")

```python
@register_rule("row")
def is_not_in_list(column: str | Column,
                   forbidden: list,
                   case_sensitive: bool = True) -> Column

```

Checks whether the values in the input column are NOT present in the list of forbidden values (null values are allowed). Can optionally perform a case-insensitive comparison.

**Notes**:

This check is not suited for `MapType` or `StructType` columns. For best performance with large lists, use the `foreign_key` check function with the `negate` parameter set to `True`.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression
* `forbidden` - list of forbidden values (actual values or Column objects)
* `case_sensitive` - whether to perform a case-sensitive comparison (default: True)

**Returns**:

Column object for condition

**Raises**:

* `MissingParameterError` - If the forbidden list is not provided.
* `InvalidParameterError` - If the forbidden parameter is not a list.

### sql\_expression[​](#sql_expression "Direct link to sql_expression")

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

* `expression` - SQL expression. Fail if expression evaluates to False, pass if it evaluates to True.
* `msg` - optional message of the *Column* type, automatically generated if None
* `name` - optional name of the resulting column, automatically generated if None
* `negate` - if the condition should be negated (true) or not. For example, "col is not null" will mark null values as "bad". Although sometimes it's easier to specify it other way around "col is null" + negate set to True
* `columns` - optional list of columns to be used for validation against the actual input DataFrame, reporting and for constructing name prefix if check name is not provided.

**Returns**:

new Column

### is\_older\_than\_col2\_for\_n\_days[​](#is_older_than_col2_for_n_days "Direct link to is_older_than_col2_for_n_days")

```python
@register_rule("row")
def is_older_than_col2_for_n_days(column1: str | Column,
                                  column2: str | Column,
                                  days: int = 0,
                                  negate: bool = False) -> Column

```

Checks whether the values in one input column are at least N days older than the values in another column.

**Arguments**:

* `column1` - first column to check; can be a string column name or a column expression
* `column2` - second column to check; can be a string column name or a column expression
* `days` - number of days
* `negate` - if the condition should be negated (true) or not; if negated, the check will fail when values in the first column are at least N days older than values in the second column

**Returns**:

new Column

### is\_older\_than\_n\_days[​](#is_older_than_n_days "Direct link to is_older_than_n_days")

```python
@register_rule("row")
def is_older_than_n_days(column: str | Column,
                         days: int,
                         curr_date: Column | None = None,
                         negate: bool = False) -> Column

```

Checks whether the values in the input column are at least N days older than the current date.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression
* `days` - number of days
* `curr_date` - (optional) set current date
* `negate` - if the condition should be negated (true) or not; if negated, the check will fail when values in the first column are at least N days older than values in the second column

**Returns**:

new Column

### is\_not\_in\_future[​](#is_not_in_future "Direct link to is_not_in_future")

```python
@register_rule("row")
def is_not_in_future(column: str | Column,
                     offset: int = 0,
                     curr_timestamp: Column | None = None) -> Column

```

Checks whether the values in the input column contain a timestamp that is not in the future, where 'future' is defined as current\_timestamp + offset (in seconds).

**Arguments**:

* `column` - column to check; can be a string column name or a column expression
* `offset` - offset (in seconds) to add to the current timestamp at time of execution
* `curr_timestamp` - (optional) set current timestamp

**Returns**:

new Column

### is\_not\_in\_near\_future[​](#is_not_in_near_future "Direct link to is_not_in_near_future")

```python
@register_rule("row")
def is_not_in_near_future(column: str | Column,
                          offset: int = 0,
                          curr_timestamp: Column | None = None) -> Column

```

Checks whether the values in the input column contain a timestamp that is not in the near future, where 'near future' is defined as greater than the current timestamp but less than the current\_timestamp + offset (in seconds).

**Arguments**:

* `column` - column to check; can be a string column name or a column expression
* `offset` - offset (in seconds) to add to the current timestamp at time of execution
* `curr_timestamp` - (optional) set current timestamp

**Returns**:

new Column

### is\_equal\_to[​](#is_equal_to "Direct link to is_equal_to")

```python
@register_rule("row")
def is_equal_to(column: str | Column,
                value: int | float | Decimal | str | datetime.date
                | datetime.datetime | Column | None = None,
                abs_tolerance: float | None = None,
                rel_tolerance: float | None = None) -> Column

```

Check whether the values in the input column are equal to the given value.

**Arguments**:

* `column` *str | Column* - Column to check. Can be a string column name or a column expression.
* `value` - The value to compare with. Can be a number, date, timestamp literal or a Spark Column. Defaults to None.
* `abs_tolerance` - Values are considered equal if the absolute difference is less than or equal to the tolerance. This is applicable to numeric columns.
* `Example` - abs(a - b) <= tolerance With tolerance=0.01: 2.001 and 2.0099 → equal (diff = 0.0089) 2.001 and 2.02 → not equal (diff = 0.019)
* `rel_tolerance` - Relative tolerance for numeric comparisons. Differences within this relative tolerance are ignored. Useful if numbers vary in scale.
* `Example` - abs(a - b) <= rel\_tolerance \* max(abs(a), abs(b)) With tolerance=0.01 (1%): 100 vs 101 → equal (diff = 1, tolerance = 1) 100 vs 102 → not equal (diff = 2, tolerance = 1)

**Returns**:

* `Column` - A Spark Column condition that fails if the column value is not equal to the given value.

**Raises**:

* `InvalidParameterError` - If absolute or relative tolerances are negative.

**Notes**:

If both tolerances are provided, the value is considered equal if it meets either tolerance condition.

### is\_not\_equal\_to[​](#is_not_equal_to "Direct link to is_not_equal_to")

```python
@register_rule("row")
def is_not_equal_to(column: str | Column,
                    value: int | float | Decimal | str | datetime.date
                    | datetime.datetime | Column | None = None,
                    abs_tolerance: float | None = None,
                    rel_tolerance: float | None = None) -> Column

```

Check whether the values in the input column are not equal to the given value.

**Arguments**:

* `column` *str | Column* - Column to check. Can be a string column name or a column expression.
* `value` - The value to compare with. Can be a number, date, timestamp literal or a Spark Column. Defaults to None.
* `abs_tolerance` - Values are considered equal if the absolute difference is less than or equal to the tolerance. This is applicable to numeric columns.
* `Example` - abs(a - b) <= tolerance With tolerance=0.01: 2.001 and 2.0099 → equal (diff = 0.0089) 2.001 and 2.02 → not equal (diff = 0.019)
* `rel_tolerance` - Relative tolerance for numeric comparisons. Differences within this relative tolerance are ignored. Useful if numbers vary in scale.
* `Example` - abs(a - b) <= rel\_tolerance \* max(abs(a), abs(b)) With tolerance=0.01 (1%): 100 vs 101 → equal (diff = 1, tolerance = 1) 100 vs 102 → not equal (diff = 2, tolerance = 1)

**Returns**:

* `Column` - A Spark Column condition that fails if the column value is equal to the given value.

**Raises**:

* `InvalidParameterError` - If absolute or relative tolerances are negative.

**Notes**:

If both tolerances are provided, the value is considered equal if it meets either tolerance condition.

### is\_not\_less\_than[​](#is_not_less_than "Direct link to is_not_less_than")

```python
@register_rule("row")
def is_not_less_than(
    column: str | Column,
    limit: int | float | Decimal | datetime.date | datetime.datetime | str
    | Column | None = None
) -> Column

```

Checks whether the values in the input column are not less than the provided limit.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression
* `limit` - limit to use in the condition as number, date, timestamp, column name or sql expression

**Returns**:

new Column

### is\_not\_greater\_than[​](#is_not_greater_than "Direct link to is_not_greater_than")

```python
@register_rule("row")
def is_not_greater_than(
    column: str | Column,
    limit: int | float | Decimal | datetime.date | datetime.datetime | str
    | Column | None = None
) -> Column

```

Checks whether the values in the input column are not greater than the provided limit.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression
* `limit` - limit to use in the condition as number, date, timestamp, column name or sql expression

**Returns**:

new Column

### is\_in\_range[​](#is_in_range "Direct link to is_in_range")

```python
@register_rule("row")
def is_in_range(
    column: str | Column,
    min_limit: int | float | Decimal | datetime.date | datetime.datetime | str
    | Column | None = None,
    max_limit: int | float | Decimal | datetime.date | datetime.datetime | str
    | Column | None = None
) -> Column

```

Checks whether the values in the input column are in the provided limits (inclusive of both boundaries).

**Arguments**:

* `column` - column to check; can be a string column name or a column expression
* `min_limit` - min limit to use in the condition as number, date, timestamp, column name or sql expression
* `max_limit` - max limit to use in the condition as number, date, timestamp, column name or sql expression

**Returns**:

new Column

### is\_not\_in\_range[​](#is_not_in_range "Direct link to is_not_in_range")

```python
@register_rule("row")
def is_not_in_range(
    column: str | Column,
    min_limit: int | float | Decimal | datetime.date | datetime.datetime | str
    | Column | None = None,
    max_limit: int | float | Decimal | datetime.date | datetime.datetime | str
    | Column | None = None
) -> Column

```

Checks whether the values in the input column are outside the provided limits (inclusive of both boundaries).

**Arguments**:

* `column` - column to check; can be a string column name or a column expression
* `min_limit` - min limit to use in the condition as number, date, timestamp, column name or sql expression
* `max_limit` - max limit to use in the condition as number, date, timestamp, column name or sql expression

**Returns**:

new Column

### regex\_match[​](#regex_match "Direct link to regex_match")

```python
@register_rule("row")
def regex_match(column: str | Column,
                regex: str,
                negate: bool = False) -> Column

```

Checks whether the values in the input column matches a given regex.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression
* `regex` - regex to check
* `negate` - if the condition should be negated (true) or not

**Returns**:

Column object for condition

### is\_not\_null\_and\_not\_empty\_array[​](#is_not_null_and_not_empty_array "Direct link to is_not_null_and_not_empty_array")

```python
@register_rule("row")
def is_not_null_and_not_empty_array(column: str | Column) -> Column

```

Checks whether the values in the array input column are not null and not empty.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression

**Returns**:

Column object for condition

### is\_valid\_date[​](#is_valid_date "Direct link to is_valid_date")

```python
@register_rule("row")
def is_valid_date(column: str | Column,
                  date_format: str | None = None) -> Column

```

Checks whether the values in the input column have valid date formats.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression
* `date_format` - date format (e.g. 'yyyy-mm-dd')

**Returns**:

Column object for condition

### is\_valid\_timestamp[​](#is_valid_timestamp "Direct link to is_valid_timestamp")

```python
@register_rule("row")
def is_valid_timestamp(column: str | Column,
                       timestamp_format: str | None = None) -> Column

```

Checks whether the values in the input column have valid timestamp formats.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression
* `timestamp_format` - timestamp format (e.g. 'yyyy-mm-dd HH:mm
  <!-- -->
  :ss
  <!-- -->
  ')

**Returns**:

Column object for condition

### is\_valid\_ipv4\_address[​](#is_valid_ipv4_address "Direct link to is_valid_ipv4_address")

```python
@register_rule("row")
def is_valid_ipv4_address(column: str | Column) -> Column

```

Checks whether the values in the input column have valid IPv4 address formats.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression

**Returns**:

Column object for condition

### is\_ipv4\_address\_in\_cidr[​](#is_ipv4_address_in_cidr "Direct link to is_ipv4_address_in_cidr")

```python
@register_rule("row")
def is_ipv4_address_in_cidr(column: str | Column, cidr_block: str) -> Column

```

Checks if an IPv4 column value falls within the given CIDR block.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression
* `cidr_block` - CIDR block string (e.g., '192.168.1.0/24')

**Returns**:

Column object for condition

**Raises**:

* `MissingParameterError` - if *cidr\_block* is None.
* `InvalidParameterError` - if *cidr\_block* is an empty string.
* `InvalidParameterError` - if *cidr\_block* is provided but not in valid IPv4 CIDR notation.

### is\_valid\_ipv6\_address[​](#is_valid_ipv6_address "Direct link to is_valid_ipv6_address")

```python
@register_rule("row")
def is_valid_ipv6_address(column: str | Column) -> Column

```

Validate if the column contains properly formatted IPv6 addresses.

**Arguments**:

* `column` - The column to check; can be a string column name or a Column expression.

**Returns**:

Column object for condition indicating whether a value is a valid IPv6 address.

### is\_ipv6\_address\_in\_cidr[​](#is_ipv6_address_in_cidr "Direct link to is_ipv6_address_in_cidr")

```python
@register_rule("row")
def is_ipv6_address_in_cidr(column: str | Column, cidr_block: str) -> Column

```

Fail if IPv6 is invalid OR (valid AND not in CIDR). Null for null inputs.

**Arguments**:

* `column` - The column to check; can be a string column name or a Column expression.
* `cidr_block` - The CIDR block to check against.

**Returns**:

* `Column` - A Column expression indicating whether each value is not a valid IPv6 address or not in the CIDR block.

**Raises**:

* `MissingParameterError` - If *cidr\_block* is None.
* `InvalidParameterError` - If *cidr\_block* is an empty string.
* `InvalidParameterError` - if *cidr\_block* is provided but not in valid IPv6 CIDR notation.

### is\_data\_fresh[​](#is_data_fresh "Direct link to is_data_fresh")

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

* `column` - column to check; can be a string column name or a column expression containing timestamp values
* `max_age_minutes` - maximum age in minutes before data is considered stale
* `base_timestamp` - (optional) set base timestamp column from which the stale check is calculated, if not provided uses current\_timestamp()

**Returns**:

Column object for condition

### has\_no\_outliers[​](#has_no_outliers "Direct link to has_no_outliers")

```python
@register_rule("dataset")
def has_no_outliers(column: str | Column,
                    row_filter: str | None = None) -> tuple[Column, Callable]

```

Build an outlier check condition and closure for dataset-level validation.

This function uses a statistical method called MAD (Median Absolute Deviation) to check whether the specified column's values are within the calculated limits. The lower limit is calculated as median - 3.5 \* MAD and the upper limit as median + 3.5 \* MAD. Values outside these limits are considered outliers.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression
* `row_filter` - Optional SQL expression for filtering rows before checking for outliers. Auto-injected from the check filter.

**Returns**:

A tuple of:

* A Spark Column representing the condition for outliers violations.
* A closure that applies the outliers check and adds the necessary condition/count columns.

### is\_unique[​](#is_unique "Direct link to is_unique")

```python
@register_rule("dataset")
def is_unique(columns: list[str | Column],
              nulls_distinct: bool = True,
              row_filter: str | None = None) -> tuple[Column, Callable]

```

Build a uniqueness check condition and closure for dataset-level validation.

This function checks whether the specified columns contain unique values within the dataset and reports rows with duplicate combinations. When *nulls\_distinct* is True (default), rows with NULLs are treated as distinct (SQL ANSI behavior); otherwise, NULLs are treated as equal when checking for duplicates.

In streaming, uniqueness is validated within individual micro-batches only.

**Arguments**:

* `columns` - List of column names (str) or Spark Column expressions to validate for uniqueness.
* `nulls_distinct` - Whether NULLs are treated as distinct (default: True).
* `row_filter` - Optional SQL expression for filtering rows before checking uniqueness. Auto-injected from the check filter.

**Returns**:

A tuple of:

* A Spark Column representing the condition for uniqueness violations.
* A closure that applies the uniqueness check and adds the necessary condition/count columns.

### foreign\_key[​](#foreign_key "Direct link to foreign_key")

```python
@register_rule("dataset")
def foreign_key(columns: list[str | Column],
                ref_columns: list[str | Column],
                ref_df_name: str | None = None,
                ref_table: str | None = None,
                negate: bool = False,
                row_filter: str | None = None,
                null_safe: bool = False) -> tuple[Column, Callable]

```

Build a foreign key check condition and closure for dataset-level validation.

This function verifies that values in the specified foreign key columns exist (or don't exist, if *negate=True*) in the corresponding reference columns of another DataFrame or table. Rows where foreign key values do not match the reference are reported as violations.

By default, NULL values in the foreign key columns are ignored (SQL ANSI behavior). When *null\_safe=True*, NULL foreign-key values are matched against NULL reference values.

**Arguments**:

* `columns` - List of column names (str) or Column expressions in the dataset (foreign key).
* `ref_columns` - List of column names (str) or Column expressions in the reference dataset.
* `ref_df_name` - Name of the reference DataFrame (used when passing DataFrames directly).
* `ref_table` - Name of the reference table (used when reading from catalog).
* `negate` - If True, the condition is negated (i.e., the check fails when the foreign key values exist in the reference DataFrame/Table). If False, the check fails when the foreign key values do not exist in the reference.
* `row_filter` - Optional SQL expression for filtering rows before checking the foreign key. Auto-injected from the check filter.
* `null_safe` - If True, checks NULL foreign key values to match NULL reference values. If False, skips NULL values in the foreign key columns. False is a default.

**Returns**:

A tuple of:

* A Spark Column representing the condition for foreign key violations.
* A closure that applies the foreign key validation by joining against the reference.

**Raises**:

MissingParameterError:

* if neither *ref\_df\_name* nor *ref\_table* is provided. InvalidParameterError:
* if both *ref\_df\_name* and *ref\_table* are provided.
* if the number of *columns* and *ref\_columns* do not match.
* if *ref\_df\_name* is not found in the provided *ref\_dfs* dictionary.

### sql\_query[​](#sql_query "Direct link to sql_query")

```python
@register_rule("dataset")
def sql_query(query: str,
              merge_columns: list[str] | None = None,
              msg: str | None = None,
              name: str | None = None,
              negate: bool = False,
              condition_column: str = "condition",
              input_placeholder: str = "input_view",
              row_filter: str | None = None) -> tuple[Column, Callable]

```

Checks whether the condition column generated by SQL query is met.

Supports two modes:

* Row-level validation (merge\_columns provided): Query results are joined back to specific rows
* Dataset-level validation (merge\_columns omitted or None): All rows get the same check result

Use dataset-level for aggregate validations like "total count > 100" or "avg(amount) < 1000". Use row-level when you need to identify specific problematic rows.

**Arguments**:

* `query` - SQL query that must return as a minimum a condition column and all merge columns (if provided). When merge\_columns are provided, the resulting DataFrame is automatically joined back to the input DataFrame. When merge\_columns are not provided, the check applies to all rows (either all pass or all fail), making it useful for dataset-level validation with custom\_metrics. Reference DataFrames when provided in the ref\_dfs parameter are registered as temp view.

* `merge_columns` - Optional (can be None or omitted). List of columns to join results back to input DataFrame.

  <!-- -->

  * If provided: Row-level validation - different rows can have different results
  * If None/omitted: Dataset-level validation - all rows get same result When provided, columns must form a unique key to avoid duplicate records.

* `condition_column` - Column name indicating violation (boolean). Fail the check if True, pass it if False

* `msg` - Optional custom message or Column expression.

* `name` - Optional name for the result.

* `negate` - If True, the condition is negated (i.e., the check fails when the condition is False).

* `input_placeholder` - Name to be used in the sql query as `{{ input_placeholder }}` to refer to the input DataFrame on which the checks are applied.

* `row_filter` - Optional SQL expression used to filter input rows before running the SQL validation. Auto-injected from the check filter.

**Returns**:

Tuple (condition column, apply function).

**Raises**:

* `UnsafeSqlQueryError` - if the SQL query fails the safety check (e.g., contains disallowed operations).

### is\_aggr\_not\_greater\_than[​](#is_aggr_not_greater_than "Direct link to is_aggr_not_greater_than")

```python
@register_rule("dataset")
def is_aggr_not_greater_than(
        column: str | Column,
        limit: int | float | Decimal | str | Column,
        aggr_type: str = "count",
        group_by: list[str | Column] | None = None,
        row_filter: str | None = None,
        aggr_params: dict[str, Any] | None = None) -> tuple[Column, Callable]

```

Build an aggregation check condition and closure for dataset-level validation.

This function verifies that an aggregation on a column or group of columns does not exceed a specified limit. Supports curated aggregate functions (count, sum, avg, stddev, percentile, etc.) and any Databricks built-in aggregate. Rows where the aggregation result exceeds the limit are flagged.

**Arguments**:

* `column` - Column name (str) or Column expression to aggregate.
* `limit` - Numeric value, column name, or SQL expression for the limit. String literals must be single quoted, e.g. 'string\_value'.
* `aggr_type` - Aggregation type (default: 'count'). Curated types include count, sum, avg, min, max, count\_distinct, stddev, percentile, and more. Any Databricks built-in aggregate is supported.
* `group_by` - Optional list of column names or Column expressions to group by.
* `row_filter` - Optional SQL expression to filter rows before aggregation. Auto-injected from the check filter.
* `aggr_params` - Optional dict of parameters for aggregates requiring them (e.g., percentile value for percentile functions, accuracy for approximate aggregates). Parameters are passed as keyword arguments to the Spark function.

**Returns**:

A tuple of:

* A Spark Column representing the condition for aggregation limit violations.
* A closure that applies the aggregation check and adds the necessary condition/metric columns.

### is\_aggr\_not\_less\_than[​](#is_aggr_not_less_than "Direct link to is_aggr_not_less_than")

```python
@register_rule("dataset")
def is_aggr_not_less_than(
        column: str | Column,
        limit: int | float | Decimal | str | Column,
        aggr_type: str = "count",
        group_by: list[str | Column] | None = None,
        row_filter: str | None = None,
        aggr_params: dict[str, Any] | None = None) -> tuple[Column, Callable]

```

Build an aggregation check condition and closure for dataset-level validation.

This function verifies that an aggregation on a column or group of columns is not below a specified limit. Supports curated aggregate functions (count, sum, avg, stddev, percentile, etc.) and any Databricks built-in aggregate. Rows where the aggregation result is below the limit are flagged.

**Arguments**:

* `column` - Column name (str) or Column expression to aggregate.
* `limit` - Numeric value, column name, or SQL expression for the limit. String literals must be single quoted, e.g. 'string\_value'.
* `aggr_type` - Aggregation type (default: 'count'). Curated types include count, sum, avg, min, max, count\_distinct, stddev, percentile, and more. Any Databricks built-in aggregate is supported.
* `group_by` - Optional list of column names or Column expressions to group by.
* `row_filter` - Optional SQL expression to filter rows before aggregation. Auto-injected from the check filter.
* `aggr_params` - Optional dict of parameters for aggregates requiring them (e.g., percentile value for percentile functions, accuracy for approximate aggregates). Parameters are passed as keyword arguments to the Spark function.

**Returns**:

A tuple of:

* A Spark Column representing the condition for aggregation limit violations.
* A closure that applies the aggregation check and adds the necessary condition/metric columns.

### is\_aggr\_equal[​](#is_aggr_equal "Direct link to is_aggr_equal")

```python
@register_rule("dataset")
def is_aggr_equal(
        column: str | Column,
        limit: int | float | Decimal | str | Column,
        aggr_type: str = "count",
        group_by: list[str | Column] | None = None,
        row_filter: str | None = None,
        aggr_params: dict[str, Any] | None = None,
        abs_tolerance: float | None = None,
        rel_tolerance: float | None = None) -> tuple[Column, Callable]

```

Build an aggregation check condition and closure for dataset-level validation.

This function verifies that an aggregation on a column or group of columns is equal to a specified limit. Supports curated aggregate functions (count, sum, avg, stddev, percentile, etc.) and any Databricks built-in aggregate. Rows where the aggregation result is not equal to the limit are flagged.

**Arguments**:

* `column` - Column name (str) or Column expression to aggregate.
* `limit` - Numeric value, column name, or SQL expression for the limit. String literals must be single quoted, e.g. 'string\_value'.
* `aggr_type` - Aggregation type (default: 'count'). Curated types include count, sum, avg, min, max, count\_distinct, stddev, percentile, and more. Any Databricks built-in aggregate is supported.
* `group_by` - Optional list of column names or Column expressions to group by.
* `row_filter` - Optional SQL expression to filter rows before aggregation. Auto-injected from the check filter.
* `aggr_params` - Optional dict of parameters for aggregates requiring them (e.g., percentile value for percentile functions, accuracy for approximate aggregates). Parameters are passed as keyword arguments to the Spark function.
* `abs_tolerance` - Optional absolute tolerance for equality comparison of numeric aggregations.
* `rel_tolerance` - Optional relative tolerance for equality comparison of numeric aggregations.

**Returns**:

A tuple of:

* A Spark Column representing the condition for aggregation limit violations.
* A closure that applies the aggregation check and adds the necessary condition/metric columns.

### is\_aggr\_not\_equal[​](#is_aggr_not_equal "Direct link to is_aggr_not_equal")

```python
@register_rule("dataset")
def is_aggr_not_equal(
        column: str | Column,
        limit: int | float | Decimal | str | Column,
        aggr_type: str = "count",
        group_by: list[str | Column] | None = None,
        row_filter: str | None = None,
        aggr_params: dict[str, Any] | None = None,
        abs_tolerance: float | None = None,
        rel_tolerance: float | None = None) -> tuple[Column, Callable]

```

Build an aggregation check condition and closure for dataset-level validation.

This function verifies that an aggregation on a column or group of columns is not equal to a specified limit. Supports curated aggregate functions (count, sum, avg, stddev, percentile, etc.) and any Databricks built-in aggregate. Rows where the aggregation result is equal to the limit are flagged.

**Arguments**:

* `column` - Column name (str) or Column expression to aggregate.
* `limit` - Numeric value, column name, or SQL expression for the limit. String literals must be single quoted, e.g. 'string\_value'.
* `aggr_type` - Aggregation type (default: 'count'). Curated types include count, sum, avg, min, max, count\_distinct, stddev, percentile, and more. Any Databricks built-in aggregate is supported.
* `group_by` - Optional list of column names or Column expressions to group by.
* `row_filter` - Optional SQL expression to filter rows before aggregation. Auto-injected from the check filter.
* `aggr_params` - Optional dict of parameters for aggregates requiring them (e.g., percentile value for percentile functions, accuracy for approximate aggregates). Parameters are passed as keyword arguments to the Spark function.
* `abs_tolerance` - Optional absolute tolerance for equality comparison of numeric aggregations.
* `rel_tolerance` - Optional relative tolerance for equality comparison of numeric aggregations.

**Returns**:

A tuple of:

* A Spark Column representing the condition for aggregation limit violations.
* A closure that applies the aggregation check and adds the necessary condition/metric columns.

### has\_no\_aggr\_outliers[​](#has_no_aggr_outliers "Direct link to has_no_aggr_outliers")

```python
@register_rule("dataset")
def has_no_aggr_outliers(
        column: str | Column,
        time_column: str,
        *,
        aggr_type: str = "avg",
        sigma: float = 3.0,
        lookback_num_intervals: int = 14,
        warmup_num_intervals: int = 7,
        time_interval: str = "day",
        group_by: list[str | Column] | None = None,
        row_filter: str | None = None,
        aggr_params: dict[str, Any] | None = None) -> tuple[Column, Callable]

```

Rolling-window sigma outlier check for a time-series aggregate.

For each combination of *group\_by* values this check:

1. Computes *metric = aggr\_type(column)* per *time\_interval* bucket.
2. Derives a rolling baseline (mean and stddev\_pop) over the preceding *lookback\_num\_intervals* buckets.
3. Passes silently when fewer than *warmup\_num\_intervals* historical buckets are available, the series is constant (stddev == 0), or the most-recent bucket is missing.
4. Fails when *|current\_metric - baseline| > sigma \* stddev*.

**Arguments**:

* `column` - Column name (str) or Column expression to aggregate (e.g. *"revenue"* or *F.col("a") - F.col("b")*). Pass *"*"\* for *count(*)\*.
* `time_column` - Name of the timestamp/date column used to bucket rows into time grains.
* `aggr_type` - Aggregation type applied per bucket (default: *"avg"*). All curated DQX aggregate types are supported (count, sum, avg, min, max, count\_distinct, stddev, percentile, etc.).
* `sigma` - Number of standard deviations that defines the outlier band
* `(default` - *3.0*). Must be positive.
* `lookback_num_intervals` - Number of preceding time-grain buckets used to build the rolling baseline (default: *14*). Must be >= 2.
* `warmup_num_intervals` - Minimum number of historical buckets required before the check fires (default: *7*). Must satisfy *1 <= warmup\_num\_intervals <= lookback\_num\_intervals*.
* `time_interval` - Granularity at which to bucket the *time\_column*
* `(default` - *"day"*). One of *"minute"*, *"hour"*, *"day"*, *"week"*, *"month"*.
* `group_by` - Optional list of column names or Column expressions to segment the outlier band (e.g. *\["csp", "region"]*). The check fires if *any* group exceeds its own band.
* `time_column`0 - Optional SQL expression to filter rows before aggregation (e.g. *"status = 'Active'"*). Auto-injected from the check filter.
* `time_column`1 - Optional dict of extra parameters for aggregate functions that require them (e.g. *percentile=0.95* for percentile aggregates).

**Returns**:

A tuple of:

* A Spark Column representing the outlier condition (string message on violation, NULL on pass).
* A closure *apply(df)* that enriches the DataFrame with the condition column.

**Raises**:

* `time_column`2 - If *sigma <= 0*, *lookback\_num\_intervals < 2*, *warmup\_num\_intervals* is out of range, or *time\_interval* is unknown.
* `time_column`3 - If *aggr\_type* requires *aggr\_params* that are not supplied (e.g. percentile functions).

### compare\_datasets[​](#compare_datasets "Direct link to compare_datasets")

```python
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
        rel_tolerance: float | None = None) -> tuple[Column, Callable]

```

Dataset-level check that compares two datasets and returns a condition for changed rows, with details on row and column-level differences.

Only columns that are common across both datasets will be compared. Mismatched columns are ignored. Detailed information about the differences is provided in the condition column. The comparison does not support Map types (any column comparison on map type is skipped automatically).

The log containing detailed differences is written to the message field of the check result as a JSON string.

**Examples**:

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

**Arguments**:

* `columns` - List of columns to use for row matching with the reference DataFrame (can be a list of string column names or column expressions). Only simple column expressions are supported, e.g. F.col("col\_name").
* `ref_columns` - List of columns in the reference DataFrame or Table to row match against the source DataFrame (can be a list of string column names or column expressions). The *columns* parameter is matched with *ref\_columns* by position, so the order of the provided columns in both lists must be exactly aligned. Only simple column expressions are supported, e.g. F.col("col\_name").
* `ref_df_name` - Name of the reference DataFrame (used when passing DataFrames directly).
* `ref_table` - Name of the reference table (used when reading from catalog).
* `check_missing_records` - Perform FULL OUTER JOIN between the DataFrames to also find records that could be missing from the DataFrame. Use with caution as it may produce output with more rows than in the original DataFrame.
* `exclude_columns` - List of columns to exclude from the value comparison but not from row matching (can be a list of string column names or column expressions). Only simple column expressions are supported, e.g. F.col("col\_name"). This parameter does not alter the list of columns used to determine row matches; it only controls which columns are skipped during the column value comparison.
* `null_safe_row_matching` - If True, treats nulls as equal when matching rows.
* `null_safe_column_value_matching` - If True, treats nulls as equal when matching column values. If enabled, (NULL, NULL) column values are equal and matching.
* `row_filter` - Optional SQL expression to filter rows in the input DataFrame. Auto-injected from the check filter.
* `columns`0 - Values are considered equal if the absolute difference is less than or equal to the tolerance. This is applicable to numeric columns.
* `columns`1 - abs(a - b) <= tolerance With tolerance=0.01: 2.001 and 2.0099 → equal (diff = 0.0089) 2.001 and 2.02 → not equal (diff = 0.019)
* `columns`2 - Relative tolerance for numeric comparisons. Differences within this relative tolerance are ignored. Useful if numbers vary in scale.
* `columns`1 - abs(a - b) <= rel\_tolerance \* max(abs(a), abs(b)) With tolerance=0.01 (1%): 100 vs 101 → equal (diff = 1, tolerance = 1) 2.001 vs 2.0099 → equal

**Returns**:

Tuple\[Column, Callable]:

* A Spark Column representing the condition for comparison violations.
* A closure that applies the comparison validation.

**Raises**:

MissingParameterError:

* if neither *ref\_df\_name* nor *ref\_table* is provided. InvalidParameterError:
* if both *ref\_df\_name* and *ref\_table* are provided.
* if the number of *columns* and *ref\_columns* do not match.
* if *abs\_tolerance* or *rel\_tolerance* is negative.

### is\_data\_fresh\_per\_time\_window[​](#is_data_fresh_per_time_window "Direct link to is_data_fresh_per_time_window")

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

Build a completeness freshness check that validates records arrive at least every X minutes with a threshold for the expected number of rows per time window.

If *lookback\_windows* is provided, only data within that lookback period will be validated. If omitted, the entire dataset will be checked.

**Arguments**:

* `column` - Column name (str) or Column expression containing timestamps to check.
* `window_minutes` - Time window in minutes to check for data arrival.
* `min_records_per_window` - Minimum number of records expected per time window.
* `lookback_windows` - Optional number of time windows to look back from *curr\_timestamp*. This filters records to include only those within the specified number of time windows from *curr\_timestamp*. If no lookback is provided, the check is applied to the entire dataset.
* `row_filter` - Optional SQL expression to filter rows before checking. Auto-injected from the check filter.
* `curr_timestamp` - Optional current timestamp column. If not provided, current\_timestamp() function is used.

**Returns**:

A tuple of:

* A Spark Column representing the condition for missing data within a time window.
* A closure that applies the completeness check and adds the necessary condition columns.

**Raises**:

* `InvalidParameterError` - If min\_records\_per\_window or window\_minutes are not positive integers, or if lookback\_windows is provided and is not a positive integer.

### has\_valid\_schema[​](#has_valid_schema "Direct link to has_valid_schema")

```python
@register_for_original_columns_preselection()
@register_rule("dataset")
def has_valid_schema(
    expected_schema: str | types.StructType | None = None,
    ref_df_name: str | None = None,
    ref_table: str | None = None,
    columns: list[str | Column] | None = None,
    strict: bool = False,
    exclude_columns: list[str | Column] | None = None
) -> tuple[Column, Callable]

```

Build a schema compatibility check condition and closure for dataset-level validation.

This function checks whether the DataFrame schema is compatible with the expected schema. The check will be skipped by the engine if the columns parameter contains column names that do not exist in the checked DataFrame.

All columns in the `exclude_columns` list will be ignored even if the column is present in the `columns` list.

**Arguments**:

* `expected_schema` - Expected schema as a DDL string (e.g., "id INT, name STRING") or StructType object.

* `ref_df_name` - Name of the reference DataFrame (used when passing DataFrames directly).

* `ref_table` - Name of the reference table to load the schema from (e.g. "catalog.schema.table")

* `columns` - Optional list of columns to validate (default: all columns in the checked DataFrame are considered). Only the input DataFrame columns are filtered by this parameter.

* `strict` - Whether to perform strict schema validation (default: False).

  <!-- -->

  * False: Validates that all expected columns (after filtering by the `columns` parameter) exist with compatible types. Allows the DataFrame to contain extra columns.
  * True: Validates an exact schema match against the full expected schema (same columns, same order, same types).

* `exclude_columns` - Optional list of columns in the checked DataFrame schema to ignore for validation.

**Returns**:

A tuple of:

* A Spark Column representing the condition for schema compatibility violations.
* A closure that applies the schema check and adds the necessary condition columns.

**Raises**:

InvalidParameterError:

* If the *expected\_schema* string is invalid or cannot be parsed
* If *expected\_schema* is neither a string nor a StructType
* If more than one of *expected\_schema*, *ref\_df\_name*, or *ref\_table* are specified
* If none of *expected\_schema*, *ref\_df\_name*, or *ref\_table* are specified

**Notes**:

Exactly one of *expected\_schema*, *ref\_df\_name*, or *ref\_table* must be specified.

### is\_valid\_json[​](#is_valid_json "Direct link to is_valid_json")

```python
@register_rule("row")
def is_valid_json(column: str | Column) -> Column

```

Checks whether the values in the input column are valid JSON strings.

**Arguments**:

* `column` - Column name (str) or Column expression to check for valid JSON.

**Returns**:

A Spark Column representing the condition for invalid JSON strings.

### has\_json\_keys[​](#has_json_keys "Direct link to has_json_keys")

```python
@register_rule("row")
def has_json_keys(column: str | Column,
                  keys: list[str],
                  require_all: bool = True) -> Column

```

Checks whether the values in the input column contain specific keys in the outermost JSON object.

**Arguments**:

* `column` - The name of the column or the column expression to check for JSON keys.
* `keys` - A list of JSON keys to verify within the outermost JSON object.
* `require_all` - If True, all specified keys must be present. If False, at least one key must be present.

**Returns**:

A Spark Column representing the condition for missing JSON keys.

### has\_valid\_json\_schema[​](#has_valid_json_schema "Direct link to has_valid_json_schema")

```python
@register_rule("row")
def has_valid_json_schema(column: str | Column,
                          schema: str | types.StructType) -> Column

```

Validates that JSON strings in the specified column conform to an expected schema.

The validation utilizes standard Spark JSON parsing rules, specifically:

* **Type Coercion is Permitted:** Values that can be successfully cast to the target schema type (e.g. a JSON number like 0.12 parsing into a field defined as STRING) are considered valid.
* **Extra Fields are Ignored:** Fields present in the JSON, but missing from the schema are ignored.
* **Missing keys imply null:** If a key is missing from the JSON object, Spark treats it as a null value.
* **Strictness:** If a schema field is defined as NOT NULL, validation will fail if the key is missing (implicit null) or explicitly set to null.
* **Nested JSON behavior:** If a nullable parent field is explicitly null (e.g. `{"parent": null}`), its children are **not** validated. However, if the parent exists (e.g. `{"parent": {}}`) but a required child is missing, validation fails.
* **Nested Depth Limit:** The validation logic supports a maximum nested depth of 10 levels.

**Arguments**:

* `column` - Column name or Column expression containing JSON strings.
* `schema` - Expected schema as a DDL string (e.g. "struct\<id
  <!-- -->
  :string
  <!-- -->
  NOT NULL>", "id INT, name STRING") or a generic StructType. To enforce strict presence of a field, you must explicitly set it to nullable=False or use NOT NULL in the DDL string.

**Returns**:

A string Column containing the error message if the JSON does not conform to the schema, or null if validation passes.

**Raises**:

* `InvalidParameterError` - If the schema string is invalid/unparsable, or if the input schema is neither a string nor a StructType.

### get\_limit\_expr[​](#get_limit_expr "Direct link to get_limit_expr")

```python
def get_limit_expr(
    limit: int | float | Decimal | datetime.date | datetime.datetime | str
    | Column | None = None
) -> Column

```

Generate a Spark Column expression for a limit value.

This helper converts the provided limit (literal, string expression, or Column) into a Spark Column expression suitable for use in conditions.

**Arguments**:

* `limit` - The limit to use in the condition. Can be a literal (int, float, date, datetime), a string SQL expression, or a Spark Column.

**Returns**:

A Spark Column expression representing the limit.

**Raises**:

* `MissingParameterError` - If the limit is not provided (None).

### get\_normalized\_column\_and\_expr[​](#get_normalized_column_and_expr "Direct link to get_normalized_column_and_expr")

```python
def get_normalized_column_and_expr(
        column: str | Column) -> tuple[str, str, Column]

```

Extract the normalized column name, original column name as string, and column expression.

This helper ensures that both a normalized string representation and a raw string representation of the column are available, along with the corresponding Spark Column expression. Useful for generating aliases, conditions, and consistent messaging.

**Arguments**:

* `column` - The input column, provided as either a string column name or a Spark Column expression.

**Returns**:

A tuple containing:

* Normalized column name as a string (suitable for use in aliases or metadata).
* Original column name as a string.
* Spark Column expression corresponding to the input.
