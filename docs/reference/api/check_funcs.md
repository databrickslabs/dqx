# databricks.labs.dqx.check_funcs

## DQPattern Objects[​](#dqpattern-objects "Direct link to DQPattern Objects")

```python
class DQPattern(Enum)

```

Enum class to represent DQ patterns used to match data in columns.

### make_condition[​](#make_condition "Direct link to make_condition")

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

### is_not_null_and_not_empty[​](#is_not_null_and_not_empty "Direct link to is_not_null_and_not_empty")

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

### is_not_empty[​](#is_not_empty "Direct link to is_not_empty")

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

### is_not_null[​](#is_not_null "Direct link to is_not_null")

```python
@register_rule("row")
def is_not_null(column: str | Column) -> Column

```

Checks whether the values in the input column are not null.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression

**Returns**:

Column object for condition

### is_null[​](#is_null "Direct link to is_null")

```python
@register_rule("row")
def is_null(column: str | Column) -> Column

```

Checks whether the values in the input column are null.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression

**Returns**:

Column object for condition

### is_empty[​](#is_empty "Direct link to is_empty")

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

### is_null_or_empty[​](#is_null_or_empty "Direct link to is_null_or_empty")

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

### has_valid_string_case[​](#has_valid_string_case "Direct link to has_valid_string_case")

```python
@register_rule("row")
def has_valid_string_case(column: str | Column, case: str) -> Column

```

Checks whether string values match the requested letter case:

* `upper` requires all alphabetic characters to be uppercase
* `lower` requires all alphabetic characters to be lowercase
* `title` requires the first character of each word to be uppercase; words are split on the ASCII space character only, so other whitespace (tabs, newlines, non-breaking spaces) is not treated as a word boundary. Only the first character of each word is checked; the rest is left as-is, so an all-uppercase word (e.g. *HELLO*) also passes.
* `sentence` requires each segment's first non-whitespace character to be uppercase; segments are split on the period only. Only that first character is checked; the rest is left as-is.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression
* `case` - expected case; one of *upper*, *lower*, *title*, or *sentence*

**Returns**:

Column object for condition

**Raises**:

* `InvalidParameterError` - If *case* is not a supported string case.

### is_not_null_and_is_in_list[​](#is_not_null_and_is_in_list "Direct link to is_not_null_and_is_in_list")

```python
@register_rule("row")
def is_not_null_and_is_in_list(column: str | Column,
                               allowed: list,
                               case_sensitive: bool = True) -> Column

```

Checks whether the values in the input column are not null and present in the list of allowed values. Can optionally perform a case-insensitive comparison. This check is not suited for `MapType` or `StructType` columns.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression
* `allowed` - list of allowed values. Each entry is resolved like the comparison-check limits: a bare string is treated as a **column expression**, a numeric string such as "3" as a number, and an ISO-date string such as "2024-01-01" as a date. To compare against a string literal, quote it (e.g. "'value'") or pass *F.lit("value")*.
* `case_sensitive` - whether to perform a case-sensitive comparison (default: True)

**Returns**:

Column object for condition

**Raises**:

* `MissingParameterError` - If the allowed list is not provided.
* `InvalidParameterError` - If the allowed parameter is not a list, or if the list is empty.

### is_in_list[​](#is_in_list "Direct link to is_in_list")

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
* `allowed` - list of allowed values. Each entry is resolved like the comparison-check limits: a bare string is treated as a **column expression**, a numeric string such as "3" as a number, and an ISO-date string such as "2024-01-01" as a date. To compare against a string literal, quote it (e.g. "'value'") or pass *F.lit("value")*.
* `case_sensitive` - whether to perform a case-sensitive comparison (default: True)

**Returns**:

Column object for condition

**Raises**:

* `MissingParameterError` - If the allowed list is not provided.
* `InvalidParameterError` - If the allowed parameter is not a list.

### is_in_distribution[​](#is_in_distribution "Direct link to is_in_distribution")

```python
@row_preserving
@register_rule("dataset")
def is_in_distribution(
        column: str | Column,
        distribution: _VALUES_DISTRIBUTION,
        distance: float,
        case_sensitive: bool = True,
        row_filter: str | None = None) -> tuple[Column, Callable]

```

Check whether the discrete value distribution of the column is within a distance less than or equal to the given distance, compared to the given values distribution. The distance is calculated using the [Total variation distance of probability measures](https://en.wikipedia.org/wiki/Total_variation_distance_of_probability_measures) method.

This check aims to supplement *is_in_list* at the dataset level by verifying that the column not only stays within the boundaries of enumerated values, but also that these values follow the expected distribution.

Supported column types: Boolean, Char, String, Byte, Short, Integer, Long, and Date. These types have been chosen because they are the most suitable for representing categorical data and produce stable, well-defined distributions when aggregated. Floating-point types (Float, Double) are excluded because equality-based grouping is unstable due to precision issues; complex types (Array, Struct, Variant) are excluded because they lack a natural equality-based grouping semantics; and potentially large types (Binary) are excluded because they can lead to performance and correctness issues when used as grouping keys. For any other column type a validation error is raised.

The sum of the supplied distribution values must be less than or equal to 1. The distance must also be between 0 and 1 (inclusive). The distribution is not allowed to contain *None* — neither as a key nor as a value; please consider using the corresponding completeness functions for that purpose. The distribution dict is not allowed to contain *inf*, *-inf*, or *nan* among its values, nor negative values. The distribution dict must be homogeneous: all keys must be of the same supported primitive type (matching the column type). Mixing key types (e.g., a dict containing both string and integer keys) raises a validation error. The values distribution has no size limitation, but it is highly recommended to keep it reasonably small as the check is intended to be used for enumerated data.

The actual distribution is calculated over all non-null values in the column (equivalent to *df.groupBy(col).count()*). *NULL* values in the checked column are skipped, so the distribution is computed over non-null values only.

A sum strictly less than 1 is allowed when the intent is to check only a subset of the possible values (e.g., some enumerated values are intentionally omitted from the expected distribution). To calculate the TVD properly in this case, the function creates an internal *residual* bucket in the expected distribution to hold the missing mass (1 - sum), and the actual distribution is computed with the same bucketing: all non-null column values not listed as explicit keys in the given distribution are aggregated into the *residual* bucket. For example, given a distribution of `{A: 0.5, B: 0.3}`, an internal *residual* bucket holds the remaining 0.2; the actual distribution is then calculated over *A*, *B*, and *residual*, where *residual* counts every non-null column value other than *A* and *B*. When the sum equals 1, the expected *residual* mass is 0 and any actual values not listed in the given distribution will contribute to the distance.

Keys present in the given distribution but missing from the actual distribution are treated as having a count of 0, so the distance is computed across the union of keys. *NOTE*: values present in the actual distribution but not listed as explicit keys in the given distribution are aggregated into the *residual* bucket rather than compared individually. Consider using *is_in_list* at the row level for checking individual values.

Case normalisation is applied when *case_sensitive* is *False*. The same normalisation is applied to the keys of the given *distribution* before the check runs, so both sides are compared consistently. If two distribution keys collide after normalisation a validation error is raised.

**Notes**:

This check is not supported for streaming DataFrames. It performs an ungrouped aggregation over the whole dataset to compute the actual distribution, which Structured Streaming does not allow on unbounded sources without a watermark and a compatible output mode. Use it on batch DataFrames only.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression.
* `distribution` - expected distribution of literal values to compare with. The dict must be homogeneous — all keys must be of the same supported primitive type matching the column type; values must be non-negative and sum to 1 or less.
* `distance` - max distance between the actual and given values distribution; must be between 0 and 1 (inclusive).
* `case_sensitive` - whether to perform a case-sensitive comparison (default: True).
* `row_filter` - Optional SQL expression for filtering rows before the distribution is computed. Auto-injected from the check filter.

**Returns**:

A tuple of:

* A Spark Column representing the condition for distribution violations.
* A closure that applies the distribution check and adds the necessary condition/count columns.

**Raises**:

* `MissingParameterError` - If the distribution or distance is not provided.
* `InvalidParameterError` - If the distribution parameter is not a dict or is empty; if the distribution dict contains a *None* key or *None* value; if the distribution values contain *inf*, *-inf*, or *nan*; if any distribution value is negative; if the sum of the distribution values is greater than 1; if the distance parameter value is not between 0 and 1 (inclusive); if the column type is not one of the supported primitive types; if the distribution dict is not homogeneous (contains keys of more than one type); if the distribution key type is not compatible with the column type (e.g. *str* keys against an integer column); or if two keys in the given distribution collide after case normalisation when *case_sensitive* is *False*.

### is_not_in_list[​](#is_not_in_list "Direct link to is_not_in_list")

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
* `forbidden` - list of forbidden values. Each entry is resolved like the comparison-check limits: a bare string is treated as a **column expression**, a numeric string such as "3" as a number, and an ISO-date string such as "2024-01-01" as a date. To compare against a string literal, quote it (e.g. "'value'") or pass *F.lit("value")*.
* `case_sensitive` - whether to perform a case-sensitive comparison (default: True)

**Returns**:

Column object for condition

**Raises**:

* `MissingParameterError` - If the forbidden list is not provided.
* `InvalidParameterError` - If the forbidden parameter is not a list.

### sql_expression[​](#sql_expression "Direct link to sql_expression")

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

* `expression` - SQL expression. Fail if expression evaluates to False, pass if it evaluates to True. Security note: this parameter accepts arbitrary SQL and is evaluated as-is, so it may include subqueries and run with the permissions of the process executing the checks. Only use check definitions from trusted sources, especially in automated or multi-tenant pipelines.
* `msg` - optional message of the *Column* type, automatically generated if None
* `name` - optional name of the resulting column, automatically generated if None
* `negate` - if the condition should be negated (true) or not. For example, "col is not null" will mark null values as "bad". Although sometimes it's easier to specify it other way around "col is null" + negate set to True
* `columns` - optional list of columns to be used for validation against the actual input DataFrame, reporting and for constructing name prefix if check name is not provided.

**Returns**:

new Column

### is_older_than_col2_for_n_days[​](#is_older_than_col2_for_n_days "Direct link to is_older_than_col2_for_n_days")

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

### is_older_than_n_days[​](#is_older_than_n_days "Direct link to is_older_than_n_days")

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

### is_not_in_future[​](#is_not_in_future "Direct link to is_not_in_future")

```python
@register_rule("row")
def is_not_in_future(column: str | Column,
                     offset: int = 0,
                     curr_timestamp: Column | None = None) -> Column

```

Checks whether the values in the input column contain a timestamp that is not in the future, where 'future' is defined as current_timestamp + offset (in seconds).

**Arguments**:

* `column` - column to check; can be a string column name or a column expression
* `offset` - offset (in seconds) to add to the current timestamp at time of execution
* `curr_timestamp` - (optional) set current timestamp

**Returns**:

new Column

### is_not_in_near_future[​](#is_not_in_near_future "Direct link to is_not_in_near_future")

```python
@register_rule("row")
def is_not_in_near_future(column: str | Column,
                          offset: int = 0,
                          curr_timestamp: Column | None = None) -> Column

```

Checks whether the values in the input column contain a timestamp that is not in the near future, where 'near future' is defined as greater than the current timestamp but less than the current_timestamp + offset (in seconds).

**Arguments**:

* `column` - column to check; can be a string column name or a column expression
* `offset` - offset (in seconds) to add to the current timestamp at time of execution
* `curr_timestamp` - (optional) set current timestamp

**Returns**:

new Column

### is_equal_to[​](#is_equal_to "Direct link to is_equal_to")

```python
@register_rule("row")
def is_equal_to(column: str | Column,
                value: int | float | Decimal | str | datetime.date
                | datetime.datetime | Column | None = None,
                abs_tolerance: float | None = None,
                rel_tolerance: float | None = None,
                allow_nulls: bool = True) -> Column

```

Check whether the values in the input column are equal to the given value.

**Arguments**:

* `column` *str | Column* - Column to check. Can be a string column name or a column expression.
* `value` - The value to compare with. Can be a number, date, timestamp literal or a Spark Column. Defaults to None.
* `abs_tolerance` - Values are considered equal if the absolute difference is less than or equal to the tolerance. This is applicable to numeric columns. For example, abs(a - b) <= tolerance. With abs_tolerance=0.01, values 2.001 and 2.0099 are equal (diff=0.0089), but 2.001 and 2.02 are not (diff=0.019).
* `rel_tolerance` - Relative tolerance for numeric comparisons. Differences within this relative tolerance are ignored. Useful if numbers vary in scale. For example, abs(a - b) <= rel_tolerance \* max(abs(a), abs(b)). With rel_tolerance=0.01 (1%), values 100 and 101 are equal (diff=1), but 100 and 102 are not (diff=2).
* `allow_nulls` - If True (default), null values pass the check, following Spark's null comparison semantics. If False, null values fail the check.

**Returns**:

* `Column` - A Spark Column condition that fails if the column value is not equal to the given value.

**Raises**:

* `InvalidParameterError` - If absolute or relative tolerances are negative.

**Notes**:

If both tolerances are provided, the value is considered equal if it meets either tolerance condition.

### is_not_equal_to[​](#is_not_equal_to "Direct link to is_not_equal_to")

```python
@register_rule("row")
def is_not_equal_to(column: str | Column,
                    value: int | float | Decimal | str | datetime.date
                    | datetime.datetime | Column | None = None,
                    abs_tolerance: float | None = None,
                    rel_tolerance: float | None = None,
                    allow_nulls: bool = True) -> Column

```

Check whether the values in the input column are not equal to the given value.

**Arguments**:

* `column` *str | Column* - Column to check. Can be a string column name or a column expression.
* `value` - The value to compare with. Can be a number, date, timestamp literal or a Spark Column. Defaults to None.
* `abs_tolerance` - Values are considered equal if the absolute difference is less than or equal to the tolerance. This is applicable to numeric columns. For example, abs(a - b) <= tolerance. With abs_tolerance=0.01, values 2.001 and 2.0099 are equal (diff=0.0089), but 2.001 and 2.02 are not (diff=0.019).
* `rel_tolerance` - Relative tolerance for numeric comparisons. Differences within this relative tolerance are ignored. Useful if numbers vary in scale. For example, abs(a - b) <= rel_tolerance \* max(abs(a), abs(b)). With rel_tolerance=0.01 (1%), values 100 and 101 are equal (diff=1), but 100 and 102 are not (diff=2).
* `allow_nulls` - If True (default), null values pass the check, following Spark's null comparison semantics. If False, null values fail the check.

**Returns**:

* `Column` - A Spark Column condition that fails if the column value is equal to the given value.

**Raises**:

* `InvalidParameterError` - If absolute or relative tolerances are negative.

**Notes**:

If both tolerances are provided, the value is considered equal if it meets either tolerance condition.

### is_not_less_than[​](#is_not_less_than "Direct link to is_not_less_than")

```python
@register_rule("row")
def is_not_less_than(column: str | Column,
                     limit: int | float | Decimal | datetime.date
                     | datetime.datetime | str | Column | None = None,
                     allow_nulls: bool = True) -> Column

```

Checks whether the values in the input column are not less than the provided limit.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression
* `limit` - limit to use in the condition as number, date, timestamp, column name or sql expression
* `allow_nulls` - If True (default), null values pass the check, following Spark's null comparison semantics. If False, null values fail the check.

**Returns**:

new Column

### is_not_greater_than[​](#is_not_greater_than "Direct link to is_not_greater_than")

```python
@register_rule("row")
def is_not_greater_than(column: str | Column,
                        limit: int | float | Decimal | datetime.date
                        | datetime.datetime | str | Column | None = None,
                        allow_nulls: bool = True) -> Column

```

Checks whether the values in the input column are not greater than the provided limit.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression
* `limit` - limit to use in the condition as number, date, timestamp, column name or sql expression
* `allow_nulls` - If True (default), null values pass the check, following Spark's null comparison semantics. If False, null values fail the check.

**Returns**:

new Column

### is_in_range[​](#is_in_range "Direct link to is_in_range")

```python
@register_rule("row")
def is_in_range(column: str | Column,
                min_limit: int | float | Decimal | datetime.date
                | datetime.datetime | str | Column | None = None,
                max_limit: int | float | Decimal | datetime.date
                | datetime.datetime | str | Column | None = None,
                allow_nulls: bool = True) -> Column

```

Checks whether the values in the input column are in the provided limits (inclusive of both boundaries).

**Arguments**:

* `column` - column to check; can be a string column name or a column expression
* `min_limit` - min limit to use in the condition as number, date, timestamp, column name or sql expression
* `max_limit` - max limit to use in the condition as number, date, timestamp, column name or sql expression
* `allow_nulls` - If True (default), null values pass the check, following Spark's null comparison semantics. If False, null values fail the check.

**Returns**:

new Column

### is_not_in_range[​](#is_not_in_range "Direct link to is_not_in_range")

```python
@register_rule("row")
def is_not_in_range(column: str | Column,
                    min_limit: int | float | Decimal | datetime.date
                    | datetime.datetime | str | Column | None = None,
                    max_limit: int | float | Decimal | datetime.date
                    | datetime.datetime | str | Column | None = None,
                    allow_nulls: bool = True) -> Column

```

Checks whether the values in the input column are outside the provided limits (inclusive of both boundaries).

**Arguments**:

* `column` - column to check; can be a string column name or a column expression
* `min_limit` - min limit to use in the condition as number, date, timestamp, column name or sql expression
* `max_limit` - max limit to use in the condition as number, date, timestamp, column name or sql expression
* `allow_nulls` - If True (default), null values pass the check, following Spark's null comparison semantics. If False, null values fail the check.

**Returns**:

new Column

### has_num_decimal_places[​](#has_num_decimal_places "Direct link to has_num_decimal_places")

```python
@register_rule("row")
def has_num_decimal_places(column: str | Column,
                           limit: int | str | Column | None = None,
                           allow_nulls: bool = True) -> Column

```

Checks whether numeric values in the input column have no more than the given number of decimal places.

Values are judged by their numeric value, so trailing zeros don't count: *DOUBLE* *5.0*, *DECIMAL(10,4)* *1.5000* and the string *'1.250'* have 0, 1 and 2 decimal places. *float*/*double* columns reflect the stored value (1.10 is stored as 1.1). Values are read via their string form, which must be a plain or exponent number: an optional sign, digits with an optional decimal point (*'1'*, *'-1.25'*, *'+1.'*, *'.5'*) and an optional exponent (*'1.5e3'*, *'1.0E-4'*). Anything else, including surrounding whitespace, *NaN* and *Infinity*, fails as not numeric.

The *limit* may be a literal or a column expression and is evaluated lazily, so it is not validated up-front. A non-null value whose *limit* evaluates to null fails with a 'null limit' message.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression.
* `limit` - maximum number of decimal places allowed; a non-negative integer, column name, or sql expression.
* `allow_nulls` - if True (default), null values pass the check; if False, null values fail the check.

**Returns**:

Column object for condition.

**Raises**:

* `MissingParameterError` - if the limit is not provided.

### regex_match[​](#regex_match "Direct link to regex_match")

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

### is_not_null_and_not_empty_array[​](#is_not_null_and_not_empty_array "Direct link to is_not_null_and_not_empty_array")

```python
@register_rule("row")
def is_not_null_and_not_empty_array(column: str | Column) -> Column

```

Checks whether the values in the array input column are not null and not empty.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression

**Returns**:

Column object for condition

### is_valid_date[​](#is_valid_date "Direct link to is_valid_date")

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

### is_valid_timestamp[​](#is_valid_timestamp "Direct link to is_valid_timestamp")

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

### is_valid_ipv4_address[​](#is_valid_ipv4_address "Direct link to is_valid_ipv4_address")

```python
@register_rule("row")
def is_valid_ipv4_address(column: str | Column) -> Column

```

Checks whether the values in the input column have valid IPv4 address formats.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression

**Returns**:

Column object for condition

### is_valid_email[​](#is_valid_email "Direct link to is_valid_email")

```python
@register_rule("row")
def is_valid_email(column: str | Column) -> Column

```

Checks whether the values in the input column are valid email addresses.

Validates against a pragmatic subset of RFC 5322:

* Local part: dot-atom (RFC 5322 §3.2.3) or quoted-string (§3.2.4).
* Domain: dot-atom hostname with LDH labels (RFC 1035 §2.3.4) and an alphabetic TLD, or an IP-literal (RFC 5321 §4.1.3) - bracketed IPv4 addresses will use octet validation while *\[IPv6:...] addresses* are only matched syntactically; Callers requiring semantic IPv6 domain validation should implement a custom Python check that uses a Pandas UDF to call *ipaddress.IPv6Address* methods.
* Length caps (per RFC 5321 §4.5.3.1): 64 octets for local-parts, 254 octets for the full address.

Comments and folding whitespace (CFWS), obsolete grammar (*obs-local-part*, *obs-domain*, *obs-qtext*, *obs-qp*), and internationalized addresses (RFC 6531 / SMTPUTF8) are not supported; a separate check would be needed for each. Numeric and single-character TLDs are rejected (ICANN policy + practical interoperability).

Null values will pass the check with no violation reported.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression

**Returns**:

Column object for condition

### is_valid_url[​](#is_valid_url "Direct link to is_valid_url")

```python
@register_rule("row")
def is_valid_url(column: str | Column) -> Column

```

Checks whether the values in the input column are valid URLs.

Validates against the RFC 3986 §4.3 *absolute-URI* grammar: *scheme ":" hier-part* with an optional *"?" query* and *"#" fragment*. A scheme is required, so relative references such as */path* or *example.com* are rejected. Sub-delimiters (*!$&'()*+,;=\*) are permitted unencoded inside a path, query, or fragment; any character outside the *unreserved* and *sub-delims* sets (spaces, or gen-delimiters such as *\[* and *]* outside their structural role) must be percent-encoded to be accepted.

Any syntactically valid scheme is accepted, which keeps non-network URLs such as *s3://*, *ftp\://*, *mailto:* and *urn:* valid alongside *http\://* and *https\://*. Two consequences are worth noting:

* This validates URL *syntax*, not safety or reachability. Script-bearing schemes (*javascript<!-- -->:alert<!-- -->(1)*) and inline payloads (*data<!-- -->:text<!-- -->/plain,hello*) are syntactically valid URLs and pass. Do not rely on this check to sanitize untrusted input before rendering or fetching it; gate the scheme explicitly for that, for example with *is_in_list* on an extracted scheme column or a *sql_expression* check.
* RFC 3986 permits an empty host, so *file:///path* passes. Host presence is not enforced.

Validation is purely syntactic: it does not verify that the host resolves or that the resource exists. Null values will pass the check with no violation reported.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression

**Returns**:

Column object for condition

### is_valid_national_id[​](#is_valid_national_id "Direct link to is_valid_national_id")

```python
@register_rule("row")
def is_valid_national_id(column: str | Column, country: str = "US") -> Column

```

Checks whether the values in the input column are valid national identification numbers (for example, US Social Security Numbers) for the given country.

Validation is limited to *format* and *number ranges*; it does not verify that a number was actually issued.

Supported countries are keyed by ISO 3166 alpha-2 code. For *US*, the *AAA-GG-SSSS* form is required, where the separators may be all hyphens, all single spaces, or omitted entirely (e.g. *123-45-6789*, *123 45 6789* or *123456789*), but must be used consistently. Structurally invalid ranges are rejected (area *000*, *666* and *900-999* - the latter covering ITINs; group *00*; serial *0000*). For *GB*, a National Insurance number consists of two letters, six digits, and a final *A*, *B*, *C*, or *D*; unallocated prefixes are rejected. For *IN*, a PAN consists of three letters, a holder-type letter (*A*, *B*, *C*, *F*, *G*, *H*, *J*, *L*, *P*, or *T*), another letter, four digits, and a final letter. These checks validate format only, not whether an identifier was issued.

Null values will pass the check with no violation reported.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression
* `country` - ISO 3166 alpha-2 country code selecting the validation pattern (default: *US*)

**Returns**:

Column object for condition

**Raises**:

* `MissingParameterError` - if *country* is None.
* `InvalidParameterError` - if *country* is not a string, or is not a supported country code.

### is_valid_uuid[​](#is_valid_uuid "Direct link to is_valid_uuid")

```python
@register_rule("row")
def is_valid_uuid(column: str | Column, strict: bool = False) -> Column

```

Checks whether the values in the input column are valid UUIDs (RFC 9562, obsoletes RFC 4122).

By default, validates the canonical 8-4-4-4-12 hyphenated hex string form, case-insensitively. The all-zero Nil UUID, the all-one Max UUID, and legacy variant GUIDs all pass, since every common UUID library treats them as valid.

When *strict* is True, the version nibble (1-8) and variant bits (8/9/a/b) are additionally enforced, so out-of-range version/variant values and the Nil and Max UUIDs are rejected.

Null values will pass the check with no violation reported.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression
* `strict` - if True, also validate the version nibble and variant bits per RFC 9562 (default: False)

**Returns**:

Column object for condition

### load_iso_codes[​](#load_iso_codes "Direct link to load_iso_codes")

```python
def load_iso_codes(resource_name: str) -> frozenset[str]

```

Load a set of standard codes from a newline-delimited data file in the resources package.

The large standard code lists are stored as data files rather than inline literals to keep them readable and easy to regenerate. See the files under *databricks/labs/dqx/resources* for the values and their authoritative sources.

### is_valid_country_code[​](#is_valid_country_code "Direct link to is_valid_country_code")

```python
@register_rule("row")
def is_valid_country_code(column: str | Column,
                          code_format: str = "alpha-2",
                          case_sensitive: bool = True) -> Column

```

Checks whether the values in the input column are valid ISO 3166-1 country codes.

ISO 3166-1 defines three code representations, selected with *code_format*:

* *alpha-2* (default): the two-letter code, e.g. *US*, *GB*, *DE*.
* *alpha-3*: the three-letter code, e.g. *USA*, *GBR*, *DEU*.
* *numeric*: the three-digit code, e.g. *840*, *826*, *276*.

The valid codes follow the ISO 3166-1 standard; see <https://www.iso.org/iso-3166-country-codes.html>. Only officially assigned codes are accepted; user-assigned codes (e.g. *XK* for Kosovo) and reserved codes are intentionally excluded. Numeric codes are the three-digit, zero-padded form (e.g. *004*), so a numeric input column must preserve the leading zeros; a non-string column is cast to string for comparison, but the cast does not add back zero-padding an integer type may have dropped (e.g. an *int* column value *4* is compared as the string *"4"*, not *"004"*, and is correctly flagged as invalid).

By default the comparison is case-sensitive; pass *case_sensitive* as False to accept values in any case. *case_sensitive* has no effect for *numeric* codes, which contain only digits. *code_format* matching itself is case-insensitive (*"NUMERIC"*/*"Alpha-2"* are also accepted). Null values will pass the check with no violation reported.

For best performance with large lists in general, prefer the *foreign_key* check function; the fixed ISO 3166-1 code lists used here are small enough (up to 249 codes) that this is not a concern.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression
* `code_format` - ISO 3166-1 code representation to validate against: *alpha-2* (default), *alpha-3*, or *numeric*; matching is case-insensitive
* `case_sensitive` - whether to perform a case-sensitive comparison (default: True); ignored when *code_format* is *numeric*

**Returns**:

Column object for condition

**Raises**:

* `MissingParameterError` - if *code_format* is None.
* `InvalidParameterError` - if *code_format* is not a string, or is not a supported representation.

### is_valid_currency_code[​](#is_valid_currency_code "Direct link to is_valid_currency_code")

```python
@register_rule("row")
def is_valid_currency_code(column: str | Column,
                           code_format: str = "alphabetic",
                           case_sensitive: bool = True) -> Column

```

Checks whether the values in the input column are valid ISO 4217 currency codes.

ISO 4217 defines two code representations, selected with *code_format*:

* *alphabetic* (default): the three-letter code, e.g. *USD*, *EUR*, *JPY*.
* *numeric*: the three-digit code, e.g. *840*, *978*, *392*.

The valid codes follow the ISO 4217 standard; see <https://www.iso.org/iso-4217-currency-codes.html>. Every code assigned by the standard is accepted, which includes codes that are not spendable currencies, such as *XXX* (no currency), *XTS* (reserved for testing), the precious metals (*XAU*, *XAG*, *XPT*, *XPD*) and *XDR* (IMF special drawing rights). Numeric codes are the three-digit, zero-padded form (e.g. *036*), so a numeric input column must preserve the leading zeros; a non-string column is cast to string for comparison, but the cast does not add back zero-padding an integer type may have dropped (e.g. an *int* column value *8* is compared as the string *"8"*, not *"008"*, and is flagged as invalid). If codes are stored as unpadded integers, zero-pad the column before calling this check, e.g. *F.lpad(column.cast("string"), 3, "0")*.

By default the comparison is case-sensitive; pass *case_sensitive* as False to accept values in any case. *case_sensitive* has no effect for *numeric* codes, which contain only digits. *code_format* matching itself is case-insensitive (*"NUMERIC"*/*"Alphabetic"* are also accepted). Null values will pass the check with no violation reported.

For best performance with large lists in general, prefer the *foreign_key* check function; the fixed ISO 4217 code lists used here are small enough (178 codes) that this is not a concern.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression
* `code_format` - ISO 4217 code representation to validate against, either *alphabetic* (default) or *numeric*; matching is case-insensitive
* `case_sensitive` - whether to perform a case-sensitive comparison (default: True); ignored when *code_format* is *numeric*

**Returns**:

Column object for condition

**Raises**:

* `MissingParameterError` - if *code_format* is None.
* `InvalidParameterError` - if *code_format* is not a string, or is not a supported representation.

### is_valid_subdivision_code[​](#is_valid_subdivision_code "Direct link to is_valid_subdivision_code")

```python
@register_rule("row")
def is_valid_subdivision_code(
        column: str | Column,
        case_sensitive: bool = True,
        country_column: str | Column | None = None) -> Column

```

Checks whether the values in the input column are valid ISO 3166-2 country subdivision codes.

ISO 3166-2 codes identify subdivisions (states, provinces, regions, etc.) of a country, e.g. *US-CA* (California, US), *GB-ENG* (England, GB), *DE-BY* (Bavaria, DE). Every code embeds its country's ISO 3166-1 alpha-2 prefix, so a plain membership check against the full code list already rejects a subdivision suffix paired with the wrong country (e.g. *US-BY* is not itself a registered code, even though *US* and *BY* are each valid on their own).

If the checked column and a country code live in separate columns, pass *country_column* to additionally verify that the subdivision's country prefix matches that column's value for the same row (e.g. *country_column="country"* with *column="subdivision"* flags a row where *subdivision="US-CA"* but *country="GB"*). *country_column* can be a string column name or a column expression.

By default the comparison is case-sensitive; pass *case_sensitive* as False to accept values in any case. *case_sensitive* also governs the *country_column* cross-check: with the default *case_sensitive=True*, *column="US-CA"* paired with a *country_column* value of *"us"* is flagged as a mismatch, since the comparison is exact on both sides. Null values will pass the check with no violation reported; a null *country_column* value for an otherwise-valid *column* value also passes, since there is nothing to cross-check.

For best performance with large lists in general, prefer the *foreign_key* check function; the ISO 3166-2 code list is large enough that *foreign_key* may perform better for high-volume checks.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression
* `case_sensitive` - whether to perform a case-sensitive comparison (default: True)
* `country_column` - optional column name or column expression holding the expected ISO 3166-1 alpha-2 country code; when provided, also flags a row where *column*'s country prefix does not match this column's value

**Returns**:

Column object for condition

### is_valid_language_code[​](#is_valid_language_code "Direct link to is_valid_language_code")

```python
@register_rule("row")
def is_valid_language_code(column: str | Column,
                           code_format: str = "alpha-2",
                           case_sensitive: bool = True) -> Column

```

Checks whether the values in the input column are valid ISO 639 language codes.

ISO 639 defines two code representations, selected with *code_format*:

* *alpha-2* (default): the two-letter ISO 639-1 code, e.g. *en*, *fr*, *de* (covering macrolanguages and individual languages in common use).
* *alpha-3*: the three-letter ISO 639-3 code, e.g. *eng*, *fra*, *deu* (the comprehensive registry covering all known languages, including ancient, extinct and constructed ones).

Unlike *is_valid_country_code*/*is_valid_currency_code*, *alpha-2* is not a subset representation of every *alpha-3* entry: most *alpha-3* codes have no *alpha-2* counterpart, since ISO 639-3 covers far more languages than ISO 639-1. Legacy ISO 639-2 bibliographic codes that differ from the terminology code (e.g. *ger* for German, instead of *deu*) are not accepted. Every code still recognized by the registration authorities is accepted, including deprecated *alpha-2* codes not yet withdrawn from circulation (e.g. *sh* for Serbo-Croatian). ISO 639 codes are conventionally lowercase; *case_sensitive* compares against the codes as registered.

By default the comparison is case-sensitive; pass *case_sensitive* as False to accept values in any case. *code_format* matching itself is case-insensitive (*"ALPHA-3"*/*"Alpha-2"* are also accepted). Null values will pass the check with no violation reported.

For best performance with large lists in general, prefer the *foreign_key* check function; the *alpha-2* list is small, but the *alpha-3* list is large enough that *foreign_key* may perform better for high-volume checks.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression
* `code_format` - ISO 639 code representation to validate against, either *alpha-2* (default) or *alpha-3*; matching is case-insensitive
* `case_sensitive` - whether to perform a case-sensitive comparison (default: True)

**Returns**:

Column object for condition

**Raises**:

* `MissingParameterError` - if *code_format* is None.
* `InvalidParameterError` - if *code_format* is not a string, or is not a supported representation.

### is_ipv4_address_in_cidr[​](#is_ipv4_address_in_cidr "Direct link to is_ipv4_address_in_cidr")

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

* `MissingParameterError` - if *cidr_block* is None.
* `InvalidParameterError` - if *cidr_block* is an empty string.
* `InvalidParameterError` - if *cidr_block* is provided but not in valid IPv4 CIDR notation.

### is_valid_ipv6_address[​](#is_valid_ipv6_address "Direct link to is_valid_ipv6_address")

```python
@register_rule("row")
def is_valid_ipv6_address(column: str | Column) -> Column

```

Validate if the column contains properly formatted IPv6 addresses.

**Arguments**:

* `column` - The column to check; can be a string column name or a Column expression.

**Returns**:

Column object for condition indicating whether a value is a valid IPv6 address.

### is_ipv6_address_in_cidr[​](#is_ipv6_address_in_cidr "Direct link to is_ipv6_address_in_cidr")

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

* `MissingParameterError` - If *cidr_block* is None.
* `InvalidParameterError` - If *cidr_block* is an empty string.
* `InvalidParameterError` - if *cidr_block* is provided but not in valid IPv6 CIDR notation.

### is_data_fresh[​](#is_data_fresh "Direct link to is_data_fresh")

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
* `base_timestamp` - (optional) set base timestamp column from which the stale check is calculated, if not provided uses current_timestamp()

**Returns**:

Column object for condition

### has_no_outliers[​](#has_no_outliers "Direct link to has_no_outliers")

```python
@row_preserving
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

### is_unique[​](#is_unique "Direct link to is_unique")

```python
@row_preserving
@register_rule("dataset")
def is_unique(columns: list[str | Column],
              nulls_distinct: bool = True,
              row_filter: str | None = None,
              trim_strings: bool = False) -> tuple[Column, Callable]

```

Build a uniqueness check condition and closure for dataset-level validation.

This function checks whether the specified columns contain unique values within the dataset and reports rows with duplicate combinations. When *nulls_distinct* is True (default), rows with NULLs are treated as distinct (SQL ANSI behavior); otherwise, NULLs are treated as equal when checking for duplicates.

In streaming, uniqueness is validated within individual micro-batches only.

**Arguments**:

* `columns` - List of column names (str) or Spark Column expressions to validate for uniqueness.
* `nulls_distinct` - Whether NULLs are treated as distinct (default: True).
* `row_filter` - Optional SQL expression for filtering rows before checking uniqueness. Auto-injected from the check filter.
* `trim_strings` - Whether to trim leading and trailing spaces from string keys before checking uniqueness (default: False). Non-string keys and input values are unchanged. Nested string values inside arrays or structs are not trimmed.

**Returns**:

A tuple of:

* A Spark Column representing the condition for uniqueness violations.
* A closure that applies the uniqueness check and adds the necessary condition/count columns.

### foreign_key[​](#foreign_key "Direct link to foreign_key")

```python
@row_preserving
@register_rule("dataset")
def foreign_key(columns: list[str | Column],
                ref_columns: list[str | Column],
                ref_df_name: str | None = None,
                ref_table: str | None = None,
                negate: bool = False,
                row_filter: str | None = None,
                null_safe: bool = False,
                broadcast_ref: bool = False) -> tuple[Column, Callable]

```

Build a foreign key check condition and closure for dataset-level validation.

This function verifies that values in the specified foreign key columns exist (or don't exist, if *negate=True*) in the corresponding reference columns of another DataFrame or table. Rows where foreign key values do not match the reference are reported as violations.

By default, NULL values in the foreign key columns are ignored (SQL ANSI behavior). When *null_safe=True*, NULL foreign-key values are matched against NULL reference values.

**Arguments**:

* `columns` - List of column names (str) or Column expressions in the dataset (foreign key).
* `ref_columns` - List of column names (str) or Column expressions in the reference dataset.
* `ref_df_name` - Name of the reference DataFrame (used when passing DataFrames directly).
* `ref_table` - Name of the reference table (used when reading from catalog).
* `negate` - If True, the condition is negated (i.e., the check fails when the foreign key values exist in the reference DataFrame/Table). If False, the check fails when the foreign key values do not exist in the reference.
* `row_filter` - Optional SQL expression for filtering rows before checking the foreign key. Auto-injected from the check filter.
* `null_safe` - If True, checks NULL foreign key values to match NULL reference values. If False, skips NULL values in the foreign key columns. False is a default.
* `broadcast_ref` - If True, adds a broadcast hint to the deduplicated reference values. Values are copied to every executor. Enable this option only for small reference tables. False is a default.

**Returns**:

A tuple of:

* A Spark Column representing the condition for foreign key violations.
* A closure that applies the foreign key validation by joining against the reference.

**Raises**:

MissingParameterError:

* if neither *ref_df_name* nor *ref_table* is provided. InvalidParameterError:
* if both *ref_df_name* and *ref_table* are provided.
* if the number of *columns* and *ref_columns* do not match.
* if *ref_df_name* is not found in the provided *ref_dfs* dictionary.

### sql_query[​](#sql_query "Direct link to sql_query")

```python
@row_preserving
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

* Row-level validation (merge_columns provided): Query results are joined back to specific rows
* Dataset-level validation (merge_columns omitted or None): All rows get the same check result

Use dataset-level for aggregate validations like "total count > 100" or "avg(amount) < 1000". Use row-level when you need to identify specific problematic rows.

**Arguments**:

* `query` - SQL query that must return as a minimum a condition column and all merge columns (if provided). When merge_columns are provided, the resulting DataFrame is automatically joined back to the input DataFrame. When merge_columns are not provided, the check applies to all rows (either all pass or all fail), making it useful for dataset-level validation with custom_metrics. Reference DataFrames provided in the ref_dfs parameter are registered as temp views when the query refers to them by name in a double-curly-brace placeholder (like *input_placeholder*).

* `merge_columns` - Optional (can be None or omitted). List of columns to join results back to input DataFrame.

  <!-- -->

  * If provided: Row-level validation - different rows can have different results
  * If None/omitted: Dataset-level validation - all rows get same result. The query must return at most one row. This is verified lazily: a query returning more than one row makes the Spark action that computes the results fail (e.g. a write or *collect*), not the *apply_checks* call. When provided, columns must form a unique key to avoid duplicate records.

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

### is_aggr_not_greater_than[​](#is_aggr_not_greater_than "Direct link to is_aggr_not_greater_than")

```python
@row_preserving
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
* `limit` - Numeric value, column name, or SQL expression for the limit. String literals must be single quoted, e.g. 'string_value'.
* `aggr_type` - Aggregation type (default: 'count'). Curated types include count, sum, avg, min, max, count_distinct, stddev, percentile, and more. Any Databricks built-in aggregate is supported.
* `group_by` - Optional list of column names or Column expressions to group by.
* `row_filter` - Optional SQL expression to filter rows before aggregation. Auto-injected from the check filter.
* `aggr_params` - Optional dict of parameters for aggregates requiring them (e.g., percentile value for percentile functions, accuracy for approximate aggregates). Parameters are passed as keyword arguments to the Spark function.

**Returns**:

A tuple of:

* A Spark Column representing the condition for aggregation limit violations.
* A closure that applies the aggregation check and adds the necessary condition/metric columns.

**Raises**:

* `InvalidParameterError` - If parameters are invalid — e.g. an unknown aggregate, negative tolerances, or column '\*' with an unsupported aggregate (see *validate_star_aggregate*).

### is_aggr_not_less_than[​](#is_aggr_not_less_than "Direct link to is_aggr_not_less_than")

```python
@row_preserving
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
* `limit` - Numeric value, column name, or SQL expression for the limit. String literals must be single quoted, e.g. 'string_value'.
* `aggr_type` - Aggregation type (default: 'count'). Curated types include count, sum, avg, min, max, count_distinct, stddev, percentile, and more. Any Databricks built-in aggregate is supported.
* `group_by` - Optional list of column names or Column expressions to group by.
* `row_filter` - Optional SQL expression to filter rows before aggregation. Auto-injected from the check filter.
* `aggr_params` - Optional dict of parameters for aggregates requiring them (e.g., percentile value for percentile functions, accuracy for approximate aggregates). Parameters are passed as keyword arguments to the Spark function.

**Returns**:

A tuple of:

* A Spark Column representing the condition for aggregation limit violations.
* A closure that applies the aggregation check and adds the necessary condition/metric columns.

**Raises**:

* `InvalidParameterError` - If parameters are invalid — e.g. an unknown aggregate, negative tolerances, or column '\*' with an unsupported aggregate (see *validate_star_aggregate*).

### is_aggr_equal[​](#is_aggr_equal "Direct link to is_aggr_equal")

```python
@row_preserving
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
* `limit` - Numeric value, column name, or SQL expression for the limit. String literals must be single quoted, e.g. 'string_value'.
* `aggr_type` - Aggregation type (default: 'count'). Curated types include count, sum, avg, min, max, count_distinct, stddev, percentile, and more. Any Databricks built-in aggregate is supported.
* `group_by` - Optional list of column names or Column expressions to group by.
* `row_filter` - Optional SQL expression to filter rows before aggregation. Auto-injected from the check filter.
* `aggr_params` - Optional dict of parameters for aggregates requiring them (e.g., percentile value for percentile functions, accuracy for approximate aggregates). Parameters are passed as keyword arguments to the Spark function.
* `abs_tolerance` - Optional absolute tolerance for equality comparison of numeric aggregations.
* `rel_tolerance` - Optional relative tolerance for equality comparison of numeric aggregations.

**Returns**:

A tuple of:

* A Spark Column representing the condition for aggregation limit violations.
* A closure that applies the aggregation check and adds the necessary condition/metric columns.

**Raises**:

* `InvalidParameterError` - If parameters are invalid — e.g. an unknown aggregate, negative tolerances, or column '\*' with an unsupported aggregate (see *validate_star_aggregate*).

### is_aggr_not_equal[​](#is_aggr_not_equal "Direct link to is_aggr_not_equal")

```python
@row_preserving
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
* `limit` - Numeric value, column name, or SQL expression for the limit. String literals must be single quoted, e.g. 'string_value'.
* `aggr_type` - Aggregation type (default: 'count'). Curated types include count, sum, avg, min, max, count_distinct, stddev, percentile, and more. Any Databricks built-in aggregate is supported.
* `group_by` - Optional list of column names or Column expressions to group by.
* `row_filter` - Optional SQL expression to filter rows before aggregation. Auto-injected from the check filter.
* `aggr_params` - Optional dict of parameters for aggregates requiring them (e.g., percentile value for percentile functions, accuracy for approximate aggregates). Parameters are passed as keyword arguments to the Spark function.
* `abs_tolerance` - Optional absolute tolerance for equality comparison of numeric aggregations.
* `rel_tolerance` - Optional relative tolerance for equality comparison of numeric aggregations.

**Returns**:

A tuple of:

* A Spark Column representing the condition for aggregation limit violations.
* A closure that applies the aggregation check and adds the necessary condition/metric columns.

**Raises**:

* `InvalidParameterError` - If parameters are invalid — e.g. an unknown aggregate, negative tolerances, or column '\*' with an unsupported aggregate (see *validate_star_aggregate*).

### has_no_aggr_outliers[​](#has_no_aggr_outliers "Direct link to has_no_aggr_outliers")

```python
@row_preserving
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

For each combination of *group_by* values this check:

1. Computes *metric = aggr_type(column)* per *time_interval* bucket.
2. Derives a rolling baseline (mean and stddev_pop) over the preceding *lookback_num_intervals* buckets.
3. Passes silently when fewer than *warmup_num_intervals* historical buckets are available, the series is constant (stddev == 0), or the most-recent bucket is missing.
4. Fails when *|current_metric - baseline| > sigma \* stddev*.

**Arguments**:

* `column` - Column name (str) or Column expression to aggregate (e.g. *"revenue"* or *F.col("a") - F.col("b")*). Pass *"*"\* for *count(*)\*.
* `time_column` - Name of the timestamp/date column used to bucket rows into time grains.
* `aggr_type` - Aggregation type applied per bucket (default: *"avg"*). All curated DQX aggregate types are supported (count, sum, avg, min, max, count_distinct, stddev, percentile, etc.).
* `sigma` - Number of standard deviations that defines the outlier band
* `(default` - *3.0*). Must be positive.
* `lookback_num_intervals` - Number of preceding time-grain buckets used to build the rolling baseline (default: *14*). Must be >= 2.
* `warmup_num_intervals` - Minimum number of historical buckets required before the check fires (default: *7*). Must satisfy *1 <= warmup_num_intervals <= lookback_num_intervals*.
* `time_interval` - Granularity at which to bucket the *time_column*
* `(default` - *"day"*). One of *"minute"*, *"hour"*, *"day"*, *"week"*, *"month"*.
* `group_by` - Optional list of column names or Column expressions to segment the outlier band (e.g. *\["csp", "region"]*). The check fires if *any* group exceeds its own band.
* `time_column`0 - Optional SQL expression to filter rows before aggregation (e.g. *"status = 'Active'"*). Auto-injected from the check filter.
* `time_column`1 - Optional dict of extra parameters for aggregate functions that require them (e.g. *percentile=0.95* for percentile aggregates).

**Returns**:

A tuple of:

* A Spark Column representing the outlier condition (string message on violation, NULL on pass).
* A closure *apply(df)* that enriches the DataFrame with the condition column.

**Raises**:

* `time_column`2 - If *sigma <= 0*, *lookback_num_intervals < 2*, *warmup_num_intervals* is out of range, *time_interval* is unknown, or *column* is *"*"\* with an unsupported aggregate (see *validate_star_aggregate*).
* `time_column`3 - If *aggr_type* requires *aggr_params* that are not supplied (e.g. percentile functions).

### aggr_matches_dataset[​](#aggr_matches_dataset "Direct link to aggr_matches_dataset")

```python
@row_preserving
@register_rule("dataset")
def aggr_matches_dataset(
        column: str | Column,
        ref_table: str | None = None,
        ref_df_name: str | None = None,
        ref_column: str | Column | None = None,
        aggr_type: str = "count",
        aggr_params: dict[str, Any] | None = None,
        group_by: list[str | Column] | None = None,
        ref_group_by: list[str | Column] | None = None,
        row_filter: str | None = None,
        ref_row_filter: str | None = None,
        abs_tolerance: float | None = None,
        rel_tolerance: float | None = None) -> tuple[Column, Callable]

```

Build an upstream table comparison check condition and closure for dataset-level validation.

This function verifies that an aggregation on a column in the checked DataFrame matches the same aggregation computed on a reference (upstream) DataFrame or table. It is commonly used to validate that a row count (or other aggregate metric) in a downstream table matches its upstream source, catching data loss or duplication introduced during ingestion.

**Arguments**:

* `column` - Column name (str) or Column expression to aggregate in the checked DataFrame. Pass *"*"\* for *count(*)\* over all rows.
* `ref_table` - Name of the reference (upstream) table to read from the catalog.
* `ref_df_name` - Name of the reference (upstream) DataFrame (used when passing DataFrames directly).
* `ref_column` - Column name (str) or Column expression to aggregate in the reference DataFrame. Defaults to *column* when not provided.
* `aggr_type` - Aggregation type (default: 'count'). Curated types include count, sum, avg, min, max, count_distinct, stddev, percentile, and more. Any Databricks built-in aggregate is supported.
* `aggr_params` - Optional dict of parameters for aggregates requiring them (e.g., percentile value for percentile functions, accuracy for approximate aggregates). Parameters are passed as keyword arguments to the Spark function.
* `group_by` - Optional list of column names or Column expressions in the checked DataFrame to compare the aggregate per group instead of dataset-wide. Only simple column expressions are supported, e.g. *F.col("region")*. A group present in the checked DataFrame but absent from the reference is reported as a mismatch; groups present only in the reference are not surfaced. Group keys are matched null-safely on both the checked and reference sides (including window-incompatible aggregates such as *count_distinct*), so a legitimately null group key is compared like any other rather than being dropped.
* `ref_group_by` - Optional list of group-by columns on the reference (upstream) side, matched to *group_by* by position. Defaults to *group_by* when omitted. Must have the same length as *group_by*. Requires *group_by* to be set.
* `row_filter` - Optional SQL expression to filter rows in the checked DataFrame before aggregation. Auto-injected from the check filter.
* `ref_row_filter` - Optional SQL expression to filter rows in the reference DataFrame or table before aggregation (e.g. to align both sides on the same date partition).
* `ref_table`0 - Values are considered equal if the absolute difference is less than or equal to the tolerance. This is applicable to numeric aggregates.
* `ref_table`1 - Relative tolerance for numeric comparisons. Differences within this relative tolerance are ignored. Useful if the aggregates vary in scale. Because it compares two separately-computed aggregates, sum/avg over floating-point columns can differ across runs/clusters (non-associative summation) even for identical data. A small rel_tolerance value is recommended for these situations.

**Returns**:

A tuple of:

* A Spark Column representing the condition for upstream comparison violations.
* A closure that applies the upstream comparison check and adds the necessary condition/metric columns.

**Raises**:

MissingParameterError:

* if neither *ref_df_name* nor *ref_table* is provided. InvalidParameterError:
* if both *ref_df_name* and *ref_table* are provided.
* if *abs_tolerance* or *rel_tolerance* is negative.
* if *ref_group_by* is provided without *group_by*.
* if *group_by* and *ref_group_by* lengths differ.

### compare_datasets[​](#compare_datasets "Direct link to compare_datasets")

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
        rel_tolerance: float | None = None,
        raise_on_duplicate_keys: bool = False) -> tuple[Column, Callable]

```

Dataset-level check that compares two datasets and returns a condition for changed rows, with details on row and column-level differences.

Only columns that are common across both datasets will be compared. Mismatched columns are ignored. Detailed information about the differences is provided in the condition column. The comparison does not support Map types (any column comparison on map type is skipped automatically).

By default, duplicate matching-key groups are paired lazily using a per-group row number. Rows are ordered by their compared values, with nulls first — numeric columns by their native value (so magnitude ordering is preserved) and other types by their string representation. This pairing prevents Cartesian fan-out but does not attempt to minimize the number of reported column changes.

When *row_filter* is set, exact-value in-scope rows are paired first. Remaining in-scope rows are paired positionally. Filtered-out rows can pair with reference rows only for keys with no in-scope source rows, so they cannot consume a surplus reference row or disturb an exact match.

With *abs_tolerance* / *rel_tolerance*, only exactly-equal values are paired up front; values that match solely within a tolerance fall to positional pairing, which is a greedy heuristic rather than an optimal tolerance matching. For duplicate matching keys whose within-tolerance partners interleave, this can pair a row with a different but still-tolerant reference row, or flag a tolerant row when an unmatched row shifts the positional order. The pairing is deterministic; when exact one-to-one tolerance matching matters, deduplicate the matching keys first.

Unfiltered lazy pairing adds two *row_number* window computations (one per dataset) that sort both datasets on the compared columns. Filtered comparisons add value-group counts and exact and residual ranking. When the matching keys are known to be unique, set *raise_on_duplicate_keys* to True to skip this pairing work and use a single join instead; this is the more efficient option on large datasets, at the cost of an eager uniqueness check that raises if duplicates are present.

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

* `columns` - List of columns to use for row matching with the reference DataFrame (can be a list of string column names or column expressions). Only simple column expressions are supported, e.g. F.col("col_name").
* `ref_columns` - List of columns in the reference DataFrame or Table to row match against the source DataFrame (can be a list of string column names or column expressions). The *columns* parameter is matched with *ref_columns* by position, so the order of the provided columns in both lists must be exactly aligned. Only simple column expressions are supported, e.g. F.col("col_name").
* `ref_df_name` - Name of the reference DataFrame (used when passing DataFrames directly).
* `ref_table` - Name of the reference table (used when reading from catalog).
* `check_missing_records` - Perform FULL OUTER JOIN between the DataFrames to also find records that could be missing from the DataFrame. Use with caution as it may produce output with more rows than in the original DataFrame.
* `exclude_columns` - List of columns to exclude from the value comparison but not from row matching (can be a list of string column names or column expressions). Only simple column expressions are supported, e.g. F.col("col_name"). This parameter does not alter the list of columns used to determine row matches; it only controls which columns are skipped during the column value comparison.
* `null_safe_row_matching` - If True, treats nulls as equal when matching rows. If False, rows containing nulls in a matching column cannot match and are excluded from matching-key uniqueness validation.
* `null_safe_column_value_matching` - If True, treats nulls as equal when matching column values. If enabled, (NULL, NULL) column values are equal and matching.
* `row_filter` - Optional SQL expression to filter rows in the input DataFrame. Auto-injected from the check filter.
* `columns`0 - Values are considered equal if the absolute difference is less than or equal to the tolerance. This is applicable to numeric columns. For example, abs(a - b) <= tolerance. With abs_tolerance=0.01, values 2.001 and 2.0099 are equal (diff=0.0089), but 2.001 and 2.02 are not (diff=0.019).
* `columns`1 - Relative tolerance for numeric comparisons. Differences within this relative tolerance are ignored. Useful if numbers vary in scale. For example, abs(a - b) <= rel_tolerance \* max(abs(a), abs(b)). With rel_tolerance=0.01 (1%), values 100 and 101 are equal (diff=1), but 100 and 102 are not (diff=2).
* `columns`2 - Controls duplicate-key handling, which is also a performance trade-off. If True, require unique matching keys among in-scope source rows and all reference rows: uniqueness is validated with a lightweight aggregation (an eager Spark job) and rows are then paired with a single join, avoiding the per-group row-number windows. This is the more efficient option when keys are known to be unique, but raises *InvalidParameterError* if duplicates are present. If False (default), pair duplicate-key rows lazily by compared values using per-group row-number windows; this is robust to duplicates but sorts both datasets on the compared columns.

**Returns**:

Tuple\[Column, Callable]:

* A Spark Column representing the condition for comparison violations.
* A closure that applies the comparison validation.

**Raises**:

MissingParameterError:

* if neither *ref_df_name* nor *ref_table* is provided. InvalidParameterError:
* if both *ref_df_name* and *ref_table* are provided.
* if the number of *columns* and *ref_columns* do not match.
* if *abs_tolerance* or *rel_tolerance* is negative.
* if either DataFrame is streaming; dataset comparison requires bounded inputs.
* if *raise_on_duplicate_keys* is True and the matching columns do not uniquely identify rows in either dataset.

### is_data_fresh_per_time_window[​](#is_data_fresh_per_time_window "Direct link to is_data_fresh_per_time_window")

```python
@row_preserving
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

If *lookback_windows* is provided, only data within that lookback period will be validated. If omitted, the entire dataset will be checked.

**Arguments**:

* `column` - Column name (str) or Column expression containing timestamps to check.
* `window_minutes` - Time window in minutes to check for data arrival.
* `min_records_per_window` - Minimum number of records expected per time window.
* `lookback_windows` - Optional number of time windows to look back from *curr_timestamp*. This filters records to include only those within the specified number of time windows from *curr_timestamp*. If no lookback is provided, the check is applied to the entire dataset.
* `row_filter` - Optional SQL expression to filter rows before checking. Auto-injected from the check filter.
* `curr_timestamp` - Optional current timestamp column. If not provided, current_timestamp() function is used.

**Returns**:

A tuple of:

* A Spark Column representing the condition for missing data within a time window.
* A closure that applies the completeness check and adds the necessary condition columns.

**Raises**:

* `InvalidParameterError` - If min_records_per_window or window_minutes are not positive integers, or if lookback_windows is provided and is not a positive integer.

### has_no_gaps_per_time_window[​](#has_no_gaps_per_time_window "Direct link to has_no_gaps_per_time_window")

```python
@row_preserving
@register_rule("dataset")
def has_no_gaps_per_time_window(
        column: str | Column,
        window_minutes: int,
        group_by: list[str | Column] | None = None,
        trailing_gap: bool = False,
        curr_timestamp: Column | None = None) -> tuple[Column, Callable]

```

Checks whether a time-series column has gaps, i.e. time windows that contain no rows at all between windows that do (for example no data for 2025-07-15 while 2025-07-14 and 2025-07-16 are present).

A missing window has no row to attach a violation to, so the gap is reported on every row in the last present window before the gap. Distinct values of *column* are bucketed into fixed time windows of *window_minutes* (a fixed grid aligned to absolute time), and a gap is flagged whenever the next present window starts more than one window after the current one. Gaps are therefore measured against this fixed absolute-time grid, not the elapsed time between consecutive events.

When *group_by* is provided, gaps are detected independently within each group (for example per device or session) and the work partitions by the group key, which is the common case for IoT or clickstream data. When it is omitted, the whole column is treated as a single series.

By default, only interior gaps are detected: a trailing gap (no data for the most recent windows up to the current time) is not reported, because there is no later row to anchor it to. Set *trailing_gap* to *True* to additionally flag the last present window when it ends more than one window before the current time, so that missing recent data (for example no rows reported today) is caught even at the tail of the series. Null values are ignored and pass with no violation reported.

In streaming, gaps are detected within individual micro-batches only.

**Arguments**:

* `column` - timestamp or date column to check; can be a string column name or a column expression
* `window_minutes` - size of the time window in minutes that defines the expected data grain (for example 1440 for daily)
* `group_by` - optional list of column names or Column expressions to detect gaps independently within each group; when omitted, the whole column is treated as a single global series
* `trailing_gap` - if *True*, also flags the last present window (per group) when it ends more than one window before *curr_timestamp*, anchoring the trailing boundary to the current time instead of leaving it unchecked; defaults to *False*
* `curr_timestamp` - optional current timestamp column used to anchor trailing-gap detection; only used when *trailing_gap* is *True*; if not provided, *current_timestamp()* is used. The anchor is bucketed onto the same absolute-time grid as the event windows, which aligns to UTC epoch boundaries regardless of *spark.sql.session.timeZone*. With daily windows this means the "current window" is the UTC day, which can differ from the local day near midnight (e.g. 21:00 in a UTC-4 timezone is already the next UTC day); pass an explicit *curr_timestamp* shifted to your timezone if you need local-day anchoring.

**Returns**:

A tuple of:

* A Spark Column representing the gap condition.
* A closure that applies the gap detection and adds the necessary condition columns.

**Raises**:

* `InvalidParameterError` - if *window_minutes* is not a positive integer, if *group_by* is not a list, or if *curr_timestamp* is provided while *trailing_gap* is *False*.

### has_valid_schema[​](#has_valid_schema "Direct link to has_valid_schema")

```python
@register_for_original_columns_preselection()
@row_preserving
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

* If the *expected_schema* string is invalid or cannot be parsed
* If *expected_schema* is neither a string nor a StructType
* If more than one of *expected_schema*, *ref_df_name*, or *ref_table* are specified
* If none of *expected_schema*, *ref_df_name*, or *ref_table* are specified

**Notes**:

Exactly one of *expected_schema*, *ref_df_name*, or *ref_table* must be specified.

### is_valid_json[​](#is_valid_json "Direct link to is_valid_json")

```python
@register_rule("row")
def is_valid_json(column: str | Column) -> Column

```

Checks whether the values in the input column are valid JSON strings.

**Arguments**:

* `column` - Column name (str) or Column expression to check for valid JSON.

**Returns**:

A Spark Column representing the condition for invalid JSON strings.

### has_json_keys[​](#has_json_keys "Direct link to has_json_keys")

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

### has_valid_json_schema[​](#has_valid_json_schema "Direct link to has_valid_json_schema")

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

## DatasetAggregate Objects[​](#datasetaggregate-objects "Direct link to DatasetAggregate Objects")

```python
@dataclass(frozen=True)
class DatasetAggregate()

```

Aggregate metric over all rows that a built-in aggregate check without *group_by* adds to the checked DataFrame.

Such checks attach it to their closure (as the *DATASET_AGGREGATE_ATTRIBUTE* attribute) so the engine can compute the metrics of all of them in one aggregation and one cross join (see *add_dataset_aggregates*), and then only add each check's condition column with *add_condition*.

**Attributes**:

* `metric_col` - Unique name of the column holding the metric.
* `aggr_type` - Aggregate function name.
* `build_expression` - Builds the (unaliased) aggregate expression, including the check filter. It is built lazily so that invalid parameters or unsafe filters raise when the checks are applied, not when the rule is created.
* `add_condition` - Adds the check's condition column to a DataFrame that already has *metric_col*.

### add_dataset_aggregates[​](#add_dataset_aggregates "Direct link to add_dataset_aggregates")

```python
def add_dataset_aggregates(df: DataFrame, input_df: DataFrame,
                           aggregates: list[DatasetAggregate]) -> DataFrame

```

Add the metric columns of the given aggregates to *df*, computed in one aggregation over all rows of *input_df*.

The aggregation never includes the columns or logical plan added by earlier checks. Using a window without partitionBy would move all rows into a single partition, so a global aggregation is cross joined onto *df* instead. It returns a single row; the limit is informational only.

**Arguments**:

* `df` - The DataFrame to add the metric columns to. Must hold the same rows as *input_df*.
* `input_df` - The untouched input DataFrame the aggregations are computed from.
* `aggregates` - The aggregates to add.

**Returns**:

*df* with one additional column per aggregate.

### get_limit_expr[​](#get_limit_expr "Direct link to get_limit_expr")

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

### get_normalized_column_and_expr[​](#get_normalized_column_and_expr "Direct link to get_normalized_column_and_expr")

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

### resolve_aggregate_column[​](#resolve_aggregate_column "Direct link to resolve_aggregate_column")

```python
def resolve_aggregate_column(column: str | Column) -> _AggregateColumn

```

Resolve an aggregate column like *get_normalized_column_and_expr*, canonicalizing every "\*" form.

Any bare-star form (the string *"*"*, *F.expr("*")*, or *F.col("*")*) is canonicalized to the *("", "*")* name pair, so a *count(*)\* check produces identical names and messages regardless of how it was constructed, and callers can detect the star with a simple *aggr_col_str == "*"\* check. See `1435`.

**Arguments**:

* `column` - Column name (str) or Column expression to aggregate.

**Returns**:

A tuple of the normalized column name, the display column name, and the Column expression.

### build_filtered_aggregate_input[​](#build_filtered_aggregate_input "Direct link to build_filtered_aggregate_input")

```python
def build_filtered_aggregate_input(row_filter: str | None, aggr_col_str: str,
                                   aggr_col_expr: Column) -> Column

```

Build the (optionally row-filtered) column expression fed into an aggregate function.

When *row_filter* is present the column is wrapped in a CASE WHEN. A star (*"*"*) cannot be the THEN value of a CASE WHEN — Spark star-expands it against every column in scope, raising *INVALID_USAGE_OF_STAR_OR_REGEX* — so for *count(*)* a non-null literal placeholder is used instead (*count()* only cares about nullness). *safe_filter_expr* rejects unsafe SQL (see `1303`, `1435`).

**Arguments**:

* `row_filter` - Optional SQL expression to filter rows before aggregation.
* `aggr_col_str` - Canonicalized display name of the column (as returned by *resolve_aggregate_column*).
* `aggr_col_expr` - The Column expression to aggregate.

**Returns**:

The column expression to pass to the aggregate function.

### validate_star_aggregate[​](#validate_star_aggregate "Direct link to validate_star_aggregate")

```python
def validate_star_aggregate(aggr_col_str: str, aggr_type: str, *,
                            uses_placeholder: bool) -> None

```

Reject star-column/aggregate combinations that are unsupported or would be silently wrong.

*"*"\* means "all rows" and is only meaningful for counting. Two evaluation paths exist:

* Native (*uses_placeholder=False*: no row filter, or *aggr_matches_dataset*'s reference side which filters the DataFrame directly): *count* and *count_distinct* both accept *"*"\*; single-arg aggregates (sum, avg, ...) do not, so they are rejected at build time with a clear error rather than failing later in Spark.
* Placeholder (*uses_placeholder=True*: a row filter on the checked/outlier side wraps the column in a CASE WHEN whose THEN is a non-null literal, see *build_filtered_aggregate_input*): only *count* is correct — the literal placeholder makes *count_distinct* collapse to 1 and other aggregates meaningless — so everything except *count* is rejected.

Comparison is case-sensitive to match the case-sensitive aggregate resolution (*getattr(F, aggr_type)*).

**Arguments**:

* `aggr_col_str` - Canonicalized display name of the column (as returned by *resolve_aggregate_column*).
* `aggr_type` - The aggregate function name.
* `uses_placeholder` - True when the star will be aggregated via the filtered-count placeholder.

**Raises**:

* `InvalidParameterError` - If *aggr_col_str* is the star *"*"\* and *aggr_type* is not a supported star aggregate for the evaluation path.
