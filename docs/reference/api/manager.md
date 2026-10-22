# databricks.labs.dqx.manager

## DQRuleManager Objects[​](#dqrulemanager-objects "Direct link to DQRuleManager Objects")

```python
@dataclass(frozen=True)
class DQRuleManager()

```

Orchestrates the application of a data quality rule to a DataFrame and builds the final check result.

The manager is responsible for:

* Executing the rule using the appropriate row or dataset executor.
* Applying any filter condition specified in the rule to the check result.
* Combining user-defined and engine-provided metadata into the result.
* Constructing the final structured output (including check name, function, columns, metadata, etc.) as a DQCheckResult.

The manager does not implement the logic of individual checks. Instead, it delegates rule application to the appropriate DQRuleExecutor based on the rule type (row-level or dataset-level).

**Attributes**:

* `check` - The DQRule instance that defines the check to apply.
* `df` - The DataFrame on which to apply the check (unless *process* is given another one).
* `engine_user_metadata` - Metadata provided by the engine (overridden by check.user_metadata if present).
* `run_time_overwrite` - Optional timestamp override. If None, current_timestamp() is used for per-micro-batch timestamps.
* `ref_dfs` - Optional reference DataFrames for dataset-level checks.
* `run_id` - Optional unique run id.
* `input_df` - Optional untouched input DataFrame (defaults to *df*). Check columns are validated against it, and dataset-level checks compute their aggregates from it.
* `reference_cache` - Optional per-run cache of compatible reference projections.

### user_metadata[​](#user_metadata "Direct link to user_metadata")

```python
@cached_property
def user_metadata() -> dict[str, str]

```

Returns user metadata as a dictionary.

### filter_condition[​](#filter_condition "Direct link to filter_condition")

```python
@cached_property
def filter_condition() -> Column

```

Returns the filter condition for the check.

### has_unsafe_filter[​](#has_unsafe_filter "Direct link to has_unsafe_filter")

```python
@cached_property
def has_unsafe_filter() -> bool

```

Returns True if the check filter contains unsafe SQL (e.g. a destructive statement keyword).

This is a non-raising check: an unsafe filter causes the check to be skipped (like any other invalid filter) rather than aborting evaluation of the whole rule set. Note this does not apply to the *sql_query* check, whose query runs via *spark.sql* and is still hard-rejected in the executor.

### invalid_columns[​](#invalid_columns "Direct link to invalid_columns")

```python
@cached_property
def invalid_columns() -> list[str]

```

Returns list of invalid check columns in the input DataFrame.

Column names that contain characters requiring SQL identifier escaping (e.g. spaces, such as "Customer Name") are back-quoted so they are unambiguous in the skip / error messages.

### has_invalid_columns[​](#has_invalid_columns "Direct link to has_invalid_columns")

```python
@cached_property
def has_invalid_columns() -> bool

```

Returns a boolean indicating whether any of the specified check columns are invalid in the input DataFrame.

### has_invalid_filter[​](#has_invalid_filter "Direct link to has_invalid_filter")

```python
@cached_property
def has_invalid_filter() -> bool

```

Returns a boolean indicating whether the filter is invalid in the input DataFrame.

An unsafe filter is short-circuited here so that `filter_condition` (which rejects unsafe SQL by raising) is never compiled for it; the check is reported as skipped via `_get_invalid_cols_message`.

### has_invalid_custom_message[​](#has_invalid_custom_message "Direct link to has_invalid_custom_message")

```python
@cached_property
def has_invalid_custom_message() -> bool

```

Returns a boolean indicating whether the custom message expression is invalid in the input DataFrame.

### invalid_sql_expression[​](#invalid_sql_expression "Direct link to invalid_sql_expression")

```python
@cached_property
def invalid_sql_expression() -> str | None

```

Returns an invalid expression for sql expression check.

### skip_message[​](#skip_message "Direct link to skip_message")

```python
@cached_property
def skip_message() -> str

```

Returns the message explaining why the check evaluation is skipped, or an empty string if it is not skipped.

### dataset_aggregate[​](#dataset_aggregate "Direct link to dataset_aggregate")

```python
@cached_property
def dataset_aggregate() -> DatasetAggregate | None

```

Returns the aggregate of a built-in aggregate check that is not skipped, otherwise None.

The engine adds the aggregates of all such checks to the DataFrame up front, computing compatible ones together, and then calls *process* with *aggregate_added=True*.

### process[​](#process "Direct link to process")

```python
def process(df: DataFrame | None = None,
            aggregate_added: bool = False) -> DQCheckResult

```

Process the data quality rule (check) and return results as DQCheckResult containing:

* Column with the check result
* optional DataFrame with the results of the check

Skip the check evaluation if column or columns, or filter in the check cannot be resolved in the input DataFrame. Return the check result preserving all fields with message identifying invalid fields.

**Arguments**:

* `df` - Optional DataFrame to apply the check to, e.g. the result of the previous checks. Defaults to *df*.
* `aggregate_added` - Whether *df* already has the metric column of *dataset_aggregate*, so only the condition column must be added.

### is_skipped[​](#is_skipped "Direct link to is_skipped")

```python
@cached_property
def is_skipped() -> bool

```

Returns True if the check evaluation is skipped, e.g. due to invalid columns, filter, or custom message.
