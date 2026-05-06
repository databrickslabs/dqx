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
* `df` - The DataFrame on which to apply the check.
* `engine_user_metadata` - Metadata provided by the engine (overridden by check.user\_metadata if present).
* `run_time_overwrite` - Optional timestamp override. If None, current\_timestamp() is used for per-micro-batch timestamps.
* `ref_dfs` - Optional reference DataFrames for dataset-level checks.
* `run_id` - Optional unique run id.

### user\_metadata[​](#user_metadata "Direct link to user_metadata")

```python
@cached_property
def user_metadata() -> dict[str, str]

```

Returns user metadata as a dictionary.

### filter\_condition[​](#filter_condition "Direct link to filter_condition")

```python
@cached_property
def filter_condition() -> Column

```

Returns the filter condition for the check.

### invalid\_columns[​](#invalid_columns "Direct link to invalid_columns")

```python
@cached_property
def invalid_columns() -> list[str]

```

Returns list of invalid check columns in the input DataFrame.

### has\_invalid\_columns[​](#has_invalid_columns "Direct link to has_invalid_columns")

```python
@cached_property
def has_invalid_columns() -> bool

```

Returns a boolean indicating whether any of the specified check columns are invalid in the input DataFrame.

### has\_invalid\_filter[​](#has_invalid_filter "Direct link to has_invalid_filter")

```python
@cached_property
def has_invalid_filter() -> bool

```

Returns a boolean indicating whether the filter is invalid in the input DataFrame.

### invalid\_sql\_expression[​](#invalid_sql_expression "Direct link to invalid_sql_expression")

```python
@cached_property
def invalid_sql_expression() -> str | None

```

Returns an invalid expression for sql expression check.

### process[​](#process "Direct link to process")

```python
def process() -> DQCheckResult

```

Process the data quality rule (check) and return results as DQCheckResult containing:

* Column with the check result
* optional DataFrame with the results of the check

Skip the check evaluation if column or columns, or filter in the check cannot be resolved in the input DataFrame. Return the check result preserving all fields with message identifying invalid fields.
