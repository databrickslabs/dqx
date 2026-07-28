---
sidebar_label: manager
title: databricks.labs.dqx.manager
---

## DQRuleManager Objects

```python
@dataclass(frozen=True)
class DQRuleManager()
```

Orchestrates the application of a data quality rule to a DataFrame and builds the final check result.

The manager is responsible for:
- Executing the rule using the appropriate row or dataset executor.
- Applying any filter condition specified in the rule to the check result.
- Combining user-defined and engine-provided metadata into the result.
- Constructing the final structured output (including check name, function, columns, metadata, etc.)
as a DQCheckResult.

The manager does not implement the logic of individual checks. Instead, it delegates
rule application to the appropriate DQRuleExecutor based on the rule type (row-level or dataset-level).

**Attributes**:

- `check` - The DQRule instance that defines the check to apply.
- `df` - The DataFrame on which to apply the check.
- `engine_user_metadata` - Metadata provided by the engine (overridden by check.user_metadata if present).
- `run_time_overwrite` - Optional timestamp override. If None, current_timestamp() is used for per-micro-batch timestamps.
- `ref_dfs` - Optional reference DataFrames for dataset-level checks.
- `run_id` - Optional unique run id.

#### user\_metadata

```python
@cached_property
def user_metadata() -> dict[str, str]
```

Returns user metadata as a dictionary.

#### filter\_condition

```python
@cached_property
def filter_condition() -> Column
```

Returns the filter condition for the check.

#### invalid\_columns

```python
@cached_property
def invalid_columns() -> list[str]
```

Returns list of invalid check columns in the input DataFrame.

#### has\_invalid\_columns

```python
@cached_property
def has_invalid_columns() -> bool
```

Returns a boolean indicating whether any of the specified check columns are invalid in the input DataFrame.

#### has\_invalid\_filter

```python
@cached_property
def has_invalid_filter() -> bool
```

Returns a boolean indicating whether the filter is invalid in the input DataFrame.

#### has\_invalid\_custom\_message

```python
@cached_property
def has_invalid_custom_message() -> bool
```

Returns a boolean indicating whether the custom message expression is invalid in the input DataFrame.

#### invalid\_sql\_expression

```python
@cached_property
def invalid_sql_expression() -> str | None
```

Returns an invalid expression for sql expression check.

#### process

```python
def process() -> DQCheckResult
```

Process the data quality rule (check) and return results as DQCheckResult containing:
- Column with the check result
- optional DataFrame with the results of the check

Skip the check evaluation if column or columns, or filter in the check cannot be resolved in the input
DataFrame. Return the check result preserving all fields with message identifying invalid fields.

