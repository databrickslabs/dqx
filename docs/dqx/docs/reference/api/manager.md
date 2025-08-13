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
- `run_time` - The timestamp when the check is executed.
- `ref_dfs` - Optional reference DataFrames for dataset-level checks.

#### user\_metadata

```python
@cached_property
def user_metadata() -> dict[str, str]
```

Returns user metadata as a dictionary.

#### filter\_condition

```python
@cached_property
def filter_condition()
```

Returns the filter condition for the check.

#### process

```python
def process() -> DQCheckResult
```

Process the data quality rule and return results as DQCheckResult containing:
- Column with the check result
- optional DataFrame with the results of the check

