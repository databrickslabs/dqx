---
sidebar_label: runner
title: databricks.labs.dqx.profiler.runner
---

## ProfilerRunner Objects

```python
class ProfilerRunner()
```

Runs the DQX profiler on the input data and saves the generated checks and profile summary stats.

#### run

```python
def run(input_config: InputConfig,
        profiler_config: ProfilerConfig) -> tuple[list[dict], dict[str, Any]]
```

Run the DQX profiler on the input data and return the generated checks and profile summary stats.

**Arguments**:

- `input_config`: Input data configuration (e.g. table name or file location, read options).
- `profiler_config`: Profiler configuration.

**Returns**:

A tuple containing the generated checks and profile summary statistics.

#### save

```python
def save(checks: list[dict], summary_stats: dict[str, Any],
         checks_location: str | None,
         profile_summary_stats_file: str | None) -> None
```

Save the generated checks and profile summary statistics to the specified files.

**Arguments**:

- `checks`: The generated checks.
- `summary_stats`: The profile summary statistics.
- `checks_location`: The file to save the checks to.
- `profile_summary_stats_file`: The file to save the profile summary statistics to.

