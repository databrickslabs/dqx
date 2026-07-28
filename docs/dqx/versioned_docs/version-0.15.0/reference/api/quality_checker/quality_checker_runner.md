---
sidebar_label: quality_checker_runner
title: databricks.labs.dqx.quality_checker.quality_checker_runner
---

## QualityCheckerRunner Objects

```python
class QualityCheckerRunner()
```

Runs the DQX data quality on the input data and saves the generated results to delta table(s).

#### run

```python
def run(
    run_configs: list[RunConfig], max_parallelism: int | None = os.cpu_count()
) -> None
```

Run the DQX data quality job for the provided run configs.

**Arguments**:

- `run_configs` - List of RunConfig objects containing the configuration for each run.
- `max_parallelism` - Maximum number of parallel runs. Defaults to the number of CPU cores.

#### run\_for\_patterns

```python
def run_for_patterns(patterns: list[str], exclude_patterns: list[str],
                     run_config_template: RunConfig, checks_location: str,
                     output_table_suffix: str, quarantine_table_suffix: str,
                     max_parallelism: int) -> None
```

Run the DQX data quality job for the provided location patterns using a run config template.

**Arguments**:

- `patterns` - List of location patterns (with wildcards) to apply the data quality checks to.
- `exclude_patterns` - List of table patterns to exclude from profiling (e.g. [&quot;*output&quot;, &quot;*quarantine&quot;]).
- `run_config_template` - A RunConfig object to be used as a template for each pattern, except location.
- `checks_location` - Location to read the checks from.
- `output_table_suffix` - Suffix to append to the output table names.
- `quarantine_table_suffix` - Suffix to append to the quarantine table names.
- `max_parallelism` - Maximum number of parallel runs. Defaults to the number of CPU cores.

