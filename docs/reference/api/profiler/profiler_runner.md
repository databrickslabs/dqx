# databricks.labs.dqx.profiler.profiler\_runner

## ProfilerRunner Objects[​](#profilerrunner-objects "Direct link to ProfilerRunner Objects")

```python
class ProfilerRunner()

```

Runs the DQX profiler on the input data and saves the generated checks and profile summary stats.

### run[​](#run "Direct link to run")

```python
def run(generator: DQGenerator, run_config: RunConfig, product: str,
        install_folder: str) -> None

```

Run the DQX profiler for the given run configuration and save the generated checks and profile summary stats.

**Arguments**:

* `generator` - DQGenerator instance to generate data quality rules.
* `run_config` - Run configuration.
* `product` - Product name for the installation (used in storage config).
* `install_folder` - Installation folder path (used in storage config).

**Returns**:

A tuple containing the generated checks and profile summary statistics.

### run\_for\_patterns[​](#run_for_patterns "Direct link to run_for_patterns")

```python
def run_for_patterns(generator: DQGenerator, run_config: RunConfig,
                     patterns: list[str], exclude_patterns: list[str],
                     install_folder: str, product: str,
                     max_parallelism: int) -> None

```

Run the DQX profiler for the given table patterns and save the generated checks and profile summary stats.

**Arguments**:

* `generator` - DQGenerator instance to generate data quality rules.
* `run_config` - Run configuration.
* `patterns` - List of table patterns to profile (e.g. \["catalog.schema.table\*"]).
* `exclude_patterns` - List of table patterns to exclude from profiling (e.g. \["\*output", "\*quarantine"]).
* `install_folder` - Installation folder path.
* `product` - Product name for the installation.
* `max_parallelism` - Maximum number of parallel threads to use for profiling.

### save[​](#save "Direct link to save")

```python
def save(checks: list[dict], summary_stats: dict[str, Any],
         storage_config: BaseChecksStorageConfig,
         profile_summary_stats_file: str) -> None

```

Save the generated checks and profile summary statistics to the specified files.

**Arguments**:

* `checks` - The generated checks.
* `summary_stats` - The profile summary statistics.
* `storage_config` - Configuration for where to save the checks.
* `profile_summary_stats_file` - The file to save the profile summary statistics to.
