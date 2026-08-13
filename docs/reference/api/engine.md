# databricks.labs.dqx.engine

## DQEngineCore Objects[​](#dqenginecore-objects "Direct link to DQEngineCore Objects")

```python
class DQEngineCore(DQEngineCoreBase)

```

Core engine to apply data quality checks to a DataFrame.

**Arguments**:

* `workspace_client` - WorkspaceClient instance used to access the workspace.
* `spark` - Optional SparkSession to use. If not provided, the active session is used.
* `extra_params` - Optional extra parameters for the engine, such as result column names and run metadata.
* `observer` - Optional DQMetricsObserver for tracking data quality summary metrics.
* `actions` - Optional list of *DQAction* instances to evaluate after checks are applied.

### apply\_checks[​](#apply_checks "Direct link to apply_checks")

```python
def apply_checks(
    df: DataFrame,
    checks: list[DQRule],
    ref_dfs: dict[str, DataFrame] | None = None
) -> DataFrame | tuple[DataFrame, Observation]

```

Apply data quality checks to the given DataFrame.

**Arguments**:

* `df` - Input DataFrame to check.
* `checks` - List of checks to apply. Each check must be a *DQRule* instance.
* `ref_dfs` - Optional reference DataFrames to use in the checks.

**Returns**:

A DataFrame with errors and warnings result columns and an optional Observation which tracks data quality summary metrics. Summary metrics are returned by any `DQEngine` with an `observer` specified.

**Raises**:

* `InvalidCheckError` - If any of the checks are invalid.

### apply\_checks\_and\_split[​](#apply_checks_and_split "Direct link to apply_checks_and_split")

```python
def apply_checks_and_split(
    df: DataFrame,
    checks: list[DQRule],
    ref_dfs: dict[str, DataFrame] | None = None
) -> tuple[DataFrame, DataFrame] | tuple[DataFrame, DataFrame, Observation]

```

Apply data quality checks to the given DataFrame and split the results into two DataFrames ("good" and "bad").

**Arguments**:

* `df` - Input DataFrame to check.
* `checks` - List of checks to apply. Each check must be a *DQRule* instance.
* `ref_dfs` - Optional reference DataFrames to use in the checks.

**Returns**:

A tuple of two DataFrames: "good" (may include rows with warnings but no result columns) and "bad" (rows with errors or warnings and the corresponding result columns) and an optional Observation which tracks data quality summary metrics. Summary metrics are returned by any `DQEngine` with an `observer` specified.

**Raises**:

* `InvalidCheckError` - If any of the checks are invalid.

### apply\_checks\_by\_metadata[​](#apply_checks_by_metadata "Direct link to apply_checks_by_metadata")

```python
def apply_checks_by_metadata(
    df: DataFrame,
    checks: list[dict],
    custom_check_functions: dict[str, Callable] | None = None,
    ref_dfs: dict[str, DataFrame] | None = None
) -> DataFrame | tuple[DataFrame, Observation]

```

Apply data quality checks defined as metadata to the given DataFrame.

**Arguments**:

* `df` - Input DataFrame to check.

* `checks` - List of dictionaries describing checks. Each check dictionary must contain the following:

  <!-- -->

  * *check* - A check definition including check function and arguments to use.
  * *name* - Optional name for the resulting column. Auto-generated if not provided.
  * *criticality* - Optional; either *error* (rows go only to the "bad" DataFrame) or *warn* (rows appear in both DataFrames).

* `custom_check_functions` - Optional dictionary with custom check functions (e.g., *globals()* of the calling module).

* `ref_dfs` - Optional reference DataFrames to use in the checks.

**Returns**:

A DataFrame with errors and warnings result columns and an optional Observation which tracks data quality summary metrics. Summary metrics are returned by any `DQEngine` with an `observer` specified.

### apply\_checks\_by\_metadata\_and\_split[​](#apply_checks_by_metadata_and_split "Direct link to apply_checks_by_metadata_and_split")

```python
def apply_checks_by_metadata_and_split(
    df: DataFrame,
    checks: list[dict],
    custom_check_functions: dict[str, Callable] | None = None,
    ref_dfs: dict[str, DataFrame] | None = None
) -> tuple[DataFrame, DataFrame] | tuple[DataFrame, DataFrame, Observation]

```

Apply data quality checks defined as metadata to the given DataFrame and split the results into two DataFrames ("good" and "bad").

**Arguments**:

* `df` - Input DataFrame to check.

* `checks` - List of dictionaries describing checks. Each check dictionary must contain the following:

  <!-- -->

  * *check* - A check definition including check function and arguments to use.
  * *name* - Optional name for the resulting column. Auto-generated if not provided.
  * *criticality* - Optional; either *error* (rows go only to the "bad" DataFrame) or *warn* (rows appear in both DataFrames).

* `custom_check_functions` - Optional dictionary with custom check functions (e.g., *globals()* of the calling module).

* `ref_dfs` - Optional reference DataFrames to use in the checks.

**Returns**:

A tuple of two DataFrames: "good" (may include rows with warnings but no result columns) and "bad" (rows with errors or warnings and the corresponding result columns) and an optional Observation which tracks data quality summary metrics. Summary metrics are returned by any `DQEngine` with an `observer` specified.

**Raises**:

* `InvalidCheckError` - If any of the checks are invalid.

### validate\_checks[​](#validate_checks "Direct link to validate_checks")

```python
@staticmethod
def validate_checks(
    checks: list[dict],
    custom_check_functions: dict[str, Callable] | None = None,
    validate_custom_check_functions: bool = True,
    semantic_validation_mode: str | None = ChecksSemanticValidationMode.WARN
) -> ChecksValidationStatus

```

Validate checks defined as metadata to ensure they conform to the expected structure and types, and are semantically consistent as a ruleset.

Structural validation checks for required keys, callable functions, and correct argument types. Semantic validation detects duplicate rules and similar rules with conflicting arguments (e.g. two is\_in\_range checks on the same column with different thresholds).

**Notes**:

Rules using raw Spark SQL expressions are not deeply inspected during semantic validation — only structured metadata is compared.

**Arguments**:

* `checks` - List of checks to apply to the DataFrame. Each check should be a dictionary.
* `custom_check_functions` - Optional dictionary with custom check functions (e.g., *globals()* of the calling module).
* `validate_custom_check_functions` - If True, validate custom check functions.
* `semantic_validation_mode` - Controls how semantic issues are surfaced. Use *ChecksSemanticValidationMode.WARN* (default) to log warnings, *ChecksSemanticValidationMode.FAIL* to raise on any issue, or *None* to skip semantic validation entirely.

**Returns**:

ChecksValidationStatus indicating the structural validation result.

**Raises**:

* `ValueError` - If semantic\_validation\_mode is FAIL and issues are found.

### get\_invalid[​](#get_invalid "Direct link to get_invalid")

```python
def get_invalid(df: DataFrame) -> DataFrame

```

Return records that violate data quality checks (rows with warnings or errors).

**Arguments**:

* `df` - Input DataFrame.

**Returns**:

DataFrame with rows that have errors or warnings and the corresponding result columns.

### get\_valid[​](#get_valid "Direct link to get_valid")

```python
def get_valid(df: DataFrame) -> DataFrame

```

Return records that do not violate data quality checks (rows with warnings but no errors).

**Arguments**:

* `df` - Input DataFrame.

**Returns**:

DataFrame with warning rows but without the results columns.

### load\_checks\_from\_local\_file[​](#load_checks_from_local_file "Direct link to load_checks_from_local_file")

```python
@staticmethod
def load_checks_from_local_file(
        filepath: str,
        variables: dict[str, VariableValue] | None = None) -> list[dict]

```

Load DQ rules (checks) from a local JSON or YAML file.

The returned checks can be used as input to *apply\_checks\_by\_metadata*.

**Security note:** variable values substituted into **sql\_expression** checks are not sanitized. Callers must ensure that variable values come from trusted sources.

**Arguments**:

* `filepath` - Path to a file containing checks definitions.
* `variables` - Optional mapping of placeholder names to replacement values. Replaces placeholders in all string values of the check definitions before returning.

**Returns**:

List of DQ rules.

### save\_checks\_in\_local\_file[​](#save_checks_in_local_file "Direct link to save_checks_in_local_file")

```python
@staticmethod
def save_checks_in_local_file(checks: list[dict], filepath: str)

```

Save DQ rules (checks) to a local YAML or JSON file.

**Arguments**:

* `checks` - List of DQ rules (checks) to save.
* `filepath` - Path to a file where the checks definitions will be saved.

## DQEngine Objects[​](#dqengine-objects "Direct link to DQEngine Objects")

```python
class DQEngine(DQEngineBase)

```

High-level engine to apply data quality checks and manage IO.

This class delegates core checking logic to *DQEngineCore* while providing helpers to read inputs, persist results, and work with different storage backends for checks.

**Arguments**:

* `workspace_client` - WorkspaceClient instance used to access the Databricks workspace.
* `spark` - Optional SparkSession to use. If not provided, the active session is used.
* `engine` - Optional DQEngineCore instance to use. If not provided, a new instance is created.
* `extra_params` - Optional extra parameters for the engine, such as result column names and run metadata.
* `checks_handler_factory` - Optional factory to create checks storage handlers. If not provided, a default factory is created.
* `config_serializer` - Optional ConfigSerializer instance to use. If not provided, a new instance is created.
* `observer` - Optional DQMetricsObserver for tracking data quality summary metrics.
* `actions` - Optional list of *DQAction* instances or raw action dicts to evaluate after checks are applied in the batch path. Dict entries are deserialized to *DQAction* via *ActionSerializer.from\_dict* at construction time; mixed lists are supported. Requires an *observer* to be provided (actions need observed metrics to evaluate conditions). When provided without an *observer*, raises *InvalidParameterError* at construction time.
* `action_evaluator_factory` - Optional factory callable that receives the list of *DQAction* instances and returns an *ActionEvaluator*. Used to inject a custom or test evaluator. When *None*, the default factory builds a real *ActionEvaluator* with *ActionStateStore*, *SecretResolver*, and *WebhookClient*.
* `action_events_config` - Optional *ActionEventsConfig* (or *LakebaseActionsStorageConfig*) for a persistent action-events table. When provided, the action state store is seeded from it on first use and every fired action is appended to it, so frequency (*HOURLY* / *DAILY*) and *STATUS\_CHANGE* suppression survive engine restarts. When *None*, alert state is in-memory only for the engine's lifetime.

### apply\_checks[​](#apply_checks-1 "Direct link to apply_checks")

```python
@telemetry_logger("engine", "apply_checks")
def apply_checks(
    df: DataFrame,
    checks: list[DQRule],
    ref_dfs: dict[str, DataFrame] | None = None
) -> DataFrame | tuple[DataFrame, Observation]

```

Apply data quality checks to the given DataFrame.

**Arguments**:

* `df` - Input DataFrame to check.
* `checks` - List of checks to apply. Each check must be a *DQRule* instance.
* `ref_dfs` - Optional reference DataFrames to use in the checks.

**Returns**:

A DataFrame with errors and warnings result columns and an optional Observation which tracks data quality summary metrics. Summary metrics are returned by any `DQEngine` with an `observer` specified.

### apply\_checks\_and\_split[​](#apply_checks_and_split-1 "Direct link to apply_checks_and_split")

```python
@telemetry_logger("engine", "apply_checks_and_split")
def apply_checks_and_split(
    df: DataFrame,
    checks: list[DQRule],
    ref_dfs: dict[str, DataFrame] | None = None
) -> tuple[DataFrame, DataFrame] | tuple[DataFrame, DataFrame, Observation]

```

Apply data quality checks to the given DataFrame and split the results into two DataFrames ("good" and "bad").

**Arguments**:

* `df` - Input DataFrame to check.
* `checks` - List of checks to apply. Each check must be a *DQRule* instance.
* `ref_dfs` - Optional reference DataFrames to use in the checks.

**Returns**:

A tuple of two DataFrames: "good" (may include rows with warnings but no result columns) and "bad" (rows with errors or warnings and the corresponding result columns) and an optional Observation which tracks data quality summary metrics. Summary metrics are returned by any `DQEngine` with an `observer` specified.

**Raises**:

* `InvalidCheckError` - If any of the checks are invalid.

### apply\_checks\_by\_metadata[​](#apply_checks_by_metadata-1 "Direct link to apply_checks_by_metadata")

```python
@telemetry_logger("engine", "apply_checks_by_metadata")
def apply_checks_by_metadata(
    df: DataFrame,
    checks: list[dict],
    custom_check_functions: dict[str, Callable] | None = None,
    ref_dfs: dict[str, DataFrame] | None = None
) -> DataFrame | tuple[DataFrame, Observation]

```

Apply data quality checks defined as metadata to the given DataFrame.

**Arguments**:

* `df` - Input DataFrame to check.

* `checks` - List of dictionaries describing checks. Each check dictionary must contain the following:

  <!-- -->

  * *check* - A check definition including check function and arguments to use.
  * *name* - Optional name for the resulting column. Auto-generated if not provided.
  * *criticality* - Optional; either *error* (rows go only to the "bad" DataFrame) or *warn* (rows appear in both DataFrames).

* `custom_check_functions` - Optional dictionary with custom check functions (e.g., *globals()* of the calling module).

* `ref_dfs` - Optional reference DataFrames to use in the checks.

**Returns**:

A DataFrame with errors and warnings result columns and an optional Observation which tracks data quality summary metrics. Summary metrics are returned by any `DQEngine` with an `observer` specified.

### apply\_checks\_by\_metadata\_and\_split[​](#apply_checks_by_metadata_and_split-1 "Direct link to apply_checks_by_metadata_and_split")

```python
@telemetry_logger("engine", "apply_checks_by_metadata_and_split")
def apply_checks_by_metadata_and_split(
    df: DataFrame,
    checks: list[dict],
    custom_check_functions: dict[str, Callable] | None = None,
    ref_dfs: dict[str, DataFrame] | None = None
) -> tuple[DataFrame, DataFrame] | tuple[DataFrame, DataFrame, Observation]

```

Apply data quality checks defined as metadata to the given DataFrame and split the results into two DataFrames ("good" and "bad").

**Arguments**:

* `df` - Input DataFrame to check.

* `checks` - List of dictionaries describing checks. Each check dictionary must contain the following:

  <!-- -->

  * *check* - A check definition including check function and arguments to use.
  * *name* - Optional name for the resulting column. Auto-generated if not provided.
  * *criticality* - Optional; either *error* (rows go only to the "bad" DataFrame) or *warn* (rows appear in both DataFrames).

* `custom_check_functions` - Optional dictionary with custom check functions (e.g., *globals()* of the calling module).

* `ref_dfs` - Optional reference DataFrames to use in the checks.

**Returns**:

A tuple of two DataFrames: "good" (may include rows with warnings but no result columns) and "bad" (rows with errors or warnings and the corresponding result columns) and an optional Observation which tracks data quality summary metrics. Summary metrics are returned by any `DQEngine` with an `observer` specified.

### evaluate\_actions[​](#evaluate_actions "Direct link to evaluate_actions")

```python
@telemetry_logger("engine", "evaluate_actions")
def evaluate_actions(
        observed_metrics: dict[str, object],
        *,
        input_location: str | None = None,
        output_location: str | None = None,
        quarantine_location: str | None = None,
        checks_location: str | None = None,
        rule_set_fingerprint: str | None = None) -> list[ActionResult]

```

Evaluate all configured actions against the observed metrics from the latest batch run.

Builds an *ActionContext* from *observed\_metrics* plus the engine's run metadata and the supplied location hints, then delegates to the lazily constructed *ActionEvaluator*.

When no actions are configured this is a no-op that returns an empty list immediately.

Note: action evaluation requires that the engine was constructed with an *observer* and that *observed\_metrics* was obtained from a triggered Spark *Observation*. If the *batch\_observation* was never triggered (e.g. no output table was written and the metrics-only path was skipped), the metrics will be empty and actions will not receive meaningful values — avoid calling *evaluate\_actions* in that case.

**Arguments**:

* `observed_metrics` - Mapping of metric name to value collected from a Spark *Observation*.
* `input_location` - Source path/URI of the data being checked, or *None*.
* `output_location` - Destination path/URI of checked output, or *None*.
* `quarantine_location` - Path/URI where quarantined rows are written, or *None*.
* `checks_location` - Path/URI of the checks definition file, or *None*.
* `rule_set_fingerprint` - Fingerprint of the rule set applied, or *None*.

**Returns**:

List of *ActionResult* instances for every action that actually fired. Returns an empty list when no actions are configured.

**Raises**:

* `PipelineFailedError` - When a *FailPipeline* action's condition is met, after all other actions have been evaluated and their notifications delivered.

### apply\_checks\_and\_save\_in\_table[​](#apply_checks_and_save_in_table "Direct link to apply_checks_and_save_in_table")

```python
@telemetry_logger("engine", "apply_checks_and_save_in_table")
def apply_checks_and_save_in_table(input_config: InputConfig,
                                   output_config: OutputConfig | None = None,
                                   checks: list[DQRule] | None = None,
                                   quarantine_config: OutputConfig
                                   | None = None,
                                   metrics_config: OutputConfig | None = None,
                                   ref_dfs: dict[str, DataFrame] | None = None,
                                   checks_location: str | None = None,
                                   run_config_name: str = "default") -> None

```

Apply data quality checks to input data and save results.

If *quarantine\_config* is provided, the data is split into valid and invalid records:

* valid records are written using *output\_config* (skipped when *output\_config* is not provided).
* invalid records are written using *quarantine\_config*.

If *quarantine\_config* is not provided and *output\_config* is provided, all rows (including result columns) are written using *output\_config*.

If *metrics\_config* is provided and the `DQEngine` has a valid `observer`, data quality summary metrics are tracked and written using *metrics\_config*.

**Arguments**:

* `input_config` - Input configuration (e.g., table/view or file location and read options).

* `output_config` - Output configuration (e.g., table name, mode, and write options). Optional when *quarantine\_config* is provided, in which case valid records are not written, or when only *metrics\_config* is provided for batch summary metrics.

* `checks` - Optional list of *DQRule* checks to apply. If not provided, checks\_location must be provided.

* `quarantine_config` - Optional configuration for writing invalid records.

* `metrics_config` - Optional configuration for writing summary metrics.

* `ref_dfs` - Optional reference DataFrames used by checks.

* `checks_location` - Optional location of the checks. At least one of the parameters 'checks' or 'checks\_location' must be provided.

  <!-- -->

  * If 'checks' parameter is provided, it is only used for reporting purposes
  * If 'checks' parameter is not provided, it is used for loading checks from the storage.

* `run_config_name` - Name of the run configuration to use when loading checks from a table.

**Raises**:

* `observer`0 - If both *checks* and *checks\_location* are not specified, if none of *output\_config*, *quarantine\_config*, and *metrics\_config* are specified, if *metrics\_config* is provided while the engine has no observer to collect metrics, or if metrics-only is requested for streaming.

### apply\_checks\_by\_metadata\_and\_save\_in\_table[​](#apply_checks_by_metadata_and_save_in_table "Direct link to apply_checks_by_metadata_and_save_in_table")

```python
@telemetry_logger("engine", "apply_checks_by_metadata_and_save_in_table")
def apply_checks_by_metadata_and_save_in_table(
        input_config: InputConfig,
        output_config: OutputConfig | None = None,
        checks: list[dict] | None = None,
        quarantine_config: OutputConfig | None = None,
        metrics_config: OutputConfig | None = None,
        custom_check_functions: dict[str, Callable] | None = None,
        ref_dfs: dict[str, DataFrame] | None = None,
        checks_location: str | None = None,
        run_config_name: str = "default") -> None

```

Apply metadata-defined data quality checks to input data and save results.

If *quarantine\_config* is provided, the data is split into valid and invalid records:

* valid records are written using *output\_config* (skipped when *output\_config* is not provided).
* invalid records are written using *quarantine\_config*.

If *quarantine\_config* is not provided and *output\_config* is provided, all rows (including result columns) are written using *output\_config*.

If *metrics\_config* is provided and the `DQEngine` has a valid `observer`, data quality summary metrics are tracked and written using *metrics\_config*.

**Arguments**:

* `input_config` - Input configuration (e.g., table/view or file location and read options).

* `output_config` - Output configuration (e.g., table name, mode, and write options). Optional when *quarantine\_config* is provided, in which case valid records are not written, or when only *metrics\_config* is provided for batch summary metrics.

* `checks` - Optional list of dicts containing checks to apply. If not provided, checks\_location must be provided. Each check dictionary must contain the following:

  <!-- -->

  * *check* - A check definition including check function and arguments to use.
  * *name* - Optional name for the resulting column. Auto-generated if not provided.
  * *criticality* - Optional; either *error* (rows go only to the "bad" DataFrame) or *warn* (rows appear in both DataFrames).

* `quarantine_config` - Optional configuration for writing invalid records.

* `metrics_config` - Optional configuration for writing summary metrics.

* `custom_check_functions` - Optional mapping of custom check function names to callables/modules (e.g., globals()).

* `ref_dfs` - Optional reference DataFrames used by checks.

* `checks_location` - Optional location of the checks. At least one of the parameters 'checks' or 'checks\_location' must be provided.

  <!-- -->

  * If 'checks' param is provided, the parameter is only used for reporting purposes.
  * If 'checks' param is not provided, the parameter is used for loading checks from the storage.

* `observer`0 - Name of the run configuration to use when loading checks from a table.

**Raises**:

* `observer`1 - If both *checks* and *checks\_location* are not specified, if none of *output\_config*, *quarantine\_config*, and *metrics\_config* are specified, if *metrics\_config* is provided while the engine has no observer to collect metrics, or if metrics-only is requested for streaming.

### apply\_checks\_and\_save\_in\_tables[​](#apply_checks_and_save_in_tables "Direct link to apply_checks_and_save_in_tables")

```python
@telemetry_logger("engine", "apply_checks_and_save_in_tables")
def apply_checks_and_save_in_tables(
    run_configs: list[RunConfig], max_parallelism: int | None = os.cpu_count()
) -> None

```

Apply data quality checks to multiple tables or views and write the results to output and/or metrics table(s).

If quarantine tables are provided in the run configuration, the data will be split into good and bad records, with good records written to the output table and bad records to the quarantine table. If quarantine tables are not provided and output tables are provided, all records (with error/warning columns) will be written to the output table. If only metrics tables are provided, only summary metrics will be written.

**Arguments**:

* `run_configs` *list\[RunConfig]* - List of run configurations containing input configs, output configs, quarantine configs, metrics configs, and a checks file location.
* `max_parallelism` *int, optional* - Maximum number of tables to check in parallel. Defaults to the number of CPU cores.

**Returns**:

None

### apply\_checks\_and\_save\_in\_tables\_for\_patterns[​](#apply_checks_and_save_in_tables_for_patterns "Direct link to apply_checks_and_save_in_tables_for_patterns")

```python
@telemetry_logger("engine", "apply_checks_and_save_in_tables_for_patterns")
def apply_checks_and_save_in_tables_for_patterns(
        patterns: list[str],
        checks_location: str,
        exclude_patterns: list[str] | None = None,
        exclude_matched: bool = False,
        run_config_template: RunConfig = RunConfig(),
        max_parallelism: int | None = os.cpu_count(),
        output_table_suffix: str = "_dq_output",
        quarantine_table_suffix: str = "_dq_quarantine") -> None

```

Apply data quality checks to tables or views matching a pattern and write the results to output table(s).

If quarantine option is enabled the data is split into good and bad records, with good records written to the output table (under the same name as input table and "\_dq" suffix) and bad records to the quarantine table (under the same name as input table and "\_quarantine" suffix). When *output\_config* is omitted on the template and *quarantine\_config* is provided, valid records are not written and only the quarantine table is produced per matched table. When only *metrics\_config* is provided on the template, only summary metrics are written. If quarantine is not enabled and metrics-only is not configured, all records (with error/warning columns) will be written to the output table.

Checks are expected to be available under the same name as the table, with a .yml extension.

**Arguments**:

* `patterns` - List of table names or filesystem-style wildcards (e.g. 'schema.\*') to include. If None, all tables are included. By default, tables matching the pattern are included.
* `checks_location` - Location of the checks files (e.g., absolute workspace or volume directory, or delta table). For file based locations, checks are expected to be found under checks\_location/table\_name.yml.
* `exclude_matched` *bool* - Specifies whether to include tables matched by the pattern. If True, matched tables are excluded. If False, matched tables are included.
* `exclude_patterns` - List of table names or filesystem-style wildcards to exclude. If None, no tables are excluded.
* `run_config_template` - Run configuration template to use for all tables. Skip location in the input\_config, output\_config, and quarantine\_config as it is derived from patterns. Skip checks\_location of the run config as it is derived separately. Autogenerate input\_config and output\_config if not provided.
* `max_parallelism` *int* - Maximum number of tables to check in parallel.
* `output_table_suffix` - Suffix to append to the original table name for the output table.
* `quarantine_table_suffix` - Suffix to append to the original table name for the quarantine table.

**Returns**:

None

### validate\_checks[​](#validate_checks-1 "Direct link to validate_checks")

```python
@staticmethod
def validate_checks(
    checks: list[dict],
    custom_check_functions: dict[str, Callable] | None = None,
    validate_custom_check_functions: bool = True,
    semantic_validation_mode: str | None = ChecksSemanticValidationMode.WARN
) -> ChecksValidationStatus

```

Validate checks defined as metadata to ensure they conform to the expected structure and types.

This method validates the presence of required keys, the existence and callability of functions, and the types of arguments passed to those functions. It also runs semantic validation across the ruleset to detect duplicate and conflicting rules.

**Arguments**:

* `checks` - List of checks to apply to the DataFrame. Each check should be a dictionary.
* `custom_check_functions` - Optional dictionary with custom check functions (e.g., *globals()* of the calling module).
* `validate_custom_check_functions` - If True, validate custom check functions.
* `semantic_validation_mode` - Controls how semantic issues are surfaced. Use *ChecksSemanticValidationMode.WARN* (default) to log warnings, *ChecksSemanticValidationMode.FAIL* to raise on any issue, or *None* to skip semantic validation entirely.

**Returns**:

ChecksValidationStatus indicating the validation result.

**Raises**:

* `ValueError` - If semantic\_validation\_mode is FAIL and issues are found.

### get\_invalid[​](#get_invalid-1 "Direct link to get_invalid")

```python
def get_invalid(df: DataFrame) -> DataFrame

```

Return records that violate data quality checks (rows with warnings or errors).

**Arguments**:

* `df` - Input DataFrame.

**Returns**:

DataFrame with rows that have errors or warnings and the corresponding result columns.

### get\_valid[​](#get_valid-1 "Direct link to get_valid")

```python
def get_valid(df: DataFrame) -> DataFrame

```

Return records that do not violate data quality checks (rows with warnings but no errors).

**Arguments**:

* `df` - Input DataFrame.

**Returns**:

DataFrame with warning rows but without the results columns.

### save\_results\_in\_table[​](#save_results_in_table "Direct link to save_results_in_table")

```python
@telemetry_logger("engine", "save_results_in_table")
def save_results_in_table(output_df: DataFrame | None = None,
                          quarantine_df: DataFrame | None = None,
                          observation: Observation | None = None,
                          output_config: OutputConfig | None = None,
                          quarantine_config: OutputConfig | None = None,
                          metrics_config: OutputConfig | None = None,
                          run_config_name: str | None = "default",
                          product_name: str = "dqx",
                          assume_user: bool = True,
                          install_folder: str | None = None,
                          rule_set_fingerprint: str | None = None)

```

Persist result DataFrames using explicit configs or the named run configuration.

Behavior:

* If *output\_df* is provided and *output\_config* is None, load the run config and use its *output\_config*.
* If *quarantine\_df* is provided and *quarantine\_config* is None, load the run config and use its *quarantine\_config*.
* If *observation* is provided and *metrics\_config* is None, load the run config and use its *metrics\_config*
* A write occurs only when both a DataFrame and its corresponding config are available.
* If only *observation* and *metrics\_config* are provided, only summary metrics are written.

**Arguments**:

* `output_df` - DataFrame with valid rows to be saved (optional).
* `quarantine_df` - DataFrame with invalid rows to be saved (optional).
* `observation` - Spark Observation with data quality summary metrics (optional). Supported for batch only. Requires run\_config\_name or metrics\_config to be provided.
* `output_config` - Configuration describing where/how to write the valid rows. If omitted, falls back to the run config (requires run\_config\_name).
* `quarantine_config` - Configuration describing where/how to write the invalid rows (optional). If omitted, falls back to the run config (requires run\_config\_name).
* `metrics_config` - Configuration describing where/how to write the summary metrics (optional). If omitted, falls back to the run config (requires run\_config\_name).
* `run_config_name` - Name of the run configuration to load when a config parameter is omitted, e.g. input table or job name (use "default" if not provided).
* `product_name` - Product/installation identifier used to resolve installation paths for config loading in install\_folder is not provided (use "dqx" if not provided).
* `assume_user` - Whether to assume a per-user installation when loading the run configuration (use *True* if not provided, skipped if install\_folder is provided).
* `install_folder` - Custom workspace installation folder. Required if DQX is installed in a custom folder.
* `quarantine_df`0 - Optional SHA-256 fingerprint of the rule set used. Included in summary metrics when metrics\_config is provided.

**Returns**:

None

### load\_checks[​](#load_checks "Direct link to load_checks")

```python
@telemetry_logger("engine", "load_checks")
def load_checks(
    config: BaseChecksStorageConfig,
    variables: dict[str, VariableValue] | None = None,
    semantic_validation_mode: str | None = ChecksSemanticValidationMode.WARN
) -> list[dict]

```

Load DQ rules (checks) from the storage backend described by *config*.

This method delegates to a storage handler selected by the factory based on the concrete type of *config* and returns the parsed list of checks (as dictionaries) ready for *apply\_checks\_by\_metadata*.

Supported storage configurations include, for example:

* *FileChecksStorageConfig* (local file);
* *WorkspaceFileChecksStorageConfig* (Databricks workspace file);
* *TableChecksStorageConfig* (table-backed storage);
* *LakebaseChecksStorageConfig* (Lakebase table);
* *InstallationChecksStorageConfig* (installation directory);
* *VolumeFileChecksStorageConfig* (Unity Catalog volume file);

Per-call *variables* are merged with engine-level defaults from *ExtraParams.variables* (per-call values take precedence on conflict).

**Security note:** variable values substituted into **sql\_expression** checks are not sanitized. Callers must ensure that variable values come from trusted sources.

**Arguments**:

* `config` - Configuration object describing the storage backend.
* `variables` - Optional mapping of placeholder names to replacement values. Replaces placeholders in all string values of the check definitions before returning.
* `semantic_validation_mode` - Controls semantic validation behavior after loading. Use *ChecksSemanticValidationMode.WARN* (default) to log warnings and continue, *ChecksSemanticValidationMode.FAIL* to raise if issues are found, or *None* to skip semantic validation entirely.

**Returns**:

List of DQ rules (checks) represented as dictionaries.

**Raises**:

* `InvalidConfigError` - If the configuration type is unsupported.
* `ValueError` - If semantic\_validation\_mode is FAIL and issues are found.

### save\_checks[​](#save_checks "Direct link to save_checks")

```python
@telemetry_logger("engine", "save_checks")
def save_checks(
    checks: list[dict],
    config: BaseChecksStorageConfig,
    variables: dict[str, VariableValue] | None = None,
    semantic_validation_mode: str | None = ChecksSemanticValidationMode.WARN
) -> None

```

Persist DQ rules (checks) to the storage backend described by *config*.

The appropriate storage handler is resolved from the configuration type and used to write the provided checks. Any write semantics (e.g., append/overwrite) are controlled by fields on *config* such as *mode* where applicable.

Supported storage configurations include, for example:

* *FileChecksStorageConfig* (local file);
* *WorkspaceFileChecksStorageConfig* (Databricks workspace file);
* *TableChecksStorageConfig* (table-backed storage);
* *LakebaseChecksStorageConfig* (Lakebase table);
* *InstallationChecksStorageConfig* (installation directory);
* *VolumeFileChecksStorageConfig* (Unity Catalog volume file);

Per-call *variables* are merged with engine-level defaults from *ExtraParams.variables* (per-call values take precedence on conflict). Variables are resolved before computing fingerprints and persisting, ensuring that stored checks and their fingerprints are consistent.

**Arguments**:

* `checks` - List of DQ rules (checks) to save (as dictionaries).
* `config` - Configuration object describing the storage backend and write options.
* `variables` - Optional mapping of placeholder names to replacement values. Replaces placeholders in all string values of the check definitions before saving.
* `semantic_validation_mode` - Controls semantic validation behavior before saving. Use *ChecksSemanticValidationMode.WARN* (default) to log warnings and continue, *ChecksSemanticValidationMode.FAIL* to abort saving if issues are found, or *None* to skip semantic validation entirely.

**Returns**:

None

**Raises**:

* `InvalidConfigError` - If the configuration type is unsupported.
* `ValueError` - If semantic\_validation\_mode is FAIL and issues are found.

### compute\_summary\_metrics[​](#compute_summary_metrics "Direct link to compute_summary_metrics")

```python
@telemetry_logger("engine", "compute_summary_metrics")
def compute_summary_metrics(checked_df: DataFrame,
                            checks: list[dict] | None = None,
                            custom_check_functions: dict[str, Callable]
                            | None = None,
                            input_config: InputConfig | None = None,
                            output_config: OutputConfig | None = None,
                            quarantine_config: OutputConfig | None = None,
                            checks_location: str | None = None,
                            run_config_name: str = "default") -> DataFrame

```

Compute data quality summary metrics from a checked DataFrame by aggregation.

Unlike the observer/listener path (which relies on Spark *observe()* and a caller-triggered action), this computes the same metrics as a plain aggregation over the result columns and returns a lazy DataFrame. This makes it usable inside Spark Declarative Pipelines (SDP / Lakeflow / DLT), where the pipeline runtime — not the caller — owns the write action: define a downstream materialized view over the checked table that returns the result of this method.

**Arguments**:

* `checked_df` - DataFrame produced by *apply\_checks* / *apply\_checks\_by\_metadata* (must still contain the DQX result columns, i.e. before *get\_valid* / *get\_invalid* drop them).
* `checks` - Optional metadata checks that were applied (the same list of dicts passed to *apply\_checks\_by\_metadata*). When provided, a per-check breakdown (*check\_metrics*) is included covering every applied check, including checks with zero violations. The breakdown is derived from the check names and cannot be reconstructed from data alone, so pass the same checks used when applying. When omitted (and no *checks\_location* is given), only dataset-level metrics (row counts and any observer custom metrics) are produced.
* `custom_check_functions` - Optional custom check functions used to resolve metadata checks. Pass the *same* functions that were used when the checks were applied — if the applied checks referenced a custom function and it is not supplied here, deserialization fails or resolves a different check name, so the *check\_metrics* breakdown and *rule\_set\_fingerprint* would not match the applied run.
* `input_config` - Optional input configuration recorded in the metrics for traceability.
* `output_config` - Optional output configuration recorded in the metrics for traceability.
* `quarantine_config` - Optional quarantine configuration recorded in the metrics for traceability.
* `checks_location` - Optional checks location. Recorded in the metrics for traceability, and — when *checks* is not passed — the checks are loaded from here so the per-check breakdown and *rule\_set\_fingerprint* are still produced.
* `run_config_name` - Name of the run configuration to use when loading checks from a table (only used when *checks* is None and *checks\_location* points to a table).

**Notes**:

A *DQMetricsObserver* must be configured on this engine (*DQEngine(..., observer=...)*); its *custom\_metrics* (if any) are included alongside the built-in dataset-level metrics and the per-check breakdown (when *checks* is provided or loaded from *checks\_location*).

**Returns**:

A lazy DataFrame matching *OBSERVATION\_TABLE\_SCHEMA* with one row per metric.

**Raises**:

* `InvalidParameterError` - If no *DQMetricsObserver* is configured on the engine, or if *checked\_df* does not contain the DQX result columns.

### save\_summary\_metrics[​](#save_summary_metrics "Direct link to save_summary_metrics")

```python
@telemetry_logger("engine", "save_summary_metrics")
def save_summary_metrics(observed_metrics: dict[str, Any],
                         metrics_config: OutputConfig,
                         input_config: InputConfig | None = None,
                         output_config: OutputConfig | None = None,
                         quarantine_config: OutputConfig | None = None,
                         checks_location: str | None = None,
                         rule_set_fingerprint: str | None = None) -> None

```

Save data quality summary metrics to a table.

This method extracts observed metrics from a Spark Observation and persists them to a configured output destination.

**Arguments**:

* `observed_metrics` - Collected summary metrics from Spark Observation.
* `metrics_config` - Output configuration specifying where to save the metrics (table name, mode, options).
* `input_config` - Optional input configuration with source data location (included in metrics for traceability).
* `output_config` - Optional output configuration with valid records location (included in metrics for traceability).
* `quarantine_config` - Optional quarantine configuration with invalid records location (included in metrics for traceability).
* `checks_location` - Location of the checks files (e.g., absolute workspace or volume directory, or delta table).
* `rule_set_fingerprint` - Optional SHA-256 fingerprint of the rule set used for this run. Enables correlation with checks storage and filtering metrics by rule set version.

**Notes**:

The observation must have been triggered by an action (e.g., count(), write()) on the observed DataFrame before calling this method, otherwise observation.get will be empty. This method is only supported by spark batch. Spark query listener must be used for streaming: For streaming use spark.streams.addListener(get\_streaming\_metrics\_listener(..))

### get\_streaming\_metrics\_listener[​](#get_streaming_metrics_listener "Direct link to get_streaming_metrics_listener")

```python
@telemetry_logger("engine", "get_streaming_metrics_listener")
def get_streaming_metrics_listener(
        metrics_config: OutputConfig | None = None,
        input_config: InputConfig | None = None,
        output_config: OutputConfig | None = None,
        quarantine_config: OutputConfig | None = None,
        checks_location: str | None = None,
        rule_set_fingerprint: str | None = None,
        target_query_id: str | None = None) -> StreamingMetricsListener

```

Gets a `StreamingMetricsListener` object for writing metrics to an output table.

**Arguments**:

* `metrics_config` - Optional configuration for writing summary metrics (table name, mode, options). When *None*, no metrics table is written; the listener still evaluates configured actions per micro-batch.
* `input_config` - Optional configuration for input data containing location.
* `output_config` - Optional configuration for output data containing location.
* `quarantine_config` - Optional configuration for quarantine data containing location.
* `checks_location` - Optional location of the checks files (e.g., absolute workspace or volume directory, or delta table).
* `rule_set_fingerprint` - Optional SHA-256 fingerprint of the rule set used for this run.
* `target_query_id` - Optional query ID of the specific streaming query to monitor. If provided, metrics will be collected only for this query.

**Returns**:

* `StreamingMetricsListener` - Listener object for monitoring and writing streaming metrics.

  Usage: spark.streams.addListener(get\_streaming\_metrics\_listener(..))
