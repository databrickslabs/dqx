# databricks.labs.dqx.base

## DQEngineBase Objects[​](#dqenginebase-objects "Direct link to DQEngineBase Objects")

```python
class DQEngineBase(abc.ABC)

```

### ws[​](#ws "Direct link to ws")

```python
@cached_property
def ws() -> WorkspaceClient

```

Return a verified *WorkspaceClient* configured for DQX.

Ensures workspace connectivity and sets the product info used for telemetry so that requests are attributed to *dqx*.

## DQEngineCoreBase Objects[​](#dqenginecorebase-objects "Direct link to DQEngineCoreBase Objects")

```python
class DQEngineCoreBase(DQEngineBase)

```

### apply\_checks[​](#apply_checks "Direct link to apply_checks")

```python
@abc.abstractmethod
def apply_checks(
    df: DataFrame,
    checks: list[DQRule],
    ref_dfs: dict[str, DataFrame] | None = None
) -> DataFrame | tuple[DataFrame, Observation]

```

Apply data quality checks to the given DataFrame.

**Arguments**:

* `df` - Input DataFrame to check.
* `checks` - List of checks to apply to the DataFrame. Each check must be a *DQRule* instance.
* `ref_dfs` - Optional reference DataFrames to use in the checks.

**Returns**:

A DataFrame with errors and warnings result columns and an optional Observation which tracks data quality summary metrics. Summary metrics are returned by any `DQEngine` with an `observer` specified.

### apply\_checks\_and\_split[​](#apply_checks_and_split "Direct link to apply_checks_and_split")

```python
@abc.abstractmethod
def apply_checks_and_split(
    df: DataFrame,
    checks: list[DQRule],
    ref_dfs: dict[str, DataFrame] | None = None
) -> tuple[DataFrame, DataFrame] | tuple[DataFrame, DataFrame, Observation]

```

Apply data quality checks to the given DataFrame and split the results into two DataFrames ("good" and "bad").

**Arguments**:

* `df` - Input DataFrame to check.
* `checks` - List of checks to apply to the DataFrame. Each check must be a *DQRule* instance.
* `ref_dfs` - Optional reference DataFrames to use in the checks.

**Returns**:

A tuple of two DataFrames: "good" (may include rows with warnings but no result columns) and "bad" (rows with errors or warnings and the corresponding result columns) and an optional Observation which tracks data quality summary metrics. Summary metrics are returned by any `DQEngine` with an `observer` specified.

### apply\_checks\_by\_metadata[​](#apply_checks_by_metadata "Direct link to apply_checks_by_metadata")

```python
@abc.abstractmethod
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
@abc.abstractmethod
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

### validate\_checks[​](#validate_checks "Direct link to validate_checks")

```python
@staticmethod
@abc.abstractmethod
def validate_checks(
        checks: list[dict],
        custom_check_functions: dict[str, Callable] | None = None,
        validate_custom_check_functions: bool = True
) -> ChecksValidationStatus

```

Validate checks defined as metadata to ensure they conform to the expected structure and types.

This method validates the presence of required keys, the existence and callability of functions, and the types of arguments passed to those functions.

**Arguments**:

* `checks` - List of checks to apply to the DataFrame. Each check should be a dictionary.
* `custom_check_functions` - Optional dictionary with custom check functions (e.g., *globals()* of the calling module).
* `validate_custom_check_functions` - If True, validate custom check functions.

**Returns**:

ChecksValidationStatus indicating the validation result.

### get\_invalid[​](#get_invalid "Direct link to get_invalid")

```python
@abc.abstractmethod
def get_invalid(df: DataFrame) -> DataFrame

```

Return records that violate data quality checks (rows with warnings or errors).

**Arguments**:

* `df` - Input DataFrame.

**Returns**:

DataFrame with rows that have errors or warnings and the corresponding result columns.

### get\_valid[​](#get_valid "Direct link to get_valid")

```python
@abc.abstractmethod
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
@abc.abstractmethod
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

List of DQ rules (checks).

### save\_checks\_in\_local\_file[​](#save_checks_in_local_file "Direct link to save_checks_in_local_file")

```python
@staticmethod
@abc.abstractmethod
def save_checks_in_local_file(checks: list[dict], filepath: str)

```

Save DQ rules (checks) to a local YAML or JSON file.

**Arguments**:

* `checks` - List of DQ rules (checks) to save.
* `filepath` - Path to a file where the checks definitions will be saved.
