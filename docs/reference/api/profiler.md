# databricks.labs.dqx.profiler.profiler

## DQProfiler Objects[​](#dqprofiler-objects "Direct link to DQProfiler Objects")

```python
class DQProfiler(DQEngineBase)

```

Data Quality Profiler class to profile input data.

### get\_columns\_or\_fields[​](#get_columns_or_fields "Direct link to get_columns_or_fields")

```python
@staticmethod
def get_columns_or_fields(columns: list[T.StructField]) -> list[T.StructField]

```

Extracts all fields from a list of StructField objects, including nested fields from StructType columns.

**Arguments**:

* `columns` - A list of StructField objects to process.

**Returns**:

A list of StructField objects, including nested fields with prefixed names.

### profile[​](#profile "Direct link to profile")

```python
@telemetry_logger("profiler", "profile")
def profile(
    df: DataFrame,
    columns: list[str] | None = None,
    options: dict[str, Any] | None = None
) -> tuple[dict[str, Any], list[DQProfile]]

```

Profiles a DataFrame to generate summary statistics and data quality rules.

**Arguments**:

* `df` - The DataFrame to profile.
* `columns` - An optional list of column names to include in the profile. If None, all columns are included.
* `options` - An optional dictionary of options for profiling.

**Returns**:

A tuple containing a dictionary of summary statistics and a list of data quality profiles.

### profile\_table[​](#profile_table "Direct link to profile_table")

```python
@telemetry_logger("profiler", "profile_table")
def profile_table(
    input_config: InputConfig,
    columns: list[str] | None = None,
    options: dict[str, Any] | None = None
) -> tuple[dict[str, Any], list[DQProfile]]

```

Profiles a table to generate summary statistics and data quality rules.

**Arguments**:

* `input_config` - Input configuration containing the table location.
* `columns` - An optional list of column names to include in the profile. If None, all columns are included.
* `options` - An optional dictionary of options for profiling.

**Returns**:

A tuple containing a dictionary of summary statistics and a list of data quality profiles.

### profile\_tables\_for\_patterns[​](#profile_tables_for_patterns "Direct link to profile_tables_for_patterns")

```python
@telemetry_logger("profiler", "profile_tables_for_patterns")
def profile_tables_for_patterns(
    patterns: list[str] | None = None,
    exclude_patterns: list[str] | None = None,
    exclude_matched: bool = False,
    columns: dict[str, list[str]] | None = None,
    options: list[dict[str, Any]] | None = None,
    max_parallelism: int | None = os.cpu_count()
) -> dict[str, tuple[dict[str, Any], list[DQProfile]]]

```

Profiles Delta tables in Unity Catalog to generate summary statistics and data quality rules.

**Arguments**:

* `patterns` - List of table names or filesystem-style wildcards (e.g. 'schema.\*') to include. If None, all tables are included. By default, tables matching the pattern are included.
* `exclude_patterns` - List of table names or filesystem-style wildcards (e.g. 'schema.\*') to exclude. If None, no tables are excluded.
* `exclude_matched` - Specifies whether to include tables matched by the pattern. If True, matched tables are excluded. If False, matched tables are included.
* `columns` - A dictionary with column names to include in the profile. Keys should be fully-qualified table names (e.g. *catalog.schema.table*) and values should be lists of column names to include in profiling.
* `options` - A dictionary with options for profiling each table. Keys should be fully-qualified table names (e.g. *catalog.schema.table*) and values should be options for profiling.
* `max_parallelism` - An optional concurrency limit for profiling concurrently

**Returns**:

A dictionary mapping table names to tuples containing summary statistics and data quality profiles.

### detect\_primary\_keys\_with\_llm[​](#detect_primary_keys_with_llm "Direct link to detect_primary_keys_with_llm")

```python
@telemetry_logger("profiler", "detect_primary_keys_with_llm")
def detect_primary_keys_with_llm(input_config: InputConfig) -> dict[str, Any]

```

Detects primary keys using LLM-based analysis.

This method analyzes table schema and metadata to identify primary key columns.

**Arguments**:

* `input_config` - Input configuration containing the table location.

**Returns**:

A dictionary containing the primary key detection result with the following keys:

* table: The table name
* success: Whether detection was successful
* primary\_key\_columns: List of detected primary key columns (if successful)
* confidence: Confidence level (high/medium/low)
* reasoning: LLM reasoning for the selection
* has\_duplicates: Whether duplicates were found (if validation performed)
* duplicate\_count: Number of duplicate combinations (if validation performed)
* error: Error message (if failed)
