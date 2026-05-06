# databricks.labs.dqx.telemetry

### log\_telemetry[​](#log_telemetry "Direct link to log_telemetry")

```python
def log_telemetry(ws: WorkspaceClient, key: str, value: str) -> None

```

Trace specific telemetry information in the Databricks workspace by setting user agent extra info.

**Arguments**:

* `ws` - WorkspaceClient
* `key` - telemetry key to log
* `value` - telemetry value to log

### telemetry\_logger[​](#telemetry_logger "Direct link to telemetry_logger")

```python
def telemetry_logger(key: str,
                     value: str,
                     workspace_client_attr: str = "ws") -> Callable

```

Decorator to log telemetry for method calls. By default, it expects the decorated method to have "ws" attribute for workspace client.

Usage: @telemetry\_logger("telemetry\_key", "telemetry\_value") # Uses "ws" attribute for workspace client by default @telemetry\_logger("telemetry\_key", "telemetry\_value", "my\_ws\_client") # Custom attribute

**Arguments**:

* `key` - Telemetry key to log
* `value` - Telemetry value to log
* `workspace_client_attr` - Name of the workspace client attribute on the class (defaults to "ws")

### log\_dataframe\_telemetry[​](#log_dataframe_telemetry "Direct link to log_dataframe_telemetry")

```python
def log_dataframe_telemetry(ws: WorkspaceClient, spark: SparkSession,
                            df: DataFrame)

```

Log telemetry information about a Spark DataFrame to the Databricks workspace including:

* List of tables used as inputs (hashed)
* List of file paths used as inputs (hashed, excluding paths from tables)
* Whether the DataFrame is streaming
* Whether running in a Delta Live Tables (DLT) pipeline

This function is designed to never throw exceptions - it will log errors but continue execution to ensure telemetry failures don't break the main application flow.

**Arguments**:

* `ws` - WorkspaceClient
* `spark` - SparkSession
* `df` - DataFrame to analyze

**Returns**:

None

### get\_tables\_from\_spark\_plan[​](#get_tables_from_spark_plan "Direct link to get_tables_from_spark_plan")

```python
def get_tables_from_spark_plan(plan_str: str) -> set[str]

```

Extract table names from the Analyzed Logical Plan section of a Spark execution plan.

This function parses the Analyzed Logical Plan section and identifies table references by finding SubqueryAlias nodes, which Spark uses to represent table references in the logical plan. File-based sources (e.g., Delta files from volumes) and in-memory DataFrames do not create SubqueryAlias nodes and therefore won't be counted as tables.

**Arguments**:

* `plan_str` - The complete Spark execution plan string (from df.explain(True))

**Returns**:

A set of distinct table names found in the plan. Returns empty set if no Analyzed Logical Plan section is found or no tables are referenced.

### get\_paths\_from\_spark\_plan[​](#get_paths_from_spark_plan "Direct link to get_paths_from_spark_plan")

```python
def get_paths_from_spark_plan(plan_str: str,
                              table_names: set[str] | None = None) -> set[str]

```

Extract file paths from the Physical Plan section of a Spark execution plan.

This function parses the Physical Plan section and identifies file path references by finding any \*FileIndex patterns in the Location field (e.g., PreparedDeltaFileIndex, ParquetFileIndex, etc.). These paths represent direct file-based data sources (e.g., files from volumes, DBFS, S3, etc.) that are not registered as tables.

**Arguments**:

* `plan_str` - The complete Spark execution plan string (from df.explain(True))
* `table_names` - Optional set of table names to exclude (paths associated with tables are skipped)

**Returns**:

A set of distinct file paths found in the plan. Returns empty set if no Physical Plan section is found or no paths are referenced.

### is\_dlt\_pipeline[​](#is_dlt_pipeline "Direct link to is_dlt_pipeline")

```python
def is_dlt_pipeline(spark: SparkSession) -> bool

```

Determine if the current Spark session is running within a Databricks Delta Live Tables (DLT) pipeline.

**Arguments**:

* `spark` - The SparkSession to check

**Returns**:

True if running in a DLT pipeline, False otherwise

### get\_spark\_plan\_as\_string[​](#get_spark_plan_as_string "Direct link to get_spark_plan_as_string")

```python
def get_spark_plan_as_string(df: DataFrame) -> str

```

Retrieve the Spark execution plan as a string by capturing df.explain() output.

This function temporarily redirects stdout to capture the output of df.explain(True), which prints the detailed execution plan including the Analyzed Logical Plan.

**Arguments**:

* `df` - The Spark DataFrame to get the execution plan from

**Returns**:

The complete execution plan as a string, or empty string if explain() fails
