# databricks.labs.dqx.io

### read\_input\_data[​](#read_input_data "Direct link to read_input_data")

```python
def read_input_data(spark: SparkSession,
                    input_config: InputConfig) -> DataFrame

```

Reads input data from the specified location and format.

**Arguments**:

* `spark` - SparkSession
* `input_config` - InputConfig with source location/table name, format, and options

**Returns**:

DataFrame with values read from the input data

### save\_dataframe\_as\_table[​](#save_dataframe_as_table "Direct link to save_dataframe_as_table")

```python
def save_dataframe_as_table(
        df: DataFrame, output_config: OutputConfig) -> StreamingQuery | None

```

Saves a DataFrame as a table using a Unity Catalog table reference or storage path.

Supports both batch and streaming writes. For streaming DataFrames, returns a StreamingQuery that can be used by the caller to monitor or wait for completion. For batch DataFrames, data is written synchronously and None is returned.

**Arguments**:

* `df` - The DataFrame to save (batch or streaming)

* `output_config` - Output configuration specifying:

  <!-- -->

  * location: Table name (e.g., 'catalog.schema.table') or storage path (e.g., '/Volumes/...', 's3://...', 'abfss\://...', 'gs\://...')
  * mode: Write mode ('overwrite', 'append', etc.)
  * format: Data format (default: 'delta')
  * options: Additional Spark write options as dict (e.g., "mergeSchema", "overwriteSchema")
  * partition\_by: Optional list of columns to partition by
  * cluster\_by: Optional list of columns to cluster by (Delta Liquid Clustering)
  * trigger: (Streaming only) Trigger configuration dict (e.g., "availableNow", "processingTime")

**Returns**:

StreamingQuery if the DataFrame is streaming, None if the DataFrame is batch

**Raises**:

* `InvalidConfigError` - If the output location format is invalid (must be a 2 or 3-level table namespace or a storage path starting with /, s3:/, abfss\:/, or gs\:/)

**Notes**:

The `cluster_by` method of Spark's `DataStreamWriter` is supported for creating liquid clustered tables as of Databricks Runtime version 16+ (see: <https://docs.databricks.com/aws/en/delta/clustering#create-tables-with-clustering>). Users must manually enable `spark.databricks.delta.liquid.eagerClustering.streaming.enabled` to allow clustering on write for streaming workloads.

### is\_one\_time\_trigger[​](#is_one_time_trigger "Direct link to is_one_time_trigger")

```python
def is_one_time_trigger(trigger: dict[str, Any] | None) -> bool

```

Checks if a trigger is a one-time trigger that should wait for completion.

**Arguments**:

* `trigger` - Trigger configuration dict

**Returns**:

True if the trigger is 'once' or 'availableNow', False otherwise

### get\_reference\_dataframes[​](#get_reference_dataframes "Direct link to get_reference_dataframes")

```python
def get_reference_dataframes(
    spark: SparkSession,
    reference_tables: dict[str, InputConfig] | None = None
) -> dict[str, DataFrame] | None

```

Get reference DataFrames from the provided reference tables configuration.

**Arguments**:

* `spark` - SparkSession
* `reference_tables` - A dictionary mapping of reference table names to their input configurations.

**Examples**:

```text
reference_tables = {
    "reference_table_1": InputConfig(location="db.schema.table1", format="delta"),
    "reference_table_2": InputConfig(location="db.schema.table2", format="delta")
}

```

**Returns**:

A dictionary mapping reference table names to their DataFrames.
