# databricks.labs.dqx.metrics\_observer

## DQMetricsObservation Objects[​](#dqmetricsobservation-objects "Direct link to DQMetricsObservation Objects")

```python
@dataclass(frozen=True)
class DQMetricsObservation()

```

Observer metrics class used to persist summary metrics.

**Arguments**:

* `run_id` - Unique observation id.
* `run_name` - Name of the observations, taken from the engine's observer. None when the engine has no observer configured (e.g. metrics persisted via save\_results\_in\_table without an observer).
* `observed_metrics` - Dictionary of observed metrics.
* `run_time_overwrite` - Run time when the data quality summary metrics were observed. If None, current\_timestamp() is used.
* `error_column_name` - Name of the error column when running quality checks.
* `warning_column_name` - Name of the warning column when running quality checks.
* `input_location` - (optional) Location where input data is loaded from when running quality checks (fully-qualified table name or file path).
* `output_location` - (optional) Location where output data is persisted when running quality checks (fully-qualified table name or file path).
* `quarantine_location` - (optional) Location where quarantined data is persisted when running quality checks (fully-qualified table name or file path).
* `checks_location` - (optional) Location where checks are loaded from when running quality checks (fully-qualified table name or file path).
* `run_name`0 - (optional) SHA-256 fingerprint of the rule set used for this run. Enables correlation with checks storage and filtering metrics by rule set version.

## DQMetricsObserver Objects[​](#dqmetricsobserver-objects "Direct link to DQMetricsObserver Objects")

```python
@dataclass
class DQMetricsObserver()

```

Observation class used to track summary metrics about data quality when validating datasets with DQX

**Arguments**:

* `name` - Name of the observations which will be displayed in listener metrics (default is 'dqx'). Also used as run\_name field when saving the metrics to a table.
* `custom_metrics` - Optional list of SQL expressions defining custom, dataset-level quality metrics

### id[​](#id "Direct link to id")

```python
@cached_property
def id() -> str

```

ID of the observer.

**Returns**:

Unique ID

### get\_metrics[​](#get_metrics "Direct link to get_metrics")

```python
def get_metrics(check_names: list[str] | None = None) -> list[str]

```

Gets the observer metrics as Spark SQL expressions.

**Arguments**:

* `check_names` - Optional list of check names from the applied quality rules. When provided, a per-check breakdown (*check\_metrics*) is included.

**Returns**:

A list of Spark SQL expressions defining the observer metrics (default, per-check, and custom).

### observation[​](#observation "Direct link to observation")

```python
@property
def observation() -> Observation

```

Spark Observation which can be attached to a DataFrame to track summary metrics. Metrics will be collected when the 1st action is triggered on the attached DataFrame. Subsequent operations on the attached DataFrame will not update the observed metrics. See: [PySpark Observation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Observation.html) for complete documentation.

**Returns**:

A Spark Observation instance

### set\_column\_names[​](#set_column_names "Direct link to set_column_names")

```python
def set_column_names(error_column_name: str, warning_column_name: str) -> None

```

Sets the default column names (e.g. *\_errors* and *\_warnings*) for monitoring summary metrics.

**Arguments**:

* `error_column_name` - Error column name
* `warning_column_name` - Warning column name

### build\_metrics\_df[​](#build_metrics_df "Direct link to build_metrics_df")

```python
@staticmethod
def build_metrics_df(spark: SparkSession,
                     observation: DQMetricsObservation) -> DataFrame

```

Builds a Spark DataFrame from a DQMetricsObservation.

**Arguments**:

* `spark` - SparkSession used to create the DataFrame
* `observation` - DQMetricsObservation with summary metrics

**Returns**:

A Spark DataFrame with summary metrics

### build\_metrics\_df\_from\_aggregation[​](#build_metrics_df_from_aggregation "Direct link to build_metrics_df_from_aggregation")

```python
@staticmethod
def build_metrics_df_from_aggregation(
        aggregated_df: DataFrame,
        observation: DQMetricsObservation) -> DataFrame

```

Reshapes a one-row wide aggregation of metric expressions into the long-format *OBSERVATION\_TABLE\_SCHEMA*, without triggering a Spark action.

Used by *DQEngine.compute\_summary\_metrics* to keep a lazily-computed aggregation lazy, so the result can back a materialized view or table in a Spark Declarative Pipeline (where the pipeline runtime — not the caller — triggers the write). The already-collected path uses *build\_metrics\_df*.

The input must be a **single-row global aggregation** (the output of *get\_metrics* selected with no *groupBy*). A multi-row input would emit one metrics row-set per input row, each stamped with the same run metadata and no grouping key, so this must not be used for windowed/grouped aggregations. The single-row property is guaranteed by construction — the only caller (*DQEngine.compute\_summary\_metrics*) feeds *get\_metrics* aggregates selected without *groupBy*, which always yield exactly one row — and is intentionally not enforced with a runtime row-count check: counting the rows would trigger a Spark action and defeat this method's whole purpose of staying lazy so it can back a Spark Declarative Pipeline materialized view.

**Arguments**:

* `aggregated_df` - A single-row DataFrame whose columns are the metric expressions produced by *DQMetricsObserver.get\_metrics* (e.g. *input\_row\_count*, *error\_row\_count*, *check\_metrics*).
* `observation` - *DQMetricsObservation* carrying the run metadata (locations, fingerprint, run time).

**Returns**:

A lazy Spark DataFrame matching *OBSERVATION\_TABLE\_SCHEMA* with one row per metric.
