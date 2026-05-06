# databricks.labs.dqx.metrics\_listener

## StreamingMetricsListener Objects[​](#streamingmetricslistener-objects "Direct link to StreamingMetricsListener Objects")

```python
class StreamingMetricsListener(listener.StreamingQueryListener)

```

Implements a Spark `StreamingQueryListener` for writing data quality summary metrics to an output destination. See the [Spark documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.StreamingQueryListener.html) for detailed information about `StreamingQueryListener`.

**Arguments**:

* `metrics_config` - Output configuration used for writing data quality summary metrics
* `metrics_observation` - `DQMetricsObservation` with data quality summary information
* `spark` - `SparkSession` for writing summary metrics
* `target_query_id` - Optional query ID of the specific streaming query to monitor. If provided, only events from this query will be processed (useful when multiple queries share the same observation).

### onQueryStarted[​](#onquerystarted "Direct link to onQueryStarted")

```python
def onQueryStarted(event: listener.QueryStartedEvent) -> None

```

Writes a message to the standard output logs when a streaming query starts.

**Arguments**:

* `event` - A `QueryStartedEvent` with details about the streaming query

### onQueryProgress[​](#onqueryprogress "Direct link to onQueryProgress")

```python
def onQueryProgress(event: listener.QueryProgressEvent) -> None

```

Writes the custom metrics from the DQMetricsObserver to the output destination.

**Arguments**:

* `event` - A `QueryProgressEvent` with details about the last processed micro-batch

### onQueryIdle[​](#onqueryidle "Direct link to onQueryIdle")

```python
def onQueryIdle(event: listener.QueryIdleEvent) -> None

```

Writes a message to the standard output logs when a streaming query is idle.

**Arguments**:

* `event` - A `QueryIdleEvent` with details about the streaming query

### onQueryTerminated[​](#onqueryterminated "Direct link to onQueryTerminated")

```python
def onQueryTerminated(event: listener.QueryTerminatedEvent) -> None

```

Writes a message to the standard output logs when a streaming query stops due to cancellation or failure.

**Arguments**:

* `event` - A `QueryTerminatedEvent` with details about the streaming query
