import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.streaming import listener
from databricks.labs.dqx.config import OutputConfig
from databricks.labs.dqx.metrics_observer import DQMetricsObservation, DQMetricsObserver
from databricks.labs.dqx.utils import save_dataframe_as_table


logger = logging.getLogger(__name__)


class StreamingMetricsListener(listener.StreamingQueryListener):
    """
    Implements a Spark `StreamingQueryListener` for writing data quality summary metrics to an output destination. See
    the [Spark documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.StreamingQueryListener.html)
    for detailed information about `StreamingQueryListener`.

    Args:
        metrics_config: Output configuration used for writing data quality summary metrics
        metrics_observation: `DQMetricsObservation` with data quality summary information
        spark: `SparkSession` for writing summary metrics
    """

    metrics_config: OutputConfig
    metrics_observation: DQMetricsObservation
    spark: SparkSession

    def __init__(
        self, metrics_config: OutputConfig, metrics_observation: DQMetricsObservation, spark: SparkSession
    ) -> None:
        self.metrics_config = metrics_config
        self.metrics_observation = metrics_observation
        self.spark = spark

    def onQueryStarted(self, event: listener.QueryStartedEvent) -> None:
        """
        Writes a message to the standard output logs when a streaming query starts.

        Args:
            event: A `QueryStartedEvent` with details about the streaming query
        """
        logger.debug(f"Streaming query '{event.name}' for summary metrics started run ID '{event.runId}'")

    def onQueryProgress(self, event: listener.QueryProgressEvent) -> None:
        """
        Writes the custom metrics from the DQObserver to the output destination.

        Args:
            event: A `QueryProgressEvent` with details about the last processed micro-batch
        """
        observed_metrics = event.progress.observedMetrics.get(self.metrics_observation.observer_name)
        if not observed_metrics:
            return

        metrics_observation = DQMetricsObservation(
            observer_name=self.metrics_observation.observer_name,
            observed_metrics=observed_metrics.asDict(),
            run_time=datetime.fromisoformat(event.progress.timestamp),
            error_column_name=self.metrics_observation.error_column_name,
            warning_column_name=self.metrics_observation.warning_column_name,
            input_location=self.metrics_observation.input_location,
            output_location=self.metrics_observation.output_location,
            quarantine_location=self.metrics_observation.quarantine_location,
            checks_location=self.metrics_observation.checks_location,
            user_metadata=self.metrics_observation.user_metadata,
        )
        metrics_df = DQMetricsObserver.build_metrics_df(self.spark, metrics_observation)
        save_dataframe_as_table(metrics_df, self.metrics_config)

    def onQueryIdle(self, event: listener.QueryIdleEvent) -> None:
        """
        Writes a message to the standard output logs when a streaming query is idle.

        Args:
            event: A `QueryIdleEvent` with details about the streaming query
        """
        logger.debug(f"Streaming query run '{event.runId}' for summary metrics was reported idle")

    def onQueryTerminated(self, event: listener.QueryTerminatedEvent) -> None:
        """
        Writes a message to the standard output logs when a streaming query stops due to cancellation or failure.

        Args:
            event: A `QueryTerminatedEvent` with details about the streaming query
        """
        if event.exception:
            logger.debug(
                f"Streaming query run '{event.runId}' for summary metrics failed with error: {event.exception}"
            )

        logger.debug(f"Streaming query run '{event.runId}' for summary metrics stopped")
