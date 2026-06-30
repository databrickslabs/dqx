import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.streaming import listener
from databricks.labs.dqx.config import OutputConfig
from databricks.labs.dqx.metrics_observer import DQMetricsObservation, DQMetricsObserver
from databricks.labs.dqx.io import save_dataframe_as_table
from databricks.labs.dqx.actions.base import ActionContext
from databricks.labs.dqx.actions.evaluator import ActionEvaluator
from databricks.labs.dqx.errors import TerminalActionError


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
        target_query_id: Optional query ID of the specific streaming query to monitor. If provided, only events
            from this query will be processed (useful when multiple queries share the same observation).
        action_evaluator: Optional *ActionEvaluator* to invoke after each micro-batch. When provided, actions
            are evaluated per micro-batch using the observed metrics from that batch. *TerminalActionError*
            (including *PipelineFailedError*) propagates out of *onQueryProgress* to abort the stream; all
            other exceptions are logged and swallowed so a single bad alert cannot kill the stream.
    """

    metrics_config: OutputConfig
    metrics_observation: DQMetricsObservation
    spark: SparkSession
    target_query_id: str | None
    action_evaluator: ActionEvaluator | None

    def __init__(
        self,
        metrics_config: OutputConfig,
        metrics_observation: DQMetricsObservation,
        spark: SparkSession,
        target_query_id: str | None = None,
        action_evaluator: ActionEvaluator | None = None,
    ) -> None:
        self.metrics_config = metrics_config
        self.metrics_observation = metrics_observation
        self.spark = spark
        self.target_query_id = target_query_id
        self.action_evaluator = action_evaluator

    def onQueryStarted(self, event: listener.QueryStartedEvent) -> None:
        """
        Writes a message to the standard output logs when a streaming query starts.

        Args:
            event: A `QueryStartedEvent` with details about the streaming query
        """
        logger.debug(f"Streaming query '{event.name}' for summary metrics started run ID '{event.runId}'")

    def onQueryProgress(self, event: listener.QueryProgressEvent) -> None:
        """
        Writes the custom metrics from the DQMetricsObserver to the output destination.

        Args:
            event: A `QueryProgressEvent` with details about the last processed micro-batch
        """
        # If a target query ID is specified, only process events from that query
        if self.target_query_id is not None and str(event.progress.id) != self.target_query_id:
            return

        observed_metrics = event.progress.observedMetrics.get(self.metrics_observation.run_id)
        if not observed_metrics:
            return

        run_time_overwrite = (
            self.metrics_observation.run_time_overwrite
            if self.metrics_observation.run_time_overwrite is not None
            else datetime.fromisoformat(event.progress.timestamp)
        )
        metrics_observation = DQMetricsObservation(
            run_id=self.metrics_observation.run_id,
            run_name=self.metrics_observation.run_name,
            observed_metrics=observed_metrics.asDict(),
            run_time_overwrite=run_time_overwrite,
            error_column_name=self.metrics_observation.error_column_name,
            warning_column_name=self.metrics_observation.warning_column_name,
            input_location=self.metrics_observation.input_location,
            output_location=self.metrics_observation.output_location,
            quarantine_location=self.metrics_observation.quarantine_location,
            checks_location=self.metrics_observation.checks_location,
            rule_set_fingerprint=self.metrics_observation.rule_set_fingerprint,
            user_metadata=self.metrics_observation.user_metadata,
        )
        metrics_df = DQMetricsObserver.build_metrics_df(self.spark, metrics_observation)
        save_dataframe_as_table(metrics_df, self.metrics_config)

        if self.action_evaluator is not None:
            context = ActionContext(
                metrics=metrics_observation.observed_metrics or {},
                run_id=self.metrics_observation.run_id,
                run_time=run_time_overwrite,
                input_location=self.metrics_observation.input_location,
                output_location=self.metrics_observation.output_location,
                quarantine_location=self.metrics_observation.quarantine_location,
                checks_location=self.metrics_observation.checks_location,
                rule_set_fingerprint=self.metrics_observation.rule_set_fingerprint,
                user_metadata=self.metrics_observation.user_metadata,
            )
            try:
                self.action_evaluator.evaluate(context)
            except TerminalActionError:
                raise
            except Exception as exc:
                safe_run_id = self.metrics_observation.run_id.replace("\r", "").replace("\n", "")
                # Sanitize the exception text too: an evaluator error may embed user-supplied
                # values (column/rule names) that could contain newlines (CWE-117).
                safe_exc = str(exc).replace("\r", "").replace("\n", " ")
                logger.warning(f"Action evaluation failed for streaming micro-batch (run_id={safe_run_id}): {safe_exc}")

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
