import logging
from collections.abc import Callable
from pyspark.sql.streaming.listener import (
    StreamingQueryListener,
    QueryStartedEvent,
    QueryProgressEvent,
    QueryIdleEvent,
    QueryTerminatedEvent,
)


logger = logging.getLogger(__name__)


class StreamingMetricsListener(StreamingQueryListener):
    """
    Implements a Spark `StreamingQueryListener` for writing data quality summary metrics to an output destination. See
    the [Spark documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.StreamingQueryListener.html)
    for detailed information about `StreamingQueryListener`.

    Args:
        handler: Python `Callable` which handles writing metrics to an output destination. Called for every
        micro-batch processed by the streaming query.
    """

    def __init__(self, handler: Callable) -> None:
        self._handler = handler

    def onQueryStarted(self, event: QueryStartedEvent) -> None:
        """
        Writes a message to the standard output logs when a streaming query starts.

        Args:
            event: A `QueryStartedEvent` with details about the streaming query
        """
        logger.debug(f"Streaming query '{event.name}' started run ID '{event.runId}'")

    def onQueryProgress(self, event: QueryProgressEvent) -> None:
        """
        Writes the custom metrics from the DQObserver to the output destination.

        Args:
            event: A `QueryProgressEvent` with details about the last processed micro-batch
        """
        self._handler()

    def onQueryIdle(self, event: QueryIdleEvent) -> None:
        """
        Writes a message to the standard output logs when a streaming query is idle.

        Args:
            event: A `QueryIdleEvent` with details about the streaming query
        """
        logger.debug(f"Streaming query run '{event.runId}' was reported idle")

    def onQueryTerminated(self, event: QueryTerminatedEvent) -> None:
        """
        Writes a message to the standard output logs when a streaming query stops due to cancellation or failure.

        Args:
            event: A `QueryTerminatedEvent` with details about the streaming query
        """
        if event.exception:
            logger.debug(f"Streaming query run '{event.runId}' failed with error: {event.exception}")

        logger.debug(f"Streaming query run '{event.runId}' stopped")
