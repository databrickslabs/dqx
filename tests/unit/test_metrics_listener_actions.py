"""Unit tests for action evaluation per streaming micro-batch.

Tests verify that *StreamingMetricsListener.onQueryProgress* correctly
evaluates actions when an *ActionEvaluator* is supplied, and is a no-op
when none is provided.

No real Spark session or streaming query is required — all Spark and
streaming primitives are faked with plain Python namespaces or autospec mocks.
"""

import logging
from datetime import datetime
from types import SimpleNamespace
from typing import cast
from unittest.mock import create_autospec, patch

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.streaming.listener import QueryProgressEvent

from databricks.labs.dqx import metrics_listener as ml_module
from databricks.labs.dqx.actions.base import ActionContext, ActionResult, ActionStatus
from databricks.labs.dqx.actions.evaluator import ActionEvaluator
from databricks.labs.dqx.config import OutputConfig
from databricks.labs.dqx.errors import PipelineFailedError
from databricks.labs.dqx.metrics_listener import StreamingMetricsListener
from databricks.labs.dqx.metrics_observer import DQMetricsObservation, DQMetricsObserver


# ---------------------------------------------------------------------------
# Helpers — fake streaming event objects
# ---------------------------------------------------------------------------

_RUN_ID = "test-run-id-123"
_TIMESTAMP = "2024-01-15T10:30:00.000"
_QUERY_ID = "query-abc-456"


def _make_observed_metrics(metrics: dict[str, object]) -> object:
    """Return an object that mimics a PySpark Row with .asDict()."""
    return SimpleNamespace(asDict=lambda: metrics)


def _make_event(
    query_id: str = _QUERY_ID,
    run_id: str = _RUN_ID,
    metrics: dict[str, object] | None = None,
    timestamp: str = _TIMESTAMP,
) -> QueryProgressEvent:
    """Build a fake QueryProgressEvent.

    Structure mirrors the real PySpark object:
      event.progress.id         → str query id
      event.progress.timestamp  → ISO timestamp string
      event.progress.observedMetrics → dict-like {run_id: Row}
    """
    observed = {}
    if metrics is not None:
        observed[run_id] = _make_observed_metrics(metrics)
    progress = SimpleNamespace(
        id=query_id,
        timestamp=timestamp,
        observedMetrics=observed,
    )
    # The duck-typed fake exposes the attributes onQueryProgress reads; cast so callers type-check.
    return cast(QueryProgressEvent, SimpleNamespace(progress=progress))


def _make_observation(run_id: str = _RUN_ID) -> DQMetricsObservation:
    return DQMetricsObservation(
        run_id=run_id,
        run_name="dqx",
        error_column_name="_errors",
        warning_column_name="_warnings",
        input_location="catalog.schema.input",
        output_location="catalog.schema.output",
        quarantine_location="catalog.schema.quarantine",
        checks_location="/mnt/checks.yml",
        rule_set_fingerprint="fp-abc123",
    )


def _make_listener(evaluator: ActionEvaluator | None = None) -> StreamingMetricsListener:
    spark = create_autospec(SparkSession)
    metrics_config = OutputConfig(location="catalog.schema.metrics")
    observation = _make_observation()
    return StreamingMetricsListener(
        metrics_config=metrics_config,
        metrics_observation=observation,
        spark=spark,
        target_query_id=_QUERY_ID,
        action_evaluator=evaluator,
    )


# ---------------------------------------------------------------------------
# Test: evaluate called with correct ActionContext when evaluator is present
# ---------------------------------------------------------------------------


def test_on_query_progress_calls_evaluator_with_correct_context() -> None:
    """evaluator.evaluate is called once with an ActionContext carrying the micro-batch metrics."""
    fake_result = ActionResult(action_name="alert", fired=True, status=ActionStatus.UNHEALTHY)
    fake_evaluator = create_autospec(ActionEvaluator)
    fake_evaluator.evaluate.return_value = [fake_result]

    lsn = _make_listener(evaluator=fake_evaluator)

    batch_metrics: dict[str, object] = {"error_row_count": 3, "warning_row_count": 1}
    event = _make_event(metrics=batch_metrics)

    with (
        patch.object(DQMetricsObserver, "build_metrics_df"),
        patch.object(ml_module, "save_dataframe_as_table"),
    ):
        lsn.onQueryProgress(event)

    fake_evaluator.evaluate.assert_called_once()
    call_args = fake_evaluator.evaluate.call_args
    context: ActionContext = call_args[0][0]

    assert context.metrics == batch_metrics
    assert context.run_id == _RUN_ID
    assert context.input_location == "catalog.schema.input"
    assert context.output_location == "catalog.schema.output"
    assert context.quarantine_location == "catalog.schema.quarantine"
    assert context.checks_location == "/mnt/checks.yml"
    assert context.rule_set_fingerprint == "fp-abc123"
    # run_time is derived from the event timestamp when no run_time_overwrite set
    assert isinstance(context.run_time, datetime)


# ---------------------------------------------------------------------------
# Test: target_query_id mismatch skips evaluation
# ---------------------------------------------------------------------------


def test_on_query_progress_skips_evaluator_on_query_id_mismatch() -> None:
    """When event.progress.id does not match target_query_id, evaluator must NOT be called."""
    fake_evaluator = create_autospec(ActionEvaluator)
    fake_evaluator.evaluate.return_value = []

    lsn = _make_listener(evaluator=fake_evaluator)

    # Use a different query ID
    event = _make_event(query_id="different-query-id", metrics={"error_row_count": 0})

    with (
        patch.object(DQMetricsObserver, "build_metrics_df"),
        patch.object(ml_module, "save_dataframe_as_table"),
    ):
        lsn.onQueryProgress(event)

    fake_evaluator.evaluate.assert_not_called()


# ---------------------------------------------------------------------------
# Test: no evaluator configured — no crash and no evaluate call
# ---------------------------------------------------------------------------


def test_on_query_progress_no_evaluator_does_not_crash() -> None:
    """When action_evaluator is None the listener is a no-op for actions (backward-compat).

    Metrics writing must NOT be gated on the evaluator: with no evaluator the metrics
    row is still written exactly once.
    """
    lsn = _make_listener(evaluator=None)
    event = _make_event(metrics={"error_row_count": 0})

    with (
        patch.object(DQMetricsObserver, "build_metrics_df"),
        patch.object(ml_module, "save_dataframe_as_table") as save_mock,
    ):
        # Must not raise
        lsn.onQueryProgress(event)

    # Metrics are still written even though no action evaluator is configured.
    save_mock.assert_called_once()


# ---------------------------------------------------------------------------
# Test: PipelineFailedError propagates out of onQueryProgress
# ---------------------------------------------------------------------------


def test_on_query_progress_propagates_pipeline_failed_error() -> None:
    """A PipelineFailedError raised by evaluate must propagate — it stops the stream."""
    fake_evaluator = create_autospec(ActionEvaluator)
    fake_evaluator.evaluate.side_effect = PipelineFailedError("pipeline aborted")

    lsn = _make_listener(evaluator=fake_evaluator)
    event = _make_event(metrics={"error_row_count": 10})

    with (
        patch.object(DQMetricsObserver, "build_metrics_df"),
        patch.object(ml_module, "save_dataframe_as_table"),
    ):
        with pytest.raises(PipelineFailedError, match="pipeline aborted"):
            lsn.onQueryProgress(event)


# ---------------------------------------------------------------------------
# Test: generic Exception from evaluate is swallowed (stream stays alive)
# ---------------------------------------------------------------------------


def test_on_query_progress_swallows_generic_exception(caplog) -> None:
    """A non-terminal Exception raised by evaluate is logged and swallowed — stream is not killed."""
    fake_evaluator = create_autospec(ActionEvaluator)
    fake_evaluator.evaluate.side_effect = RuntimeError("transient network error")

    lsn = _make_listener(evaluator=fake_evaluator)
    event = _make_event(metrics={"error_row_count": 0})

    with (
        patch.object(DQMetricsObserver, "build_metrics_df"),
        patch.object(ml_module, "save_dataframe_as_table"),
    ):
        with caplog.at_level(logging.WARNING, logger="databricks.labs.dqx.metrics_listener"):
            # Must NOT raise
            lsn.onQueryProgress(event)

    assert any("Action evaluation failed" in record.message for record in caplog.records)


# ---------------------------------------------------------------------------
# Test: missing observed metrics (empty observedMetrics) — evaluator not called
# ---------------------------------------------------------------------------


def test_on_query_progress_no_observed_metrics_skips_evaluator() -> None:
    """When observedMetrics for the run_id is absent, evaluator is not called."""
    fake_evaluator = create_autospec(ActionEvaluator)
    fake_evaluator.evaluate.return_value = []

    lsn = _make_listener(evaluator=fake_evaluator)
    # metrics=None → observedMetrics dict does not contain the run_id key
    event = _make_event(metrics=None)

    with (
        patch.object(DQMetricsObserver, "build_metrics_df"),
        patch.object(ml_module, "save_dataframe_as_table"),
    ):
        lsn.onQueryProgress(event)

    fake_evaluator.evaluate.assert_not_called()


def test_on_query_progress_evaluates_actions_without_metrics_config() -> None:
    """With metrics_config=None the listener skips the metrics write but still evaluates actions."""
    spark = create_autospec(SparkSession)
    evaluator = create_autospec(ActionEvaluator)
    evaluator.evaluate.return_value = []
    listener = StreamingMetricsListener(
        metrics_config=None,
        metrics_observation=_make_observation(),
        spark=spark,
        target_query_id=_QUERY_ID,
        action_evaluator=evaluator,
    )
    event = _make_event(metrics={"error_row_count": 2})

    with patch.object(ml_module, "save_dataframe_as_table") as save_mock:
        listener.onQueryProgress(event)

    save_mock.assert_not_called()  # no metrics destination -> no write
    evaluator.evaluate.assert_called_once()  # actions still evaluated
    ctx = evaluator.evaluate.call_args.args[0]
    assert ctx.metrics == {"error_row_count": 2}
