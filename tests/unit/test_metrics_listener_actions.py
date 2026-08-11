"""Unit tests for the per-micro-batch callback on *StreamingMetricsListener*.

Tests verify that *StreamingMetricsListener.onQueryProgress* invokes the
configured callback with the per-batch *DQMetricsObservation* and resolved run
time, and is a no-op for the callback when none is provided. The listener is
intentionally decoupled from the actions subsystem — it only calls a callback —
so these tests use a plain fake callback. The engine-side callback that builds
an *ActionContext* and drives the *ActionEvaluator* is tested separately in
*tests/unit/test_engine_actions.py*.

No real Spark session or streaming query is required — all Spark and
streaming primitives are faked with plain Python namespaces or autospec mocks.
"""

from collections.abc import Callable
from datetime import datetime
from types import SimpleNamespace
from typing import cast
from unittest.mock import create_autospec, patch

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.streaming.listener import QueryProgressEvent

from databricks.labs.dqx import metrics_listener as ml_module
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


def _make_listener(
    callback: Callable[[DQMetricsObservation, datetime], None] | None = None,
    metrics_config: OutputConfig | None = OutputConfig(location="catalog.schema.metrics"),
) -> StreamingMetricsListener:
    spark = create_autospec(SparkSession)
    return StreamingMetricsListener(
        metrics_config=metrics_config,
        metrics_observation=_make_observation(),
        spark=spark,
        target_query_id=_QUERY_ID,
        action_callback=callback,
    )


# ---------------------------------------------------------------------------
# Test: callback invoked with per-batch observation and run time
# ---------------------------------------------------------------------------


def test_on_query_progress_calls_callback_with_batch_observation() -> None:
    """The callback is called once with the per-batch observation and a resolved run time."""
    calls: list[tuple[DQMetricsObservation, datetime]] = []

    def callback(observation: DQMetricsObservation, run_time: datetime) -> None:
        calls.append((observation, run_time))

    lsn = _make_listener(callback=callback)
    batch_metrics: dict[str, object] = {"error_row_count": 3, "warning_row_count": 1}
    event = _make_event(metrics=batch_metrics)

    with (
        patch.object(DQMetricsObserver, "build_metrics_df"),
        patch.object(ml_module, "save_dataframe_as_table"),
    ):
        lsn.onQueryProgress(event)

    assert len(calls) == 1
    observation, run_time = calls[0]
    assert observation.observed_metrics == batch_metrics
    assert observation.run_id == _RUN_ID
    assert observation.input_location == "catalog.schema.input"
    assert observation.output_location == "catalog.schema.output"
    assert observation.quarantine_location == "catalog.schema.quarantine"
    assert observation.checks_location == "/mnt/checks.yml"
    assert observation.rule_set_fingerprint == "fp-abc123"
    # run_time is derived from the event timestamp when no run_time_overwrite is set
    assert isinstance(run_time, datetime)


# ---------------------------------------------------------------------------
# Test: target_query_id mismatch skips the callback
# ---------------------------------------------------------------------------


def test_on_query_progress_skips_callback_on_query_id_mismatch() -> None:
    """When event.progress.id does not match target_query_id, the callback must NOT be called."""
    calls: list[object] = []
    lsn = _make_listener(callback=lambda *_: calls.append(object()))
    event = _make_event(query_id="different-query-id", metrics={"error_row_count": 0})

    with (
        patch.object(DQMetricsObserver, "build_metrics_df"),
        patch.object(ml_module, "save_dataframe_as_table"),
    ):
        lsn.onQueryProgress(event)

    assert not calls


# ---------------------------------------------------------------------------
# Test: no callback configured — no crash, metrics still written
# ---------------------------------------------------------------------------


def test_on_query_progress_no_callback_does_not_crash() -> None:
    """When action_callback is None the listener is a no-op for actions (backward-compat).

    Metrics writing must NOT be gated on the callback: with no callback the metrics
    row is still written exactly once.
    """
    lsn = _make_listener(callback=None)
    event = _make_event(metrics={"error_row_count": 0})

    with (
        patch.object(DQMetricsObserver, "build_metrics_df"),
        patch.object(ml_module, "save_dataframe_as_table") as save_mock,
    ):
        # Must not raise
        lsn.onQueryProgress(event)

    # Metrics are still written even though no callback is configured.
    save_mock.assert_called_once()


# ---------------------------------------------------------------------------
# Test: callback exception propagates out of onQueryProgress
# ---------------------------------------------------------------------------


def test_on_query_progress_propagates_callback_exception() -> None:
    """Any exception the callback raises propagates — the callback owns swallow-vs-re-raise policy."""

    def callback(_observation: DQMetricsObservation, _run_time: datetime) -> None:
        raise PipelineFailedError("pipeline aborted")

    lsn = _make_listener(callback=callback)
    event = _make_event(metrics={"error_row_count": 10})

    with (
        patch.object(DQMetricsObserver, "build_metrics_df"),
        patch.object(ml_module, "save_dataframe_as_table"),
    ):
        with pytest.raises(PipelineFailedError, match="pipeline aborted"):
            lsn.onQueryProgress(event)


# ---------------------------------------------------------------------------
# Test: missing observed metrics (empty observedMetrics) — callback not called
# ---------------------------------------------------------------------------


def test_on_query_progress_no_observed_metrics_skips_callback() -> None:
    """When observedMetrics for the run_id is absent, the callback is not called."""
    calls: list[object] = []
    lsn = _make_listener(callback=lambda *_: calls.append(object()))
    # metrics=None → observedMetrics dict does not contain the run_id key
    event = _make_event(metrics=None)

    with (
        patch.object(DQMetricsObserver, "build_metrics_df"),
        patch.object(ml_module, "save_dataframe_as_table"),
    ):
        lsn.onQueryProgress(event)

    assert not calls


def test_on_query_progress_invokes_callback_without_metrics_config() -> None:
    """With metrics_config=None the listener skips the metrics write but still invokes the callback."""
    seen: list[dict[str, object]] = []

    def callback(observation: DQMetricsObservation, _run_time: datetime) -> None:
        seen.append(observation.observed_metrics or {})

    lsn = _make_listener(callback=callback, metrics_config=None)
    event = _make_event(metrics={"error_row_count": 2})

    with patch.object(ml_module, "save_dataframe_as_table") as save_mock:
        lsn.onQueryProgress(event)

    save_mock.assert_not_called()  # no metrics destination -> no write
    assert seen == [{"error_row_count": 2}]  # callback still invoked
