"""Unit tests for Task 14: actions wired into DQEngine batch flow.

Tests cover:
- DQEngine raises InvalidParameterError when actions are provided but no observer.
- evaluate_actions delegates to the injected evaluator with the correct ActionContext.
- evaluate_actions returns [] and never builds an evaluator when no actions are configured.
- PipelineFailedError raised by the evaluator propagates out of evaluate_actions.
"""

import threading
from unittest.mock import create_autospec, patch

import pytest
from pyspark.sql import SparkSession

from databricks.labs.dqx.actions.base import ActionContext, DQAction, ActionResult, ActionStatus
from databricks.labs.dqx.actions.evaluator import ActionEvaluator
from databricks.labs.dqx.actions.event_storage import ActionEventStoreFactory
from databricks.labs.dqx.actions.fail_pipeline import FailPipeline
from databricks.labs.dqx.actions.state import ActionEventStore
from databricks.labs.dqx.config import ActionEventsConfig
from databricks.labs.dqx.engine import DQEngine, DQEngineCore
from databricks.labs.dqx.errors import InvalidParameterError, PipelineFailedError
from databricks.labs.dqx.metrics_observer import DQMetricsObserver


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _make_observer() -> DQMetricsObserver:
    return DQMetricsObserver()


# ---------------------------------------------------------------------------
# Test: actions without observer raises InvalidParameterError
# ---------------------------------------------------------------------------


def test_dqengine_actions_without_observer_raises(mock_workspace_client) -> None:
    spark = create_autospec(SparkSession)
    action = create_autospec(DQAction, instance=True)

    with pytest.raises(InvalidParameterError, match="Actions require a metrics observer"):
        DQEngine(mock_workspace_client, spark=spark, actions=[action])


def test_dqenginecore_actions_without_observer_raises(mock_workspace_client) -> None:
    spark = create_autospec(SparkSession)
    action = DQAction(action=FailPipeline(), name="fail_on_errors")

    with pytest.raises(InvalidParameterError, match="Actions require a metrics observer"):
        DQEngineCore(mock_workspace_client, spark=spark, actions=[action])


# ---------------------------------------------------------------------------
# Test: evaluate_actions delegates to the injected evaluator
# ---------------------------------------------------------------------------


def test_evaluate_actions_calls_evaluator_with_correct_context(mock_workspace_client) -> None:
    spark = create_autospec(SparkSession)
    observer = _make_observer()
    action = create_autospec(DQAction, instance=True)

    # Build a fake evaluator whose evaluate() returns a known result list.
    fake_result = ActionResult(action_name="test_action", fired=True, status=ActionStatus.UNHEALTHY)
    fake_evaluator = create_autospec(ActionEvaluator)
    fake_evaluator.evaluate.return_value = [fake_result]

    def fake_factory(_actions: list[DQAction]) -> ActionEvaluator:
        return fake_evaluator

    engine = DQEngine(
        mock_workspace_client,
        spark=spark,
        observer=observer,
        actions=[action],
        action_evaluator_factory=fake_factory,
    )

    metrics: dict[str, object] = {"error_row_count": 5, "warning_row_count": 0}
    results = engine.evaluate_actions(
        metrics,
        input_location="catalog.schema.input",
        output_location="catalog.schema.output",
        quarantine_location="catalog.schema.quarantine",
        checks_location="/mnt/checks.yml",
        rule_set_fingerprint="abc123",
    )

    # Should return the evaluator result
    assert results == [fake_result]

    # Evaluator must have been called exactly once
    fake_evaluator.evaluate.assert_called_once()

    # Extract the ActionContext passed to evaluate
    call_args = fake_evaluator.evaluate.call_args
    context: ActionContext = call_args[0][0]

    assert context.metrics == metrics
    assert context.input_location == "catalog.schema.input"
    assert context.output_location == "catalog.schema.output"
    assert context.quarantine_location == "catalog.schema.quarantine"
    assert context.checks_location == "/mnt/checks.yml"
    assert context.rule_set_fingerprint == "abc123"
    assert context.run_name == "dqx"


# ---------------------------------------------------------------------------
# Test: evaluate_actions with no actions returns [] without building evaluator
# ---------------------------------------------------------------------------


def test_evaluate_actions_no_actions_returns_empty_list(mock_workspace_client) -> None:
    spark = create_autospec(SparkSession)

    factory_call_count = 0

    def counting_factory(_actions: list[DQAction]) -> ActionEvaluator:
        nonlocal factory_call_count
        factory_call_count += 1
        mock_evaluator = create_autospec(ActionEvaluator)
        return mock_evaluator

    engine = DQEngine(
        mock_workspace_client,
        spark=spark,
        action_evaluator_factory=counting_factory,
    )

    results = engine.evaluate_actions({"error_row_count": 0})

    assert results == []
    assert factory_call_count == 0, "Evaluator factory must not be called when no actions are configured"


# ---------------------------------------------------------------------------
# Test: PipelineFailedError raised by evaluator propagates
# ---------------------------------------------------------------------------


def test_evaluate_actions_propagates_pipeline_failed_error(mock_workspace_client) -> None:
    spark = create_autospec(SparkSession)
    observer = _make_observer()
    action = create_autospec(DQAction, instance=True)

    fake_evaluator = create_autospec(ActionEvaluator)
    fake_evaluator.evaluate.side_effect = PipelineFailedError("pipeline aborted")

    engine = DQEngine(
        mock_workspace_client,
        spark=spark,
        observer=observer,
        actions=[action],
        action_evaluator_factory=lambda _: fake_evaluator,
    )

    with pytest.raises(PipelineFailedError, match="pipeline aborted"):
        engine.evaluate_actions({"error_row_count": 99})


# ---------------------------------------------------------------------------
# Test: evaluator is cached (same instance across two calls)
# ---------------------------------------------------------------------------


def test_evaluate_actions_evaluator_is_cached(mock_workspace_client) -> None:
    spark = create_autospec(SparkSession)
    observer = _make_observer()
    action = create_autospec(DQAction, instance=True)

    instances: list[ActionEvaluator] = []

    def factory(_actions: list[DQAction]) -> ActionEvaluator:
        evaluator = create_autospec(ActionEvaluator)
        evaluator.evaluate.return_value = []
        instances.append(evaluator)
        return evaluator

    engine = DQEngine(
        mock_workspace_client,
        spark=spark,
        observer=observer,
        actions=[action],
        action_evaluator_factory=factory,
    )

    engine.evaluate_actions({})
    engine.evaluate_actions({})

    assert len(instances) == 1, "Factory must be called only once; evaluator must be cached"


# ---------------------------------------------------------------------------
# Test: concurrent _get_action_evaluator builds exactly one evaluator (thread-safe)
# ---------------------------------------------------------------------------


def test_concurrent_get_action_evaluator_builds_single_instance(mock_workspace_client) -> None:
    """Under parallel run-config processing, the lazy init must build ONE evaluator/state store."""
    spark = create_autospec(SparkSession)
    observer = _make_observer()
    action = create_autospec(DQAction, instance=True)

    instances: list[ActionEvaluator] = []
    barrier = threading.Barrier(8)

    def factory(_actions: list[DQAction]) -> ActionEvaluator:
        evaluator = create_autospec(ActionEvaluator)
        evaluator.evaluate.return_value = []
        instances.append(evaluator)
        return evaluator

    engine = DQEngine(
        mock_workspace_client,
        spark=spark,
        observer=observer,
        actions=[action],
        action_evaluator_factory=factory,
    )

    def worker() -> None:
        barrier.wait()  # maximize contention on the lazy-init
        engine.evaluate_actions({"error_row_count": 1})  # drives _get_action_evaluator under the lock

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(instances) == 1, "Factory must build exactly one evaluator under concurrency"


# ---------------------------------------------------------------------------
# Test: action_events_config seeds an event-store-backed state store
# ---------------------------------------------------------------------------


def test_action_events_config_seeds_state_from_event_store(mock_workspace_client) -> None:
    """When action_events_config is set, the default state store is seeded from the event store.

    Driven through the public evaluate_actions path: a non-firing condition means no action
    executes, but building the (default) evaluator must still create an event-store-backed store
    and seed it, which calls load_latest_per_action exactly once.
    """
    fake_event_store = create_autospec(ActionEventStore, instance=True)
    fake_event_store.load_latest_per_action.return_value = {}

    spark = create_autospec(SparkSession)
    observer = _make_observer()
    # Condition is False for the metrics below, so no action executes (no FailPipeline raise).
    action = DQAction(condition="error_row_count > 100", action=FailPipeline(), name="fail_on_many_errors")

    with patch.object(ActionEventStoreFactory, "create", return_value=fake_event_store) as create_mock:
        engine = DQEngine(
            mock_workspace_client,
            spark=spark,
            observer=observer,
            actions=[action],
            action_events_config=ActionEventsConfig(location="catalog.schema.events"),
        )
        results = engine.evaluate_actions({"error_row_count": 0})

    assert results == []  # condition false -> nothing fired
    create_mock.assert_called_once()  # event store built from the config
    fake_event_store.load_latest_per_action.assert_called_once()  # seed() ran
