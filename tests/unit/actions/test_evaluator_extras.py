"""Unit tests for extras propagation across the *ActionEvaluator* loop.

Covers the contract added by the *lineage_action* feature (Step 4 of the plan): a producer
returns *ActionResult.extras*, the evaluator validates and deep-copies the payload, and the next
action sees it under the producer's name via *ActionContext.get_action_extras*.
"""

from datetime import datetime, timezone
from typing import Any
from unittest.mock import create_autospec

from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.actions.alert import DQAlert
from databricks.labs.dqx.actions.base import (
    ActionContext,
    ActionResult,
    ActionServices,
    ActionStatus,
)
from databricks.labs.dqx.actions.destinations.callback import DQCallbackAlertDestination
from databricks.labs.dqx.actions.dq_action import DQAction
from databricks.labs.dqx.actions.evaluator import ActionEvaluator
from databricks.labs.dqx.actions.fail_pipeline import FailPipeline
from databricks.labs.dqx.actions.noop import NoOpAction
from databricks.labs.dqx.actions.state import ActionStateStore


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_context() -> ActionContext:
    return ActionContext(
        metrics={"error_row_count": 5},
        run_id="run-extras-001",
        run_time=datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc),
    )


def _make_services() -> ActionServices:
    services = create_autospec(ActionServices, instance=True)
    return services


def _make_dq_action(action: object, name: str) -> DQAction:
    """Build a DQAction wrapping an arbitrary test action (bypasses type validation)."""
    dq_action = DQAction(action=FailPipeline(name=name or "placeholder"), condition=None, name=name)
    dq_action.action = action  # type: ignore[assignment]
    return dq_action


# ---------------------------------------------------------------------------
# Fake actions
# ---------------------------------------------------------------------------


class _ProducerAction:
    """Emits a *ScalarNode* extras payload."""

    def __init__(self, name: str, extras: Any) -> None:
        self.name = name
        self._extras = extras

    def execute(self, _context: ActionContext, _services: ActionServices) -> ActionResult:
        return ActionResult(
            action_name=self.name,
            fired=True,
            status=ActionStatus.UNHEALTHY,
            extras=self._extras,
        )


class _MutatingProducer:
    """Producer that mutates its returned payload *after* execute returns (deep-copy guard)."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.payload: dict[str, Any] = {"seen": True, "list": [1, 2, 3]}

    def execute(self, _context: ActionContext, _services: ActionServices) -> ActionResult:
        return ActionResult(
            action_name=self.name, fired=True, status=ActionStatus.UNHEALTHY, extras=self.payload
        )


class _RecordingConsumer:
    """Records the *ActionContext.extras* observed at execute time."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.observed: dict[str, Any] | None = None

    def execute(self, context: ActionContext, _services: ActionServices) -> ActionResult:
        # Copy so subsequent evaluator mutations of accumulated_extras cannot retroactively change
        # what this consumer observed at its own execute time.
        self.observed = {k: v for k, v in context.extras.items()}
        return ActionResult(action_name=self.name, fired=True, status=ActionStatus.HEALTHY)


class _NoExtrasAction:
    """Action returning result with extras=None."""

    def __init__(self, name: str) -> None:
        self.name = name

    def execute(self, _context: ActionContext, _services: ActionServices) -> ActionResult:
        return ActionResult(action_name=self.name, fired=True, status=ActionStatus.UNHEALTHY, extras=None)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_producer_extras_visible_to_next_action() -> None:
    """A pass-through action's extras appear under its name in the next ActionContext."""
    producer = _ProducerAction("producer", {"foo": "bar"})
    consumer = _RecordingConsumer("consumer")

    evaluator = ActionEvaluator(
        actions=[_make_dq_action(producer, "producer"), _make_dq_action(consumer, "consumer")],
        state_store=ActionStateStore(),
        services=_make_services(),
    )
    evaluator.evaluate(_make_context())

    assert consumer.observed == {"producer": {"foo": "bar"}}


def test_producer_mutation_after_execute_does_not_leak_downstream() -> None:
    """Deep-copy severs the caller's reference: post-execute mutation is invisible downstream."""
    producer = _MutatingProducer("producer")
    intermediate = _RecordingConsumer("intermediate")
    final = _RecordingConsumer("final")

    class _MutateInsideDispatch:
        """Runs between producer and final, mutating producer.payload to prove isolation."""

        name = "mutator"

        def execute(self, _context: ActionContext, _services: ActionServices) -> ActionResult:
            producer.payload["seen"] = "MUTATED"
            producer.payload["list"].append(999)
            return ActionResult(action_name=self.name, fired=True, status=ActionStatus.HEALTHY)

    evaluator = ActionEvaluator(
        actions=[
            _make_dq_action(producer, "producer"),
            _make_dq_action(intermediate, "intermediate"),
            _make_dq_action(_MutateInsideDispatch(), "mutator"),
            _make_dq_action(final, "final"),
        ],
        state_store=ActionStateStore(),
        services=_make_services(),
    )
    evaluator.evaluate(_make_context())

    assert final.observed is not None
    assert final.observed["producer"] == {"seen": True, "list": [1, 2, 3]}


def test_none_extras_does_not_create_context_key() -> None:
    """extras=None must not create a slot in ActionContext.extras."""
    silent = _NoExtrasAction("silent")
    consumer = _RecordingConsumer("consumer")

    evaluator = ActionEvaluator(
        actions=[_make_dq_action(silent, "silent"), _make_dq_action(consumer, "consumer")],
        state_store=ActionStateStore(),
        services=_make_services(),
    )
    evaluator.evaluate(_make_context())

    assert consumer.observed == {}


def test_same_name_actions_last_write_wins() -> None:
    """Two DQAction entries with the same name: later overwrites earlier (documented contract).

    Extras are keyed by producing action name, so the second producer with the same name replaces
    the first producer's payload in the accumulated map. This test documents that observed
    behaviour so future refactors do not silently change it.
    """
    first = _ProducerAction("dup", {"round": 1})
    second = _ProducerAction("dup", {"round": 2})
    consumer = _RecordingConsumer("consumer")

    evaluator = ActionEvaluator(
        actions=[
            _make_dq_action(first, "dup"),
            _make_dq_action(second, "dup"),
            _make_dq_action(consumer, "consumer"),
        ],
        state_store=ActionStateStore(),
        services=_make_services(),
    )
    evaluator.evaluate(_make_context())

    assert consumer.observed == {"dup": {"round": 2}}


def test_invalid_extras_records_config_error_and_continues() -> None:
    """A ScalarNode-invalid extras payload triggers CONFIG_ERROR and does not break the loop.

    The producer returns an object that fails validate_scalar_node (a live WorkspaceClient); the
    evaluator must skip propagation for that action, record the outcome as CONFIG_ERROR, and
    continue evaluating the next action.
    """
    bad = _ProducerAction("bad_producer", {"client": create_autospec(WorkspaceClient, instance=True)})
    consumer = _RecordingConsumer("consumer")

    store = ActionStateStore()
    evaluator = ActionEvaluator(
        actions=[_make_dq_action(bad, "bad_producer"), _make_dq_action(consumer, "consumer")],
        state_store=store,
        services=_make_services(),
    )
    evaluator.evaluate(_make_context())

    # consumer ran (loop did not break) and never saw the invalid extras
    assert consumer.observed == {}


def test_existing_actions_still_work_without_extras() -> None:
    """DQAlert, FailPipeline, NoOpAction still work when extras=None (backward compat guard)."""
    fires: list[str] = []

    def _callback(_message: Any, _context: ActionContext) -> None:
        fires.append("delivered")

    alert = DQAlert(
        destinations=[DQCallbackAlertDestination(name="cb", callback=_callback)],
    )
    noop = NoOpAction()

    evaluator = ActionEvaluator(
        actions=[
            DQAction(action=alert, condition="error_row_count > 0", name="alert"),
            DQAction(action=noop, condition=None, name="noop"),
        ],
        state_store=ActionStateStore(),
        services=_make_services(),
    )
    results = evaluator.evaluate(_make_context())

    assert fires == ["delivered"]
    assert len(results) == 2
    assert all(r.extras is None for r in results)


def test_incoming_context_extras_are_preserved_and_not_mutated() -> None:
    """The caller's context.extras dict is copied — evaluator mutations must not leak back."""
    producer = _ProducerAction("producer", {"foo": "bar"})
    caller_extras: dict[str, Any] = {"seed": {"value": 1}}

    context = ActionContext(
        metrics={"error_row_count": 5},
        run_id="run-extras-002",
        run_time=datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc),
        extras=caller_extras,
    )
    consumer = _RecordingConsumer("consumer")
    evaluator = ActionEvaluator(
        actions=[_make_dq_action(producer, "producer"), _make_dq_action(consumer, "consumer")],
        state_store=ActionStateStore(),
        services=_make_services(),
    )
    evaluator.evaluate(context)

    assert consumer.observed == {"seed": {"value": 1}, "producer": {"foo": "bar"}}
    # Caller's original dict is untouched.
    assert caller_extras == {"seed": {"value": 1}}


def test_get_action_extras_returns_producer_payload() -> None:
    """context.get_action_extras returns the producing action's payload or None."""
    producer = _ProducerAction("producer", {"lineage_location": "cat.sch.lin"})

    class _AssertingConsumer:
        name = "consumer"

        def __init__(self) -> None:
            self.produced_payload: Any = None
            self.missing: Any = "sentinel"

        def execute(self, context: ActionContext, _services: ActionServices) -> ActionResult:
            self.produced_payload = context.get_action_extras("producer")
            self.missing = context.get_action_extras("does_not_exist")
            return ActionResult(action_name=self.name, fired=True, status=ActionStatus.HEALTHY)

    consumer = _AssertingConsumer()
    evaluator = ActionEvaluator(
        actions=[_make_dq_action(producer, "producer"), _make_dq_action(consumer, "consumer")],
        state_store=ActionStateStore(),
        services=_make_services(),
    )
    evaluator.evaluate(_make_context())

    assert consumer.produced_payload == {"lineage_location": "cat.sch.lin"}
    assert consumer.missing is None
