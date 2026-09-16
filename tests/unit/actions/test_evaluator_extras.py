"""Unit tests for extras propagation across the *ActionEvaluator* loop.

Contract: a producer returns *ActionResult.extras* as *dict[str, str] | None*; the evaluator
copies the payload into *ActionContext.extras* under the producer's name so the next action can
read it back via ``context.extras.get(<producer-name>)``.
"""

from datetime import datetime, timezone
from typing import Any
from unittest.mock import create_autospec

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
    """Emits a *dict[str, str]* extras payload."""

    def __init__(self, name: str, extras: dict[str, str] | None) -> None:
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
    """Producer that mutates its returned payload *after* execute returns (copy guard)."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.payload: dict[str, str] = {"seen": "true"}

    def execute(self, _context: ActionContext, _services: ActionServices) -> ActionResult:
        return ActionResult(action_name=self.name, fired=True, status=ActionStatus.UNHEALTHY, extras=self.payload)


class _RecordingConsumer:
    """Records the *ActionContext.extras* observed at execute time."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.observed: dict[str, dict[str, str]] | None = None

    def execute(self, context: ActionContext, _services: ActionServices) -> ActionResult:
        # Snapshot so subsequent evaluator mutations of accumulated_extras cannot retroactively
        # change what this consumer observed at its own execute time.
        self.observed = None if context.extras is None else {k: dict(v) for k, v in context.extras.items()}
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
    """A dict-copy at the evaluator boundary severs the caller's reference — post-execute
    mutation is invisible downstream."""
    producer = _MutatingProducer("producer")
    intermediate = _RecordingConsumer("intermediate")
    final = _RecordingConsumer("final")

    class _MutateInsideDispatch:
        """Runs between producer and final, mutating producer.payload to prove isolation."""

        name = "mutator"

        def execute(self, _context: ActionContext, _services: ActionServices) -> ActionResult:
            producer.payload["seen"] = "MUTATED"
            producer.payload["extra"] = "sneaked"
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
    assert final.observed["producer"] == {"seen": "true"}


def test_none_extras_keeps_context_extras_none_for_next_action() -> None:
    """Without any producer contributing, *context.extras* stays *None* for downstream actions."""
    silent = _NoExtrasAction("silent")
    consumer = _RecordingConsumer("consumer")

    evaluator = ActionEvaluator(
        actions=[_make_dq_action(silent, "silent"), _make_dq_action(consumer, "consumer")],
        state_store=ActionStateStore(),
        services=_make_services(),
    )
    evaluator.evaluate(_make_context())

    assert consumer.observed is None


def test_same_name_actions_last_write_wins() -> None:
    """Two DQAction entries with the same name: later overwrites earlier (documented contract)."""
    first = _ProducerAction("dup", {"round": "1"})
    second = _ProducerAction("dup", {"round": "2"})
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

    assert consumer.observed == {"dup": {"round": "2"}}


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
    """The caller's *context.extras* dict is copied — evaluator mutations must not leak back."""
    producer = _ProducerAction("producer", {"foo": "bar"})
    caller_extras: dict[str, dict[str, str]] = {"seed": {"value": "1"}}

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

    assert consumer.observed == {"seed": {"value": "1"}, "producer": {"foo": "bar"}}
    # Caller's original dict is untouched.
    assert caller_extras == {"seed": {"value": "1"}}


def test_consumer_reads_extras_via_get_extras() -> None:
    """Consumers read producer payloads via ``context.get_extras("<name>")``.

    The accessor collapses both *None* cases (no producer yet, missing key) into an empty dict, so
    callers never need the ``(context.extras or {}).get(...) or {}`` dance.
    """
    producer = _ProducerAction("producer", {"lineage_location": "cat.sch.lin"})

    class _AssertingConsumer:
        name = "consumer"

        def __init__(self) -> None:
            self.produced_payload: dict[str, str] = {"sentinel": "1"}
            self.missing: dict[str, str] = {"sentinel": "1"}

        def execute(self, context: ActionContext, _services: ActionServices) -> ActionResult:
            self.produced_payload = context.get_extras("producer")
            self.missing = context.get_extras("does_not_exist")
            return ActionResult(action_name=self.name, fired=True, status=ActionStatus.HEALTHY)

    consumer = _AssertingConsumer()
    evaluator = ActionEvaluator(
        actions=[_make_dq_action(producer, "producer"), _make_dq_action(consumer, "consumer")],
        state_store=ActionStateStore(),
        services=_make_services(),
    )
    evaluator.evaluate(_make_context())

    assert consumer.produced_payload == {"lineage_location": "cat.sch.lin"}
    assert consumer.missing == {}


def test_get_extras_returns_empty_dict_when_no_producer_ran() -> None:
    """*get_extras* returns *{}* both when the outer *extras* is *None* and when the key is missing."""
    context = ActionContext(
        metrics={},
        run_id="run-get-extras",
        run_time=datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc),
    )
    assert context.extras is None
    assert context.get_extras("anything") == {}

    populated = ActionContext(
        metrics={},
        run_id="run-get-extras",
        run_time=datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc),
        extras={"producer": {"k": "v"}},
    )
    assert populated.get_extras("producer") == {"k": "v"}
    assert populated.get_extras("other") == {}
