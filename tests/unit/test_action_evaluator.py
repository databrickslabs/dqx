"""Unit tests for ActionEvaluator: condition gating, suppression, polymorphic dispatch, terminal-error deferral, and alert-before-abort ordering."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import create_autospec

import pytest

from databricks.labs.dqx.actions.base import (
    Action,
    ActionContext,
    ActionResult,
    ActionServices,
    ActionStatus,
)
from databricks.labs.dqx.actions.dq_action import DQAction
from databricks.labs.dqx.actions.evaluator import ActionEvaluator
from databricks.labs.dqx.actions.fail_pipeline import FailPipeline
from databricks.labs.dqx.actions.state import ActionStateStore, AlertEvent
from databricks.labs.dqx.errors import PipelineFailedError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_context() -> ActionContext:
    return ActionContext(
        metrics={"error_row_count": 5},
        run_id="run-test-001",
        run_time=datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc),
        input_location="s3://bucket/data",
    )


def _make_services() -> ActionServices:
    services = create_autospec(ActionServices, instance=True)
    return services


def _make_dq_action(action: object, condition: str | None, name: str) -> DQAction:
    """Build a *DQAction* wrapping an arbitrary test *action*.

    *DQAction.action* is the discriminated *AnyAction* union, which only accepts
    real *DQAlert* / *FailPipeline* instances at construction.  These evaluator
    tests instead need to inject mocks or lightweight fakes to observe dispatch.
    The action is therefore assigned after construction (attribute assignment
    bypasses validation, since *validate_assignment* is not enabled), which is
    exactly the seam the evaluator exercises: it only ever calls
    *dq_action.action.execute(...)*.
    """
    dq_action = DQAction(action=FailPipeline(name=name or "placeholder"), condition=condition, name=name)
    dq_action.action = action  # type: ignore[assignment]
    return dq_action


# ---------------------------------------------------------------------------
# Lightweight fake actions (duck-typed; injected post-construction)
# ---------------------------------------------------------------------------


class FakeActionA:
    """Fake action type A — always returns UNHEALTHY."""

    name = "fake_a"

    def execute(self, _context: ActionContext, _services: ActionServices) -> ActionResult:
        return ActionResult(action_name="fake_a", fired=True, status=ActionStatus.UNHEALTHY)


class FakeActionB:
    """Fake action type B — always returns HEALTHY."""

    name = "fake_b"

    def execute(self, _context: ActionContext, _services: ActionServices) -> ActionResult:
        return ActionResult(action_name="fake_b", fired=True, status=ActionStatus.HEALTHY)


class FakeTerminalAction:
    """Fake action that raises PipelineFailedError on execute."""

    name = "terminal"

    def execute(self, _context: ActionContext, _services: ActionServices) -> ActionResult:
        raise PipelineFailedError("pipeline failed")


class FakeTerminalActionFirst:
    """Fake terminal action that raises PipelineFailedError with a distinct 'first failure' message."""

    name = "terminal_first"

    def execute(self, _context: ActionContext, _services: ActionServices) -> ActionResult:
        raise PipelineFailedError("first failure")


class FakeTerminalActionSecond:
    """Fake terminal action that raises PipelineFailedError with a distinct 'second failure' message."""

    name = "terminal_second"

    def execute(self, _context: ActionContext, _services: ActionServices) -> ActionResult:
        raise PipelineFailedError("second failure")


class FakeAlertAction:
    """Fake non-terminal alert action that records execution and returns UNHEALTHY."""

    name = "alert"

    def __init__(self, executed: list[str]) -> None:
        self._executed = executed

    def execute(self, _context: ActionContext, _services: ActionServices) -> ActionResult:
        self._executed.append("ran")
        return ActionResult(action_name="alert", fired=True, status=ActionStatus.UNHEALTHY)


# ---------------------------------------------------------------------------
# Test: condition False → execute skipped, not-fired event recorded
# ---------------------------------------------------------------------------


def test_condition_false_skips_execute() -> None:
    """When condition evaluates to False, execute is never called and a not-fired event is recorded."""
    action_mock = create_autospec(Action, instance=True)
    action_mock.name = "test_action"

    dq_action = _make_dq_action(action_mock, condition="error_row_count > 100", name="check_errors")
    state_store = create_autospec(ActionStateStore, instance=True)

    evaluator = ActionEvaluator(
        actions=[dq_action],
        state_store=state_store,
        services=_make_services(),
    )

    context = _make_context()  # metrics: error_row_count=5, condition requires >100 → False
    results = evaluator.evaluate(context)

    # execute must NOT be called
    action_mock.execute.assert_not_called()

    # results should be empty (action was skipped)
    assert not results

    # state_store.record must be called once with fired=False
    assert state_store.record.call_count == 1
    event: AlertEvent = state_store.record.call_args[0][0]
    assert isinstance(event, AlertEvent)
    assert event.fired is False
    assert event.status == ActionStatus.HEALTHY
    assert event.action_name == "check_errors"


# ---------------------------------------------------------------------------
# Test: condition None → fires unconditionally (no condition check at all)
# ---------------------------------------------------------------------------


def test_condition_none_fires_unconditionally() -> None:
    """When dq.condition is None, should_fire is consulted and if True, execute is called."""
    action_mock = create_autospec(Action, instance=True)
    action_mock.name = "unconditional"

    result_val = ActionResult(action_name="unconditional", fired=True, status=ActionStatus.UNHEALTHY)
    action_mock.execute.return_value = result_val

    dq_action = _make_dq_action(action_mock, condition=None, name="unconditional")
    state_store = create_autospec(ActionStateStore, instance=True)
    state_store.should_fire.return_value = True

    evaluator = ActionEvaluator(
        actions=[dq_action],
        state_store=state_store,
        services=_make_services(),
    )

    context = _make_context()
    results = evaluator.evaluate(context)

    # execute must be called
    action_mock.execute.assert_called_once()
    assert result_val in results

    # state_store.record called with fired=True
    assert state_store.record.call_count == 1
    event: AlertEvent = state_store.record.call_args[0][0]
    assert event.fired is True


# ---------------------------------------------------------------------------
# Test: condition True + should_fire True → execute + event recorded with fired=True
# ---------------------------------------------------------------------------


def test_condition_true_should_fire_true_executes_and_records() -> None:
    """When condition is True and should_fire is True, execute is called and event is recorded with fired=True."""
    action_mock = create_autospec(Action, instance=True)
    action_mock.name = "alert_action"

    result_val = ActionResult(action_name="alert_action", fired=True, status=ActionStatus.UNHEALTHY)
    action_mock.execute.return_value = result_val

    dq_action = _make_dq_action(action_mock, condition="error_row_count > 0", name="alert_action")
    state_store = create_autospec(ActionStateStore, instance=True)
    state_store.should_fire.return_value = True

    evaluator = ActionEvaluator(
        actions=[dq_action],
        state_store=state_store,
        services=_make_services(),
    )

    context = _make_context()  # error_row_count=5, condition >0 → True
    results = evaluator.evaluate(context)

    action_mock.execute.assert_called_once()
    assert result_val in results

    assert state_store.record.call_count == 1
    event: AlertEvent = state_store.record.call_args[0][0]
    assert event.fired is True
    assert event.status == ActionStatus.UNHEALTHY
    assert event.action_name == "alert_action"
    assert event.observed_metrics == context.metrics
    assert event.run_id == context.run_id
    assert event.run_time == context.run_time
    assert event.input_location == context.input_location


# ---------------------------------------------------------------------------
# Test: should_fire False → execute skipped, fired=False event recorded
# ---------------------------------------------------------------------------


def test_should_fire_false_suppresses() -> None:
    """When condition is True but should_fire returns False, execute is NOT called."""
    action_mock = create_autospec(Action, instance=True)
    action_mock.name = "suppressed_action"

    dq_action = _make_dq_action(action_mock, condition="error_row_count > 0", name="suppressed_action")
    state_store = create_autospec(ActionStateStore, instance=True)
    state_store.should_fire.return_value = False  # suppressed

    evaluator = ActionEvaluator(
        actions=[dq_action],
        state_store=state_store,
        services=_make_services(),
    )

    context = _make_context()  # error_row_count=5, condition >0 → True
    results = evaluator.evaluate(context)

    # execute must NOT be called
    action_mock.execute.assert_not_called()
    assert not results

    # record called with fired=False
    assert state_store.record.call_count == 1
    event: AlertEvent = state_store.record.call_args[0][0]
    assert event.fired is False
    assert event.status == ActionStatus.HEALTHY


# ---------------------------------------------------------------------------
# Test: TerminalActionError is deferred — first action executes before raise
# ---------------------------------------------------------------------------


def test_terminal_action_deferred() -> None:
    """A terminal action (raises PipelineFailedError) is deferred; preceding actions execute first."""
    # First action: a normal recording action
    action_a = create_autospec(Action, instance=True)
    action_a.name = "recording_action"
    result_a = ActionResult(action_name="recording_action", fired=True, status=ActionStatus.UNHEALTHY)
    action_a.execute.return_value = result_a

    # Second action: raises PipelineFailedError
    action_b = FakeTerminalAction()

    dq_a = _make_dq_action(action_a, condition=None, name="recording_action")
    dq_b = _make_dq_action(action_b, condition=None, name="terminal_action")

    state_store = create_autospec(ActionStateStore, instance=True)
    state_store.should_fire.return_value = True

    evaluator = ActionEvaluator(
        actions=[dq_a, dq_b],
        state_store=state_store,
        services=_make_services(),
    )

    context = _make_context()

    # The error must be raised
    with pytest.raises(PipelineFailedError):
        evaluator.evaluate(context)

    # But action_a.execute was called BEFORE the raise (deferral semantics)
    action_a.execute.assert_called_once()

    # state_store.record was called for both actions (2 times total)
    assert state_store.record.call_count == 2


# ---------------------------------------------------------------------------
# Test: no isinstance on action type — two different subclasses both dispatch
# ---------------------------------------------------------------------------


def test_no_isinstance_on_action_type() -> None:
    """Two different Action subclasses both dispatch polymorphically through execute."""
    action_a = FakeActionA()
    action_b = FakeActionB()

    dq_a = _make_dq_action(action_a, condition=None, name="fake_a")
    dq_b = _make_dq_action(action_b, condition=None, name="fake_b")

    state_store = create_autospec(ActionStateStore, instance=True)
    state_store.should_fire.return_value = True

    evaluator = ActionEvaluator(
        actions=[dq_a, dq_b],
        state_store=state_store,
        services=_make_services(),
    )

    context = _make_context()
    results = evaluator.evaluate(context)

    # Both actions must have produced results
    assert len(results) == 2
    action_names = {r.action_name for r in results}
    assert "fake_a" in action_names
    assert "fake_b" in action_names


# ---------------------------------------------------------------------------
# Test: destination_errors → AlertEvent.delivery_errors populated
# ---------------------------------------------------------------------------


def test_destination_errors_recorded_in_event() -> None:
    """When result.destination_errors is populated, AlertEvent.delivery_errors contains those values."""
    action_mock = create_autospec(Action, instance=True)
    action_mock.name = "delivery_action"

    dest_errors = {"slack": "connection refused", "teams": "timeout"}
    result_val = ActionResult(
        action_name="delivery_action",
        fired=True,
        status=ActionStatus.UNHEALTHY,
        destination_errors=dest_errors,
    )
    action_mock.execute.return_value = result_val

    dq_action = _make_dq_action(action_mock, condition=None, name="delivery_action")
    state_store = create_autospec(ActionStateStore, instance=True)
    state_store.should_fire.return_value = True

    evaluator = ActionEvaluator(
        actions=[dq_action],
        state_store=state_store,
        services=_make_services(),
    )

    context = _make_context()
    results = evaluator.evaluate(context)

    assert result_val in results

    event: AlertEvent = state_store.record.call_args[0][0]
    # destinations should be the keys of destination_errors
    assert set(event.destinations) == {"slack", "teams"}
    # delivery_errors should be the values
    assert set(event.delivery_errors) == {"connection refused", "timeout"}


# ---------------------------------------------------------------------------
# Test: deferred[0] ordering — first terminal error is raised, not the last
# ---------------------------------------------------------------------------


def test_terminal_error_ordering_raises_first() -> None:
    """When two terminal actions both fail, evaluate() raises the FIRST error (deferred[0])."""
    term1 = FakeTerminalActionFirst()
    term2 = FakeTerminalActionSecond()

    dq_term1 = _make_dq_action(term1, condition=None, name="terminal_first")
    dq_term2 = _make_dq_action(term2, condition=None, name="terminal_second")

    state_store = create_autospec(ActionStateStore, instance=True)
    state_store.should_fire.return_value = True

    evaluator = ActionEvaluator(
        actions=[dq_term1, dq_term2],
        state_store=state_store,
        services=_make_services(),
    )

    context = _make_context()

    with pytest.raises(PipelineFailedError, match="first failure"):
        evaluator.evaluate(context)


# ---------------------------------------------------------------------------
# Test: alert executes and records before terminal action aborts the pipeline
# ---------------------------------------------------------------------------


def test_alert_fires_before_terminal_abort() -> None:
    """Alert action executes (and records) before a subsequent terminal action aborts the pipeline."""
    executed: list[str] = []
    alert_action = FakeAlertAction(executed)
    terminal_action = FakeTerminalAction()

    dq_alert = _make_dq_action(alert_action, condition=None, name="alert")
    dq_terminal = _make_dq_action(terminal_action, condition=None, name="terminal")

    state_store = create_autospec(ActionStateStore, instance=True)
    state_store.should_fire.return_value = True

    evaluator = ActionEvaluator(
        actions=[dq_alert, dq_terminal],
        state_store=state_store,
        services=_make_services(),
    )

    context = _make_context()

    with pytest.raises(PipelineFailedError):
        evaluator.evaluate(context)

    # Alert action must have run before the pipeline was aborted
    assert executed == ["ran"]
