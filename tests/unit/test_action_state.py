"""Unit tests for databricks.labs.dqx.actions.state.

TDD step 1: write failing tests before implementation.

Tests cover:
- AlertEvent is a frozen dataclass (FrozenInstanceError on setattr).
- ActionStateStore.should_fire with ALWAYS + EACH: returns True when condition True.
- ActionStateStore.should_fire: returns False when condition_result is False.
- ActionStateStore.should_fire with HOURLY, no prior state: returns True.
- ActionStateStore.should_fire with HOURLY, prior fire 30 min ago: returns False.
- ActionStateStore.should_fire with HOURLY, prior fire 61 min ago: returns True.
- ActionStateStore.should_fire with DAILY, prior fire 23h ago: returns False.
- ActionStateStore.should_fire with DAILY, prior fire 25h ago: returns True.
- ActionStateStore.should_fire with STATUS_CHANGE, last HEALTHY: returns True.
- ActionStateStore.should_fire with STATUS_CHANGE, last UNHEALTHY: returns False.
- ActionStateStore.should_fire with STATUS_CHANGE, no prior state: returns True.
- ActionStateStore.seed loads state from event store.
- ActionStateStore.record updates in-memory state and calls event_store.append.
- Non-DQAlert action always fires when condition True.
"""

from __future__ import annotations

import dataclasses
from datetime import datetime, timedelta, timezone

import pytest

from databricks.labs.dqx.actions.alert import DQAlert, DQAlertFrequency, NotifyOn
from databricks.labs.dqx.actions.base import ActionContext, ActionStatus
from databricks.labs.dqx.actions.dq_action import DQAction
from databricks.labs.dqx.actions.fail_pipeline import FailPipeline
from databricks.labs.dqx.actions.destinations.callback import CallbackDQAlertDestination
from databricks.labs.dqx.actions.state import ActionEventStore, ActionStateStore, AlertEvent


# ---------------------------------------------------------------------------
# Fake / stub helpers
# ---------------------------------------------------------------------------


class FakeEventStore(ActionEventStore):
    """In-memory event store for testing."""

    def __init__(
        self,
        initial_events: dict[str, AlertEvent] | None = None,
        last_fired: dict[str, datetime] | None = None,
    ) -> None:
        self.appended: list[AlertEvent] = []
        self._initial: dict[str, AlertEvent] = initial_events or {}
        # Explicit last-fired map decouples "latest event" from "latest fired event"; when None it is
        # derived from the fired initial events (matching the real stores' semantics).
        self._last_fired: dict[str, datetime] | None = last_fired

    def append(self, events: list[AlertEvent]) -> None:
        self.appended.extend(events)

    def load_latest_per_action(self) -> dict[str, AlertEvent]:
        return dict(self._initial)

    def load_last_fired_per_action(self) -> dict[str, datetime]:
        if self._last_fired is not None:
            return dict(self._last_fired)
        return {name: event.run_time for name, event in self._initial.items() if event.fired}


def _make_destination() -> CallbackDQAlertDestination:
    """Return a real in-process callback destination (a member of *AnyDestination*)."""
    return CallbackDQAlertDestination(name="stub-dest", callback=lambda message, context: None)


def _make_dq_alert(
    *,
    frequency: DQAlertFrequency = DQAlertFrequency.ALWAYS,
    notify_on: NotifyOn = NotifyOn.EACH,
) -> DQAlert:
    return DQAlert(destinations=[_make_destination()], alert_frequency=frequency, notify_on=notify_on)


def _make_dq_action(alert: DQAlert, condition: str | None = None) -> DQAction:
    return DQAction(action=alert, condition=condition)


def _make_context(run_time: datetime | None = None) -> ActionContext:
    return ActionContext(
        metrics={},
        run_id="run-001",
        run_time=run_time or datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc),
    )


def _make_event(
    *,
    action_name: str = "test-action",
    fired: bool = True,
    status: ActionStatus = ActionStatus.UNHEALTHY,
    run_time: datetime | None = None,
) -> AlertEvent:
    return AlertEvent(
        action_name=action_name,
        condition="error_count > 0",
        fired=fired,
        status=status,
        observed_metrics={"error_count": 5},
        run_id="run-001",
        run_time=run_time or datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc),
        input_location=None,
        destinations=["stub-dest"],
        delivery_errors=[],
    )


def _pre_seed_store(
    store: ActionStateStore, *, action_name: str, fired: bool, status: ActionStatus, run_time: datetime
) -> None:
    """Pre-seed store state via the public record() API."""
    event = _make_event(action_name=action_name, fired=fired, status=status, run_time=run_time)
    store.record(event)


# ---------------------------------------------------------------------------
# AlertEvent tests
# ---------------------------------------------------------------------------


def test_alert_event_is_frozen() -> None:
    """AlertEvent must be immutable (frozen dataclass)."""
    evt = _make_event()
    with pytest.raises(dataclasses.FrozenInstanceError):
        setattr(evt, "action_name", "other")


# ---------------------------------------------------------------------------
# should_fire: condition result gate
# ---------------------------------------------------------------------------


def test_never_fires_when_condition_false() -> None:
    """should_fire returns False regardless of alert config when condition_result is False."""
    store = ActionStateStore()
    dq_action = _make_dq_action(_make_dq_alert(frequency=DQAlertFrequency.ALWAYS, notify_on=NotifyOn.EACH))
    ctx = _make_context()
    assert store.should_fire(dq_action, ctx, condition_result=False) is False


# ---------------------------------------------------------------------------
# should_fire: ALWAYS frequency
# ---------------------------------------------------------------------------


def test_always_fires_when_condition_true() -> None:
    """DQAlert with ALWAYS + EACH always fires when condition is True."""
    store = ActionStateStore()
    dq_action = _make_dq_action(_make_dq_alert(frequency=DQAlertFrequency.ALWAYS, notify_on=NotifyOn.EACH))
    ctx = _make_context()
    assert store.should_fire(dq_action, ctx, condition_result=True) is True


# ---------------------------------------------------------------------------
# should_fire: HOURLY frequency
# ---------------------------------------------------------------------------


def test_hourly_first_fire_allowed() -> None:
    """HOURLY alert with no prior state fires on first run."""
    store = ActionStateStore()
    dq_action = _make_dq_action(_make_dq_alert(frequency=DQAlertFrequency.HOURLY))
    ctx = _make_context()
    assert store.should_fire(dq_action, ctx, condition_result=True) is True


def test_hourly_suppressed_within_hour() -> None:
    """HOURLY alert is suppressed when last fire was 30 minutes ago."""
    base_time = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    store = ActionStateStore()
    _pre_seed_store(store, action_name="default-action", fired=True, status=ActionStatus.UNHEALTHY, run_time=base_time)

    alert = _make_dq_alert(frequency=DQAlertFrequency.HOURLY)
    dq_action = DQAction(action=alert, name="default-action")
    ctx = _make_context(run_time=base_time + timedelta(minutes=30))
    assert store.should_fire(dq_action, ctx, condition_result=True) is False


def test_hourly_allowed_after_hour() -> None:
    """HOURLY alert fires again when last fire was 61 minutes ago."""
    base_time = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    store = ActionStateStore()
    _pre_seed_store(store, action_name="default-action", fired=True, status=ActionStatus.UNHEALTHY, run_time=base_time)

    alert = _make_dq_alert(frequency=DQAlertFrequency.HOURLY)
    dq_action = DQAction(action=alert, name="default-action")
    ctx = _make_context(run_time=base_time + timedelta(minutes=61))
    assert store.should_fire(dq_action, ctx, condition_result=True) is True


def test_hourly_allowed_at_exact_boundary() -> None:
    """HOURLY alert fires at exactly the 1-hour boundary (suppression is strictly < 1h)."""
    base_time = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    store = ActionStateStore()
    _pre_seed_store(store, action_name="default-action", fired=True, status=ActionStatus.UNHEALTHY, run_time=base_time)

    alert = _make_dq_alert(frequency=DQAlertFrequency.HOURLY)
    dq_action = DQAction(action=alert, name="default-action")
    ctx = _make_context(run_time=base_time + timedelta(hours=1))
    assert store.should_fire(dq_action, ctx, condition_result=True) is True


# ---------------------------------------------------------------------------
# should_fire: DAILY frequency
# ---------------------------------------------------------------------------


def test_daily_suppressed_within_24h() -> None:
    """DAILY alert is suppressed when last fire was 23 hours ago."""
    base_time = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    store = ActionStateStore()
    _pre_seed_store(store, action_name="default-action", fired=True, status=ActionStatus.UNHEALTHY, run_time=base_time)

    alert = _make_dq_alert(frequency=DQAlertFrequency.DAILY)
    dq_action = DQAction(action=alert, name="default-action")
    ctx = _make_context(run_time=base_time + timedelta(hours=23))
    assert store.should_fire(dq_action, ctx, condition_result=True) is False


def test_daily_allowed_after_24h() -> None:
    """DAILY alert fires again when last fire was 25 hours ago."""
    base_time = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    store = ActionStateStore()
    _pre_seed_store(store, action_name="default-action", fired=True, status=ActionStatus.UNHEALTHY, run_time=base_time)

    alert = _make_dq_alert(frequency=DQAlertFrequency.DAILY)
    dq_action = DQAction(action=alert, name="default-action")
    ctx = _make_context(run_time=base_time + timedelta(hours=25))
    assert store.should_fire(dq_action, ctx, condition_result=True) is True


# ---------------------------------------------------------------------------
# should_fire: STATUS_CHANGE notify_on
# ---------------------------------------------------------------------------


def test_status_change_fires_when_no_prior_state() -> None:
    """STATUS_CHANGE fires when there is no prior status recorded."""
    store = ActionStateStore()
    dq_action = _make_dq_action(_make_dq_alert(notify_on=NotifyOn.STATUS_CHANGE))
    ctx = _make_context()
    assert store.should_fire(dq_action, ctx, condition_result=True) is True


def test_status_change_fires_when_healthy() -> None:
    """STATUS_CHANGE fires when last status was HEALTHY (transition to UNHEALTHY)."""
    store = ActionStateStore()
    # Record a HEALTHY event (fired=False since condition was false → not fired)
    _pre_seed_store(
        store,
        action_name="default-action",
        fired=False,
        status=ActionStatus.HEALTHY,
        run_time=datetime(2024, 6, 1, 11, 0, 0, tzinfo=timezone.utc),
    )

    alert = _make_dq_alert(notify_on=NotifyOn.STATUS_CHANGE)
    dq_action = DQAction(action=alert, name="default-action")
    ctx = _make_context()
    assert store.should_fire(dq_action, ctx, condition_result=True) is True


def test_status_change_suppressed_when_already_unhealthy() -> None:
    """STATUS_CHANGE suppresses when last status was already UNHEALTHY."""
    store = ActionStateStore()
    _pre_seed_store(
        store,
        action_name="default-action",
        fired=True,
        status=ActionStatus.UNHEALTHY,
        run_time=datetime(2024, 6, 1, 11, 0, 0, tzinfo=timezone.utc),
    )

    alert = _make_dq_alert(notify_on=NotifyOn.STATUS_CHANGE)
    dq_action = DQAction(action=alert, name="default-action")
    ctx = _make_context()
    assert store.should_fire(dq_action, ctx, condition_result=True) is False


# ---------------------------------------------------------------------------
# seed
# ---------------------------------------------------------------------------


def test_seed_loads_state_from_store() -> None:
    """seed() populates in-memory state from the event store so restarts respect prior state."""
    base_time = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    prior_event = _make_event(action_name="my-action", fired=True, status=ActionStatus.UNHEALTHY, run_time=base_time)
    fake_store = FakeEventStore(initial_events={"my-action": prior_event})

    store = ActionStateStore(event_store=fake_store)
    store.seed()

    # Consequentially, a STATUS_CHANGE alert should be suppressed.
    alert = _make_dq_alert(notify_on=NotifyOn.STATUS_CHANGE)
    dq_action = DQAction(action=alert, name="my-action")
    ctx = _make_context(run_time=base_time + timedelta(hours=1))
    assert store.should_fire(dq_action, ctx, condition_result=True) is False


def test_seed_does_not_set_last_fired_for_unfired_event() -> None:
    """seed() does NOT suppress HOURLY when the seeded event was not fired (fired=False)."""
    base_time = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    # An event where fired=False means the action did not execute; last_fired must not be set.
    prior_event = _make_event(action_name="my-action", fired=False, status=ActionStatus.HEALTHY, run_time=base_time)
    fake_store = FakeEventStore(initial_events={"my-action": prior_event})

    store = ActionStateStore(event_store=fake_store)
    store.seed()

    # HOURLY should be allowed because last_fired was not set (fired=False in seed data).
    alert = _make_dq_alert(frequency=DQAlertFrequency.HOURLY)
    dq_action = DQAction(action=alert, name="my-action")
    ctx = _make_context(run_time=base_time + timedelta(minutes=30))
    assert store.should_fire(dq_action, ctx, condition_result=True) is True


def test_seed_keeps_frequency_suppression_when_latest_event_not_fired() -> None:
    """HOURLY suppression survives a restart even when the latest persisted event was suppressed (not fired).

    For a chronically unhealthy action the most recent event is a suppressed, non-fired UNHEALTHY row, so
    seeding *_last_fired* must come from the last *fired* event (load_last_fired_per_action), not the latest
    event. Otherwise the frequency window would be lost on restart and the alert would re-fire early.
    """
    base_time = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    # Latest persisted event: unhealthy but suppressed (fired=False), 30 min after the last actual fire.
    latest_event = _make_event(
        action_name="my-action",
        fired=False,
        status=ActionStatus.UNHEALTHY,
        run_time=base_time + timedelta(minutes=30),
    )
    # The action actually fired at base_time.
    fake_store = FakeEventStore(initial_events={"my-action": latest_event}, last_fired={"my-action": base_time})

    store = ActionStateStore(event_store=fake_store)
    store.seed()

    alert = _make_dq_alert(frequency=DQAlertFrequency.HOURLY)
    dq_action = DQAction(action=alert, name="my-action")
    # A run 45 min after the last fire is still inside the 1h window -> must stay suppressed.
    ctx = _make_context(run_time=base_time + timedelta(minutes=45))
    assert store.should_fire(dq_action, ctx, condition_result=True) is False


# ---------------------------------------------------------------------------
# record
# ---------------------------------------------------------------------------


def test_record_updates_memory_and_appends() -> None:
    """record() updates in-memory state and delegates to event_store.append."""
    fake_store = FakeEventStore()
    store = ActionStateStore(event_store=fake_store)
    evt = _make_event(action_name="my-action", fired=True, status=ActionStatus.UNHEALTHY)

    store.record(evt)

    # Verify in-memory state was updated by checking subsequent should_fire behaviour.
    alert = _make_dq_alert(frequency=DQAlertFrequency.HOURLY)
    dq_action = DQAction(action=alert, name="my-action")
    ctx_soon = _make_context(run_time=evt.run_time + timedelta(minutes=10))
    assert store.should_fire(dq_action, ctx_soon, condition_result=True) is False

    # event_store.append called with the event.
    assert len(fake_store.appended) == 1
    assert fake_store.appended[0] is evt


def test_record_does_not_update_last_fired_when_not_fired() -> None:
    """record() does NOT suppress frequency when event.fired is False."""
    fake_store = FakeEventStore()
    store = ActionStateStore(event_store=fake_store)
    evt = _make_event(action_name="my-action", fired=False, status=ActionStatus.HEALTHY)

    store.record(evt)

    # Because fired=False, last_fired should not be set → HOURLY should still fire.
    alert = _make_dq_alert(frequency=DQAlertFrequency.HOURLY)
    dq_action = DQAction(action=alert, name="my-action")
    ctx = _make_context(run_time=evt.run_time + timedelta(minutes=30))
    assert store.should_fire(dq_action, ctx, condition_result=True) is True


def test_record_without_event_store_updates_only_memory() -> None:
    """record() without an event store only updates in-memory state (no error)."""
    store = ActionStateStore(event_store=None)
    evt = _make_event(action_name="standalone", fired=True, status=ActionStatus.UNHEALTHY)

    store.record(evt)

    # Verify state was recorded: HOURLY suppressed immediately after.
    alert = _make_dq_alert(frequency=DQAlertFrequency.HOURLY)
    dq_action = DQAction(action=alert, name="standalone")
    ctx_soon = _make_context(run_time=evt.run_time + timedelta(minutes=5))
    assert store.should_fire(dq_action, ctx_soon, condition_result=True) is False


# ---------------------------------------------------------------------------
# Non-DQAlert action always fires
# ---------------------------------------------------------------------------


def test_non_alert_action_always_fires() -> None:
    """A DQAction whose action is NOT a DQAlert always fires when condition_result is True."""
    dq_action = DQAction(action=FailPipeline(name="simple"))
    store = ActionStateStore()
    ctx = _make_context()
    assert store.should_fire(dq_action, ctx, condition_result=True) is True
