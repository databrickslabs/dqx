"""Alert state store and event persistence interfaces for the DQX actions subsystem.

This module defines:

- *AlertEvent* — immutable record of a single action execution outcome.
- *ActionEventStore* — abstract interface for persisting and loading alert events.
- *ActionStateStore* — in-memory state manager that evaluates whether an alert
  should fire based on frequency windows (*HOURLY*, *DAILY*) and status-change
  semantics (*STATUS_CHANGE* vs *EACH*), optionally seeded from a persistent
  *ActionEventStore* so that state survives process restarts.
"""

import abc
import logging
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta

from databricks.labs.dqx.actions.alert import DQAlert, DQAlertFrequency, NotifyOn
from databricks.labs.dqx.actions.base import ActionContext, ActionStatus
from databricks.labs.dqx.utils import to_utc
from databricks.labs.dqx.actions.dq_action import DQAction
from databricks.labs.dqx.actions.log_sanitize import sanitize_for_log

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# AlertEvent
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AlertEvent:
    """Immutable record of a single DQX action execution outcome.

    Attributes:
        action_name: Logical name of the *DQAction* that was evaluated.
        condition: The condition expression string that gated the action, or
            *None* when the action fires unconditionally.
        fired: Whether the action executed (condition evaluated to *True* AND
            frequency/status-change checks passed).
        status: Aggregate *ActionStatus* of the execution.
        observed_metrics: Snapshot of the metrics observed during the run.
        run_id: Unique identifier for the DQX run that produced this event.
        run_time: Timestamp when the DQX run executed.
        input_location: Source path/URI of the data being checked, or *None*.
        destinations: Names of the destinations that were targeted.
        delivery_errors: Error messages for any destinations that failed delivery.
        run_config_name: Run configuration the event belongs to, for auditing. Populated from the
            events table on load. On append the event store stamps its own run configuration (from its
            storage config), so this field is informational and is not consulted on write; defaults to
            *default* for engines that are not scoped to a run config.
    """

    action_name: str
    condition: str | None
    fired: bool
    status: ActionStatus
    observed_metrics: dict[str, object]
    run_id: str
    run_time: datetime
    input_location: str | None
    destinations: list[str]
    delivery_errors: list[str]
    run_config_name: str = "default"


# ---------------------------------------------------------------------------
# ActionEventStore
# ---------------------------------------------------------------------------


class ActionEventStore(abc.ABC):
    """Abstract interface for persisting and loading *AlertEvent* records.

    Concrete implementations include *TableActionEventStore* (Delta table via
    Spark) and *LakebaseActionEventStore* (PostgreSQL via SQLAlchemy).
    """

    @abc.abstractmethod
    def append(self, events: list[AlertEvent]) -> None:
        """Persist *events* to the backing store.

        Args:
            events: One or more *AlertEvent* records to append.
        """

    @abc.abstractmethod
    def load_latest_per_action(self) -> dict[str, AlertEvent]:
        """Load the most recent *AlertEvent* for each distinct *action_name*.

        Returns:
            A mapping of *action_name* to the latest *AlertEvent* for that
            action.  Returns an empty dict when the store has no data.
        """

    @abc.abstractmethod
    def load_last_fired_per_action(self) -> dict[str, datetime]:
        """Load the *run_time* of the most recent **fired** event for each *action_name*.

        Unlike *load_latest_per_action* — whose latest row may be a non-fired, suppressed
        evaluation — this surfaces the last time each action actually fired. Seeding
        *_last_fired* from it keeps *HOURLY* / *DAILY* frequency suppression durable across a
        process restart even while an action stays unhealthy (and thus keeps recording
        non-fired events).

        Returns:
            A mapping of *action_name* to the *run_time* of its most recent fired event.
            Returns an empty dict when the store has no fired events.
        """


# ---------------------------------------------------------------------------
# ActionStateStore
# ---------------------------------------------------------------------------


class ActionStateStore:
    """In-memory state manager for DQX alert frequency and status-change gating.

    Maintains a per-action record of:

    - *last_fired_time* — the *run_time* of the most recent run where the
      action actually fired (i.e. *event.fired* was *True*).
    - *last_status* — the *ActionStatus* recorded by the most recent event,
      regardless of whether the action fired.

    These are consulted by *should_fire* to suppress repeated alerts within
    *HOURLY* / *DAILY* windows and to gate *STATUS_CHANGE* notifications.

    When an *event_store* is provided, *seed()* hydrates the in-memory maps
    from persistent storage so that state survives process restarts, and
    *record()* propagates new events to the store in addition to updating
    the in-memory maps.

    Args:
        event_store: Optional persistent event store.  When *None*, state is
            purely in-memory and does not survive restarts.
    """

    def __init__(self, event_store: ActionEventStore | None = None) -> None:
        self._event_store = event_store
        # action_name -> last run_time where the action fired
        self._last_fired: dict[str, datetime] = {}
        # action_name -> last recorded ActionStatus
        self._last_status: dict[str, ActionStatus] = {}
        # Guards the in-memory maps. In the multi-run-config runner each run config gets its own
        # engine and its own state store, but a single store may still be driven concurrently (e.g. a
        # streaming query's progress callback), so should_fire/record/seed must be serialized. The
        # maps are keyed by action_name only; cross-run-config isolation comes from each store being
        # scoped to one run config (its event store filters by run_config_name).
        self._lock = threading.RLock()

    def seed(self) -> None:
        """Hydrate in-memory state from the persistent event store.

        If no *event_store* was provided, this is a no-op.  Otherwise the in-memory maps are
        reset and repopulated:

        - *_last_status*: set from the latest *AlertEvent* per action.
        - *_last_fired*: set from the latest **fired** event per action, loaded independently
          (*load_last_fired_per_action*) so a streak of suppressed, non-fired unhealthy
          evaluations does not erase the durable last-fired timestamp that *HOURLY* / *DAILY*
          suppression relies on.
        """
        if self._event_store is None:
            return

        latest = self._event_store.load_latest_per_action()
        last_fired = self._event_store.load_last_fired_per_action()
        with self._lock:
            # Reset before repopulating so a re-seed cannot layer stale state under new data.
            self._last_fired.clear()
            self._last_status.clear()
            for action_name, event in latest.items():
                self._last_status[action_name] = event.status
            for action_name, run_time in last_fired.items():
                self._last_fired[action_name] = run_time
        logger.info(f"Seeded action state for {len(latest)} action(s) from event store.")

    def should_fire(self, dq_action: DQAction, context: ActionContext, condition_result: bool) -> bool:
        """Decide whether *dq_action* should fire for this run.

        Decision logic:

        1. If *condition_result* is *False* → do **not** fire.
        2. If *dq_action.action* is **not** a *DQAlert* → fire (no
           frequency/status gating for non-alert actions).
        3. If *dq_action.action* **is** a *DQAlert*:
           a. **Frequency check** — based on *alert.alert_frequency*:
              - *ALWAYS*: frequency always allows fire.
              - *HOURLY*: suppress if last fire was less than 1 hour ago.
              - *DAILY*: suppress if last fire was less than 24 hours ago.
           b. **Notify-on check** — based on *alert.notify_on*:
              - *EACH*: always allows fire.
              - *STATUS_CHANGE*: fire only when the recorded *last_status* is
                **not** *UNHEALTHY* (i.e., fire on transition to UNHEALTHY;
                suppress if already UNHEALTHY).
           Both checks must pass for the action to fire. The frequency window is an **absolute
           cap** evaluated *before* the notify-on gate: while an *HOURLY* / *DAILY* window is
           active the action is suppressed even when *STATUS_CHANGE* would otherwise fire on a
           genuine recovery→failure transition. Use *ALWAYS* if every transition must alert.

        All time comparisons use *context.run_time*; *datetime.now()* is never
        called.

        Args:
            dq_action: The bound action configuration being evaluated.
            context: Immutable run-time snapshot carrying *run_time* and
                *metrics*.
            condition_result: Result of evaluating *dq_action.condition*
                against *context.metrics*; *True* means the condition passed.

        Returns:
            *True* if the action should execute this run; *False* otherwise.
        """
        if not condition_result:
            return False

        if not isinstance(dq_action.action, DQAlert):
            return True

        alert: DQAlert = dq_action.action
        action_name = dq_action.name
        safe_name = sanitize_for_log(action_name)  # action_name is operator-supplied (CWE-117)

        # --- Frequency gate ---
        frequency_allows = self._check_frequency(alert.alert_frequency, action_name, context.run_time)
        if not frequency_allows:
            logger.debug(f"Action '{safe_name}' suppressed by frequency window ({alert.alert_frequency}).")
            return False

        # --- Notify-on gate ---
        notify_allows = self._check_notify_on(alert.notify_on, action_name)
        if not notify_allows:
            logger.debug(f"Action '{safe_name}' suppressed by notify_on={alert.notify_on} (already UNHEALTHY).")
            return False

        return True

    def record(self, event: AlertEvent) -> None:
        """Record *event* in in-memory state and, optionally, the persistent store.

        In-memory updates:
        - *_last_fired* is updated to *event.run_time* only when
          *event.fired* is *True*.
        - *_last_status* is always updated to *event.status*.

        If an *event_store* was provided, *event_store.append([event])* is
        called to persist the record.

        Args:
            event: The *AlertEvent* to record.
        """
        with self._lock:
            if event.fired:
                self._last_fired[event.action_name] = event.run_time
            self._last_status[event.action_name] = event.status

            if self._event_store is not None:
                self._event_store.append([event])

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _check_frequency(self, frequency: DQAlertFrequency, action_name: str, run_time: datetime) -> bool:
        """Return *True* when the frequency window permits firing.

        Args:
            frequency: The *DQAlertFrequency* configured on the alert.
            action_name: Logical name of the action (used to look up prior state).
            run_time: Current run timestamp from *ActionContext.run_time*.

        Returns:
            *True* when frequency allows the action to fire.
        """
        if frequency == DQAlertFrequency.ALWAYS:
            return True

        last_fired = self._last_fired.get(action_name)
        if last_fired is None:
            return True

        elapsed = to_utc(run_time) - to_utc(last_fired)
        if frequency == DQAlertFrequency.HOURLY:
            return elapsed >= timedelta(hours=1)
        if frequency == DQAlertFrequency.DAILY:
            return elapsed >= timedelta(hours=24)

        return True

    def _check_notify_on(self, notify_on: NotifyOn, action_name: str) -> bool:
        """Return *True* when the notify-on policy permits firing.

        For *EACH*, this always returns *True*.  For *STATUS_CHANGE*, it
        returns *True* only when the last recorded status is **not**
        *UNHEALTHY* — i.e. there is a transition to UNHEALTHY.

        Args:
            notify_on: The *NotifyOn* policy configured on the alert.
            action_name: Logical name of the action.

        Returns:
            *True* when the notify-on policy allows the action to fire.
        """
        if notify_on == NotifyOn.EACH:
            return True

        # STATUS_CHANGE: fire only when transitioning to UNHEALTHY.
        last_status = self._last_status.get(action_name)
        return last_status != ActionStatus.UNHEALTHY


__all__ = [
    "ActionEventStore",
    "ActionStateStore",
    "AlertEvent",
]
