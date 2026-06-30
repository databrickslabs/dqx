"""DQAlert action and associated enumerations.

This module defines *DQAlertFrequency*, *NotifyOn*, and *DQAlert* — the
primary alerting action for the DQX actions subsystem.  *DQAlert* dispatches
an *AlertMessage* to one or more *AlertDestination* instances concurrently
using *Threads.gather* so that a single delivery failure cannot block the
remaining destinations.
"""

from __future__ import annotations

import enum
import logging
import re
from dataclasses import dataclass

from databricks.labs.blueprint.parallel import Threads

from databricks.labs.dqx.actions.base import Action, ActionContext, ActionResult, ActionServices, ActionStatus
from databricks.labs.dqx.actions.destinations.base import AlertDestination
from databricks.labs.dqx.actions.message import AlertMessage, StandardMessageBuilder
from databricks.labs.dqx.errors import InvalidActionError

logger = logging.getLogger(__name__)

# Pre-compiled pattern for sanitizing control characters (CWE-117).
_CONTROL_CHAR_RE = re.compile(r"[\r\n\t\x00-\x1f\x7f]")


def _sanitize(text: str) -> str:
    """Strip newlines and control characters from *text* to prevent log injection.

    Replaces all characters in the Unicode control-character range (U+0000–U+001F
    and U+007F), including CR (``\\r``) and LF (``\\n``), with a space.

    Args:
        text: Raw string that may contain control characters.

    Returns:
        A copy of *text* with all control characters replaced by a space.
    """
    return _CONTROL_CHAR_RE.sub(" ", text)


class DQAlertFrequency(enum.Enum):
    """Controls how often a *DQAlert* may fire relative to prior alerts.

    Attributes:
        ALWAYS: Fire on every DQX run where the condition evaluates to *True*.
        HOURLY: Fire at most once per hour.
        DAILY: Fire at most once per day.
    """

    ALWAYS = "always"
    HOURLY = "hourly"
    DAILY = "daily"


class NotifyOn(enum.Enum):
    """Controls which state transitions cause *DQAlert* to notify.

    Attributes:
        EACH: Send a notification every time the condition fires.
        STATUS_CHANGE: Send a notification only when the quality status
            changes (e.g. healthy → unhealthy or vice-versa).
    """

    EACH = "each"
    STATUS_CHANGE = "status_change"


def _make_deliver_task(
    destination: AlertDestination,
    message: AlertMessage,
    context: ActionContext,
    services: ActionServices,
    error_map: dict[str, str],
) -> None:
    """Deliver *message* to *destination*, recording any error into *error_map*.

    This function is called as a *Threads.gather* task.  Errors are captured
    inside the task body so that *Threads.gather* sees a successful return (the
    ``None`` sentinel) for every destination — the isolation logic lives here,
    not in the caller.

    Args:
        destination: The *AlertDestination* that should receive *message*.
        message: The *AlertMessage* to deliver.
        context: Run-time context passed through to *destination.deliver*.
        services: Injected services passed through to *destination.deliver*.
        error_map: Shared dict (mutated in-place) that maps destination name
            to a sanitized error string when delivery fails.
    """
    try:
        destination.deliver(message, context, services)
    except Exception as exc:  # broad catch is intentional: isolation boundary
        sanitized = _sanitize(str(exc))
        logger.warning(f"DQAlert destination '{destination.name}' failed: {sanitized}")
        error_map[destination.name] = sanitized


@dataclass
class DQAlert(Action):
    """Sends alert notifications to one or more *AlertDestination* instances.

    When *execute* is called the action builds a single *AlertMessage* via
    *StandardMessageBuilder* and dispatches it to every configured destination
    concurrently.  A failure in one destination is isolated: the remaining
    destinations are still attempted and the error is recorded in
    *ActionResult.destination_errors* rather than re-raised.

    Attributes:
        destinations: One or more *AlertDestination* adapters that receive the
            alert (Slack, Teams, webhook, …).  Must not be empty.
        name: Logical identifier for this alert action; defaults to ``"alert"``.
        alert_frequency: Controls how often alerts may be sent; defaults to
            *DQAlertFrequency.ALWAYS*.
        notify_on: Controls which state transitions trigger a notification;
            defaults to *NotifyOn.EACH*.
        severity: Alert severity level included in the message payload;
            defaults to ``"error"``.
    """

    destinations: list[AlertDestination]
    name: str = ""
    alert_frequency: DQAlertFrequency = DQAlertFrequency.ALWAYS
    notify_on: NotifyOn = NotifyOn.EACH
    severity: str = "error"

    def __post_init__(self) -> None:
        if not self.name:
            self.name = "alert"

    def validate(self) -> None:
        """Validate that at least one destination is configured and each destination is valid.

        Raises:
            InvalidActionError: If *destinations* is empty.
            InvalidActionError: If any destination's own *validate()* raises.
        """
        if not self.destinations:
            raise InvalidActionError("DQAlert must have at least one destination configured.")
        for destination in self.destinations:
            destination.validate()

    def execute(self, context: ActionContext, services: ActionServices) -> ActionResult:
        """Build an alert message and deliver it concurrently to all destinations.

        Builds a single *AlertMessage* from *context* using
        *StandardMessageBuilder*, then dispatches it to every destination in
        *self.destinations* concurrently via *Threads.gather*.  Delivery
        failures are isolated per-destination: a failure in one destination
        does not prevent the others from being attempted.

        The action always returns *fired=True* and *status=UNHEALTHY* — it is
        only called when a condition has already been found to be True (i.e. a
        data quality violation was detected), so the status always reflects an
        unhealthy state.

        Args:
            context: Immutable snapshot of run-time state including metrics,
                run identifiers, and location metadata.
            services: Injected services (secret resolver, webhook client, etc.).

        Returns:
            An *ActionResult* with *fired=True*, *status=UNHEALTHY*, and
            *destination_errors* populated for any destinations that failed.
        """
        message = StandardMessageBuilder.build(
            action_name=self.name,
            condition=None,
            metrics=context.metrics,
            run_id=context.run_id,
            run_time=context.run_time,
            table=context.input_location,
            severity=self.severity,
        )

        # error_map is populated inside each task via _make_deliver_task, which
        # catches exceptions so Threads.gather never sees a task-level failure.
        # This gives us both concurrency and per-destination error isolation.
        error_map: dict[str, str] = {}

        tasks = [
            lambda dest=destination: _make_deliver_task(dest, message, context, services, error_map)
            for destination in self.destinations
        ]
        Threads.gather("dqx-alert", tasks)

        return ActionResult(
            action_name=self.name,
            fired=True,
            status=ActionStatus.UNHEALTHY,
            destination_errors=error_map,
        )


__all__ = [
    "DQAlert",
    "DQAlertFrequency",
    "NotifyOn",
]
