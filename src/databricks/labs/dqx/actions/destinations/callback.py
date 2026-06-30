"""Callback alert destination for DQX actions.

Delivers DQX alert messages by invoking an in-process Python callable.
This destination is not serializable; Task 11's serializer skips it with
a warning when persisting destination configurations to storage.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import ClassVar

from databricks.labs.dqx.actions.base import ActionContext, ActionServices
from databricks.labs.dqx.actions.destinations.base import AlertDestination
from databricks.labs.dqx.actions.message import AlertMessage
from databricks.labs.dqx.errors import InvalidActionError


@dataclass(eq=False)
class CallbackDQAlertDestination(AlertDestination):
    """In-process callback destination that invokes a Python callable on delivery.

    Useful for testing and for scenarios where alert handling must occur
    in the same process (e.g. raising exceptions, writing to an in-memory
    buffer, or triggering a custom side effect).

    This destination is intentionally not serializable.  Task 11's
    serializer skips instances of this class with a warning rather than
    attempting to persist the *callback* field.

    Class attributes:
        type: Always ``"callback"``.

    Attributes:
        name: Logical name for this destination instance.
        callback: Python callable invoked by *deliver*.  Must accept an
            *AlertMessage* and an *ActionContext* and return *None*.
    """

    type: ClassVar[str] = "callback"
    name: str
    callback: Callable[[AlertMessage, ActionContext], None]

    def validate(self) -> None:
        """Validate that *name* is non-empty and *callback* is callable.

        Raises:
            InvalidActionError: If *name* is empty or *callback* is not callable.
        """
        if not self.name:
            raise InvalidActionError("CallbackDQAlertDestination.name must not be empty")
        if not callable(self.callback):
            raise InvalidActionError(
                f"CallbackDQAlertDestination.callback must be callable, got {type(self.callback).__name__!r}"
            )

    def deliver(self, message: AlertMessage, context: ActionContext, _services: ActionServices) -> None:
        """Invoke *callback* with *message* and *context*.

        The *_services* parameter is unused by this destination because delivery
        is entirely in-process; it is accepted to satisfy the *AlertDestination*
        interface.

        Args:
            message: Immutable alert message payload assembled by *StandardMessageBuilder*.
            context: Immutable snapshot of run-time state for the DQX run.
        """
        self.callback(message, context)


__all__ = ["CallbackDQAlertDestination"]
