"""Callback alert destination for DQX actions.

Delivers DQX alert messages by invoking an in-process Python callable.
This destination is not persistable; the serializer skips it with a
warning when persisting destination configurations to storage.
"""

from collections.abc import Callable
from typing import Any, Literal

from pydantic import ValidationError, model_validator

from databricks.labs.dqx.actions.base import ActionContext, ActionServices
from databricks.labs.dqx.actions.destinations.base import AlertDestination
from databricks.labs.dqx.actions.message import AlertMessage
from databricks.labs.dqx.errors import InvalidActionError


class CallbackDQAlertDestination(AlertDestination):
    """In-process callback destination that invokes a Python callable on delivery.

    Useful for testing and for scenarios where alert handling must occur
    in the same process (e.g. raising exceptions, writing to an in-memory
    buffer, or triggering a custom side effect).

    This destination is intentionally not persistable.  The serializer skips
    instances of this class with a warning rather than attempting to persist
    the *callback* field.  It is still a valid runtime destination.

    Attributes:
        type: Discriminator literal, always *"callback"*.
        name: Logical name for this destination instance.
        callback: Python callable invoked by *deliver*.  Must accept an
            *AlertMessage* and an *ActionContext* and return *None*.
    """

    type: Literal["callback"] = "callback"
    callback: Callable[[AlertMessage, ActionContext], None]

    def __init__(self, **data: Any) -> None:
        try:
            super().__init__(**data)
        except ValidationError as exc:
            raise InvalidActionError(str(exc)) from exc

    @model_validator(mode="after")
    def _validate_callback(self) -> "CallbackDQAlertDestination":
        """Validate that *callback* is callable.

        Returns:
            This destination instance.

        Raises:
            InvalidActionError: If *callback* is not callable.
        """
        if not callable(self.callback):
            raise InvalidActionError(
                f"CallbackDQAlertDestination.callback must be callable, got {type(self.callback).__name__!r}"
            )
        return self

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
