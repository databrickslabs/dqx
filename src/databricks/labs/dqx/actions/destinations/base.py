"""Abstract base class for DQX alert destinations.

Defines *AlertDestination*, the contract that all concrete destination
adapters (Slack, Teams, generic webhook, …) must satisfy.
"""

from __future__ import annotations

import abc
from typing import ClassVar

from databricks.labs.dqx.actions.base import ActionContext, ActionServices
from databricks.labs.dqx.actions.message import AlertMessage


class AlertDestination(abc.ABC):
    """Abstract base for all DQX alert destination implementations.

    Subclasses must set *type* as a class-level string identifier and
    override *deliver* to send the alert via their own transport.  They may
    optionally override *validate* to check their own configuration at
    construction time.

    Class attributes:
        type: Short string identifier for this destination type (e.g.
            ``"slack"``, ``"teams"``, ``"webhook"``).  Set by each
            concrete subclass.

    Instance attributes:
        name: Logical name for this destination instance.  Set by subclasses.
    """

    type: ClassVar[str]
    name: str

    def validate(self) -> None:
        """Validate this destination's configuration.

        The default implementation is a no-op.  Override in subclasses to
        raise *InvalidActionError* when the destination's own configuration
        is invalid.

        Raises:
            InvalidActionError: If the destination configuration is invalid.
        """

    @abc.abstractmethod
    def deliver(self, message: AlertMessage, context: ActionContext, services: ActionServices) -> None:
        """Deliver *message* to this destination.

        Args:
            message: Immutable alert message payload assembled by *StandardMessageBuilder*.
            context: Immutable snapshot of run-time state for the DQX run.
            services: Injected services (secret resolver, webhook client, etc.).
        """


__all__ = ["AlertDestination"]
