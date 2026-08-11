"""Abstract base class for DQX alert destinations.

Defines *AlertDestination*, the contract that all concrete destination
adapters (Slack, Teams, generic webhook, …) must satisfy.
"""

import abc

from pydantic import BaseModel, model_validator

from databricks.labs.dqx.actions.base import ActionContext, ActionServices
from databricks.labs.dqx.actions.message import AlertMessage
from databricks.labs.dqx.errors import InvalidActionError


class AlertDestination(BaseModel, abc.ABC):
    """Abstract Pydantic base for all DQX alert destination implementations.

    Subclasses declare a literal *type* discriminator field and override
    *deliver* to send the alert via their own transport.  Construction-time
    validation of a subclass's own configuration is performed by Pydantic
    validators rather than a separate *validate* method.

    Attributes:
        name: Logical name for this destination instance.  Must be non-empty.
    """

    model_config = {"arbitrary_types_allowed": True}

    name: str

    @model_validator(mode="after")
    def _validate_name(self) -> "AlertDestination":
        """Validate that *name* is a non-empty string.

        Returns:
            This destination instance.

        Raises:
            InvalidActionError: If *name* is empty.
        """
        if not self.name:
            raise InvalidActionError("AlertDestination 'name' must be a non-empty string.")
        return self

    @abc.abstractmethod
    def deliver(self, message: AlertMessage, context: ActionContext, services: ActionServices) -> None:
        """Deliver *message* to this destination.

        Args:
            message: Immutable alert message payload assembled by *StandardMessageBuilder*.
            context: Immutable snapshot of run-time state for the DQX run.
            services: Injected services (secret resolver, webhook client, etc.).
        """


__all__ = ["AlertDestination"]
