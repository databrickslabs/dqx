"""Abstract base for webhook-based alert destinations.

*WebhookAlertDestination* handles the common delivery logic — URL resolution,
auth building, and the actual HTTP POST — while delegating payload assembly to
concrete subclasses via *_build_payload*.
"""

from __future__ import annotations

import abc
from dataclasses import dataclass
from typing import ClassVar

from databricks.labs.dqx.actions.base import ActionContext, ActionServices
from databricks.labs.dqx.actions.delivery import WebhookAuth
from databricks.labs.dqx.actions.destinations.base import AlertDestination
from databricks.labs.dqx.actions.message import AlertMessage
from databricks.labs.dqx.config import DQSecret
from databricks.labs.dqx.errors import InvalidActionError


@dataclass
class WebhookAlertDestination(AlertDestination, abc.ABC):
    """Abstract dataclass base for webhook alert destinations.

    Provides concrete *deliver* and *validate* implementations, and exposes
    two extension hooks: *_build_payload* (required) for producing the
    wire-format payload, and *_build_auth* (optional, returns *None* by
    default) for attaching HTTP Basic-auth credentials.

    Subclasses set *type* and *allowed_host_suffixes* as class variables, and
    implement *_build_payload*.

    Class attributes:
        allowed_host_suffixes: Optional list of allowed host suffixes passed
            to *WebhookClient.post*.  *None* means no restriction (use for
            internal or generic webhooks).

    Attributes:
        name: Logical name for this destination instance.
        webhook_url: The webhook endpoint URL, either as a plain string or as
            a *DQSecret* scope/key reference resolved at delivery time.
    """

    allowed_host_suffixes: ClassVar[list[str] | None] = None

    name: str
    webhook_url: str | DQSecret

    def validate(self) -> None:
        """Validate *name* and *webhook_url* are non-empty.

        Raises:
            InvalidActionError: If *name* is empty, or if *webhook_url* is an
                empty string.
        """
        if not self.name:
            raise InvalidActionError("AlertDestination 'name' must be a non-empty string.")
        if isinstance(self.webhook_url, str) and not self.webhook_url:
            raise InvalidActionError("AlertDestination 'webhook_url' must be a non-empty string or DQSecret.")

    @abc.abstractmethod
    def _build_payload(self, message: AlertMessage) -> dict[str, object]:
        """Build the wire-format payload dict for *message*.

        Args:
            message: The alert message to render.

        Returns:
            A JSON-serialisable dict representing the notification payload.
        """

    def _build_auth(self, _services: ActionServices) -> WebhookAuth | None:
        """Build HTTP Basic-auth credentials, if applicable.

        The default implementation returns *None* (no auth).  Override in
        subclasses that support credential injection.  The *services* argument
        is available to overrides for secret resolution; the base
        implementation does not use it.

        Returns:
            A *WebhookAuth* instance when credentials are available, or *None*.
        """
        return None

    def deliver(self, message: AlertMessage, context: ActionContext, services: ActionServices) -> None:
        """Resolve the URL, build the payload, and POST it to the endpoint.

        Args:
            message: Immutable alert message to deliver.
            context: Immutable snapshot of run-time DQX state.
            services: Injected services (secret resolver, webhook client, etc.).
        """
        resolved_url = services.secret_resolver.resolve(self.webhook_url)
        services.webhook_client.post(
            resolved_url,
            self._build_payload(message),
            auth=self._build_auth(services),
            allowed_host_suffixes=self.allowed_host_suffixes,
        )


__all__ = ["WebhookAlertDestination"]
