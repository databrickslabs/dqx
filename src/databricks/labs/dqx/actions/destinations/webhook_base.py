"""Abstract base for webhook-based alert destinations.

*WebhookAlertDestination* handles the common delivery logic — URL resolution,
auth building, and the actual HTTP POST — while delegating payload assembly to
concrete subclasses via *_build_payload*.
"""

import abc
from typing import ClassVar

from pydantic import model_validator

from databricks.labs.dqx.actions.base import ActionContext, ActionServices
from databricks.labs.dqx.actions.delivery import WebhookAuth
from databricks.labs.dqx.actions.destinations.base import AlertDestination
from databricks.labs.dqx.actions.message import AlertMessage
from databricks.labs.dqx.actions.secret_field import SecretOrStr
from databricks.labs.dqx.errors import InvalidActionError


class WebhookAlertDestination(AlertDestination, abc.ABC):
    """Abstract Pydantic base for webhook alert destinations.

    Provides a concrete *deliver* implementation and exposes two extension
    hooks: *_build_payload* (required) for producing the wire-format payload,
    and *_build_auth* (optional, returns *None* by default) for attaching HTTP
    Basic-auth credentials.

    Subclasses declare *type* (the literal discriminator) and may override
    *allowed_host_suffixes*, and implement *_build_payload*.

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

    webhook_url: SecretOrStr

    @model_validator(mode="after")
    def _validate_webhook_url(self) -> "WebhookAlertDestination":
        """Validate that *webhook_url* is a non-empty string or a *DQSecret*.

        Returns:
            This destination instance.

        Raises:
            InvalidActionError: If *webhook_url* is an empty string.
        """
        if isinstance(self.webhook_url, str) and not self.webhook_url:
            raise InvalidActionError("AlertDestination 'webhook_url' must be a non-empty string or DQSecret.")
        return self

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
