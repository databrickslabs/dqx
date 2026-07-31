"""Generic webhook alert destination with optional Basic-auth support.

Delivers DQX alert messages to arbitrary HTTPS webhook endpoints as a
canonical DQX JSON payload.  Unlike the Slack and Teams destinations there
is no host restriction — any HTTPS endpoint is accepted.  Optional
*username* and *password* fields (plain strings or *DQSecret* references)
enable HTTP Basic-auth.
"""

from typing import ClassVar, Literal

from pydantic import Field, model_validator

from databricks.labs.dqx.actions.base import ActionServices
from databricks.labs.dqx.actions.delivery import WebhookAuth
from databricks.labs.dqx.actions.destinations.webhook_base import WebhookAlertDestination
from databricks.labs.dqx.actions.message import AlertMessage
from databricks.labs.dqx.actions.secret_field import SecretOrStr
from databricks.labs.dqx.errors import InvalidActionError


class DQWebhookAlertDestination(WebhookAlertDestination):
    """Generic HTTPS webhook destination with optional Basic-auth.

    Posts a canonical DQX JSON payload to any HTTPS endpoint.  No host
    suffix restrictions are applied.  When both *username* and *password*
    are provided they are resolved (via the secret resolver when given as
    *DQSecret* references) and packaged into a *WebhookAuth* passed to the
    *WebhookClient*.

    Class attributes:
        allowed_host_suffixes: *None* — no host restriction.

    Attributes:
        type: Discriminator literal, always *"webhook"*.
        name: Logical name for this destination instance.
        webhook_url: The endpoint URL (plain string or *DQSecret*).
        username: Optional Basic-auth username (plain string or *DQSecret*).
            Both *username* and *password* must be set for auth to be applied.
        password: Optional Basic-auth password (plain string or *DQSecret*).
            Both *username* and *password* must be set for auth to be applied.
            Treat plaintext values as development-only; use *DQSecret* in
            production.
    """

    type: Literal["webhook"] = "webhook"
    allowed_host_suffixes: ClassVar[list[str] | None] = None

    username: SecretOrStr | None = Field(default=None)
    password: SecretOrStr | None = Field(default=None)

    @model_validator(mode="after")
    def _validate_auth_pair(self) -> "DQWebhookAlertDestination":
        """Reject a half-configured Basic-auth credential.

        Basic-auth needs both *username* and *password*. Providing exactly one is almost certainly a
        mistake, and silently sending an unauthenticated request (the previous behavior) could leak a
        payload to an endpoint that expected auth. Fail fast at construction instead.

        Raises:
            InvalidActionError: If exactly one of *username* / *password* is set.
        """
        # XOR: `!=` on the two "is None" booleans is True only when exactly one of username/password
        # is set (both-set and both-None are allowed; exactly-one is the misconfiguration).
        if (self.username is None) != (self.password is None):
            raise InvalidActionError(
                "DQWebhookAlertDestination Basic-auth requires both 'username' and 'password' to be set "
                "(or neither); exactly one was provided."
            )
        return self

    def _build_auth(self, services: ActionServices) -> WebhookAuth | None:
        """Build *WebhookAuth* when both *username* and *password* are set.

        Credentials are resolved through *services.secret_resolver* before
        being packaged — this handles both plain strings and *DQSecret*
        references transparently.

        Construction enforces that *username* and *password* are set together (see
        *_validate_auth_pair*), so in normal use this returns *WebhookAuth* when both are present and
        *None* when neither is. The *is None* guard here is a defensive backstop for the case where a
        field is cleared by post-construction mutation (the model does not set *validate_assignment*).

        Args:
            services: Injected services containing the secret resolver.

        Returns:
            A *WebhookAuth* instance when both credentials are present, or *None* when neither is
            (or, defensively, if one was cleared after construction).
        """
        if self.username is None or self.password is None:
            return None
        resolved_username = services.secret_resolver.resolve(self.username)
        resolved_password = services.secret_resolver.resolve(self.password)
        return WebhookAuth(username=resolved_username, password=resolved_password)

    def _build_payload(self, message: AlertMessage) -> dict[str, object]:
        """Build a canonical DQX JSON payload from *message*.

        Args:
            message: The alert message to render.

        Returns:
            A dict with keys: *title*, *summary*, *condition*, *table*,
            *run_id*, *run_time* (ISO 8601 string), *severity*,
            *observed_metrics*, and *fields*.
        """
        return {
            "title": message.title,
            "summary": message.summary,
            "condition": message.condition,
            "table": message.table,
            "run_id": message.run_id,
            "run_time": message.run_time.isoformat(),
            "severity": message.severity,
            "observed_metrics": dict(message.observed_metrics),
            "fields": dict(message.fields),
        }


__all__ = ["DQWebhookAlertDestination"]
