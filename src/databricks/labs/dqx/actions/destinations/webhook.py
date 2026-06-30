"""Generic webhook alert destination with optional Basic-auth support.

Delivers DQX alert messages to arbitrary HTTPS webhook endpoints as a
canonical DQX JSON payload.  Unlike the Slack and Teams destinations there
is no host restriction — any HTTPS endpoint is accepted.  Optional
*username* and *password* fields (plain strings or *DQSecret* references)
enable HTTP Basic-auth.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import ClassVar

from databricks.labs.dqx.actions.base import ActionServices
from databricks.labs.dqx.actions.delivery import WebhookAuth
from databricks.labs.dqx.actions.destinations.webhook_base import WebhookAlertDestination
from databricks.labs.dqx.actions.message import AlertMessage
from databricks.labs.dqx.config import DQSecret


@dataclass
class WebhookDQAlertDestination(WebhookAlertDestination):
    """Generic HTTPS webhook destination with optional Basic-auth.

    Posts a canonical DQX JSON payload to any HTTPS endpoint.  No host
    suffix restrictions are applied.  When both *username* and *password*
    are provided they are resolved (via the secret resolver when given as
    *DQSecret* references) and packaged into a *WebhookAuth* passed to the
    *WebhookClient*.

    Class attributes:
        type: Always ``"webhook"``.
        allowed_host_suffixes: *None* — no host restriction.

    Attributes:
        name: Logical name for this destination instance.
        webhook_url: The endpoint URL (plain string or *DQSecret*).
        username: Optional Basic-auth username (plain string or *DQSecret*).
            Both *username* and *password* must be set for auth to be applied.
        password: Optional Basic-auth password (plain string or *DQSecret*).
            Both *username* and *password* must be set for auth to be applied.
            Treat plaintext values as development-only; use *DQSecret* in
            production.
    """

    type: ClassVar[str] = "webhook"
    allowed_host_suffixes: ClassVar[list[str] | None] = None

    username: str | DQSecret | None = field(default=None)
    password: str | DQSecret | None = field(default=None)

    def _build_auth(self, services: ActionServices) -> WebhookAuth | None:
        """Build *WebhookAuth* when both *username* and *password* are set.

        Credentials are resolved through *services.secret_resolver* before
        being packaged — this handles both plain strings and *DQSecret*
        references transparently.

        Args:
            services: Injected services containing the secret resolver.

        Returns:
            A *WebhookAuth* instance when both credentials are present, or
            *None* when either is absent.
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


__all__ = ["WebhookDQAlertDestination"]
