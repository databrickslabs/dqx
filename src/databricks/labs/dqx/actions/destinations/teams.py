"""Microsoft Teams alert destination (Power Automate Workflows webhook).

Delivers DQX alert messages to a Microsoft Teams channel via a Power Automate
*Workflows* webhook ("Post to a channel when a webhook request is received"),
using the MessageCard payload format.

Microsoft retired the legacy Office 365 Connector incoming webhooks
(*webhook.office.com*) in May 2026; the Workflows webhook is the supported
replacement. Workflows still accepts MessageCard payloads — text and facts
render — but interactive elements such as buttons do not. Delivery is restricted
to the *logic.azure.com* Workflows webhook domain, whose URLs carry their
authorization in the URL signature so an anonymous POST works.
"""

from typing import ClassVar, Literal

from databricks.labs.dqx.actions.destinations.webhook_base import WebhookAlertDestination
from databricks.labs.dqx.actions.message import AlertMessage


class TeamsDQAlertDestination(WebhookAlertDestination):
    """Microsoft Teams destination using a Power Automate Workflows webhook.

    Posts a Teams MessageCard to a Workflows webhook URL created via the
    Workflows app in Teams ("Post to a channel when a webhook request is
    received"). The card includes an activity title with the alert title and a
    facts section derived from *message.fields*.

    The legacy Office 365 Connector webhooks (*webhook.office.com*) were retired
    by Microsoft in May 2026 and are no longer supported; create a Workflows
    webhook instead. Workflows renders the MessageCard text and facts but not
    interactive buttons.

    Note:
        Workflows webhook URLs on *logic.azure.com* carry their authorization in
        the URL (a signature query parameter), so an anonymous POST works. Newer
        *environment.api.powerplatform.com* endpoints require an Entra bearer
        token, which this destination does not send, so those endpoints are not
        supported and are not on the allowlist.

    Class attributes:
        allowed_host_suffixes: Restricts delivery to the *logic.azure.com*
            Workflows webhook host.

    Attributes:
        type: Discriminator literal, always *"teams"*.
        name: Logical name for this destination instance.
        webhook_url: The Teams Workflows webhook URL (plain string or *DQSecret*).
    """

    type: Literal["teams"] = "teams"
    # Only logic.azure.com is supported: Workflows webhooks there carry auth in the URL signature so
    # an anonymous POST works. environment.api.powerplatform.com endpoints require an Entra bearer
    # token that this destination does not send, so they are intentionally not allowed.
    allowed_host_suffixes: ClassVar[list[str] | None] = ["logic.azure.com"]
    url_contains_secret: ClassVar[bool] = True  # Teams Workflows URL embeds a signature; prefer DQSecret

    def _build_payload(self, message: AlertMessage) -> dict[str, object]:
        """Build a Teams MessageCard payload from *message*.

        Constructs a MessageCard with the alert title as *activityTitle* and
        all *message.fields* entries as facts inside a single section.

        Args:
            message: The alert message to render.

        Returns:
            A dict representing a valid Teams MessageCard payload with
            *"@type"*, *"@context"*, *"summary"*, and *"sections"*
            keys.
        """
        facts = [{"name": k, "value": v} for k, v in message.fields.items()]

        return {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "summary": message.summary,
            "sections": [
                {
                    "activityTitle": message.title,
                    "facts": facts,
                }
            ],
        }


__all__ = ["TeamsDQAlertDestination"]
