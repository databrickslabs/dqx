"""Microsoft Teams MessageCard alert destination.

Delivers DQX alert messages to Microsoft Teams incoming webhook URLs using
the legacy MessageCard format.  Hosts are restricted to ``webhook.office.com``
and ``office.com`` suffixes.
"""

from __future__ import annotations

from typing import ClassVar, Literal

from databricks.labs.dqx.actions.destinations.webhook_base import WebhookAlertDestination
from databricks.labs.dqx.actions.message import AlertMessage


class TeamsDQAlertDestination(WebhookAlertDestination):
    """Microsoft Teams incoming-webhook destination using MessageCard format.

    Posts a Teams MessageCard to a Teams incoming webhook URL.  The card
    includes an activity title with the alert title, and a facts section
    derived from *message.fields*.

    Class attributes:
        allowed_host_suffixes: Restricts delivery to ``webhook.office.com``
            and ``office.com`` hosts.

    Attributes:
        type: Discriminator literal, always ``"teams"``.
        name: Logical name for this destination instance.
        webhook_url: The Teams incoming webhook URL (plain string or *DQSecret*).
    """

    type: Literal["teams"] = "teams"
    allowed_host_suffixes: ClassVar[list[str] | None] = ["webhook.office.com", "office.com"]

    def _build_payload(self, message: AlertMessage) -> dict[str, object]:
        """Build a Teams MessageCard payload from *message*.

        Constructs a MessageCard with the alert title as *activityTitle* and
        all *message.fields* entries as facts inside a single section.

        Args:
            message: The alert message to render.

        Returns:
            A dict representing a valid Teams MessageCard payload with
            ``"@type"``, ``"@context"``, ``"summary"``, and ``"sections"``
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
