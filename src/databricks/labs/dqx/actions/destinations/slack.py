"""Slack Block Kit alert destination.

Delivers DQX alert messages to Slack incoming webhook URLs as Block Kit
payloads.  The host is restricted to *hooks.slack.com* to prevent
accidental or malicious redirection to non-Slack endpoints.
"""

from typing import ClassVar, Literal

from databricks.labs.dqx.actions.destinations.webhook_base import WebhookAlertDestination
from databricks.labs.dqx.actions.message import AlertMessage


class SlackDQAlertDestination(WebhookAlertDestination):
    """Slack incoming-webhook destination using Slack Block Kit format.

    Posts a structured Block Kit message to a Slack incoming webhook URL.
    The payload includes sections for the alert title, summary, condition,
    table, run metadata, severity, and all observed metrics.

    Note:
        Create the webhook via a Slack App (the *Incoming Webhooks* feature) rather than the
        deprecated legacy custom integration. Both yield a *hooks.slack.com* URL that works here;
        the app-based one is the supported path. The URL carries its own token, so no additional
        authentication is sent.

    Class attributes:
        allowed_host_suffixes: Restricts delivery to *hooks.slack.com*.

    Attributes:
        type: Discriminator literal, always *"slack"*.
        name: Logical name for this destination instance.
        webhook_url: The Slack incoming webhook URL (plain string or *DQSecret*).
    """

    type: Literal["slack"] = "slack"
    allowed_host_suffixes: ClassVar[list[str] | None] = ["hooks.slack.com"]

    def _build_payload(self, message: AlertMessage) -> dict[str, object]:
        """Build a Slack Block Kit payload from *message*.

        Constructs a *blocks* list with a header block containing the alert
        title, followed by section blocks for summary, condition, table,
        run_id, run_time, severity, and observed metrics.

        Args:
            message: The alert message to render.

        Returns:
            A dict with a top-level *"blocks"* key containing a valid Slack
            Block Kit block array.
        """
        condition_text = message.condition if message.condition is not None else "unconditional"
        table_text = message.table if message.table is not None else "unspecified"
        metrics_text = ", ".join(f"{k}: {v}" for k, v in message.observed_metrics.items())

        blocks: list[dict] = [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": message.title, "emoji": True},
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": message.summary},
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Condition:*\n{condition_text}"},
                    {"type": "mrkdwn", "text": f"*Table:*\n{table_text}"},
                ],
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Run ID:*\n{message.run_id}"},
                    {"type": "mrkdwn", "text": f"*Run Time:*\n{message.run_time!s}"},
                ],
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Severity:*\n{message.severity}"},
                    {"type": "mrkdwn", "text": f"*Metrics:*\n{metrics_text}"},
                ],
            },
        ]

        return {"blocks": blocks}


__all__ = ["SlackDQAlertDestination"]
