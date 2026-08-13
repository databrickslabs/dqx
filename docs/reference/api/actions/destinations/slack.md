# databricks.labs.dqx.actions.destinations.slack

Slack Block Kit alert destination.

Delivers DQX alert messages to Slack incoming webhook URLs as Block Kit payloads. The host is restricted to *hooks.slack.com* to prevent accidental or malicious redirection to non-Slack endpoints.

## DQSlackAlertDestination Objects[​](#dqslackalertdestination-objects "Direct link to DQSlackAlertDestination Objects")

```python
class DQSlackAlertDestination(WebhookAlertDestination)

```

Slack incoming-webhook destination using Slack Block Kit format.

Posts a structured Block Kit message to a Slack incoming webhook URL. The payload includes sections for the alert title, summary, condition, table, run metadata, severity, and all observed metrics.

**Notes**:

Create the webhook via a Slack App (the *Incoming Webhooks* feature) rather than the deprecated legacy custom integration. Both yield a *hooks.slack.com* URL that works here; the app-based one is the supported path. The URL carries its own token, so no additional authentication is sent.

Class attributes:

* `allowed_host_suffixes` - Restricts delivery to *hooks.slack.com*.

**Attributes**:

* `type` - Discriminator literal, always *"slack"*.
* `name` - Logical name for this destination instance.
* `webhook_url` - The Slack incoming webhook URL (plain string or *DQSecret*).

#### url\_contains\_secret[​](#url_contains_secret "Direct link to url_contains_secret")

Slack webhook URL embeds a token; prefer DQSecret
