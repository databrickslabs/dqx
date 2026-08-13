# databricks.labs.dqx.actions.destinations.teams

Microsoft Teams alert destination (Power Automate Workflows webhook).

Delivers DQX alert messages to a Microsoft Teams channel via a Power Automate *Workflows* webhook ("Post to a channel when a webhook request is received"), using the MessageCard payload format.

Microsoft retired the legacy Office 365 Connector incoming webhooks (*webhook.office.com*) in May 2026; the Workflows webhook is the supported replacement. Workflows still accepts MessageCard payloads — text and facts render — but interactive elements such as buttons do not. Delivery is restricted to the two Workflows webhook hosts, *logic.azure.com* and *environment.api.powerplatform.com*; a manual-trigger Workflows URL carries its authorization in the URL signature (the *sig=* query parameter), so an anonymous POST works on either host.

## DQTeamsAlertDestination Objects[​](#dqteamsalertdestination-objects "Direct link to DQTeamsAlertDestination Objects")

```python
class DQTeamsAlertDestination(WebhookAlertDestination)

```

Microsoft Teams destination using a Power Automate Workflows webhook.

Posts a Teams MessageCard to a Workflows webhook URL created via the Workflows app in Teams ("Post to a channel when a webhook request is received"). The card includes an activity title with the alert title and a facts section derived from *message.fields*.

The legacy Office 365 Connector webhooks (*webhook.office.com*) were retired by Microsoft in May 2026 and are no longer supported; create a Workflows webhook instead. Workflows renders the MessageCard text and facts but not interactive buttons.

**Notes**:

Manual-trigger Workflows webhook URLs carry their authorization in the URL (a *sig=* signature query parameter), so an anonymous POST works — on both the classic *logic.azure.com* URLs and the newer Power Automate "direct" trigger URLs on *environment.api.powerplatform.com*. Both hosts are allowed. Only a Workflows URL explicitly configured to require Entra auth would need a bearer token this destination does not send; that is a URL-level choice, not something the host allowlist can distinguish.

Class attributes:

* `allowed_host_suffixes` - Restricts delivery to the *logic.azure.com* and *environment.api.powerplatform.com* Workflows webhook hosts.

**Attributes**:

* `type` - Discriminator literal, always *"teams"*.
* `name` - Logical name for this destination instance.
* `webhook_url` - The Teams Workflows webhook URL (plain string or *DQSecret*).

#### url\_contains\_secret[​](#url_contains_secret "Direct link to url_contains_secret")

Teams Workflows URL embeds a signature; prefer DQSecret
