# databricks.labs.dqx.actions.destinations.webhook

Generic webhook alert destination with optional Basic-auth support.

Delivers DQX alert messages to arbitrary HTTPS webhook endpoints as a canonical DQX JSON payload. Unlike the Slack and Teams destinations there is no host restriction — any HTTPS endpoint is accepted. Optional *username* and *password* fields (plain strings or *DQSecret* references) enable HTTP Basic-auth.

## DQWebhookAlertDestination Objects[​](#dqwebhookalertdestination-objects "Direct link to DQWebhookAlertDestination Objects")

```python
class DQWebhookAlertDestination(WebhookAlertDestination)

```

Generic HTTPS webhook destination with optional Basic-auth.

Posts a canonical DQX JSON payload to any HTTPS endpoint. No host suffix restrictions are applied. When both *username* and *password* are provided they are resolved (via the secret resolver when given as *DQSecret* references) and packaged into a *WebhookAuth* passed to the *WebhookClient*.

Class attributes: allowed\_host\_suffixes: *None* — no host restriction.

**Attributes**:

* `type` - Discriminator literal, always *"webhook"*.
* `name` - Logical name for this destination instance.
* `webhook_url` - The endpoint URL (plain string or *DQSecret*).
* `username` - Optional Basic-auth username (plain string or *DQSecret*). Both *username* and *password* must be set for auth to be applied.
* `password` - Optional Basic-auth password (plain string or *DQSecret*). Both *username* and *password* must be set for auth to be applied. Treat plaintext values as development-only; use *DQSecret* in production.
