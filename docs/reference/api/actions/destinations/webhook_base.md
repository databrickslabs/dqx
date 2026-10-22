# databricks.labs.dqx.actions.destinations.webhook_base

Abstract base for webhook-based alert destinations.

*WebhookAlertDestination* handles the common delivery logic — URL resolution, auth building, and the actual HTTP POST — while delegating payload assembly to concrete subclasses via *\_build_payload*.

## WebhookAlertDestination Objects[​](#webhookalertdestination-objects "Direct link to WebhookAlertDestination Objects")

```python
class WebhookAlertDestination(AlertDestination, abc.ABC)

```

Abstract Pydantic base for webhook alert destinations.

Provides a concrete *deliver* implementation and exposes two extension hooks: *\_build_payload* (required) for producing the wire-format payload, and *\_build_auth* (optional, returns *None* by default) for attaching HTTP Basic-auth credentials.

Subclasses declare *type* (the literal discriminator) and may override *allowed_host_suffixes*, and implement *\_build_payload*.

Class attributes: allowed_host_suffixes: Optional list of allowed host suffixes passed to *WebhookClient.post*. *None* means no restriction (use for internal or generic webhooks).

**Attributes**:

* `name` - Logical name for this destination instance.
* `webhook_url` - The webhook endpoint URL, either as a plain string or as a *DQSecret* scope/key reference resolved at delivery time.

### deliver[​](#deliver "Direct link to deliver")

```python
def deliver(message: AlertMessage, context: ActionContext,
            services: ActionServices) -> None

```

Resolve the URL, build the payload, and POST it to the endpoint.

**Arguments**:

* `message` - Immutable alert message to deliver.
* `context` - Immutable snapshot of run-time DQX state.
* `services` - Injected services (secret resolver, webhook client, etc.).
