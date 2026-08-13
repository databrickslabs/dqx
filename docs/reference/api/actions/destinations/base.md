# databricks.labs.dqx.actions.destinations.base

Abstract base class for DQX alert destinations.

Defines *AlertDestination*, the contract that all concrete destination adapters (Slack, Teams, generic webhook, …) must satisfy.

## AlertDestination Objects[​](#alertdestination-objects "Direct link to AlertDestination Objects")

```python
class AlertDestination(BaseModel, abc.ABC)

```

Abstract Pydantic base for all DQX alert destination implementations.

Subclasses declare a literal *type* discriminator field and override *deliver* to send the alert via their own transport. Construction-time validation of a subclass's own configuration is performed by Pydantic validators rather than a separate *validate* method.

**Attributes**:

* `name` - Logical name for this destination instance. Must be non-empty.

### deliver[​](#deliver "Direct link to deliver")

```python
@abc.abstractmethod
def deliver(message: AlertMessage, context: ActionContext,
            services: ActionServices) -> None

```

Deliver *message* to this destination.

**Arguments**:

* `message` - Immutable alert message payload assembled by *StandardMessageBuilder*.
* `context` - Immutable snapshot of run-time state for the DQX run.
* `services` - Injected services (secret resolver, webhook client, etc.).
