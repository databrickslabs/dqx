# databricks.labs.dqx.actions.destinations.callback

Callback alert destination for DQX actions.

Delivers DQX alert messages by invoking an in-process Python callable. This destination is not persistable; the serializer skips it with a warning when persisting destination configurations to storage.

## DQCallbackAlertDestination Objects[​](#dqcallbackalertdestination-objects "Direct link to DQCallbackAlertDestination Objects")

```python
class DQCallbackAlertDestination(AlertDestination)

```

In-process callback destination that invokes a Python callable on delivery.

Useful for testing and for scenarios where alert handling must occur in the same process (e.g. raising exceptions, writing to an in-memory buffer, or triggering a custom side effect).

This destination is intentionally not persistable. The serializer skips instances of this class with a warning rather than attempting to persist the *callback* field. It is still a valid runtime destination.

**Attributes**:

* `type` - Discriminator literal, always *"callback"*.
* `name` - Logical name for this destination instance.
* `callback` - Python callable invoked by *deliver*. Must accept an *AlertMessage* and an *ActionContext* and return *None*.

### deliver[​](#deliver "Direct link to deliver")

```python
def deliver(message: AlertMessage, context: ActionContext,
            _services: ActionServices) -> None

```

Invoke *callback* with *message* and *context*.

The *\_services* parameter is unused by this destination because delivery is entirely in-process; it is accepted to satisfy the *AlertDestination* interface.

**Arguments**:

* `message` - Immutable alert message payload assembled by *StandardMessageBuilder*.
* `context` - Immutable snapshot of run-time state for the DQX run.
