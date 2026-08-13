# databricks.labs.dqx.actions.destinations.log

Log alert destination for DQX actions.

Delivers DQX alert messages by writing them to the standard Python logger on the Spark driver. Unlike webhook destinations it contacts no external system, which makes it ideal for local development, demos, and end-to-end tests where the alerting mechanism must be exercised without a live Slack / Teams / webhook endpoint. Unlike *DQCallbackAlertDestination* it is fully serializable, so it has a metadata (*type: log*) form and can be persisted and loaded like any other destination.

## DQLogAlertDestination Objects[​](#dqlogalertdestination-objects "Direct link to DQLogAlertDestination Objects")

```python
class DQLogAlertDestination(AlertDestination)

```

Alert destination that logs the alert message to the driver logger.

Writes a single, sanitized log record summarizing the alert — title, condition, severity, table, run identifiers, and observed metrics — at the configured *level*. It performs no network I/O, so it never triggers SSRF validation and cannot stall a streaming micro-batch. Because it is serializable, it round-trips through the metadata (*type: log*) form and can be persisted alongside webhook destinations.

**Attributes**:

* `type` - Discriminator literal, always *"log"*.
* `name` - Logical name for this destination instance.
* `level` - Logging level used to emit the alert. One of *debug*, *info*, *warning*, *error*, or *critical* (case-insensitive); defaults to *warning*.

### deliver[​](#deliver "Direct link to deliver")

```python
def deliver(message: AlertMessage, _context: ActionContext,
            _services: ActionServices) -> None

```

Log *message* at the configured level.

The rendered line is sanitized (CWE-117) because it embeds user-influenced values (condition, table, observed metrics).

The *\_context* and *\_services* parameters are unused; they are accepted to satisfy the *AlertDestination* interface.

**Arguments**:

* `message` - Immutable alert message payload assembled by *StandardMessageBuilder*.
