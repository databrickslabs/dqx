# databricks.labs.dqx.actions.alert

DQAlert action and associated enumerations.

This module defines *DQAlertFrequency*, *NotifyOn*, and *DQAlert* — the primary alerting action for the DQX actions subsystem. *DQAlert* dispatches an *AlertMessage* to one or more *AlertDestination* instances concurrently using *Threads.gather* so that a single delivery failure cannot block the remaining destinations.

## DQAlertFrequency Objects[​](#dqalertfrequency-objects "Direct link to DQAlertFrequency Objects")

```python
class DQAlertFrequency(enum.Enum)

```

Controls how often a *DQAlert* may fire relative to prior alerts.

**Attributes**:

* `ALWAYS` - Fire on every DQX run where the condition evaluates to *True*.
* `HOURLY` - Fire at most once per hour.
* `DAILY` - Fire at most once per day.

## NotifyOn Objects[​](#notifyon-objects "Direct link to NotifyOn Objects")

```python
class NotifyOn(enum.Enum)

```

Controls which state transitions cause *DQAlert* to notify.

**Attributes**:

* `EACH` - Send a notification every time the condition fires.
* `STATUS_CHANGE` - Send a notification only on the transition INTO an unhealthy state — i.e. the first run whose condition fires after a healthy (or unseen) run. While the status stays unhealthy, repeat notifications are suppressed. Recovery (unhealthy → healthy) is not notified: a healthy run does not fire the condition, so no recovery alert is sent.

## DQAlert Objects[​](#dqalert-objects "Direct link to DQAlert Objects")

```python
@register_action
class DQAlert(Action)

```

Sends alert notifications to one or more *AlertDestination* instances.

When *execute* is called the action builds a single *AlertMessage* via *StandardMessageBuilder* and dispatches it to every configured destination concurrently. A failure in one destination is isolated: the remaining destinations are still attempted and the error is recorded in *ActionResult.destination\_errors* rather than re-raised.

**Attributes**:

* `type` - Discriminator literal, always *"alert"*.
* `destinations` - One or more *AlertDestination* adapters that receive the alert (Slack, Teams, webhook, …). Must not be empty, and names must be unique.
* `name` - Logical identifier for this alert action; defaults to *"alert"*.
* `alert_frequency` - Controls how often alerts may be sent; defaults to *DQAlertFrequency.ALWAYS*.
* `notify_on` - Controls which state transitions trigger a notification; defaults to *NotifyOn.EACH*.
* `severity` - Alert severity level included in the message payload; defaults to *"error"*.

### execute[​](#execute "Direct link to execute")

```python
def execute(context: ActionContext, services: ActionServices) -> ActionResult

```

Build an alert message and deliver it concurrently to all destinations.

Builds a single *AlertMessage* from *context* using *StandardMessageBuilder*, then dispatches it to every destination in *self.destinations* concurrently via *Threads.gather*. Delivery failures are isolated per-destination: a failure in one destination does not prevent the others from being attempted.

The action always returns *fired=True* and *status=UNHEALTHY* — it is only called when a condition has already been found to be True (i.e. a data quality violation was detected), so the status always reflects an unhealthy state.

**Arguments**:

* `context` - Immutable snapshot of run-time state including metrics, run identifiers, and location metadata.
* `services` - Injected services (secret resolver, webhook client, etc.).

**Returns**:

An *ActionResult* with *fired=True*, *status=UNHEALTHY*, and *destination\_errors* populated for any destinations that failed.
