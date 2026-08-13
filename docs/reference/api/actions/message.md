# databricks.labs.dqx.actions.message

Alert message representation and builder for DQX actions.

This module defines the canonical alert-message structure (*AlertMessage*) that destination adapters (Slack, Teams, webhook, …) receive and render into their own wire formats, together with *StandardMessageBuilder* — a stateless factory that assembles an *AlertMessage* from run-time primitives.

Keeping the builder free of any reference to *ActionContext* prevents circular imports: the evaluator can call *StandardMessageBuilder.build(...)* using only primitive values already available at evaluation time.

## AlertMessage Objects[​](#alertmessage-objects "Direct link to AlertMessage Objects")

```python
@dataclass(frozen=True)
class AlertMessage()

```

Immutable snapshot of a triggered DQX action alert.

Instances are created exclusively by *StandardMessageBuilder.build* and consumed by destination adapters that render them into Slack blocks, Teams cards, webhook payloads, etc.

**Attributes**:

* `title` - Short human-readable headline, always includes the action name.
* `summary` - Longer human-readable description of what triggered the alert.
* `condition` - The condition expression that triggered this alert, or *None* when the action fires unconditionally.
* `table` - Fully-qualified table name being checked, or *None* if not associated with a specific table.
* `observed_metrics` - Mapping of metric name to its observed value at alert time.
* `run_id` - Identifier of the DQX run that produced this alert.
* `run_time` - Timestamp when the DQX run executed.
* `severity` - Alert severity level (e.g. "error", "warn").
* `user_metadata` - Engine-level user metadata (from *ExtraParams.user\_metadata*) as a raw string-to-string mapping. Empty when no metadata was configured. Rendered by every destination so run-level context (e.g. pipeline name) reaches the notification.
* `fields` - Flat string-to-string mapping suitable for key-value rendering in notification payloads. Contains one entry per observed metric under a key of the form *metric.NAME* (for example, *metric.error\_row\_count*), one *user\_metadata.KEY* entry per user-metadata item, plus un-prefixed reserved entries for *condition*, *run\_id*, *run\_time*, and *table*. The prefixes ensure metric and metadata names never silently overwrite the reserved metadata keys.

## StandardMessageBuilder Objects[​](#standardmessagebuilder-objects "Direct link to StandardMessageBuilder Objects")

```python
class StandardMessageBuilder()

```

Stateless factory that builds an *AlertMessage* from run-time primitives.

The builder is intentionally decoupled from *ActionContext* to avoid a circular import. Call it as a static method — no constructor arguments are needed:

```python
msg = StandardMessageBuilder.build(
    action_name="notify_on_errors",
    condition="error_row_count > 0",
    metrics={"error_row_count": 5},
    run_id="run-abc",
    run_time=datetime.now(timezone.utc),
    table="catalog.schema.my_table",
)

```

### build[​](#build "Direct link to build")

```python
@staticmethod
def build(*,
          action_name: str,
          condition: str | None,
          metrics: dict[str, object],
          run_id: str,
          run_time: datetime,
          table: str | None,
          severity: str = "error",
          user_metadata: dict[str, str] | None = None) -> AlertMessage

```

Build an *AlertMessage* from run-time primitives.

Composes a human-readable *title* (always contains *action\_name*), a *summary* that mentions the *table* and *condition* (or states that the action fires unconditionally when *condition* is *None*), and a *fields* dict suitable for flat key-value rendering in notification payloads.

Metric entries in *fields* are stored under a key of the form *metric.NAME* (for example, *metric.error\_row\_count*) so that they never collide with the reserved metadata keys *condition*, *run\_id*, *run\_time*, and *table*, which are always un-prefixed. User metadata entries are likewise stored under a *user\_metadata.KEY* prefix. *observed\_metrics* on the returned *AlertMessage* is always the raw, un-prefixed metrics dict.

**Arguments**:

* `action_name` - Logical name of the DQX action that was triggered.
* `condition` - Condition expression that triggered the action, or *None* for unconditional actions.
* `metrics` - Mapping of metric name to observed value at alert time.
* `run_id` - Identifier of the DQX run that produced this alert.
* `run_time` - Timestamp when the DQX run executed.
* `table` - Fully-qualified table name being checked, or *None*.
* `severity` - Alert severity level; defaults to "error".
* `user_metadata` - Optional engine-level user metadata (from *ExtraParams.user\_metadata*) to surface in the alert payload; included under *user\_metadata.KEY* prefixed keys.

**Returns**:

A frozen *AlertMessage* instance populated from the supplied arguments.
