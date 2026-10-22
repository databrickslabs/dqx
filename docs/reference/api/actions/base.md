# databricks.labs.dqx.actions.base

Core action types for the DQX actions & alerting subsystem.

This module defines the foundational building blocks used throughout the *databricks.labs.dqx.actions* package:

* *ActionStatus* — outcome enum (healthy / unhealthy).
* *ActionContext* — frozen snapshot of run-time state passed to every action.
* *ActionResult* — frozen record of a single action's outcome.
* *ActionServices* — container for injectable services (secret resolver, webhook client, workspace client, Spark session).
* *Action* — abstract Pydantic base class that concrete actions extend.

The *DQAction* binding lives in *actions/dq_action.py* rather than here: its *action* field is the discriminated union over the concrete action classes, which import this module, so declaring it here would create an import cycle.

## ActionStatus Objects[​](#actionstatus-objects "Direct link to ActionStatus Objects")

```python
class ActionStatus(enum.Enum)

```

Outcome of a triggered DQX action.

**Attributes**:

* `HEALTHY` - The action completed without detecting a quality violation.
* `UNHEALTHY` - The action detected a quality violation.
* `CONFIG_ERROR` - The action could not be evaluated because of a configuration problem (e.g. its condition failed to evaluate against the observed metrics). This is distinct from *UNHEALTHY*: the data is not known to be bad, the action itself is misconfigured.

## ActionContext Objects[​](#actioncontext-objects "Direct link to ActionContext Objects")

```python
@dataclass(frozen=True)
class ActionContext()

```

Immutable snapshot of run-time state passed to every *Action.execute* call.

All location fields are optional — populate only the ones that are meaningful for a given run.

**Attributes**:

* `metrics` - Mapping of metric name to observed value (for example, the metric *error_row_count* with a value of 12).
* `run_id` - Unique identifier for the DQX run that produced these metrics.
* `run_time` - Timestamp when the DQX run executed.
* `run_name` - Human-readable name for the run; defaults to *"dqx"*.
* `input_location` - Source path/URI of the data being checked, or *None*.
* `output_location` - Destination path/URI of checked output, or *None*.
* `quarantine_location` - Path/URI where quarantined rows are written, or *None*.
* `checks_location` - Path/URI of the checks definition file, or *None*.
* `rule_set_fingerprint` - Fingerprint of the rule set applied, or *None*.
* `user_metadata` - Arbitrary string-valued metadata supplied by the caller, or *None* when not provided.
* `run_id`0 - The gating condition expression of the action being executed, or *None* when the action fires unconditionally. Set per-action by the evaluator so an action (e.g. an alert message) can report *why* it fired; the engine leaves it *None* on the shared run context.
* `run_id`1 - Mapping keyed by producing action name to the *dict\[str, str]* payload that action returned via *ActionResult.extras*, or *None* when no producer has contributed yet. Populated by the evaluator as each action completes: the payload is copied before insertion, and the outer *ActionContext* is rebuilt via *dataclasses.replace* for the next action, so downstream actions cannot observe post-execute mutations by the producer.

### get_extras[​](#get_extras "Direct link to get_extras")

```python
def get_extras(action_name: str) -> dict[str, str]

```

Return the *extras* payload produced by *action_name*, or an empty dict if absent.

Both levels of the *extras* structure are optional (outer `None` = no producer has run yet; missing key = that producer did not contribute). This accessor collapses both cases into an empty `dict[str, str]` so callers can write `context.get_extras(&quot;collect_lineage&quot;).get(&quot;lineage_location&quot;)` without the `(context.extras or {}).get(...) or {}` dance.

**Arguments**:

* `action_name` - The producing action's *name* to look up.

**Returns**:

The producer's *dict\[str, str]* payload, or an empty dict when there is none.

## ActionResult Objects[​](#actionresult-objects "Direct link to ActionResult Objects")

```python
@dataclass(frozen=True)
class ActionResult()

```

Immutable record of a single action's outcome.

**Attributes**:

* `action_name` - Logical name of the action that was executed.
* `fired` - Whether the action's condition evaluated to *True* (and the action was therefore executed).
* `status` - Aggregate outcome of the action execution.
* `destination_errors` - Mapping of destination name to error message for any delivery failures. Empty when all deliveries succeeded.
* `extras` - Optional *dict\[str, str]* payload produced by this action for consumption by later actions in the evaluator loop. *None* (the default) means the action produced no payload. The evaluator copies this value before inserting it into the next *ActionContext.extras* under the action's name, so authors do not need to defensively copy the payload themselves.

## ActionServices Objects[​](#actionservices-objects "Direct link to ActionServices Objects")

```python
@dataclass(frozen=True)
class ActionServices()

```

Frozen container of injectable services available to action implementations.

**Attributes**:

* `secret_resolver` - Resolver for plain-string or *DQSecret* credentials.
* `webhook_client` - HTTP client for delivering webhook-based notifications.
* `ws` - An authenticated *WorkspaceClient*, or *None* when workspace access is not required by this action.
* `spark` - An active *SparkSession*, or *None* when Spark is not required.

## Action Objects[​](#action-objects "Direct link to Action Objects")

```python
class Action(BaseModel, abc.ABC)

```

Abstract Pydantic base class for all DQX action implementations.

Subclasses must declare a literal *type* discriminator field and override *execute*. Construction-time validation of a subclass's own configuration is performed by Pydantic validators on the subclass (for example, a *model_validator* that raises *InvalidActionError*) rather than a separate *validate* method.

**Attributes**:

* `name` - Logical identifier for this action instance. Default is an empty string; concrete subclasses set a meaningful value.

### execute[​](#execute "Direct link to execute")

```python
@abc.abstractmethod
def execute(context: ActionContext, services: ActionServices) -> ActionResult

```

Execute this action and return its result.

**Arguments**:

* `context` - Immutable snapshot of run-time state including observed metrics, run identifiers, and location metadata.
* `services` - Injected services (secret resolver, webhook client, workspace client, Spark session).

**Returns**:

An *ActionResult* describing whether the action fired and its aggregate outcome.
