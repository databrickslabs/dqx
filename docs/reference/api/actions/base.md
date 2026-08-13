# databricks.labs.dqx.actions.base

Core action types for the DQX actions & alerting subsystem.

This module defines the foundational building blocks used throughout the *databricks.labs.dqx.actions* package:

* *ActionStatus* — outcome enum (healthy / unhealthy).
* *ActionContext* — frozen snapshot of run-time state passed to every action.
* *ActionResult* — frozen record of a single action's outcome.
* *ActionServices* — container for injectable services (secret resolver, webhook client, workspace client, Spark session).
* *Action* — abstract Pydantic base class that concrete actions extend.

The *DQAction* binding lives in *actions/dq\_action.py* rather than here: its *action* field is the discriminated union over the concrete action classes, which import this module, so declaring it here would create an import cycle.

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

* `metrics` - Mapping of metric name to observed value (for example, the metric *error\_row\_count* with a value of 12).
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

Subclasses must declare a literal *type* discriminator field and override *execute*. Construction-time validation of a subclass's own configuration is performed by Pydantic validators on the subclass (for example, a *model\_validator* that raises *InvalidActionError*) rather than a separate *validate* method.

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
