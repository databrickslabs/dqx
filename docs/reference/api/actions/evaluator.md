# databricks.labs.dqx.actions.evaluator

Action evaluator orchestrator for the DQX actions & alerting subsystem.

This module provides *ActionEvaluator* — the top-level orchestrator that drives all configured *DQAction* instances through their full lifecycle:

1. **Condition evaluation** — if *dq.condition* is not *None*, the condition is evaluated against *context.metrics* via *ConditionEvaluator*. A *False* result short-circuits the action (not-fired event recorded, continue).

2. **Suppression check** — *ActionStateStore.should\_fire* gates frequency and status-change suppression for alert actions. A *False* result short-circuits the action (not-fired event recorded, continue).

3. **Polymorphic dispatch** — *dq.action.execute(context, services)* is called uniformly for all action types. No *isinstance* checks are performed here; new action types extend the system without modifying this evaluator (Open/Closed principle).

4. **Terminal error deferral** — *TerminalActionError* (e.g. *PipelineFailedError*) is caught and deferred until after all actions have been evaluated. This ensures every alert is delivered before the pipeline is terminated.

## ActionEvaluator Objects[​](#actionevaluator-objects "Direct link to ActionEvaluator Objects")

```python
class ActionEvaluator()

```

Orchestrator that drives all configured *DQAction* instances.

For each *DQAction* in *actions*, *evaluate* follows this pipeline:

1. If *dq.condition* is not *None*, evaluate it. On *False*: record a not-fired *AlertEvent* and continue.
2. Call *state\_store.should\_fire*. On *False*: record a not-fired *AlertEvent* and continue.
3. Execute the action via *dq.action.execute(context, services)* — **no** *isinstance* checks, purely polymorphic dispatch. Record a fired *AlertEvent* and append the *ActionResult*.
4. Catch *TerminalActionError* into a deferred list.

After all actions: raise the first deferred *TerminalActionError* (if any), so that all alerts are delivered before the run is aborted.

**Arguments**:

* `actions` - Ordered list of *DQAction* configurations to evaluate.
* `state_store` - State manager for frequency/status suppression and event persistence.
* `services` - Injectable services passed through to each *Action.execute*.
* `message_builder` - Optional message builder; reserved for subclasses or future extensions. Not used by this evaluator directly.

### evaluate[​](#evaluate "Direct link to evaluate")

```python
def evaluate(context: ActionContext) -> list[ActionResult]

```

Evaluate all configured actions against *context*.

Iterates through every *DQAction* in *self.\_actions* and applies the condition/suppression/execute pipeline described in the class docstring. *TerminalActionError* exceptions are deferred until after all actions have been processed, then the first is re-raised.

**Arguments**:

* `context` - Immutable run-time snapshot carrying *metrics*, *run\_id*, *run\_time*, and optional location fields.

**Returns**:

List of *ActionResult* instances for every action that actually fired. Skipped/suppressed actions do not appear in the list.

**Raises**:

* `TerminalActionError` - The first terminal error encountered across all actions, raised after the full action loop completes.
