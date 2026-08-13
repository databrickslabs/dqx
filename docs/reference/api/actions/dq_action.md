# databricks.labs.dqx.actions.dq\_action

The *DQAction* binding.

This module binds a concrete *Action* (any registered type — the built-in *DQAlert*, *FailPipeline*, and *NoOpAction*, plus any custom action registered via *register\_action*) to an optional gating condition and a logical name.

*DQAction* lives in its own module — separate from *actions/base.py* — because resolving the *action* field imports the action registry, which imports the concrete action classes. *base.py* is imported by those concrete actions, so declaring *DQAction* there would create an import cycle.

Extensibility: instead of a closed discriminated union of the built-ins, the *action* field accepts any *Action* subclass registered in *ACTION\_REGISTRY*. A metadata dict is resolved to its concrete class by the *type* discriminator via the registry, so custom actions round-trip through metadata exactly like the built-ins (their defining module must be imported first so registration has run).

## DQAction Objects[​](#dqaction-objects "Direct link to DQAction Objects")

```python
class DQAction(BaseModel)

```

Binds an *Action* to an optional gating condition and a logical name.

A *None* *condition* means the action fires unconditionally after every DQX run. A non-*None* *condition* is validated at construction time via *ConditionEvaluator.validate* — a malformed expression surfaces immediately rather than at evaluation time.

The *action* may be any registered *Action* subclass (see *register\_action*), so custom actions work both programmatically and as metadata.

Name derivation (applied when *name* is empty):

1. Use *action.name* if non-empty.
2. Otherwise, if *condition* is set, derive a compact label from the condition string.
3. Otherwise, use the action's class name.

**Attributes**:

* `action` - The *Action* instance to execute.
* `condition` - Optional gating expression. When *None* the action fires unconditionally.
* `name` - Logical name for this *DQAction* configuration entry. Derived automatically when left empty.
