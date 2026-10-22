# databricks.labs.dqx.actions.registry

Registry of DQX action types, enabling custom (user-defined) actions.

An *Action* subclass becomes usable by *DQAction* — both programmatically and from YAML/JSON metadata — once it is registered here, keyed on its literal *type* discriminator. This follows the same registry idea as *CHECK_FUNC_REGISTRY* / *register_rule* for check functions, but with stricter collision handling: whereas *register_rule* is unconditionally last-write-wins, this registry *raises* *InvalidActionError* when a **genuinely different** class (different ***module*** / ***qualname***) tries to claim a *type* that is already taken, so an accidental clash between two distinct actions surfaces loudly instead of one silently shadowing the other. Re-registering the same class — including the same definition re-run in a notebook cell or reloaded module, which yields a new class object with identical identity — is allowed and simply overwrites the previous registration, so interactive iteration on a custom action works without restarting the Python session.

Built-in actions register themselves at import time. A custom action registers via the *register_action* decorator (or *register_action_class*); the module defining it must be imported before the action is constructed or a metadata definition referencing its *type* is deserialized, so that registration has run.

### register_action_class[​](#register_action_class "Direct link to register_action_class")

```python
def register_action_class(action_cls: type[Action]) -> type[Action]

```

Register *action_cls* under its declared *type* discriminator.

**Arguments**:

* `action_cls` - An *Action* subclass declaring a non-empty literal *type* field.

**Returns**:

The same *action_cls*, so this can be used as a decorator.

**Raises**:

* `InvalidActionError` - If *action_cls* has no resolvable *type*, or if a *genuinely different* class (different ***module*** / ***qualname***) is already registered under the same *type*. Re-registering the same class — or the same class redefined by a notebook cell re-run or a module reload, which produces a new class object with identical identity — is allowed and overwrites the previous registration.

### register_action[​](#register_action "Direct link to register_action")

```python
def register_action(action_cls: type[Action]) -> type[Action]

```

Decorator form of *register_action_class*.

**Example**:

\>>> @register_action ... class MyAction(Action): ... type: Literal\["my_action"] = "my_action" ... def execute(self, context, services): ...

### resolve_action_type[​](#resolve_action_type "Direct link to resolve_action_type")

```python
def resolve_action_type(action_type: str) -> type[Action]

```

Return the registered *Action* subclass for *action_type*.

**Arguments**:

* `action_type` - The literal *type* discriminator to look up.

**Returns**:

The registered *Action* subclass.

**Raises**:

* `InvalidActionError` - If no action is registered under *action_type*.
