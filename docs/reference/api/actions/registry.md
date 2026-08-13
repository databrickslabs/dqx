# databricks.labs.dqx.actions.registry

Registry of DQX action types, enabling custom (user-defined) actions.

An *Action* subclass becomes usable by *DQAction* — both programmatically and from YAML/JSON metadata — once it is registered here, keyed on its literal *type* discriminator. This follows the same registry idea as *CHECK\_FUNC\_REGISTRY* / *register\_rule* for check functions, but with stricter collision handling: whereas *register\_rule* is unconditionally last-write-wins, this registry *raises* *InvalidActionError* when a **genuinely different** class (different ***module*** / ***qualname***) tries to claim a *type* that is already taken, so an accidental clash between two distinct actions surfaces loudly instead of one silently shadowing the other. Re-registering the same class — including the same definition re-run in a notebook cell or reloaded module, which yields a new class object with identical identity — is allowed and simply overwrites the previous registration, so interactive iteration on a custom action works without restarting the Python session.

Built-in actions register themselves at import time. A custom action registers via the *register\_action* decorator (or *register\_action\_class*); the module defining it must be imported before the action is constructed or a metadata definition referencing its *type* is deserialized, so that registration has run.

### register\_action\_class[​](#register_action_class "Direct link to register_action_class")

```python
def register_action_class(action_cls: type[Action]) -> type[Action]

```

Register *action\_cls* under its declared *type* discriminator.

**Arguments**:

* `action_cls` - An *Action* subclass declaring a non-empty literal *type* field.

**Returns**:

The same *action\_cls*, so this can be used as a decorator.

**Raises**:

* `InvalidActionError` - If *action\_cls* has no resolvable *type*, or if a *genuinely different* class (different ***module*** / ***qualname***) is already registered under the same *type*. Re-registering the same class — or the same class redefined by a notebook cell re-run or a module reload, which produces a new class object with identical identity — is allowed and overwrites the previous registration.

### register\_action[​](#register_action "Direct link to register_action")

```python
def register_action(action_cls: type[Action]) -> type[Action]

```

Decorator form of *register\_action\_class*.

**Example**:

\>>> @register\_action ... class MyAction(Action): ... type: Literal\["my\_action"] = "my\_action" ... def execute(self, context, services): ...

### resolve\_action\_type[​](#resolve_action_type "Direct link to resolve_action_type")

```python
def resolve_action_type(action_type: str) -> type[Action]

```

Return the registered *Action* subclass for *action\_type*.

**Arguments**:

* `action_type` - The literal *type* discriminator to look up.

**Returns**:

The registered *Action* subclass.

**Raises**:

* `InvalidActionError` - If no action is registered under *action\_type*.
