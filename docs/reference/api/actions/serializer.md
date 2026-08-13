# databricks.labs.dqx.actions.serializer

Serializer for *DQAction* instances.

*ActionSerializer* is a thin facade over Pydantic: it converts *DQAction* objects to plain Python dicts (suitable for JSON / YAML persistence) and back. Serialization and validation are driven entirely by the Pydantic models (*DQAction*, *DQAlert*, *FailPipeline*, and the destination types) and their discriminated unions — no per-type registries are needed.

## Wire format[​](#wire-format "Direct link to Wire format")

* Enums (*DQAlertFrequency*, *NotifyOn*) serialize to their string *value*.
* A *DQSecret* credential serializes to a tagged dict mapping the key *secret* to a *scope/key* reference (via *SecretOrStr*), so it round-trips losslessly without being confused with a plain string.
* *DQCallbackAlertDestination* instances are skipped during *to\_dict* with a *WARNING*-level log message because they hold a live Python callable that cannot be persisted (see *DQAlert.\_serialize\_destinations*).
* The *"condition"* field is omitted from the output of *to\_dict* when it is *None*, and defaults back to *None* when absent on *from\_dict*.

## Security[​](#security "Direct link to Security")

User-supplied names are sanitized before they appear in log messages to prevent log injection (CWE-117).

## ActionSerializer Objects[​](#actionserializer-objects "Direct link to ActionSerializer Objects")

```python
class ActionSerializer()

```

Converts *DQAction* instances to plain dicts and back via Pydantic.

Adding a new action type requires only declaring the new Pydantic model with its literal *type* discriminator and registering it via *@register\_action*; a new destination type is added to the *AnyDestination* discriminated union. In both cases the serializer itself needs no changes.

*DQSecret* values round-trip as a tagged dict (key *secret* mapped to a *scope/key* reference) so the reference survives JSON / YAML without being confused with a plain string.

*DQCallbackAlertDestination* instances are skipped during *to\_dict* because they hold a live Python callable that cannot be persisted. The *"condition"* field is omitted when *None*.

### to\_dict[​](#to_dict "Direct link to to_dict")

```python
@staticmethod
def to_dict(action: DQAction) -> dict[str, object]

```

Serialize *action* to a plain Python dict.

**Arguments**:

* `action` - The *DQAction* to serialize.

**Returns**:

A JSON-serializable dict representing *action*. The *"condition"* key is omitted when the condition is *None*.

### from\_dict[​](#from_dict "Direct link to from_dict")

```python
@staticmethod
def from_dict(raw: dict[str, object]) -> DQAction

```

Deserialize a plain dict into a *DQAction*.

**Arguments**:

* `raw` - Dict produced by *to\_dict* (or loaded from JSON / YAML).

**Returns**:

A fully reconstructed *DQAction*.

**Raises**:

* `InvalidActionError` - If the payload is invalid — for example an unknown action or destination *"type"*, or a missing required field.

### serialize\_actions[​](#serialize_actions "Direct link to serialize_actions")

```python
def serialize_actions(actions: list[DQAction]) -> list[dict[str, object]]

```

Serialize a list of *DQAction* instances to plain Python dicts.

Convenience wrapper around *ActionSerializer.to\_dict* for operating on a whole list at once. The output is suitable for YAML or JSON persistence.

**Arguments**:

* `actions` - List of *DQAction* instances to serialize.

**Returns**:

List of JSON-serializable dicts, one per action.

### deserialize\_actions[​](#deserialize_actions "Direct link to deserialize_actions")

```python
def deserialize_actions(metadata: list[dict[str, object]]) -> list[DQAction]

```

Deserialize a list of plain dicts into *DQAction* instances.

Convenience wrapper around *ActionSerializer.from\_dict* for operating on a whole list at once.

**Arguments**:

* `metadata` - List of dicts produced by *serialize\_actions* (or loaded from YAML / JSON). Each element must be a *dict*; passing a non-dict element raises *InvalidActionError*.

**Returns**:

List of fully reconstructed *DQAction* instances.

**Raises**:

* `InvalidActionError` - If any element is not a *dict*, or if a dict cannot be validated as a *DQAction* (unknown *type*, missing required field, etc.).
