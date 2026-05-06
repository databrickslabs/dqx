# databricks.labs.dqx.rule\_fingerprint

Rule fingerprint functions for deterministic hashing of DQ rules and rule sets.

### compute\_rule\_set\_fingerprint[​](#compute_rule_set_fingerprint "Direct link to compute_rule_set_fingerprint")

```python
def compute_rule_set_fingerprint(checks: list[DQRule]) -> str

```

Compute a deterministic SHA-256 hash of the complete rule set.

The hash is order-independent: individual rule fingerprints are sorted before combining. Expects expanded rules (for\_each\_column already expanded via deserialization).

**Arguments**:

* `checks` - List of DQRule objects (expanded form).

**Returns**:

A hex-encoded SHA-256 hash string representing the entire rule set.

### compute\_rule\_set\_fingerprint\_by\_metadata[​](#compute_rule_set_fingerprint_by_metadata "Direct link to compute_rule_set_fingerprint_by_metadata")

```python
def compute_rule_set_fingerprint_by_metadata(
        checks: list[dict],
        custom_checks: dict[str, Callable] | None = None) -> str

```

Compute rule set fingerprint from metadata. Thin wrapper: deserialize then fingerprint.

Ensures for\_each\_column is expanded via deserialization so compact and expanded metadata produce the same fingerprint.

**Arguments**:

* `checks` - List of check dictionaries (may contain for\_each\_column).
* `custom_checks` - Optional mapping of custom function names to callables.

**Returns**:

A hex-encoded SHA-256 hash string representing the entire rule set.
