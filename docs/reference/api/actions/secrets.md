# databricks.labs.dqx.actions.secrets

Secret resolution for DQX action destinations.

Destination configs may carry credentials as either plain strings (suitable for local development) or *DQSecret* references that are resolved at delivery time via the Databricks secrets API.

## SecretResolver Objects[​](#secretresolver-objects "Direct link to SecretResolver Objects")

```python
class SecretResolver()

```

Resolves credential values from plain strings or Databricks secret references.

Plain strings are returned unchanged. *DQSecret* references are resolved via *ws.dbutils.secrets.get* so that sensitive values are never stored in DQX configuration files.

**Arguments**:

* `ws` - An authenticated *WorkspaceClient* instance. The client is injected rather than constructed internally to keep this class testable and to respect the workspace context established by the caller.

### resolve[​](#resolve "Direct link to resolve")

```python
def resolve(value: str | DQSecret) -> str

```

Resolve *value* to a plaintext string.

If *value* is already a plain *str* it is returned as-is without contacting the secrets API. If it is a *DQSecret* the secret is fetched from the Databricks secret scope identified by *value.scope* and *value.key*.

The resolved secret is **never** logged or included in exception messages.

**Arguments**:

* `value` - Either a plain string credential or a *DQSecret* scope/key reference.

**Returns**:

The plaintext credential string.

**Raises**:

* `InvalidParameterError` - If *value* is a *DQSecret* and the secrets API call fails. The error message names the *scope* and *key* but never contains the resolved secret value.
