# databricks.labs.dqx.contexts.global\_context

## GlobalContext Objects[​](#globalcontext-objects "Direct link to GlobalContext Objects")

```python
class GlobalContext(abc.ABC)

```

GlobalContext class that provides a global context, including workspace client,

### replace[​](#replace "Direct link to replace")

```python
def replace(**kwargs)

```

Replace cached properties.

**Arguments**:

* `kwargs` - Key-value pairs of properties to replace.

**Returns**:

The updated GlobalContext instance.
