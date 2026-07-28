---
sidebar_label: global_context
title: databricks.labs.dqx.contexts.global_context
---

## GlobalContext Objects

```python
class GlobalContext(abc.ABC)
```

GlobalContext class that provides a global context, including workspace client,

#### replace

```python
def replace(**kwargs)
```

Replace cached properties.

**Arguments**:

- `kwargs` - Key-value pairs of properties to replace.
  

**Returns**:

  The updated GlobalContext instance.

