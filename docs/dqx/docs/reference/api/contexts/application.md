---
sidebar_label: application
title: databricks.labs.dqx.contexts.application
---

## GlobalContext Objects

```python
class GlobalContext(abc.ABC)
```

Returns the parent run ID.

**Returns**:

The parent run ID as an integer.

#### replace

```python
def replace(**kwargs)
```

Replace cached properties.

**Arguments**:

- `kwargs`: Key-value pairs of properties to replace.

**Returns**:

The updated GlobalContext instance.

## CliContext Objects

```python
class CliContext(GlobalContext, abc.ABC)
```

Abstract base class for global context, providing common properties and methods for workspace management.

