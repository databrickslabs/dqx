---
sidebar_label: checks_resolver
title: databricks.labs.dqx.checks_resolver
---

#### resolve\_check\_function

```python
def resolve_check_function(function_name: str,
                           custom_check_functions: dict[str, Any]
                           | None = None,
                           fail_on_missing: bool = True) -> Callable | None
```

Resolves a function by name from the predefined functions and custom checks.

**Arguments**:

- `function_name`: name of the function to resolve.
- `custom_check_functions`: dictionary with custom check functions (eg. ``globals()`` of the calling module).
- `fail_on_missing`: if True, raise an AttributeError if the function is not found.

**Returns**:

function or None if not found.

