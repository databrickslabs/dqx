---
sidebar_label: utils
title: databricks.labs.dqx.llm.utils
---

#### get\_check\_function\_definition

```python
def get_check_function_definition(
    custom_check_functions: dict[str, Any] | None = None
) -> list[dict[str, str]]
```

A utility function to get the definition of all check functions.

This function is primarily used to generate a prompt for the LLM to generate check functions.

**Arguments**:

- `custom_check_functions`: A dictionary of custom check functions.
If provided, the function will use the custom check functions to resolve the check function.
    If not provided, the function will use only the built-in check functions.

Returns:
  list[dict]: A list of dictionaries, each containing the definition of a check function.

#### load\_yaml\_checks\_examples

```python
def load_yaml_checks_examples() -> str
```

Load yaml_checks_examples.yml file from the llm/resources folder.

**Returns**:

checks examples as yaml string.

