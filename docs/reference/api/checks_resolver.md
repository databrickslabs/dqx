# databricks.labs.dqx.checks\_resolver

### resolve\_check\_function[​](#resolve_check_function "Direct link to resolve_check_function")

```python
def resolve_check_function(function_name: str,
                           custom_check_functions: dict[str, Callable]
                           | None = None,
                           fail_on_missing: bool = True) -> Callable | None

```

Resolves a function by name from the predefined functions and custom checks.

**Arguments**:

* `function_name` - name of the function to resolve.
* `custom_check_functions` - dictionary with custom check functions (e.g. *globals()* of the calling module).
* `fail_on_missing` - if True, raise an InvalidCheckError if the function is not found.

**Returns**:

function or None if not found.

**Raises**:

* `InvalidCheckError` - if the function is not found and fail\_on\_missing is True.

### resolve\_custom\_check\_functions\_from\_path[​](#resolve_custom_check_functions_from_path "Direct link to resolve_custom_check_functions_from_path")

```python
def resolve_custom_check_functions_from_path(
        check_functions: dict[str, str] | None = None) -> dict[str, Callable]

```

Resolve custom check functions from a path in the local filesystem, Databricks workspace, or Unity Catalog volume.

**Arguments**:

* `check_functions` - a mapping where each key is the name of a function (e.g., "my\_func") and each value is the file path to the Python module that defines it. The path can be absolute or relative to the installation folder, and may refer to a local filesystem location, a Databricks workspace path (e.g. /Workspace/my\_repo/my\_module.py), or a Unity Catalog volume (e.g. /Volumes/catalog/schema/volume/my\_module.py).

**Returns**:

A dictionary mapping function names to the actual function objects.
