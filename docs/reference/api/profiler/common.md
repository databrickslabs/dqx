# databricks.labs.dqx.profiler.common

### val\_to\_str[​](#val_to_str "Direct link to val_to_str")

```python
def val_to_str(value: Any, include_sql_quotes: bool = True)

```

Converts a value to a string.

**Arguments**:

* `value` - The value to convert. Can be a datetime, date, int, float, or other type.
* `include_sql_quotes` - Whether to include quotes around the value. Default is True.

**Returns**:

The string representation of the value

### val\_maybe\_to\_str[​](#val_maybe_to_str "Direct link to val_maybe_to_str")

```python
def val_maybe_to_str(value: Any, include_sql_quotes: bool = True)

```

Converts a value to a string if it is a datetime or date.

**Arguments**:

* `value` - The value to convert. Can be a datetime, date, or other type.
* `include_sql_quotes` - Whether to include quotes around the value. Default is True.

**Returns**:

The string representation of the value.
