# databricks.labs.dqx.profiler.common

### is_text[​](#is_text "Direct link to is_text")

```python
def is_text(column_type: T.DataType) -> bool

```

Validates that the input column type is a Spark text type.

**Arguments**:

* `column_type` - Input column type

**Returns**:

True if the column is a Spark text type, otherwise False

### is_geospatial[​](#is_geospatial "Direct link to is_geospatial")

```python
def is_geospatial(column_type: T.DataType) -> bool

```

Validates that the input column type is a native GEOMETRY or GEOGRAPHY type.

**Arguments**:

* `column_type` - Input column type

**Returns**:

True if the column is a native geometry/geography type, otherwise False

**Notes**:

*pyspark.sql.types* does not define *GeometryType* and *GeographyType* across all versions. This method checks the type name to ensure compatibility.

### is_geometry[​](#is_geometry "Direct link to is_geometry")

```python
def is_geometry(column_type: T.DataType) -> bool

```

Validates that the input column type is a native GEOMETRY type.

**Arguments**:

* `column_type` - Input column type

**Returns**:

True if the column is a native geometry type, otherwise False

### is_geography[​](#is_geography "Direct link to is_geography")

```python
def is_geography(column_type: T.DataType) -> bool

```

Validates that the input column type is a native GEOGRAPHY type.

**Arguments**:

* `column_type` - Input column type

**Returns**:

True if the column is a native geography type, otherwise False

### val_to_str[​](#val_to_str "Direct link to val_to_str")

```python
def val_to_str(value: Any, include_sql_quotes: bool = True)

```

Converts a value to a string.

**Arguments**:

* `value` - The value to convert. Can be a datetime, date, int, float, or other type.
* `include_sql_quotes` - Whether to include quotes around the value. Default is True.

**Returns**:

The string representation of the value

### val_maybe_to_str[​](#val_maybe_to_str "Direct link to val_maybe_to_str")

```python
def val_maybe_to_str(value: Any, include_sql_quotes: bool = True)

```

Converts a value to a string if it is a datetime or date.

**Arguments**:

* `value` - The value to convert. Can be a datetime, date, or other type.
* `include_sql_quotes` - Whether to include quotes around the value. Default is True.

**Returns**:

The string representation of the value.
