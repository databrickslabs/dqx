---
sidebar_label: utils
title: databricks.labs.dqx.utils
---

#### get\_column\_name\_or\_alias

```python
def get_column_name_or_alias(
        column: str | Column | ConnectColumn,
        normalize: bool = False,
        allow_simple_expressions_only: bool = False) -> str
```

Extracts the column alias or name from a PySpark Column or ConnectColumn expression.

PySpark does not provide direct access to the alias of an unbound column, so this function
parses the alias from the column&#x27;s string representation.

- Supports columns with one or multiple aliases.
- Ensures the extracted expression is truncated to 255 characters.
- Provides an optional normalization step for consistent naming.

**Arguments**:

- `column`: Column, ConnectColumn or string representing a column.
- `normalize`: If True, normalizes the column name (removes special characters, converts to lowercase).
- `allow_simple_expressions_only`: If True, raises an error if the column expression is not a simple expression.
Complex PySpark expressions (e.g., conditionals, arithmetic, or nested transformations), cannot be fully
reconstructed correctly when converting to string (e.g. F.col(&quot;a&quot;) + F.lit(1)).
However, in certain situations this is acceptable, e.g. when using the output for reporting purposes.

**Raises**:

- `ValueError`: If the column expression is invalid.
- `TypeError`: If the column type is unsupported.

**Returns**:

The extracted column alias or name.

#### get\_columns\_as\_strings

```python
def get_columns_as_strings(
        columns: list[str | Column],
        allow_simple_expressions_only: bool = True) -> list[str]
```

Extracts column names from a list of PySpark Column or ConnectColumn expressions.

This function processes each column, ensuring that only valid column names are returned.

**Arguments**:

- `columns`: List of columns, ConnectColumns or strings representing columns.
- `allow_simple_expressions_only`: If True, raises an error if the column expression is not a simple expression.

**Returns**:

List of column names as strings.

#### is\_simple\_column\_expression

```python
def is_simple_column_expression(col_name: str) -> bool
```

Returns True if the column name does not contain any disallowed characters:

space, comma, semicolon, curly braces, parentheses, newline, tab, or equals sign.

**Arguments**:

- `col_name`: Column name to validate.

**Returns**:

True if the column name is valid, False otherwise.

#### normalize\_bound\_args

```python
def normalize_bound_args(val: Any) -> Any
```

Normalize a value or collection of values for consistent processing.

Handles primitives, dates, and column-like objects. Lists, tuples, and sets are
recursively normalized with type preserved.

**Arguments**:

- `val`: Value or collection of values to normalize.

**Raises**:

- `ValueError`: If a column resolves to an invalid name.
- `TypeError`: If a column type is unsupported.

**Returns**:

Normalized value or collection.

#### normalize\_col\_str

```python
def normalize_col_str(col_str: str) -> str
```

Normalizes string to be compatible with metastore column names by applying the following transformations:

* remove special characters
* convert to lowercase
* limit the length to 255 characters to be compatible with metastore column names

**Arguments**:

- `col_str`: Column or string representing a column.

**Returns**:

Normalized column name.

#### read\_input\_data

```python
def read_input_data(spark: SparkSession,
                    input_config: InputConfig) -> DataFrame
```

Reads input data from the specified location and format.

**Arguments**:

- `spark`: SparkSession
- `input_config`: InputConfig with source location/table name, format, and options

**Returns**:

DataFrame with values read from the input data

#### save\_dataframe\_as\_table

```python
def save_dataframe_as_table(df: DataFrame, output_config: OutputConfig)
```

Helper method to save a DataFrame to a Delta table.

**Arguments**:

- `df`: The DataFrame to save
- `output_config`: Output table name, write mode, and options

#### safe\_json\_load

```python
def safe_json_load(value: str)
```

Safely load a JSON string, returning the original value if it fails to parse.

This allows to specify string value without a need to escape the quotes.

**Arguments**:

- `value`: The value to parse as JSON.

