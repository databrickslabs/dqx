# databricks.labs.dqx.utils

### to\_utc[​](#to_utc "Direct link to to_utc")

```python
def to_utc(value: datetime.datetime) -> datetime.datetime

```

Return *value* as a timezone-aware UTC datetime.

Datetimes read back from Spark or SQL *TIMESTAMP* columns can be timezone-naive (for example, Spark Connect returns naive datetimes), while values produced in-process are timezone-aware UTC. Mixing the two in arithmetic raises *TypeError*, so this normalizes everything to UTC: a naive value is assumed to already be in UTC and given UTC tzinfo, and an aware value is converted to UTC.

**Arguments**:

* `value` - A timezone-aware or timezone-naive datetime.

**Returns**:

The equivalent timezone-aware UTC datetime.

#### SparkColumn[​](#sparkcolumn "Direct link to SparkColumn")

Pydantic-compatible Column type that accepts both classic and Spark Connect columns.

#### VariableValue[​](#variablevalue "Direct link to VariableValue")

Supported scalar types for variable substitution values.

### get\_column\_name\_or\_alias[​](#get_column_name_or_alias "Direct link to get_column_name_or_alias")

```python
def get_column_name_or_alias(
        column: "str | Column | ConnectColumn",
        normalize: bool = False,
        allow_simple_expressions_only: bool = False) -> str

```

Extracts the column alias or name from a PySpark Column or ConnectColumn expression.

PySpark does not provide direct access to the alias of an unbound column, so this function parses the alias from the column's string representation.

* Supports columns with one or multiple aliases.
* Ensures the extracted expression is truncated to 255 characters.
* Provides an optional normalization step for consistent naming.
* Supports ConnectColumn when PySpark Connect is available (falls back gracefully when not available).

**Arguments**:

* `column` - Column, ConnectColumn (if PySpark Connect available), or string representing a column.
* `normalize` - If True, normalizes the column name (removes special characters, converts to lowercase).
* `allow_simple_expressions_only` - If True, raises an error if the column expression is not a simple expression. Complex PySpark expressions (e.g., conditionals, arithmetic, or nested transformations), cannot be fully reconstructed correctly when converting to string (e.g. F.col("a") + F.lit(1)). However, in certain situations this is acceptable, e.g. when using the output for reporting purposes.

**Returns**:

The extracted column alias or name.

**Raises**:

* `InvalidParameterError` - If the column expression is invalid or unsupported.

### get\_columns\_as\_strings[​](#get_columns_as_strings "Direct link to get_columns_as_strings")

```python
def get_columns_as_strings(
        columns: list[str | Column],
        allow_simple_expressions_only: bool = True) -> list[str]

```

Extracts column names from a list of PySpark Column or ConnectColumn expressions.

This function processes each column, ensuring that only valid column names are returned. Supports ConnectColumn when PySpark Connect is available (falls back gracefully when not available).

**Arguments**:

* `columns` - List of columns, ConnectColumns (if PySpark Connect available), or strings representing columns.
* `allow_simple_expressions_only` - If True, raises an error if the column expression is not a simple expression.

**Returns**:

List of column names as strings.

**Raises**:

* `InvalidParameterError` - If any column expression is invalid or unsupported.

### is\_simple\_column\_expression[​](#is_simple_column_expression "Direct link to is_simple_column_expression")

```python
def is_simple_column_expression(col_name: str) -> bool

```

Returns True if the column name does not contain any disallowed characters: space, comma, semicolon, curly braces, parentheses, newline, tab, or equals sign.

**Arguments**:

* `col_name` - Column name to validate.

**Returns**:

True if the column name is valid, False otherwise.

### normalize\_bound\_args[​](#normalize_bound_args "Direct link to normalize_bound_args")

```python
def normalize_bound_args(val: Any,
                         allow_simple_expressions_only: bool = True) -> Any

```

Normalize a value or collection of values for consistent processing.

Handles primitives, dates, Decimal, and column-like objects. Collections are recursively normalized, preserving list and tuple order while canonicalizing set and frozenset order.

For Decimal values, uses a special JSON-serializable format to preserve type information for round-trip deserialization.

**Arguments**:

* `val` - Value or collection of values to normalize.
* `allow_simple_expressions_only` - If True (default), Column values must be simple expressions (e.g. F.col("name")). If False, complex expressions (e.g. F.try\_element\_at(...)) are allowed and serialized as their string representation. Use False when serializing for fingerprinting/metadata only, where round-trip reconstruction is not required.

**Returns**:

Normalized value or collection.

**Raises**:

* `TypeError` - If a column type is unsupported.

### quote\_column\_name[​](#quote_column_name "Direct link to quote_column_name")

```python
def quote_column_name(name: str) -> str

```

Wraps a column name in backticks so it can be used as a SQL identifier.

Column names containing spaces, non-ASCII characters, or other characters that require escaping (e.g. "Customer Name", "Ääkkönen") are not valid bare SQL identifiers and must be back-quoted before being parsed by `F.expr`.

**Arguments**:

* `name` - Column name to quote.

**Returns**:

The column name wrapped in backticks with embedded backticks escaped.

### normalize\_col\_str[​](#normalize_col_str "Direct link to normalize_col_str")

```python
def normalize_col_str(col_str: str) -> str

```

Normalizes string to be compatible with metastore column names by applying the following transformations:

* remove special characters
* convert to lowercase
* limit the length to 255 characters to be compatible with metastore column names

**Arguments**:

* `col_str` - Column or string representing a column.

**Returns**:

Normalized column name.

### is\_sql\_query\_safe[​](#is_sql_query_safe "Direct link to is_sql_query_safe")

```python
def is_sql_query_safe(query: str) -> bool

```

Returns True if the query contains no destructive SQL statement keyword, otherwise False.

Quoted string literals and backtick-quoted identifiers are stripped before scanning so that a forbidden keyword appearing as data (e.g. `status = &#x27;drop&#x27;`) or as a quoted column name (e.g. `` `drop` ``) is not mistaken for a statement. Comments are checked two ways so both a keyword *inside* a comment (e.g. `/* delete */`) and a keyword *split* by a comment (e.g. `dr/**/op`) are caught. `SELECT` is allowed: filters may legitimately use subqueries and are authored by trusted operators.

**Arguments**:

* \`\`1 - The SQL query or filter predicate to validate.

**Returns**:

True if the query is free of destructive statement keywords, False otherwise.

### safe\_filter\_expr[​](#safe_filter_expr "Direct link to safe_filter_expr")

```python
def safe_filter_expr(filter_expr: str | None) -> Column

```

Build a Spark column from a filter expression, rejecting unsafe SQL.

Validates the filter with *is\_sql\_query\_safe* before compiling it. Destructive statements (e.g. DELETE, DROP) are rejected; SELECT and subqueries are allowed, since filters are authored by trusted operators. Used for both check filters and *row\_filter* parameters.

**Arguments**:

* `filter_expr` - The filter predicate as a string, or None.

**Returns**:

The compiled filter column, or a literal true column when no filter is given.

**Raises**:

* `UnsafeSqlQueryError` - If the filter contains a destructive statement such as DELETE or DROP.

### sanitize\_for\_logging[​](#sanitize_for_logging "Direct link to sanitize_for_logging")

```python
def sanitize_for_logging(value: str) -> str

```

Escapes newline and carriage-return characters in a user-supplied string so it cannot forge or corrupt log entries (CWE-117) when interpolated into a log message or user-facing result message.

**Arguments**:

* `value` - The untrusted string to sanitize.

**Returns**:

The string with CR and LF replaced by their escaped literal representations.

### safe\_json\_load[​](#safe_json_load "Direct link to safe_json_load")

```python
def safe_json_load(value: str | None) -> object

```

Safely load a JSON string, returning the original value if it fails to parse. This allows to specify string value without a need to escape the quotes.

A *None* value is returned unchanged (a stored MAP\<STRING, STRING> may contain SQL NULL values, which surface as None and must not be passed to json.loads).

**Arguments**:

* `value` - The value to parse as JSON, or None.

### safe\_strip\_file\_from\_path[​](#safe_strip_file_from_path "Direct link to safe_strip_file_from_path")

```python
def safe_strip_file_from_path(path: str) -> str

```

Safely removes the file name from a given path, treating it as a directory if no file extension is present.

* Hidden directories (e.g., .folder) are preserved.
* Hidden files with extensions (e.g., .file.yml) are treated as files.

**Arguments**:

* `path` - The input path from which to remove the file name.

**Returns**:

The path without the file name, or the original path if it is already a directory.

### list\_tables[​](#list_tables "Direct link to list_tables")

```python
@rate_limited(max_requests=100)
def list_tables(workspace_client: WorkspaceClient,
                patterns: list[str] | None,
                exclude_matched: bool = False,
                exclude_patterns: list[str] | None = None) -> list[str]

```

Gets a list of table names from Unity Catalog given a list of wildcard patterns.

**Arguments**:

* `workspace_client` *WorkspaceClient* - Databricks SDK WorkspaceClient.
* `patterns` *list\[str] | None* - A list of wildcard patterns to match against the table name.
* `exclude_matched` *bool* - Specifies whether to include tables matched by the pattern. If True, matched tables are excluded. If False, matched tables are included.
* `exclude_patterns` *list\[str] | None* - A list of wildcard patterns to exclude from the table names.

**Returns**:

* `list[str]` - A list of fully qualified table names. DataFrame with values read from the input data

**Raises**:

* `NotFound` - If no tables are found matching the include or exclude criteria.

### to\_lowercase[​](#to_lowercase "Direct link to to_lowercase")

```python
def to_lowercase(col_expr: Column, is_array: bool = False) -> Column

```

Converts a column expression to lowercase, handling both scalar and array types.

**Arguments**:

* `col_expr` - Column expression to convert
* `is_array` - Whether the column contains array values

**Returns**:

Column expression with lowercase transformation applied

### table\_exists[​](#table_exists "Direct link to table_exists")

```python
def table_exists(spark: Any, table: str) -> bool

```

Check if a table exists (Unity Catalog compatible).

Uses the catalog API only (no Spark job). Requires Spark 3.4+ for fully qualified table names (e.g. catalog.schema.table).

**Arguments**:

* `spark` - SparkSession instance.
* `table` - Fully qualified table name (e.g. "catalog.schema.table").

**Returns**:

True if the table exists, False otherwise.

### get\_table\_primary\_keys[​](#get_table_primary_keys "Direct link to get_table_primary_keys")

```python
def get_table_primary_keys(table: str, spark: Any) -> set[str]

```

Retrieve primary key columns from Unity Catalog table metadata.

Uses SparkTableDataProvider (table\_manager) to read table properties and parses the primary key constraint into a set of column names.

**Arguments**:

* `table` - Fully qualified table name (e.g., "catalog.schema.table")
* `spark` - SparkSession instance

**Returns**:

Set of column names that are primary keys. Returns empty set if:

* Table doesn't exist
* No primary key is defined
* Metadata is not accessible

**Examples**:

\>>> pk\_cols = get\_table\_primary\_keys("main.default.users", spark) >>> if "user\_id" in pk\_cols: ... print("user\_id is a primary key")

### get\_table\_column\_metadata[​](#get_table_column_metadata "Direct link to get_table_column_metadata")

```python
def get_table_column_metadata(workspace_client: WorkspaceClient,
                              table: str) -> str

```

Gets column metadata for a Unity Catalog table using the Databricks SDK.

**Notes**:

This method is used in place of *databricks.labs.dqx.llm.llm\_utils.get\_column\_metadata* when no Spark session is available. Schemas are read from Unity Catalog. The returned JSON has the same shape, so methods can be used interchangeably for LLM-based rule generation.

**Arguments**:

* `workspace_client` - Databricks WorkspaceClient instance.
* `table` - Fully qualified table name (e.g. *catalog.schema.table*). Backtick-quoted identifiers are accepted and unquoted before the lookup.

**Returns**:

A JSON string containing the column metadata with columns wrapped in a "columns" key.

**Raises**:

* `NotFound` - If the table does not exist or is not accessible.

### missing\_required\_packages[​](#missing_required_packages "Direct link to missing_required_packages")

```python
def missing_required_packages(packages: list[str]) -> bool

```

Checks if any of the required packages are missing.

**Arguments**:

* `packages` - A list of package names to check.

**Returns**:

True if any package is missing, False otherwise.

### resolve\_variables[​](#resolve_variables "Direct link to resolve_variables")

```python
def resolve_variables(
        checks: list[dict],
        variables: dict[str, VariableValue] | None) -> list[dict]

```

Resolve variable substitution in check definitions.

Replaces placeholders in all string values of *checks* with the corresponding values from *variables*.

Variable values must be scalar types (e.g. *str*, *int*, *float*, *bool*, *Decimal*, *datetime.date*, *datetime.datetime*, *datetime.time*). Non-string scalars are converted to strings via *str()* in the substituted string. Collection type variables (e.g. *list*, *dict*, *set*, etc.) are rejected with *databricks.labs.dqx.errors.InvalidParameterError* because their string representation is rarely meaningful in SQL or column expressions.

Logs a warning for any placeholders that remain unresolved after substitution (e.g. misspelled variable names).

**Notes**:

Variable values substituted into *sql\_expression* checks are not sanitized and are passed directly to *F.expr()*. Callers must **ensure variable values come from trusted sources** to prevent SQL injection.

**Arguments**:

* `checks` - List of check definition dictionaries (metadata format).
* `variables` - Mapping of placeholder names to scalar replacement values. If *None* or empty the checks are returned unchanged.

**Returns**:

A new list of check dicts with placeholders resolved, or the original list when no substitution is needed.

**Raises**:

* `InvalidParameterError` - If any variable value is not a supported scalar type.

### get\_file\_extension[​](#get_file_extension "Direct link to get_file_extension")

```python
def get_file_extension(file_path: str | os.PathLike) -> str

```

Extract file extension from a file path.

**Arguments**:

* `file_path` - File path as string or path-like object.

**Returns**:

File extension (e.g., ".json", ".yaml", ".yml") or empty string if no extension.
