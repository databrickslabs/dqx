# databricks.labs.dqx.table\_manager

## TableDataProvider Objects[​](#tabledataprovider-objects "Direct link to TableDataProvider Objects")

```python
class TableDataProvider(Protocol)

```

Protocol defining the interface for table data access operations.

### get\_table\_columns[​](#get_table_columns "Direct link to get_table_columns")

```python
def get_table_columns(table: str) -> DataFrame

```

Retrieve table column definitions.

**Arguments**:

* `table` - Fully qualified table name.

**Returns**:

DataFrame with columns: col\_name, data\_type, comment.

### get\_existing\_primary\_key[​](#get_existing_primary_key "Direct link to get_existing_primary_key")

```python
def get_existing_primary_key(table: str) -> str | None

```

Retrieve existing primary key constraint from table properties.

**Arguments**:

* `table` - Fully qualified table name.

**Returns**:

Primary key constraint string if exists, None otherwise.

### get\_table\_properties[​](#get_table_properties "Direct link to get_table_properties")

```python
def get_table_properties(table: str) -> DataFrame

```

Retrieve table properties/metadata.

**Arguments**:

* `table` - Fully qualified table name.

**Returns**:

DataFrame with columns: key, value containing table properties.

### get\_table\_foreign\_keys[​](#get_table_foreign_keys "Direct link to get_table_foreign_keys")

```python
def get_table_foreign_keys(table: str) -> dict[str, dict[str, Any]]

```

Retrieve foreign key constraints from table properties.

**Arguments**:

* `table` - Fully qualified table name.

**Returns**:

Dictionary mapping foreign key names to their metadata.

### get\_column\_statistics[​](#get_column_statistics "Direct link to get_column_statistics")

```python
def get_column_statistics(table: str) -> DataFrame

```

Retrieve column-level statistics and metadata.

**Arguments**:

* `table` - Fully qualified table name.

**Returns**:

DataFrame with columns: col\_name, data\_type, and other stats.

### get\_table\_column\_names[​](#get_table_column_names "Direct link to get_table_column_names")

```python
def get_table_column_names(table: str) -> list[str]

```

Get list of column names for a table.

**Arguments**:

* `table` - Fully qualified table name.

**Returns**:

List of column names.

### execute\_query[​](#execute_query "Direct link to execute_query")

```python
def execute_query(query: str) -> DataFrame

```

Execute a SQL query and return results.

**Arguments**:

* `query` - SQL query string.

**Returns**:

DataFrame containing query results.

**Raises**:

* `ValueError` - If query execution fails.

## SparkTableDataProvider Objects[​](#sparktabledataprovider-objects "Direct link to SparkTableDataProvider Objects")

```python
class SparkTableDataProvider()

```

Spark implementation of the TableDataProvider protocol.

This class encapsulates all Spark SQL operations for table metadata retrieval, providing a clean interface for accessing table data and structure.

**Attributes**:

* `spark` - SparkSession instance for executing SQL queries.

### \_\_init\_\_[​](#__init__ "Direct link to __init__")

```python
def __init__(spark: SparkSession | None = None) -> None

```

Initialize the Spark data provider.

**Arguments**:

* `spark` - SparkSession instance. If None, gets or creates a session.

### get\_table\_columns[​](#get_table_columns-1 "Direct link to get_table_columns")

```python
def get_table_columns(table: str) -> DataFrame

```

Retrieve table column definitions from DESCRIBE TABLE EXTENDED.

**Arguments**:

* `table` - Fully qualified table name.

**Returns**:

Pandas DataFrame with columns: col\_name, data\_type, comment.

**Raises**:

* `ValueError` - If table is not found.
* `TypeError` - If there's a type error in processing.

### get\_existing\_primary\_key[​](#get_existing_primary_key-1 "Direct link to get_existing_primary_key")

```python
def get_existing_primary_key(table: str) -> str | None

```

Retrieve existing primary key from table properties.

**Arguments**:

* `table` - Fully qualified table name.

**Returns**:

Primary key constraint string if exists, None otherwise.

### get\_table\_properties[​](#get_table_properties-1 "Direct link to get_table_properties")

```python
def get_table_properties(table: str) -> DataFrame

```

Retrieve table properties using SHOW TBLPROPERTIES.

**Arguments**:

* `table` - Fully qualified table name.

**Returns**:

Pandas DataFrame with columns: key, value.

### get\_table\_foreign\_keys[​](#get_table_foreign_keys-1 "Direct link to get_table_foreign_keys")

```python
def get_table_foreign_keys(table: str) -> dict[str, dict[str, Any]]

```

Retrieve foreign key constraints from table properties.

**Arguments**:

* `table` - Fully qualified table name.

**Returns**:

Dictionary mapping foreign key names to their metadata.

### get\_column\_statistics[​](#get_column_statistics-1 "Direct link to get_column_statistics")

```python
def get_column_statistics(table: str) -> DataFrame

```

Retrieve column statistics from DESCRIBE TABLE EXTENDED.

**Arguments**:

* `table` - Fully qualified table name.

**Returns**:

Pandas DataFrame with column information.

### get\_table\_column\_names[​](#get_table_column_names-1 "Direct link to get_table_column_names")

```python
def get_table_column_names(table: str) -> list[str]

```

Get list of column names for a table.

**Arguments**:

* `table` - Fully qualified table name.

**Returns**:

List of column names.

### execute\_query[​](#execute_query-1 "Direct link to execute_query")

```python
def execute_query(query: str) -> DataFrame

```

Execute a SQL query and return Spark DataFrame.

**Arguments**:

* `query` - SQL query string.

**Returns**:

Spark DataFrame containing query results.

**Raises**:

* `Exception` - If query execution fails.

## TableDefinitionBuilder Objects[​](#tabledefinitionbuilder-objects "Direct link to TableDefinitionBuilder Objects")

```python
class TableDefinitionBuilder()

```

Builder for constructing table definition strings.

This class uses the Builder pattern to construct complex table definition strings step by step, separating the construction logic from representation.

### \_\_init\_\_[​](#__init__-1 "Direct link to __init__")

```python
def __init__() -> None

```

Initialize the builder with empty state.

### add\_columns[​](#add_columns "Direct link to add_columns")

```python
def add_columns(columns: list[str]) -> "TableDefinitionBuilder"

```

Add column definitions to the table.

**Arguments**:

* `columns` - List of column definition strings (e.g., "id bigint").

**Returns**:

Self for method chaining.

### add\_primary\_key[​](#add_primary_key "Direct link to add_primary_key")

```python
def add_primary_key(primary_key: str | None) -> "TableDefinitionBuilder"

```

Add primary key constraint information.

**Arguments**:

* `primary_key` - Primary key constraint string, or None if no PK exists.

**Returns**:

Self for method chaining.

### build[​](#build "Direct link to build")

```python
def build() -> str

```

Build and return the final table definition string.

**Returns**:

Formatted table definition string.

## MetadataFormatter Objects[​](#metadataformatter-objects "Direct link to MetadataFormatter Objects")

```python
class MetadataFormatter(ABC)

```

Abstract base class for metadata formatting strategies.

This uses the Strategy pattern to allow different formatting approaches for various types of metadata.

### format[​](#format "Direct link to format")

```python
@abstractmethod
def format(data: DataFrame) -> list[str]

```

Format metadata from a DataFrame into string lines.

**Arguments**:

* `data` - DataFrame containing metadata to format.

**Returns**:

List of formatted string lines.

## PropertyMetadataFormatter Objects[​](#propertymetadataformatter-objects "Direct link to PropertyMetadataFormatter Objects")

```python
class PropertyMetadataFormatter(MetadataFormatter)

```

Formatter for table property metadata.

Extracts and formats useful properties like row counts, data sizes, and constraint information.

### format[​](#format-1 "Direct link to format")

```python
def format(data: DataFrame) -> list[str]

```

Extract useful properties from table properties DataFrame.

**Arguments**:

* `data` - DataFrame with columns: key, value.

**Returns**:

List of formatted property strings.

## ColumnStatisticsFormatter Objects[​](#columnstatisticsformatter-objects "Direct link to ColumnStatisticsFormatter Objects")

```python
class ColumnStatisticsFormatter(MetadataFormatter)

```

Formatter for column statistics and type distribution.

Categorizes columns by data type and formats distribution information.

### format[​](#format-2 "Direct link to format")

```python
def format(data: DataFrame) -> list[str]

```

Format column type distribution from column statistics.

**Arguments**:

* `data` - DataFrame with columns: col\_name, data\_type.

**Returns**:

List of formatted column distribution strings.

## ColumnDefinitionExtractor Objects[​](#columndefinitionextractor-objects "Direct link to ColumnDefinitionExtractor Objects")

```python
class ColumnDefinitionExtractor()

```

Extracts and formats column definitions from DESCRIBE TABLE results.

This class handles the parsing of DESCRIBE TABLE output and converts it into formatted column definition strings.

### extract\_columns[​](#extract_columns "Direct link to extract_columns")

```python
@staticmethod
def extract_columns(describe_df: DataFrame) -> list[str]

```

Extract column definitions from DESCRIBE TABLE DataFrame.

**Arguments**:

* `describe_df` - DataFrame from DESCRIBE TABLE EXTENDED query.

**Returns**:

List of formatted column definition strings.

## TableManager Objects[​](#tablemanager-objects "Direct link to TableManager Objects")

```python
class TableManager()

```

Facade for table operations providing schema retrieval and metadata checking.

This class acts as a simplified interface (Facade pattern) that coordinates between the data repository and formatters. It delegates actual operations to specialized components.

**Attributes**:

* `repository` - Data provider for table operations (defaults to SparkTableDataProvider)
* `property_formatter` - Formatter for table property metadata
* `stats_formatter` - Formatter for column statistics and distribution

### \_\_init\_\_[​](#__init__-2 "Direct link to __init__")

```python
def __init__(spark: SparkSession | None = None, repository=None) -> None

```

Initialize TableManager with optional dependency injection.

**Arguments**:

* `spark` - SparkSession instance. Used if repository is not provided.
* `repository` - Optional TableDataProvider implementation. If None, creates SparkTableDataProvider with the provided spark session.

### get\_table\_definition[​](#get_table_definition "Direct link to get_table_definition")

```python
def get_table_definition(table: str) -> str

```

Retrieve table definition using repository and formatters.

This method coordinates between the repository for data access and the builder/extractor for formatting the result.

**Arguments**:

* `table` - Fully qualified table name.

**Returns**:

Formatted table definition string with columns and primary key.

### get\_table\_metadata\_info[​](#get_table_metadata_info "Direct link to get_table_metadata_info")

```python
def get_table_metadata_info(table: str) -> str

```

Get additional metadata information to help with primary key detection.

This method coordinates multiple formatters to build comprehensive metadata information from the repository.

**Arguments**:

* `table` - Fully qualified table name.

**Returns**:

Formatted metadata information string.

### get\_table\_column\_names[​](#get_table_column_names-2 "Direct link to get_table_column_names")

```python
def get_table_column_names(table: str) -> list[str]

```

Get table column names.

**Arguments**:

* `table` - Fully qualified table name.

**Returns**:

List of column names.

### run\_sql[​](#run_sql "Direct link to run_sql")

```python
def run_sql(query: str)

```

Run a SQL query and return the result DataFrame.

**Arguments**:

* `query` - SQL query string.

**Returns**:

Spark DataFrame containing query results.
