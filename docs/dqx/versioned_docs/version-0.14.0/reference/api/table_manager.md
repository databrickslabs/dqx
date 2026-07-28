---
sidebar_label: table_manager
title: databricks.labs.dqx.table_manager
---

## TableDataProvider Objects

```python
class TableDataProvider(Protocol)
```

Protocol defining the interface for table data access operations.

#### get\_table\_columns

```python
def get_table_columns(table: str) -> DataFrame
```

Retrieve table column definitions.

**Arguments**:

- `table` - Fully qualified table name.
  

**Returns**:

  DataFrame with columns: col_name, data_type, comment.

#### get\_existing\_primary\_key

```python
def get_existing_primary_key(table: str) -> str | None
```

Retrieve existing primary key constraint from table properties.

**Arguments**:

- `table` - Fully qualified table name.
  

**Returns**:

  Primary key constraint string if exists, None otherwise.

#### get\_table\_properties

```python
def get_table_properties(table: str) -> DataFrame
```

Retrieve table properties/metadata.

**Arguments**:

- `table` - Fully qualified table name.
  

**Returns**:

  DataFrame with columns: key, value containing table properties.

#### get\_table\_foreign\_keys

```python
def get_table_foreign_keys(table: str) -> dict[str, dict[str, Any]]
```

Retrieve foreign key constraints from table properties.

**Arguments**:

- `table` - Fully qualified table name.
  

**Returns**:

  Dictionary mapping foreign key names to their metadata.

#### get\_column\_statistics

```python
def get_column_statistics(table: str) -> DataFrame
```

Retrieve column-level statistics and metadata.

**Arguments**:

- `table` - Fully qualified table name.
  

**Returns**:

  DataFrame with columns: col_name, data_type, and other stats.

#### get\_table\_column\_names

```python
def get_table_column_names(table: str) -> list[str]
```

Get list of column names for a table.

**Arguments**:

- `table` - Fully qualified table name.
  

**Returns**:

  List of column names.

#### execute\_query

```python
def execute_query(query: str) -> DataFrame
```

Execute a SQL query and return results.

**Arguments**:

- `query` - SQL query string.
  

**Returns**:

  DataFrame containing query results.
  

**Raises**:

- `ValueError` - If query execution fails.

## SparkTableDataProvider Objects

```python
class SparkTableDataProvider()
```

Spark implementation of the TableDataProvider protocol.

This class encapsulates all Spark SQL operations for table metadata retrieval,
providing a clean interface for accessing table data and structure.

**Attributes**:

- `spark` - SparkSession instance for executing SQL queries.

#### \_\_init\_\_

```python
def __init__(spark: SparkSession | None = None) -> None
```

Initialize the Spark data provider.

**Arguments**:

- `spark` - SparkSession instance. If None, gets or creates a session.

#### get\_table\_columns

```python
def get_table_columns(table: str) -> DataFrame
```

Retrieve table column definitions from DESCRIBE TABLE EXTENDED.

**Arguments**:

- `table` - Fully qualified table name.
  

**Returns**:

  Pandas DataFrame with columns: col_name, data_type, comment.
  

**Raises**:

- `ValueError` - If table is not found.
- `TypeError` - If there&#x27;s a type error in processing.

#### get\_existing\_primary\_key

```python
def get_existing_primary_key(table: str) -> str | None
```

Retrieve existing primary key from table properties.

**Arguments**:

- `table` - Fully qualified table name.
  

**Returns**:

  Primary key constraint string if exists, None otherwise.

#### get\_table\_properties

```python
def get_table_properties(table: str) -> DataFrame
```

Retrieve table properties using SHOW TBLPROPERTIES.

**Arguments**:

- `table` - Fully qualified table name.
  

**Returns**:

  Pandas DataFrame with columns: key, value.

#### get\_table\_foreign\_keys

```python
def get_table_foreign_keys(table: str) -> dict[str, dict[str, Any]]
```

Retrieve foreign key constraints from table properties.

**Arguments**:

- `table` - Fully qualified table name.
  

**Returns**:

  Dictionary mapping foreign key names to their metadata.

#### get\_column\_statistics

```python
def get_column_statistics(table: str) -> DataFrame
```

Retrieve column statistics from DESCRIBE TABLE EXTENDED.

**Arguments**:

- `table` - Fully qualified table name.
  

**Returns**:

  Pandas DataFrame with column information.

#### get\_table\_column\_names

```python
def get_table_column_names(table: str) -> list[str]
```

Get list of column names for a table.

**Arguments**:

- `table` - Fully qualified table name.
  

**Returns**:

  List of column names.

#### execute\_query

```python
def execute_query(query: str) -> DataFrame
```

Execute a SQL query and return Spark DataFrame.

**Arguments**:

- `query` - SQL query string.
  

**Returns**:

  Spark DataFrame containing query results.
  

**Raises**:

- `Exception` - If query execution fails.

## TableDefinitionBuilder Objects

```python
class TableDefinitionBuilder()
```

Builder for constructing table definition strings.

This class uses the Builder pattern to construct complex table definition
strings step by step, separating the construction logic from representation.

#### \_\_init\_\_

```python
def __init__() -> None
```

Initialize the builder with empty state.

#### add\_columns

```python
def add_columns(columns: list[str]) -> "TableDefinitionBuilder"
```

Add column definitions to the table.

**Arguments**:

- `columns` - List of column definition strings (e.g., &quot;id bigint&quot;).
  

**Returns**:

  Self for method chaining.

#### add\_primary\_key

```python
def add_primary_key(primary_key: str | None) -> "TableDefinitionBuilder"
```

Add primary key constraint information.

**Arguments**:

- `primary_key` - Primary key constraint string, or None if no PK exists.
  

**Returns**:

  Self for method chaining.

#### build

```python
def build() -> str
```

Build and return the final table definition string.

**Returns**:

  Formatted table definition string.

## MetadataFormatter Objects

```python
class MetadataFormatter(ABC)
```

Abstract base class for metadata formatting strategies.

This uses the Strategy pattern to allow different formatting
approaches for various types of metadata.

#### format

```python
@abstractmethod
def format(data: DataFrame) -> list[str]
```

Format metadata from a DataFrame into string lines.

**Arguments**:

- `data` - DataFrame containing metadata to format.
  

**Returns**:

  List of formatted string lines.

## PropertyMetadataFormatter Objects

```python
class PropertyMetadataFormatter(MetadataFormatter)
```

Formatter for table property metadata.

Extracts and formats useful properties like row counts, data sizes,
and constraint information.

#### format

```python
def format(data: DataFrame) -> list[str]
```

Extract useful properties from table properties DataFrame.

**Arguments**:

- `data` - DataFrame with columns: key, value.
  

**Returns**:

  List of formatted property strings.

## ColumnStatisticsFormatter Objects

```python
class ColumnStatisticsFormatter(MetadataFormatter)
```

Formatter for column statistics and type distribution.

Categorizes columns by data type and formats distribution information.

#### format

```python
def format(data: DataFrame) -> list[str]
```

Format column type distribution from column statistics.

**Arguments**:

- `data` - DataFrame with columns: col_name, data_type.
  

**Returns**:

  List of formatted column distribution strings.

## ColumnDefinitionExtractor Objects

```python
class ColumnDefinitionExtractor()
```

Extracts and formats column definitions from DESCRIBE TABLE results.

This class handles the parsing of DESCRIBE TABLE output and converts
it into formatted column definition strings.

#### extract\_columns

```python
@staticmethod
def extract_columns(describe_df: DataFrame) -> list[str]
```

Extract column definitions from DESCRIBE TABLE DataFrame.

**Arguments**:

- `describe_df` - DataFrame from DESCRIBE TABLE EXTENDED query.
  

**Returns**:

  List of formatted column definition strings.

## TableManager Objects

```python
class TableManager()
```

Facade for table operations providing schema retrieval and metadata checking.

This class acts as a simplified interface (Facade pattern) that coordinates
between the data repository and formatters. It delegates actual operations
to specialized components.

**Attributes**:

- `repository` - Data provider for table operations (defaults to SparkTableDataProvider)
- `property_formatter` - Formatter for table property metadata
- `stats_formatter` - Formatter for column statistics and distribution

#### \_\_init\_\_

```python
def __init__(spark: SparkSession | None = None, repository=None) -> None
```

Initialize TableManager with optional dependency injection.

**Arguments**:

- `spark` - SparkSession instance. Used if repository is not provided.
- `repository` - Optional TableDataProvider implementation. If None,
  creates SparkTableDataProvider with the provided spark session.

#### get\_table\_definition

```python
def get_table_definition(table: str) -> str
```

Retrieve table definition using repository and formatters.

This method coordinates between the repository for data access and
the builder/extractor for formatting the result.

**Arguments**:

- `table` - Fully qualified table name.
  

**Returns**:

  Formatted table definition string with columns and primary key.

#### get\_table\_metadata\_info

```python
def get_table_metadata_info(table: str) -> str
```

Get additional metadata information to help with primary key detection.

This method coordinates multiple formatters to build comprehensive
metadata information from the repository.

**Arguments**:

- `table` - Fully qualified table name.
  

**Returns**:

  Formatted metadata information string.

#### get\_table\_column\_names

```python
def get_table_column_names(table: str) -> list[str]
```

Get table column names.

**Arguments**:

- `table` - Fully qualified table name.
  

**Returns**:

  List of column names.

#### run\_sql

```python
def run_sql(query: str)
```

Run a SQL query and return the result DataFrame.

**Arguments**:

- `query` - SQL query string.
  

**Returns**:

  Spark DataFrame containing query results.

