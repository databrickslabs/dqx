# databricks.labs.dqx.actions.definition\_storage

Storage handlers for persisting *DQAction* definitions.

This module provides:

* *ActionsStorageHandler* — abstract base class with *save* and *load* methods.
* *TableActionsStorageHandler* — persists actions to a Unity Catalog Delta table via Spark.
* *LakebaseActionsStorageHandler* — persists actions to a Lakebase (PostgreSQL) table via SQLAlchemy.
* *ActionsStorageHandlerFactory* — selects the right handler from config type.

## Schema[​](#schema "Direct link to Schema")

Both handlers store each *DQAction* as a serialized JSON string alongside the *run\_config\_name* and a *created\_at* timestamp:

* *action\_json* — JSON string produced by *ActionSerializer.to\_dict*.
* *run\_config\_name* — run configuration name for multi-run isolation.
* *created\_at* — timestamp at write time.

## Security[​](#security "Direct link to Security")

User-supplied values are sanitized before appearing in log messages (CWE-117).

### build\_replace\_where\_predicate[​](#build_replace_where_predicate "Direct link to build_replace_where_predicate")

```python
def build_replace_where_predicate(run_config_name: str) -> str

```

Build a safe Delta *replaceWhere* predicate for *run\_config\_name*.

Validates that *run\_config\_name* contains only characters in the set *\[A-Za-z0-9\_.-]* to prevent SQL injection (CWE-89) in the predicate string. If the name contains any other character, *UnsafeSqlQueryError* is raised before the predicate is constructed.

This mirrors the identical guard in *TableChecksStorageHandler.save* in *checks\_storage.py*.

**Arguments**:

* `run_config_name` - The run configuration name to embed in the predicate.

**Returns**:

A SQL predicate string that compares *run\_config\_name* against its validated value, safe for use as a Delta *replaceWhere* option.

**Raises**:

* `UnsafeSqlQueryError` - If *run\_config\_name* contains characters outside *\[A-Za-z0-9\_.-]*.

## ActionsStorageHandler Objects[​](#actionsstoragehandler-objects "Direct link to ActionsStorageHandler Objects")

```python
class ActionsStorageHandler(ABC, Generic[T])

```

Abstract base class for DQAction definition storage handlers.

Subclasses implement persistence to a specific backend (Delta table or Lakebase PostgreSQL).

**Arguments**:

* `T` - The config type that parameterizes this handler.

### save[​](#save "Direct link to save")

```python
@abstractmethod
def save(actions: list[DQAction], config: T) -> None

```

Persist *actions* to storage.

**Arguments**:

* `actions` - List of *DQAction* definitions to persist.
* `config` - Backend-specific configuration.

### load[​](#load "Direct link to load")

```python
@abstractmethod
def load(config: T) -> list[DQAction]

```

Load *DQAction* definitions from storage.

**Arguments**:

* `config` - Backend-specific configuration.

**Returns**:

List of *DQAction* instances loaded from storage.

## TableActionsStorageHandler Objects[​](#tableactionsstoragehandler-objects "Direct link to TableActionsStorageHandler Objects")

```python
class TableActionsStorageHandler(
        ActionsStorageHandler[TableActionsStorageConfig])

```

Persists *DQAction* definitions to a Unity Catalog Delta table via Spark.

Each *DQAction* is serialized to JSON via *ActionSerializer.to\_dict* and stored as a single row alongside the *run\_config\_name* and a *created\_at* timestamp.

On *save*, the write mode from *config.mode* controls whether existing rows for the *run\_config\_name* are replaced (*"overwrite"*) or kept (*"append"*).

On *load*, all rows matching *config.run\_config\_name* are read and deserialized.

**Arguments**:

* `spark` - Active *SparkSession* for Spark-based read/write.
* `ws` - Authenticated *WorkspaceClient* (reserved for future use such as table-existence checks).

### save[​](#save-1 "Direct link to save")

```python
def save(actions: list[DQAction], config: TableActionsStorageConfig) -> None

```

Serialize and write *actions* to the configured Delta table.

**Arguments**:

* `actions` - List of *DQAction* definitions to persist.
* `config` - *TableActionsStorageConfig* with target table, mode, and run config name.

**Raises**:

* `UnsafeSqlQueryError` - If *run\_config\_name* contains characters outside *\[A-Za-z0-9\_.-]* when *mode* is *"overwrite"*, raised by *\_build\_replace\_where* to prevent SQL injection in the Delta *replaceWhere* predicate (CWE-89).

### load[​](#load-1 "Direct link to load")

```python
def load(config: TableActionsStorageConfig) -> list[DQAction]

```

Read and deserialize *DQAction* definitions from the Delta table.

Returns an empty list when the table does not exist.

**Arguments**:

* `config` - *TableActionsStorageConfig* with source table and run config name.

**Returns**:

List of *DQAction* instances.

## LakebaseActionsStorageHandler Objects[​](#lakebaseactionsstoragehandler-objects "Direct link to LakebaseActionsStorageHandler Objects")

```python
class LakebaseActionsStorageHandler(
        LakebaseConnectionMixin,
        ActionsStorageHandler[LakebaseActionsStorageConfig])

```

Persists *DQAction* definitions to a Lakebase (PostgreSQL) table via SQLAlchemy.

Inherits engine lifecycle management from *LakebaseConnectionMixin*: a lazily created, cached engine with a *do\_connect* listener that refreshes the Databricks-generated credential token before each connection, and schema / table bootstrap on first use.

The handler accepts an optional pre-built *Engine* for testability — pass an in-memory or test engine via the *engine* constructor parameter to avoid needing a real Lakebase instance in unit tests.

**Arguments**:

* `spark` - Active *SparkSession* (kept for interface symmetry; not used for PostgreSQL queries).
* `ws` - Authenticated *WorkspaceClient* used to retrieve the Lakebase DNS and generate short-lived credentials.
* `config` - *LakebaseActionsStorageConfig* with instance and table details.
* `engine` - Optional pre-built SQLAlchemy *Engine* (useful for testing).

### save[​](#save-2 "Direct link to save")

```python
def save(actions: list[DQAction],
         config: LakebaseActionsStorageConfig) -> None

```

Serialize and write *actions* to the Lakebase table.

Bootstraps the schema and table on first use. When *config.mode* is *"overwrite"*, all existing rows for *config.run\_config\_name* are deleted before inserting the new rows.

**Arguments**:

* `actions` - List of *DQAction* definitions to persist.
* `config` - *LakebaseActionsStorageConfig* with instance and table details.

### load[​](#load-2 "Direct link to load")

```python
def load(config: LakebaseActionsStorageConfig) -> list[DQAction]

```

Read and deserialize *DQAction* definitions from the Lakebase table.

Returns an empty list when the table does not exist.

**Arguments**:

* `config` - *LakebaseActionsStorageConfig* with instance and table details.

**Returns**:

List of *DQAction* instances.

## ActionsStorageHandlerFactory Objects[​](#actionsstoragehandlerfactory-objects "Direct link to ActionsStorageHandlerFactory Objects")

```python
class ActionsStorageHandlerFactory()

```

Creates the appropriate *ActionsStorageHandler* for a given config type.

Selection logic:

* *LakebaseActionsStorageConfig* → *LakebaseActionsStorageHandler*
* *TableActionsStorageConfig* → *TableActionsStorageHandler*

**Arguments**:

* `config` - Storage configuration; either *TableActionsStorageConfig* or *LakebaseActionsStorageConfig*.
* `spark` - Active *SparkSession*.
* `ws` - Authenticated *WorkspaceClient*.

**Returns**:

A concrete *ActionsStorageHandler* instance.

### create[​](#create "Direct link to create")

```python
@staticmethod
def create(
    config: TableActionsStorageConfig | LakebaseActionsStorageConfig,
    spark: SparkSession, ws: WorkspaceClient
) -> ActionsStorageHandler[TableActionsStorageConfig] | ActionsStorageHandler[
        LakebaseActionsStorageConfig]

```

Instantiate the correct storage handler for *config*.

**Arguments**:

* `config` - Storage configuration.
* `spark` - Active *SparkSession*.
* `ws` - Authenticated *WorkspaceClient*.

**Returns**:

*LakebaseActionsStorageHandler* when *config* is a *LakebaseActionsStorageConfig*; *TableActionsStorageHandler* otherwise.

### save[​](#save-3 "Direct link to save")

```python
@staticmethod
def save(actions: list[DQAction],
         config: TableActionsStorageConfig | LakebaseActionsStorageConfig,
         spark: SparkSession, ws: WorkspaceClient) -> None

```

Create the appropriate handler and persist *actions* to storage.

**Arguments**:

* `actions` - List of *DQAction* definitions to persist.
* `config` - Backend-specific configuration.
* `spark` - Active *SparkSession*.
* `ws` - Authenticated *WorkspaceClient*.

### load[​](#load-3 "Direct link to load")

```python
@staticmethod
def load(config: TableActionsStorageConfig | LakebaseActionsStorageConfig,
         spark: SparkSession, ws: WorkspaceClient) -> list[DQAction]

```

Create the appropriate handler and load *DQAction* definitions from storage.

**Arguments**:

* `config` - Backend-specific configuration.
* `spark` - Active *SparkSession*.
* `ws` - Authenticated *WorkspaceClient*.

**Returns**:

List of *DQAction* instances loaded from storage.
