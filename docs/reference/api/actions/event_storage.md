# databricks.labs.dqx.actions.event\_storage

Concrete event store implementations for the DQX actions & alerting subsystem.

This module provides:

* *ACTION\_EVENT\_TABLE\_SCHEMA* — Spark DDL schema string for the events Delta table.
* *TableActionEventStore* — appends *AlertEvent* records to a Unity Catalog Delta table via Spark and reads back the latest event per action using a window function.
* *LakebaseActionEventStore* — mirrors the pattern from *LakebaseChecksStorageHandler* to persist and load events via SQLAlchemy / PostgreSQL (Databricks Lakebase).
* *ActionEventStoreFactory* — selects the appropriate store based on *ActionEventsConfig* fields.

## TableActionEventStore Objects[​](#tableactioneventstore-objects "Direct link to TableActionEventStore Objects")

```python
class TableActionEventStore(ActionEventStore)

```

Persists *AlertEvent* records to a Unity Catalog Delta table via Spark.

Events are appended to a Delta table with the schema defined in *ACTION\_EVENT\_TABLE\_SCHEMA*. Every row is stamped with the store's *run\_config\_name*, and loading the latest event per action first filters to that *run\_config\_name* — so several run configs can share one events table without their alert suppression interfering. Loading uses a window function partitioned by *action\_name* and ordered by *run\_time* descending, selecting rank == 1.

**Arguments**:

* `spark` - Active *SparkSession*.
* `ws` - Authenticated *WorkspaceClient* (reserved for future use such as table-existence checks).
* `config` - *ActionEventsConfig* carrying the target table name and the *run\_config\_name* the events are scoped to.

### append[​](#append "Direct link to append")

```python
def append(events: list[AlertEvent]) -> None

```

Convert *events* to rows and append them to the configured Delta table.

*observed\_metrics* values are coerced to *str* because the Delta schema stores them as a Spark *MAP* of string to string.

**Arguments**:

* `events` - One or more *AlertEvent* records to persist.

### load\_latest\_per\_action[​](#load_latest_per_action "Direct link to load_latest_per_action")

```python
def load_latest_per_action() -> dict[str, AlertEvent]

```

Read the Delta table and return the most recent *AlertEvent* per action.

Returns an empty dict when the table does not exist or contains no rows.

**Returns**:

Mapping of *action\_name* to its latest *AlertEvent*.

### load\_last\_fired\_per\_action[​](#load_last_fired_per_action "Direct link to load_last_fired_per_action")

```python
def load_last_fired_per_action() -> dict[str, datetime]

```

Return the *run\_time* of the most recent fired event per action for this run config.

Returns an empty dict when the table does not exist or has no fired events.

**Returns**:

Mapping of *action\_name* to the latest fired *run\_time*.

## LakebaseActionEventStore Objects[​](#lakebaseactioneventstore-objects "Direct link to LakebaseActionEventStore Objects")

```python
class LakebaseActionEventStore(LakebaseConnectionMixin, ActionEventStore)

```

Persists *AlertEvent* records to a Lakebase (PostgreSQL) table via SQLAlchemy.

Inherits engine lifecycle management from *LakebaseConnectionMixin*: a lazily created, cached engine with a *do\_connect* listener that refreshes the Databricks-generated credential token before each connection, and schema / table bootstrap on first use.

The *observed\_metrics* dict is serialized to a PostgreSQL *JSONB* column; values are stored as JSON-compatible objects (coerced to *str* on write and returned as *dict\[str, object]* on read).

**Arguments**:

* `spark` - Active *SparkSession* (kept for interface symmetry; not used for PostgreSQL queries but may be used for future cross-engine queries).
* `ws` - Authenticated *WorkspaceClient* used to retrieve the Lakebase instance DNS and generate short-lived credentials.
* `config` - *LakebaseActionsStorageConfig* with instance and table details.
* `engine` - Optional pre-built SQLAlchemy *Engine* (useful for testing without a real Lakebase instance).

### append[​](#append-1 "Direct link to append")

```python
def append(events: list[AlertEvent]) -> None

```

Persist *events* to the Lakebase table.

Bootstraps the schema and table on first use. *observed\_metrics* values are coerced to *str* before serialization into JSONB.

**Arguments**:

* `events` - One or more *AlertEvent* records to persist.

### load\_latest\_per\_action[​](#load_latest_per_action-1 "Direct link to load_latest_per_action")

```python
def load_latest_per_action() -> dict[str, AlertEvent]

```

Read the Lakebase table and return the most recent *AlertEvent* per action.

Returns an empty dict when the table does not exist or contains no rows.

**Returns**:

Mapping of *action\_name* to its latest *AlertEvent*.

### load\_last\_fired\_per\_action[​](#load_last_fired_per_action-1 "Direct link to load_last_fired_per_action")

```python
def load_last_fired_per_action() -> dict[str, datetime]

```

Return the *run\_time* of the most recent fired event per action for this run config.

Returns an empty dict when the table does not exist or has no fired events.

**Returns**:

Mapping of *action\_name* to the latest fired *run\_time*.

## ActionEventStoreFactory Objects[​](#actioneventstorefactory-objects "Direct link to ActionEventStoreFactory Objects")

```python
class ActionEventStoreFactory()

```

Creates the appropriate *ActionEventStore* implementation.

Selection logic:

* If *config* is a *LakebaseActionsStorageConfig* (has an *instance\_name* field), return a *LakebaseActionEventStore*.
* Otherwise (plain *ActionEventsConfig*), return a *TableActionEventStore*.

**Arguments**:

* `config` - Storage configuration; either *ActionEventsConfig* or *LakebaseActionsStorageConfig*.
* `spark` - Active *SparkSession*.
* `ws` - Authenticated *WorkspaceClient*.

**Returns**:

A concrete *ActionEventStore* instance.

### create[​](#create "Direct link to create")

```python
@staticmethod
def create(config: ActionEventsConfig | LakebaseActionsStorageConfig,
           spark: SparkSession, ws: WorkspaceClient) -> ActionEventStore

```

Instantiate the correct event store for *config*.

**Arguments**:

* `config` - Storage configuration.
* `spark` - Active *SparkSession*.
* `ws` - Authenticated *WorkspaceClient*.

**Returns**:

*LakebaseActionEventStore* when *config* is a *LakebaseActionsStorageConfig*; *TableActionEventStore* otherwise.
