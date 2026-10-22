# databricks.labs.dqx.lakebase_engine

Shared helpers for creating SQLAlchemy engines connected to Databricks Lakebase (PostgreSQL).

This module centralises two concerns:

1. *create_lakebase_engine* — a pure function that builds a SQLAlchemy *Engine* with the standard DQX pool settings (pool_recycle, sslmode, pool_size) and a *do_connect* event listener that injects a freshly generated Databricks credential token before every connection attempt.

2. *LakebaseConnectionMixin* — a lightweight mixin that supplies a lazily created, cached *Engine* to any class that stores a *WorkspaceClient*, a *LakebaseActionsStorageConfig*, and an optional pre-built engine. The mixin eliminates duplicated `_get_engine` / `_create_engine` logic across *LakebaseActionsStorageHandler* and *LakebaseActionEventStore*.

## Security[​](#security "Direct link to Security")

Credential tokens are injected only into the *cparams* dict immediately before each connection attempt and are never logged (CWE-532).

### create_lakebase_engine[​](#create_lakebase_engine "Direct link to create_lakebase_engine")

```python
def create_lakebase_engine(ws: WorkspaceClient, instance_name: str, host: str,
                           user: str, port: str, database: str) -> Engine

```

Build a SQLAlchemy engine for a Databricks Lakebase PostgreSQL instance.

The returned engine is configured with:

* *pool_recycle=45*60\* so connections are recycled before the Lakebase idle-connection timeout.
* *sslmode=require* via *connect_args* for encrypted transport.
* *pool_size=4* to limit concurrent connections.
* A *do_connect* listener that injects a freshly generated Databricks database credential token before every connection attempt, so short-lived credentials are never stale.

**Arguments**:

* `ws` - Authenticated *WorkspaceClient* used to call *database.generate_database_credential*.
* `instance_name` - Lakebase instance identifier, e.g. `&quot;my-lakebase&quot;`. Used in the credential request.
* `host` - Read-write DNS hostname of the Lakebase instance (obtained via *ws.database.get_database_instance(instance_name).read_write_dns*).
* `user` - PostgreSQL user name; typically the service-principal client ID or the current user's e-mail address.
* `port` - TCP port string, e.g. `&quot;5432&quot;`.
* `database` - Name of the PostgreSQL database to connect to.

**Returns**:

A configured SQLAlchemy *Engine* instance ready for use.

## LakebaseConnectionMixin Objects[​](#lakebaseconnectionmixin-objects "Direct link to LakebaseConnectionMixin Objects")

```python
class LakebaseConnectionMixin()

```

Mixin that supplies a lazily created, cached SQLAlchemy *Engine*.

Any class that stores *\_ws*, *\_spark*, *\_config*, and *\_engine* can inherit from this mixin to avoid duplicating the `_get_engine` / `_create_engine` boilerplate.

Concrete classes must call `super().__init__(spark, ws, config, engine)` to initialise the shared attributes.

**Arguments**:

* `spark` - Active *SparkSession* (kept for interface symmetry; not used for PostgreSQL queries).
* `ws` - Authenticated *WorkspaceClient* used to resolve the Lakebase DNS and generate short-lived credentials.
* `config` - *LakebaseActionsStorageConfig* with instance and table details.
* `engine` - Optional pre-built SQLAlchemy *Engine* (useful for testing without a real Lakebase instance).
