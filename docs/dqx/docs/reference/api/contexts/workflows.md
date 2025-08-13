---
sidebar_label: workflows
title: databricks.labs.dqx.contexts.workflows
---

## RuntimeContext Objects

```python
class RuntimeContext(GlobalContext)
```

#### config

```python
@cached_property
def config() -> WorkspaceConfig
```

Loads and returns the workspace configuration.

#### run\_config

```python
@cached_property
def run_config() -> RunConfig
```

Loads and returns the run configuration.

#### connect\_config

```python
@cached_property
def connect_config() -> core.Config
```

Returns the connection configuration.

**Raises**:

- `AssertionError`: If the connect configuration is not provided.

**Returns**:

The core.Config instance.

#### workspace\_client

```python
@cached_property
def workspace_client() -> WorkspaceClient
```

Returns the WorkspaceClient instance.

#### installation

```python
@cached_property
def installation() -> Installation
```

Returns the installation instance for the runtime.

#### workspace\_id

```python
@cached_property
def workspace_id() -> int
```

Returns the workspace ID.

#### parent\_run\_id

```python
@cached_property
def parent_run_id() -> int
```

Returns the parent run ID.

#### profiler

```python
@cached_property
def profiler() -> ProfilerRunner
```

Returns the ProfilerRunner instance.

