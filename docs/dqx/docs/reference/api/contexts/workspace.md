---
sidebar_label: workspace
title: databricks.labs.dqx.contexts.workspace
---

## WorkspaceContext Objects

```python
class WorkspaceContext(CliContext)
```

WorkspaceContext class that extends CliContext to provide workspace-specific functionality.

**Arguments**:

- `ws`: The WorkspaceClient instance to use for accessing the workspace.
- `named_parameters`: Optional dictionary of named parameters.

#### workspace\_client

```python
@cached_property
def workspace_client() -> WorkspaceClient
```

Returns the WorkspaceClient instance.

