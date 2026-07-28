---
sidebar_label: workspace_context
title: databricks.labs.dqx.contexts.workspace_context
---

## WorkspaceContext Objects

```python
class WorkspaceContext(CliContext)
```

WorkspaceContext class that extends CliContext to provide workspace-specific functionality.

#### workspace\_client

```python
@cached_property
def workspace_client() -> WorkspaceClient
```

Returns the WorkspaceClient instance.

