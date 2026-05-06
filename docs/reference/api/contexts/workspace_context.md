# databricks.labs.dqx.contexts.workspace\_context

## WorkspaceContext Objects[​](#workspacecontext-objects "Direct link to WorkspaceContext Objects")

```python
class WorkspaceContext(CliContext)

```

WorkspaceContext class that extends CliContext to provide workspace-specific functionality.

### workspace\_client[​](#workspace_client "Direct link to workspace_client")

```python
@cached_property
def workspace_client() -> WorkspaceClient

```

Returns the WorkspaceClient instance.
