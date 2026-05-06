# databricks.labs.dqx.workflows\_runner

## WorkflowsRunner Objects[​](#workflowsrunner-objects "Direct link to WorkflowsRunner Objects")

```python
class WorkflowsRunner()

```

### all[​](#all "Direct link to all")

```python
@classmethod
def all(cls, config: WorkspaceConfig) -> "WorkflowsRunner"

```

Return all workflows.

### tasks[​](#tasks "Direct link to tasks")

```python
def tasks() -> list[Task]

```

Return all tasks.

### trigger[​](#trigger "Direct link to trigger")

```python
def trigger(*argv)

```

Trigger a workflow.

### main[​](#main "Direct link to main")

```python
def main(*argv)

```

Main entry point.
