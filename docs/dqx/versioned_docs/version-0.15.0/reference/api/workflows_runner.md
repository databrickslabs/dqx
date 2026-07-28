---
sidebar_label: workflows_runner
title: databricks.labs.dqx.workflows_runner
---

## WorkflowsRunner Objects

```python
class WorkflowsRunner()
```

#### all

```python
@classmethod
def all(cls, config: WorkspaceConfig) -> "WorkflowsRunner"
```

Return all workflows.

#### tasks

```python
def tasks() -> list[Task]
```

Return all tasks.

#### trigger

```python
def trigger(*argv)
```

Trigger a workflow.

#### main

```python
def main(*argv)
```

Main entry point.

