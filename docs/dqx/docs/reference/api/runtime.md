---
sidebar_label: runtime
title: databricks.labs.dqx.runtime
---

## Workflows Objects

```python
class Workflows()
```

#### all

```python
@classmethod
def all(cls, config: WorkspaceConfig)
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

