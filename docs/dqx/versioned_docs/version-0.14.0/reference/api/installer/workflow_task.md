---
sidebar_label: workflow_task
title: databricks.labs.dqx.installer.workflow_task
---

## Task Objects

```python
@dataclass
class Task()
```

#### job\_cluster

cluster key for job clusters; if None, uses serverless environment

#### run\_job\_name

when set, this task will run another job

#### dependencies

```python
def dependencies()
```

List of dependencies

## Workflow Objects

```python
class Workflow()
```

#### name

```python
@property
def name()
```

Name of the workflow

#### spark\_conf

```python
@property
def spark_conf() -> dict[str, str] | None
```

Spark configuration for the workflow

#### override\_clusters

```python
@property
def override_clusters() -> dict[str, str] | None
```

Override clusters for the workflow

#### tasks

```python
def tasks() -> Iterable[Task]
```

List of tasks

#### workflow\_task

```python
def workflow_task(
        fn=None,
        *,
        depends_on=None,
        job_cluster=Task.job_cluster,
        run_job_name: str | None = None) -> Callable[[Callable], Callable]
```

Decorator to register a task in a workflow.

#### remove\_extra\_indentation

```python
def remove_extra_indentation(doc: str) -> str
```

Remove extra indentation from docstring.

**Arguments**:

- `doc` _str_ - Docstring to process.
  

**Returns**:

- `str` - Processed docstring with extra indentation removed.

