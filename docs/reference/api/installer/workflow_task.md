# databricks.labs.dqx.installer.workflow\_task

## Task Objects[​](#task-objects "Direct link to Task Objects")

```python
@dataclass
class Task()

```

#### job\_cluster[​](#job_cluster "Direct link to job_cluster")

cluster key for job clusters; if None, uses serverless environment

#### run\_job\_name[​](#run_job_name "Direct link to run_job_name")

when set, this task will run another job

### dependencies[​](#dependencies "Direct link to dependencies")

```python
def dependencies()

```

List of dependencies

## Workflow Objects[​](#workflow-objects "Direct link to Workflow Objects")

```python
class Workflow()

```

### name[​](#name "Direct link to name")

```python
@property
def name()

```

Name of the workflow

### spark\_conf[​](#spark_conf "Direct link to spark_conf")

```python
@property
def spark_conf() -> dict[str, str] | None

```

Spark configuration for the workflow

### override\_clusters[​](#override_clusters "Direct link to override_clusters")

```python
@property
def override_clusters() -> dict[str, str] | None

```

Override clusters for the workflow

### tasks[​](#tasks "Direct link to tasks")

```python
def tasks() -> Iterable[Task]

```

List of tasks

### workflow\_task[​](#workflow_task "Direct link to workflow_task")

```python
def workflow_task(
        fn=None,
        *,
        depends_on=None,
        job_cluster=Task.job_cluster,
        run_job_name: str | None = None) -> Callable[[Callable], Callable]

```

Decorator to register a task in a workflow.

### remove\_extra\_indentation[​](#remove_extra_indentation "Direct link to remove_extra_indentation")

```python
def remove_extra_indentation(doc: str) -> str

```

Remove extra indentation from docstring.

**Arguments**:

* `doc` *str* - Docstring to process.

**Returns**:

* `str` - Processed docstring with extra indentation removed.
