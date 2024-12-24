from collections.abc import Callable, Iterable
from dataclasses import dataclass

from databricks.labs.blueprint.installation import Installation
from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

from databricks.labs.dqx.config import WorkspaceConfig

_TASKS: dict[str, "Task"] = {}


@dataclass
class Task:
    workflow: str
    name: str
    doc: str
    fn: Callable[[WorkspaceConfig, WorkspaceClient, SqlBackend, Installation], None]
    depends_on: list[str] | None = None
    job_cluster: str = "main"
    cloud: str | None = None

    def is_testing(self):
        """Test if the task is a test"""
        return self.workflow in {"failing"}

    def dependencies(self):
        """List of dependencies"""
        if not self.depends_on:
            return []
        return self.depends_on

    def cloud_compatible(self, config: Config) -> bool:
        """Test compatibility between workspace config and task"""
        if self.cloud:
            if self.cloud.lower() == "aws":
                return config.is_aws
            if self.cloud.lower() == "azure":
                return config.is_azure
            if self.cloud.lower() == "gcp":
                return config.is_gcp
            return True
        return True


def remove_extra_indentation(doc: str) -> str:
    """Remove extra indentation from docstring"""
    lines = doc.splitlines()
    stripped = []
    for line in lines:
        if line.startswith(" " * 4):
            stripped.append(line[4:])
        else:
            stripped.append(line)
    return "\n".join(stripped)


def parse_args(*argv) -> dict[str, str]:
    """Parse command line arguments"""
    args = dict(a[2:].split("=") for a in argv if a[0:2] == "--")
    if "config" not in args:
        msg = "no --config specified"
        raise KeyError(msg)
    return args


class Workflow:
    def __init__(self, name: str):
        self._name = name

    @property
    def name(self):
        """Name of the workflow"""
        return self._name

    def tasks(self) -> Iterable[Task]:
        """List of tasks"""
        # return __task__ from every method in this class that has this attribute
        for attr in dir(self):
            if attr.startswith("_"):
                continue
            fn = getattr(self, attr)
            if hasattr(fn, "__task__"):
                yield fn.__task__


def workflow_task(
    fn=None,
    *,
    depends_on=None,
    job_cluster=Task.job_cluster,
    cloud: str | None = None,
) -> Callable[[Callable], Callable]:
    def register(func):
        """Register a task"""
        if not func.__doc__:
            raise SyntaxError(f"{func.__name__} must have some doc comment")
        deps = []
        this_class = func.__qualname__.split('.')[0]
        if depends_on is not None:
            if not isinstance(depends_on, list):
                msg = "depends_on has to be a list"
                raise SyntaxError(msg)
            for fn in depends_on:
                other_class, task_name = fn.__qualname__.split('.')
                if other_class != this_class:
                    continue
                deps.append(task_name)
        func.__task__ = Task(
            workflow='<unknown>',
            name=func.__name__,
            doc=remove_extra_indentation(func.__doc__),
            fn=func,
            depends_on=deps,
            job_cluster=job_cluster,
            cloud=cloud,
        )
        return func

    if fn is None:
        return register
    register(fn)
    return fn
