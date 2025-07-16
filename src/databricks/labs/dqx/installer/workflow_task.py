import logging
from collections.abc import Callable, Iterable
from dataclasses import dataclass

from databricks.labs.blueprint.installation import Installation
from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.config import WorkspaceConfig

logger = logging.getLogger(__name__)

_TASKS: dict[str, "Task"] = {}


@dataclass
class Task:
    workflow: str
    name: str
    doc: str
    fn: Callable[[WorkspaceConfig, WorkspaceClient, SqlBackend, Installation], None]
    depends_on: list[str] | None = None
    job_cluster: str = "main"
    override_clusters: dict[str, str] | None = None
    spark_conf: dict[str, str] | None = None

    def dependencies(self):
        """List of dependencies"""
        if not self.depends_on:
            return []
        return self.depends_on


class Workflow:
    def __init__(
        self, name: str, spark_conf: dict[str, str] | None = None, override_clusters: dict[str, str] | None = None
    ):
        self._name = name
        self._spark_conf = spark_conf
        self._override_clusters = override_clusters

    @property
    def name(self):
        """Name of the workflow"""
        return self._name

    @property
    def spark_conf(self) -> dict[str, str] | None:
        """Spark configuration for the workflow"""
        return self._spark_conf

    @property
    def override_clusters(self) -> dict[str, str] | None:
        """Override clusters for the workflow"""
        return self._override_clusters

    def tasks(self) -> Iterable[Task]:
        """List of tasks"""
        # return __task__ from every method in this class that has this attribute
        for attr in dir(self):
            if attr.startswith("_"):  # skip private methods
                continue
            fn = getattr(self, attr)
            if hasattr(fn, "__task__"):
                yield fn.__task__


def workflow_task(fn=None, *, depends_on=None, job_cluster=Task.job_cluster) -> Callable[[Callable], Callable]:
    """Decorator to register a task in a workflow."""

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
            for dep in depends_on:
                other_class, task_name = dep.__qualname__.split('.')
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
        )
        return func

    if fn is None:
        return register
    register(fn)
    return fn


def remove_extra_indentation(doc: str) -> str:
    """
    Remove extra indentation from docstring.

    :param doc: Docstring
    """
    lines = doc.splitlines()
    stripped = []
    for line in lines:
        if line.startswith(" " * 4):
            stripped.append(line[4:])
        else:
            stripped.append(line)
    return "\n".join(stripped)
