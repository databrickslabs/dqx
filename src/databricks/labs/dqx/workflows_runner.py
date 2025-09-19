import dataclasses
import logging
import os
import sys
from pathlib import Path

from databricks.sdk.config import with_user_agent_extra

from databricks.labs.dqx.__about__ import __version__
from databricks.labs.dqx.config import WorkspaceConfig, RunConfig
from databricks.labs.dqx.profiler.profiler_workflow import ProfilerWorkflow
from databricks.labs.dqx.quality_checker.quality_checker_workflow import DataQualityWorkflow
from databricks.labs.dqx.quality_checker.e2e_workflow import EndToEndWorkflow
from databricks.labs.dqx.contexts.workflow_context import WorkflowContext
from databricks.labs.dqx.installer.workflow_task import Task, Workflow
from databricks.labs.dqx.installer.logs import TaskLogger

logger = logging.getLogger(__name__)


class WorkflowsRunner:
    def __init__(self, workflows: list[Workflow]):
        self._tasks: list[Task] = []
        self._workflows: dict[str, Workflow] = {}
        for workflow in workflows:
            self._workflows[workflow.name] = workflow
            for task_definition in workflow.tasks():
                # Add the workflow params to the task definition, because we cannot access
                # them from the workflow_task decorator
                with_workflow = dataclasses.replace(
                    task_definition,
                    workflow=workflow.name,
                    spark_conf=workflow.spark_conf,
                    override_clusters=workflow.override_clusters,
                )
                self._tasks.append(with_workflow)

    @classmethod
    def all(cls, config: WorkspaceConfig) -> "WorkflowsRunner":
        """Return all workflows."""
        profiler = ProfilerWorkflow(
            spark_conf=config.profiler_spark_conf,
            override_clusters=config.profiler_override_clusters,
        )
        quality_checker = DataQualityWorkflow(
            spark_conf=config.quality_checker_spark_conf,
            override_clusters=config.quality_checker_override_clusters,
        )
        e2e = EndToEndWorkflow(
            profiler,
            quality_checker,
            spark_conf=config.e2e_spark_conf,
            override_clusters=config.e2e_override_clusters,
        )
        return cls([profiler, quality_checker, e2e])

    def tasks(self) -> list[Task]:
        """Return all tasks."""
        return self._tasks

    def trigger(self, *argv):
        """Trigger a workflow."""
        named_parameters = self._parse_args(*argv)
        config_path = Path(named_parameters["config"])
        ctx = WorkflowContext(named_parameters)
        install_dir = config_path.parent
        task_name = named_parameters.get("task", "not specified")
        workflow_name = named_parameters.get("workflow", "not specified")
        attempt = named_parameters.get("attempt", "0")
        if workflow_name not in self._workflows:
            msg = f'Workflow "{workflow_name}" not found. Valid workflows are: {", ".join(self._workflows.keys())}'
            raise KeyError(msg)
        workflow = self._workflows[workflow_name]

        # both CLI commands and workflow names appear in telemetry under `cmd`
        with_user_agent_extra("cmd", workflow_name)
        # `{{parent_run_id}}` is the run of entire workflow, whereas `{{run_id}}` is the run of a task
        job_run_id = named_parameters.get("parent_run_id", "unknown_run_id")
        job_id = named_parameters.get("job_id", "unknown_job_id")
        with TaskLogger(
            install_dir,
            workflow=workflow_name,
            job_id=job_id,
            task_name=task_name,
            job_run_id=job_run_id,
            log_level=ctx.config.log_level,
            attempt=attempt,
        ) as task_logger:
            dqx_logger = logging.getLogger("databricks.labs.dqx")
            dqx_logger.info(f"DQX v{__version__} After workflow finishes, see debug logs at {task_logger}")
            current_task = getattr(workflow, task_name)
            current_task(ctx)
            return None

    @staticmethod
    def _parse_args(*argv) -> dict[str, str]:
        """Parse command line arguments"""
        args = dict(a[2:].split("=") for a in argv if a[0:2] == "--")
        if "config" not in args:
            msg = "no --config specified"
            raise KeyError(msg)
        return args


def main(*argv):
    """Main entry point."""
    if len(argv) == 0:
        argv = sys.argv
    WorkflowsRunner.all(WorkspaceConfig(run_configs=[RunConfig()])).trigger(*argv)


if __name__ == "__main__":
    if "DATABRICKS_RUNTIME_VERSION" not in os.environ:
        raise SystemExit("Only intended to run in Databricks Runtime")
    main(*sys.argv)
