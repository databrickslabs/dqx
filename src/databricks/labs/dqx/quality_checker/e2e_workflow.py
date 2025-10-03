import logging

from databricks.labs.dqx.contexts.workflow_context import WorkflowContext
from databricks.labs.dqx.installer.workflow_task import Workflow, workflow_task


logger = logging.getLogger(__name__)


class EndToEndWorkflow(Workflow):
    """
    Unified workflow that orchestrates individual jobs such as profiler and quality checker.
    Run Job tasks execute referenced jobs with their own settings.
    """

    def __init__(
        self,
        profiler: Workflow,
        quality_checker: Workflow,
        *,
        spark_conf: dict[str, str] | None = None,
        override_clusters: dict[str, str] | None = None,
    ):
        super().__init__("e2e", spark_conf=spark_conf, override_clusters=override_clusters)
        self._profiler = profiler
        self._quality_checker = quality_checker

    @workflow_task
    def prepare(self, ctx: WorkflowContext):
        """
        Initialize end-to-end workflow and emit a log record for traceability.

        Args:
            ctx (WorkflowContext): Runtime context.
        """
        self._log_task_run_info(ctx, "prepare start")

    @workflow_task(depends_on=[prepare], run_job_name="profiler")
    def run_profiler(self, ctx: WorkflowContext):
        """
        Run the profiler to generate checks and summary statistics.

        Args:
            ctx: Runtime context.
        """
        self._log_task_run_info(ctx, "starting profiler task")

    @workflow_task(depends_on=[run_profiler], run_job_name="quality-checker")
    def run_quality_checker(self, ctx: WorkflowContext):
        """
        Run the quality checker after the profiler has generated checks.

        Args:
            ctx: Runtime context.
        """
        self._log_task_run_info(ctx, "starting quality_checker task")

    @workflow_task(depends_on=[run_quality_checker])
    def finalize(self, ctx: WorkflowContext):
        """
        Finalize end-to-end workflow and emit a log record for traceability.

        Args:
            ctx (WorkflowContext): Runtime context.
        """
        self._log_task_run_info(ctx, "finalize complete")
        logger.info("For more details please check the run logs of the profiler and quality checker jobs.")

    @staticmethod
    def _log_task_run_info(ctx: WorkflowContext, task_name: str):
        """
        Log whether the workflow is running for patterns, all run configs or a specific run config.

        Args:
            ctx (WorkflowContext): Runtime context.
            task_name (str): Name of the task being executed.
        """
        if ctx.runnable_for_patterns:
            logger.info(f"End-to-end: {task_name} for patterns: {ctx.patterns}")
        elif ctx.runnable_for_run_config:
            logger.info(f"End-to-end: {task_name} for run config: {ctx.run_config.name}")
        else:
            logger.info(f"End-to-end: {task_name} for all run configs")
