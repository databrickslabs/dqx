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

    @workflow_task(run_job_name="profiler")
    def run_profiler(self, ctx: WorkflowContext):
        """
        Run the profiler to generate checks and summary statistics.

        :param ctx: Runtime context.
        """
        run_config = ctx.run_config
        logger.info(f"End-to-end: starting profiler task for run config: {run_config.name}")

    @workflow_task(depends_on=[run_profiler], run_job_name="quality_checker")
    def run_quality_checker(self, ctx: WorkflowContext):
        """
        Run the quality checker after the profiler has generated checks.

        :param ctx: Runtime context.
        """
        run_config = ctx.run_config
        logger.info(f"End-to-end: starting quality_checker task for run config: {run_config.name}")
