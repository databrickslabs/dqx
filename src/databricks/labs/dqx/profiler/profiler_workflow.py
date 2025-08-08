import logging

from databricks.labs.dqx.contexts.workflow_context import WorkflowContext
from databricks.labs.dqx.installer.workflow_task import Workflow, workflow_task


logger = logging.getLogger(__name__)


class ProfilerWorkflow(Workflow):
    def __init__(self, spark_conf: dict[str, str] | None = None, override_clusters: dict[str, str] | None = None):
        super().__init__("profiler", spark_conf=spark_conf, override_clusters=override_clusters)

    @workflow_task
    def profile(self, ctx: WorkflowContext):
        """
        Profile the input data and save the generated checks and profile summary stats.

        :param ctx: Runtime context.
        """
        run_config = ctx.run_config
        logger.info(f"Running profiler workflow for run config: {run_config.name}")

        if not run_config.input_config:
            raise ValueError("No input data source configured during installation")

        checks, profile_summary_stats = ctx.profiler.run(
            run_config.input_config,
            run_config.profiler_config,
        )

        ctx.profiler.save(
            checks, profile_summary_stats, run_config.checks_location, run_config.profiler_config.summary_stats_file
        )
