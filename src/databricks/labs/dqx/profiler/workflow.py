import logging

from databricks.labs.dqx.contexts.workflows import RuntimeContext
from databricks.labs.dqx.installer.workflow_task import Workflow, workflow_task


logger = logging.getLogger(__name__)


class ProfilerWorkflow(Workflow):
    def __init__(self):
        super().__init__('profiler')

    @workflow_task
    def profile(self, ctx: RuntimeContext):
        """
        Profile the input data and save the generated checks and profile summary stats.

        :param ctx: Runtime context.
        """
        run_config = ctx.run_config
        checks, profile_summary_stats = ctx.profiler.run(
            run_config.input_location,
            run_config.input_format,
            run_config.profiler_sample_fraction,
            run_config.profiler_sample_seed,
            run_config.profiler_limit,
        )
        ctx.profiler.save(checks, profile_summary_stats, run_config.checks_file, run_config.profile_summary_stats_file)
