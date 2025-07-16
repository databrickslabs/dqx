import logging

from databricks.labs.dqx.contexts.workflow import RuntimeContext
from databricks.labs.dqx.installer.workflow_task import Workflow, workflow_task


logger = logging.getLogger(__name__)


class ProfilerWorkflow(Workflow):
    def __init__(self, spark_conf: dict[str, str] | None = None, override_clusters: dict[str, str] | None = None):
        super().__init__("profiler", spark_conf=spark_conf, override_clusters=override_clusters)

    @workflow_task
    def profile(self, ctx: RuntimeContext):
        """
        Profile the input data and save the generated checks and profile summary stats.

        :param ctx: Runtime context.
        """
        run_config = ctx.run_config
        if not run_config.input_config:
            raise ValueError("No input data source configured during installation")

        checks, profile_summary_stats = ctx.profiler.run(
            run_config.input_config,
            run_config.profiler_config,
        )

        ctx.profiler.save(
            checks, profile_summary_stats, run_config.checks_file, run_config.profiler_config.summary_stats_file
        )


class DataQualityWorkflow(Workflow):
    def __init__(self, spark_conf: dict[str, str] | None = None, override_clusters: dict[str, str] | None = None):
        super().__init__("data_quality", spark_conf=spark_conf, override_clusters=override_clusters)

    @workflow_task
    def apply_checks(self, ctx: RuntimeContext):
        """
        Apply data quality checks to the input data and save the results.

        :param ctx: Runtime context.
        """
        run_config = ctx.run_config
        if not run_config.input_config:
            raise ValueError("No input data source configured during installation")

        if not run_config.output_config:
            raise ValueError("No output configured during installation")

        ctx.data_quality.run(
            run_config.input_config,
            run_config.output_config,
            run_config.quarantine_config,
            run_config.checks_file,
            run_config.checks_table,
            run_config.custom_check_functions,
            run_config.reference_tables,
        )
