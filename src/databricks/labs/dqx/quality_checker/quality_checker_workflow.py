import logging

from databricks.labs.dqx.contexts.workflow_context import WorkflowContext
from databricks.labs.dqx.installer.workflow_task import Workflow, workflow_task

logger = logging.getLogger(__name__)


class DataQualityWorkflow(Workflow):
    def __init__(self, spark_conf: dict[str, str] | None = None, override_clusters: dict[str, str] | None = None):
        super().__init__("quality-checker", spark_conf=spark_conf, override_clusters=override_clusters)

    @workflow_task
    def apply_checks(self, ctx: WorkflowContext):
        """
        Apply data quality checks to the input data and save the results.

        Logic:
        * If location patterns are provided, only tables matching the patterns will be used,
            and the provided run config name will be used as a template for all fields except location.
            Additionally, exclude patterns can be specified to skip specific tables.
            Output and quarantine tables are excluded by default based on output_table_suffix and quarantine_table_suffix
            job parameters to avoid re-applying checks on them.
        * If no location patterns are provided, but a run config name is given, only that run config will be used.
        * If neither location patterns nor a run config name are provided, all run configs will be used.

        Args:
            ctx: Runtime context.
        """
        if ctx.runnable_for_patterns:
            logger.info(f"Running data quality workflow for patterns: {ctx.patterns}")
            run_config = ctx.run_config
            patterns, exclude_patterns = ctx.resolved_patterns
            ctx.quality_checker.run_for_patterns(
                patterns=patterns,
                exclude_patterns=exclude_patterns,
                run_config_template=run_config,
                checks_location=run_config.checks_location,
                output_table_suffix=ctx.output_table_suffix,
                quarantine_table_suffix=ctx.quarantine_table_suffix,
                max_parallelism=ctx.config.quality_checker_max_parallelism,
            )
        elif ctx.runnable_for_run_config:
            logger.info(f"Running data quality workflow for run config: {ctx.run_config_name}")
            ctx.quality_checker.run([ctx.run_config])
        else:
            logger.info("Running data quality workflow for all run configs")
            run_configs = [ctx.prepare_run_config(run_config) for run_config in ctx.config.run_configs]
            ctx.quality_checker.run(run_configs, ctx.config.quality_checker_max_parallelism)
