import logging
from concurrent import futures

from databricks.labs.dqx.config import RunConfig
from databricks.labs.dqx.contexts.workflow_context import WorkflowContext
from databricks.labs.dqx.installer.workflow_task import Workflow, workflow_task

logger = logging.getLogger(__name__)


class ProfilerWorkflow(Workflow):
    def __init__(self, spark_conf: dict[str, str] | None = None, override_clusters: dict[str, str] | None = None):
        super().__init__("profiler", spark_conf=spark_conf, override_clusters=override_clusters)

    @workflow_task
    def profile(self, ctx: WorkflowContext):
        """
        Profile input data and save the generated checks and profile summary stats.

        Logic: Profile based on the provided run config name and location patterns as follows:
        * If location patterns are provided, only tables matching the patterns will be profiled,
            and the provided run config name will be used as a template for all fields except location.
            Additionally, exclude patterns can be specified to skip profiling specific tables.
            Output and quarantine tables are excluded by default based on output_table_suffix and quarantine_table_suffix
            job parameters to avoid profiling them.
        * If no location patterns are provided, but a run config name is given, only that run config will be profiled.
        * If neither location patterns nor a run config name are provided, all run configs will be profiled.

        Args:
            ctx: Runtime context.

        Raises:
            InvalidConfigError: If no input data source is configured during installation.
        """
        if ctx.runnable_for_patterns:
            logger.info(f"Running profiler workflow for patterns: {ctx.patterns}")
            patterns, exclude_patterns = ctx.resolved_patterns
            run_config = ctx.run_config

            ctx.profiler.run_for_patterns(
                patterns=patterns,
                exclude_patterns=exclude_patterns,
                profiler_config=run_config.profiler_config,
                checks_location=run_config.checks_location,
                product=ctx.installation.product(),
                install_folder=ctx.installation.install_folder(),
                max_parallelism=ctx.config.profiler_max_parallelism,
            )
        elif ctx.runnable_for_run_config:
            self._profile_for_run_config(ctx, ctx.run_config)
        else:
            logger.info("Running profiler workflow for all run configs")
            self._profile_for_run_configs(ctx, ctx.config.run_configs, ctx.config.profiler_max_parallelism)

    def _profile_for_run_configs(self, ctx: WorkflowContext, run_configs: list[RunConfig], max_parallelism: int):
        logger.info(f"Profiling {len(run_configs)} tables with parallelism {max_parallelism}")
        with futures.ThreadPoolExecutor(max_workers=max_parallelism) as executor:
            apply_checks_runs = [
                executor.submit(self._profile_for_run_config, ctx, ctx.prepare_run_config(run_config))
                for run_config in run_configs
            ]
            for future in futures.as_completed(apply_checks_runs):
                # Retrieve the result to propagate any exceptions
                future.result()

    @staticmethod
    def _profile_for_run_config(ctx, run_config):
        logger.info(f"Running profiler workflow for run config: {run_config.name}")

        ctx.profiler.run(
            run_config_name=run_config.name,
            input_config=run_config.input_config,
            profiler_config=run_config.profiler_config,
            product=ctx.installation.product(),
            install_folder=ctx.installation.install_folder(),
        )
