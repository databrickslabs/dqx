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

        Logic:
        * If location patterns are provided, only those patterns will be profiled, the provided run config name
        will be used as a template for all fields except the location.
        * If no location patterns are provided, but a run config name is given, only that run config will be profiled.
        * If neither location patterns nor a run config name are provided, all run configs will be profiled.

        Args:
            ctx: Runtime context.
        """
        if ctx.patterns and ctx.run_config_name:
            logger.info(f"Running profiler workflow for patterns: {ctx.patterns}")
            patterns = [pattern.strip() for pattern in ctx.patterns.split(';')]
            ctx.profiler.run_for_patterns(
                patterns=patterns,
                profiler_config=ctx.run_config.profiler_config,
                checks_location=ctx.run_config.checks_location,
                install_folder=ctx.installation.install_folder(),
                max_parallelism=ctx.config.profiler_max_parallelism,
            )
        elif ctx.run_config_name:
            self._profile_for_run_config(ctx, ctx.run_config)
        else:
            logger.info("Running profiler workflow for all run configs")
            self._profile_for_run_configs(ctx, ctx.config.run_configs, ctx.config.profiler_max_parallelism)

    def _profile_for_run_configs(self, ctx: WorkflowContext, run_configs: list[RunConfig], max_parallelism: int):
        logger.info(f"Profiling {len(run_configs)} tables with parallelism {max_parallelism}")
        with futures.ThreadPoolExecutor(max_workers=max_parallelism) as executor:
            apply_checks_runs = [
                executor.submit(self._profile_for_run_config, ctx, run_config) for run_config in run_configs
            ]
            for future in futures.as_completed(apply_checks_runs):
                # Retrieve the result to propagate any exceptions
                future.result()

    @staticmethod
    def _profile_for_run_config(ctx, run_config):
        logger.info(f"Running profiler workflow for run config: {ctx.run_config_name}")

        if not run_config.input_config:
            raise ValueError("No input data source configured during installation")

        ctx.profiler.run(
            run_config_name=run_config.name,
            input_config=run_config.input_config,
            profiler_config=run_config.profiler_config,
            product=ctx.installation.product(),
            install_folder=ctx.installation.install_folder(),
        )
