import logging
from concurrent import futures

from databricks.labs.dqx.checks_resolver import resolve_custom_check_functions_from_path
from databricks.labs.dqx.config import RunConfig
from databricks.labs.dqx.contexts.workflow_context import WorkflowContext
from databricks.labs.dqx.installer.workflow_task import Workflow, workflow_task
from databricks.labs.dqx.profiler.generator import DQGenerator

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

            generator = ProfilerWorkflow._create_generator(ctx, run_config)
            ctx.profiler.run_for_patterns(
                generator=generator,
                run_config=run_config,
                patterns=patterns,
                exclude_patterns=exclude_patterns,
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

        generator = ProfilerWorkflow._create_generator(ctx, run_config)
        ctx.profiler.run(
            generator=generator,
            run_config=run_config,
            product=ctx.installation.product(),
            install_folder=ctx.installation.install_folder(),
        )

    @staticmethod
    def _create_generator(ctx, run_config):
        llm_model_config = ctx.config.llm_config.model if ctx.config.llm_config else None

        if llm_model_config:
            if llm_model_config.api_base and "/" in llm_model_config.api_base:
                logger.info("Retrieving LLM API base from secret store")
                # if api api base stored as secret: secret_scope/secret_key
                api_base = ProfilerWorkflow._get_secret_value(ctx, llm_model_config.api_base)
                llm_model_config.api_base = api_base
            if llm_model_config.api_key and "/" in llm_model_config.api_key:
                # if api key stored as secret: secret_scope/secret_key
                logger.info("Retrieving LLM API key from secret store")
                api_key = ProfilerWorkflow._get_secret_value(ctx, llm_model_config.api_key)
                llm_model_config.api_key = api_key

        custom_check_functions = resolve_custom_check_functions_from_path(run_config.custom_check_functions)
        return DQGenerator(ctx.workspace_client, ctx.spark, llm_model_config, custom_check_functions)

    @staticmethod
    def _get_secret_value(ctx, scope_key):
        scope_key_data = scope_key.split("/")

        scope = scope_key_data[0]
        key = scope_key_data[1]

        return ctx.workspace_client.secrets.get_secret(scope, key).value
