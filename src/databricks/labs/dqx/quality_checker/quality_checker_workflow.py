import logging

from databricks.labs.dqx.config import RunConfig
from databricks.labs.dqx.contexts.workflow_context import WorkflowContext
from databricks.labs.dqx.installer.workflow_task import Workflow, workflow_task
from databricks.labs.dqx.errors import InvalidConfigError

logger = logging.getLogger(__name__)


class DataQualityWorkflow(Workflow):
    def __init__(self, spark_conf: dict[str, str] | None = None, override_clusters: dict[str, str] | None = None):
        super().__init__("quality-checker", spark_conf=spark_conf, override_clusters=override_clusters)

    @workflow_task
    def apply_checks(self, ctx: WorkflowContext):
        """
        Apply data quality checks to the input data and save the results.

        Logic:
        * If location patterns are provided, only those patterns will be used, and the provided run config name
            will be used as a template for all fields except the location.
            Additionally, exclude patterns can be specified to skip specific tables.
            Output and quarantine tables are excluded by default based on output_table_suffix and quarantine_table_suffix
            to avoid re-applying checks on them.
        * If no location patterns are provided, but a run config name is given, only that run config will be used.
        * If neither location patterns nor a run config name are provided, all run configs will be used.

        Args:
            ctx: Runtime context.
        """
        if ctx.patterns and ctx.run_config_name:
            logger.info(f"Running data quality workflow for patterns: {ctx.patterns}")
            run_config = self._prepare_run_config(ctx, ctx.run_config)
            patterns, exclude_patterns = ctx.resolved_patterns
            ctx.quality_checker.run_for_patterns(
                patterns=patterns,
                exclude_patterns=exclude_patterns,
                run_config_template=run_config,
                checks_location=ctx.generic_checks_location,
                output_table_suffix=ctx.output_table_suffix,
                quarantine_table_suffix=ctx.quarantine_table_suffix,
                max_parallelism=ctx.config.quality_checker_max_parallelism,
            )
        elif ctx.run_config_name:
            logger.info(f"Running data quality workflow for run config: {ctx.run_config_name}")
            run_config = self._prepare_run_config(ctx, ctx.run_config)
            ctx.quality_checker.run([run_config])
        else:
            logger.info("Running data quality workflow for all run configs")
            run_configs = [self._prepare_run_config(ctx, run_config) for run_config in ctx.config.run_configs]
            ctx.quality_checker.run(run_configs, ctx.config.quality_checker_max_parallelism)

    def _prepare_run_config(self, ctx: WorkflowContext, run_config: RunConfig) -> RunConfig:
        """
        Apply common path prefixing to a run configuration in-place and return it.

        Ensures custom check function paths and checks location are absolute in the Databricks Workspace.

        Args:
            ctx: Runtime context.
            run_config: The run configuration to prepare.

        Returns:
            The prepared run configuration with absolute paths.
        """
        if not run_config.input_config:
            raise InvalidConfigError("No input data source configured during installation")

        run_config.custom_check_functions = self._prefix_custom_check_paths(ctx, run_config.custom_check_functions)
        run_config.checks_location = ctx.checks_location
        return run_config

    @staticmethod
    def _prefix_custom_check_paths(ctx: WorkflowContext, custom_check_functions: dict[str, str]) -> dict[str, str]:
        """
        Prefixes custom check function paths with the installation folder if they are not absolute paths.

        Args:
            ctx: Runtime context.
            custom_check_functions: A mapping where each key is the name of a function (e.g., "my_func")
                and each value is the file path to the Python module that defines it. The path can be absolute
                or relative to the installation folder, and may refer to a local filesystem location, a
                Databricks workspace path (e.g. /Workspace/my_repo/my_module.py), or a Unity Catalog volume
                (e.g. /Volumes/catalog/schema/volume/my_module.py).

        Returns:
            A dictionary with function names as keys and prefixed paths as values.
        """
        if custom_check_functions:
            return {
                func_name: path if path.startswith("/") else f"/Workspace{ctx.installation.install_folder()}/{path}"
                for func_name, path in custom_check_functions.items()
            }
        return custom_check_functions
