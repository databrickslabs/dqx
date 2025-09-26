import logging

from databricks.labs.dqx.config import RunConfig
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

        Args:
            ctx: Runtime context.
        """
        if ctx.run_config_name:
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
            raise ValueError("No input data source configured during installation")

        run_config.custom_check_functions = self._prefix_custom_check_paths(ctx, run_config.custom_check_functions)
        run_config.checks_location = self._prefix_checks_location(ctx, run_config.checks_location)
        return run_config

    @staticmethod
    def _prefix_checks_location(ctx: WorkflowContext, location: str) -> str:
        """
        Prefixes the checks location installation folder if it is not an absolute path.

        Args:
            ctx: Runtime context.
            location: The location of the checks, either as an absolute path or relative to the installation folder.

        Returns:
            The prefixed checks location.
        """
        return location if location.startswith("/") else f"{ctx.installation.install_folder()}/{location}"

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
