import logging

from databricks.labs.dqx.config import InstallationChecksStorageConfig
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
        run_config = ctx.run_config
        logger.info(f"Running data quality workflow for run config: {run_config.name}")

        if not run_config.input_config:
            raise ValueError("No input data source configured during installation")

        if not run_config.output_config:
            raise ValueError("No output storage configured during installation")

        checks = ctx.quality_checker.dq_engine.load_checks(
            config=InstallationChecksStorageConfig(
                location=run_config.checks_location,
                run_config_name=run_config.name,
                product_name=ctx.product_info.product_name(),
            )
        )

        custom_check_functions = self._prefix_custom_check_paths(ctx, run_config.custom_check_functions)

        ctx.quality_checker.run(
            checks,
            run_config.input_config,
            run_config.output_config,
            run_config.quarantine_config,
            custom_check_functions,
            run_config.reference_tables,
        )

    @staticmethod
    def _prefix_custom_check_paths(ctx: WorkflowContext, custom_check_functions: dict[str, str]) -> dict[str, str]:
        """
        Prefixes custom check function paths with the installation folder if they are not absolute paths.

        Args:
            ctx: Installation context.
            custom_check_functions: A dictionary mapping function names to their paths.

        Returns:
            A dictionary with function names as keys and prefixed paths as values.
        """
        if custom_check_functions:
            install_folder = f"/Workspace/{ctx.installation.install_folder()}"
            return {
                func_name: path if path.startswith("/") else f"{install_folder}/{path}"
                for func_name, path in custom_check_functions.items()
            }
        return custom_check_functions
