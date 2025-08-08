import logging

from databricks.labs.dqx.config import InstallationChecksStorageConfig
from databricks.labs.dqx.contexts.workflow import WorkflowContext
from databricks.labs.dqx.installer.workflow_task import Workflow, workflow_task


logger = logging.getLogger(__name__)


class DataQualityWorkflow(Workflow):
    def __init__(self, spark_conf: dict[str, str] | None = None, override_clusters: dict[str, str] | None = None):
        super().__init__("quality_checker", spark_conf=spark_conf, override_clusters=override_clusters)

    @workflow_task
    def apply_checks(self, ctx: WorkflowContext):
        """
        Apply data quality checks to the input data and save the results.

        :param ctx: Runtime context.
        """
        run_config = ctx.run_config
        logger.info(f"Running data quality workflow for run config: {run_config.name}")

        if not run_config.input_config:
            raise ValueError("No input data source configured during installation")

        if not run_config.output_config:
            raise ValueError("No output storage configured during installation")

        checks = ctx.quality_checker.dq_engine.load_checks(
            config=InstallationChecksStorageConfig(location=run_config.checks_location, run_config_name=run_config.name)
        )

        ctx.quality_checker.run(
            checks,
            run_config.input_config,
            run_config.output_config,
            run_config.quarantine_config,
            run_config.custom_check_functions,
            run_config.reference_tables,
        )
