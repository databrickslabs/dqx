import logging
from pyspark.sql import SparkSession

from databricks.labs.dqx.checks_resolver import resolve_custom_check_functions_from_path
from databricks.labs.dqx.config import InputConfig, OutputConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.utils import get_reference_dataframes

logger = logging.getLogger(__name__)


class QualityCheckerRunner:
    """Runs the DQX data quality on the input data and saves the generated results to delta table(s)."""

    def __init__(self, spark: SparkSession, dq_engine: DQEngine):
        self.spark = spark
        self.dq_engine = dq_engine

    def run(
        self,
        checks: list[dict],
        input_config: InputConfig,
        output_config: OutputConfig,
        quarantine_config: OutputConfig | None,
        custom_check_functions: dict[str, str] | None = None,
        reference_tables: dict[str, InputConfig] | None = None,
    ) -> None:
        """
        Run the DQX data quality job on the input data and saves the generated results to delta table(s).

        Args:
            checks: The data quality checks to apply.
            input_config: Input data configuration (e.g. table name or file location, read options).
            output_config: Output data configuration (e.g. table name or file location, write options).
            quarantine_config: Quarantine data configuration (e.g. table name or file location, write options).
            custom_check_functions: A mapping where each key is the name of a function (e.g., "my_func")
                and each value is the file path to the Python module that defines it. The path can be absolute
                or relative to the installation folder, and may refer to a local filesystem location, a
                Databricks workspace path (e.g. /Workspace/my_repo/my_module.py), or a Unity Catalog volume
                (e.g. /Volumes/catalog/schema/volume/my_module.py).
            reference_tables: Reference tables to use in the checks.
        """
        ref_dfs = get_reference_dataframes(self.spark, reference_tables)
        custom_check_functions_resolved = resolve_custom_check_functions_from_path(custom_check_functions)

        logger.info(f"Applying checks to {input_config.location}.")

        self.dq_engine.apply_checks_by_metadata_and_save_in_table(
            checks=checks,
            input_config=input_config,
            output_config=output_config,
            quarantine_config=quarantine_config,
            custom_check_functions=custom_check_functions_resolved,
            ref_dfs=ref_dfs,
        )

        if quarantine_config and quarantine_config.location:
            logger.info(
                f"Data quality checks applied, "
                f"valid data saved to {output_config.location} and "
                f"invalid data saved to {quarantine_config.location}."
            )
        else:
            logger.info(f"Data quality checks applied, output saved to {output_config.location}.")
