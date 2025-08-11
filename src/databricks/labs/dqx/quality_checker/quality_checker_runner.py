from collections.abc import Callable
import logging
from pyspark.sql import SparkSession, DataFrame

from databricks.labs.dqx.checks_resolver import import_check_function_from_path
from databricks.labs.dqx.config import InputConfig, OutputConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.utils import read_input_data


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

        :param checks: The data quality checks to apply.
        :param input_config: Input data configuration (e.g. table name or file location, read options).
        :param output_config: Output data configuration (e.g. table name or file location, write options).
        :param quarantine_config: Quarantine data configuration (e.g. table name or file location, write options).
        :param custom_check_functions: Custom check functions to use in the checks which is a mapping of
        fully qualified function name (e.g. my_module.my_func) to the module workspace location
        (e.g. /Workspace/my_repo/my_module.py).
        :param reference_tables: Reference tables to use in the checks.
        """
        ref_dfs = self._get_ref_dfs(reference_tables)
        custom_check_functions_resolved = self._resolve_custom_check_functions(custom_check_functions)

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

    @staticmethod
    def _resolve_custom_check_functions(check_functions: dict[str, str] | None = None) -> dict[str, Callable]:
        """
        Resolve custom check functions from their fully qualified names to actual function objects.

        :param check_functions: A dictionary mapping fully qualified function names to their module paths.
        First element is the function name (e.g. my_func), second is the module path in the workspace
        (e.g. /Workspace/my_repo/my_module.py).
        :return: A dictionary mapping function names to the actual function objects.
        """
        resolved_funcs: dict[str, Callable] = {}
        if check_functions:
            logger.info("Resolving custom check functions.")
            for func_name, func_module_full_path in check_functions.items():
                resolved_funcs[func_name] = import_check_function_from_path(func_module_full_path, func_name)
        return resolved_funcs

    def _get_ref_dfs(self, reference_tables: dict[str, InputConfig] | None = None) -> dict[str, DataFrame] | None:
        """
        Get reference DataFrames from the provided reference tables configuration.

        :param reference_tables: A dictionary mapping reference table names to their input configurations.
        :return: A dictionary mapping reference table names to their DataFrames.
        """
        ref_dfs: dict[str, DataFrame] | None = None
        if reference_tables:
            logger.info("Reading reference tables.")
            ref_dfs = {
                name: read_input_data(self.spark, input_config) for name, input_config in reference_tables.items()
            }
        return ref_dfs
