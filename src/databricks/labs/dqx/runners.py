import sys
import importlib
import importlib.util
from typing import Any
import logging
import yaml
from pyspark.sql import SparkSession, DataFrame

from databricks.labs.dqx.config import InputConfig, ProfilerConfig, OutputConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.utils import read_input_data
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.blueprint.installation import Installation


logger = logging.getLogger(__name__)


class ProfilerRunner:
    """Runs the DQX profiler on the input data and saves the generated checks and profile summary stats."""

    def __init__(
        self,
        spark: SparkSession,
        installation: Installation,
        profiler: DQProfiler,
        generator: DQGenerator,
    ):
        self.spark = spark
        self.installation = installation
        self.profiler = profiler
        self.generator = generator

    def run(
        self,
        input_config: InputConfig,
        profiler_config: ProfilerConfig,
    ) -> tuple[list[dict], dict[str, Any]]:
        """
        Run the DQX profiler on the input data and return the generated checks and profile summary stats.

        :param input_config: Input data configuration (e.g. table name or file location, read options).
        :param profiler_config: Profiler configuration.
        :return: A tuple containing the generated checks and profile summary statistics.
        """
        df = read_input_data(self.spark, input_config)
        summary_stats, profiles = self.profiler.profile(
            df,
            options={
                "sample_fraction": profiler_config.sample_fraction,
                "sample_seed": profiler_config.sample_seed,
                "limit": profiler_config.limit,
            },
        )
        checks = self.generator.generate_dq_rules(profiles)  # use default criticality level "error"
        logger.info(f"Generated checks:\n{checks}")
        logger.info(f"Generated summary statistics:\n{summary_stats}")
        return checks, summary_stats

    def save(
        self,
        checks: list[dict],
        summary_stats: dict[str, Any],
        checks_file: str | None,
        profile_summary_stats_file: str | None,
    ) -> None:
        """
        Save the generated checks and profile summary statistics to the specified files.

        :param checks: The generated checks.
        :param summary_stats: The profile summary statistics.
        :param checks_file: The file to save the checks to.
        :param profile_summary_stats_file: The file to save the profile summary statistics to.
        """
        if not checks_file:
            raise ValueError("Check file not configured")
        if not profile_summary_stats_file:
            raise ValueError("Profile summary stats file not configured")

        install_folder = self.installation.install_folder()

        logger.info(f"Uploading checks to {install_folder}/{checks_file}")
        self.installation.upload(checks_file, yaml.safe_dump(checks).encode('utf-8'))

        logger.info(f"Uploading profile summary stats to {install_folder}/{profile_summary_stats_file}")
        self.installation.upload(profile_summary_stats_file, yaml.dump(summary_stats).encode('utf-8'))


class DataQualityRunner:
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
        custom_check_functions_resolved = self._resolve_check_functions(custom_check_functions)

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

    def _resolve_check_functions(self, check_functions: dict[str, str] | None = None) -> dict[str, Any]:
        resolved_funcs: dict[str, Any] = {}
        if check_functions:
            for full_func_name, module_workspace_path in check_functions.items():
                sys.path.append(module_workspace_path)
                func_name = full_func_name.split(".")[-1]  # get func name from the full name, e.g. my_module.my_func
                resolved_funcs[func_name] = self._import_func_from_string(func_name)

        return resolved_funcs

    def _get_ref_dfs(self, reference_tables) -> dict[str, DataFrame] | None:
        ref_dfs: dict[str, DataFrame] | None = None
        if reference_tables:
            ref_dfs = {
                name: read_input_data(self.spark, input_config) for name, input_config in reference_tables.items()
            }
        return ref_dfs

    def _import_func_from_string(self, full_func_name: str) -> Any:
        """Import a function or class given a dotted module path."""
        path, _, attr = full_func_name.rpartition(".")
        module = importlib.import_module(path)
        return getattr(module, attr)  # get the function from the module
