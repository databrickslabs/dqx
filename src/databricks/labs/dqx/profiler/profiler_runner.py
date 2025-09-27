from typing import Any
import logging
import yaml
from pyspark.sql import SparkSession
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat

from databricks.labs.dqx.checks_storage import ChecksStorageHandlerFactory
from databricks.labs.dqx.config import (
    InputConfig,
    ProfilerConfig,
    BaseChecksStorageConfig,
    InstallationChecksStorageConfig,
)
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.io import read_input_data, TABLE_PATTERN
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.blueprint.installation import Installation


logger = logging.getLogger(__name__)


class ProfilerRunner:
    """Runs the DQX profiler on the input data and saves the generated checks and profile summary stats."""

    def __init__(
        self,
        ws: WorkspaceClient,
        spark: SparkSession,
        dq_engine: DQEngine,
        installation: Installation,
        profiler: DQProfiler,
        generator: DQGenerator,
    ):
        self.ws = ws
        self.spark = spark
        self.dq_engine = dq_engine
        self.installation = installation
        self.profiler = profiler
        self.generator = generator

    def run(
        self,
        run_config_name: str,
        input_config: InputConfig,
        profiler_config: ProfilerConfig,
        product: str,
        install_folder: str,
    ) -> None:
        """
        Run the DQX profiler for the given run configuration and save the generated checks and profile summary stats.

        Args:
            run_config_name: Name of the run configuration (used in storage config).
            input_config: Input data configuration.
            profiler_config: Profiler configuration.
            product: Product name for the installation (used in storage config).
            install_folder: Installation folder path (used in storage config).

        Returns:
            A tuple containing the generated checks and profile summary statistics.
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
        logger.info(f"Generated checks: \n{checks}")
        logger.info(f"Generated summary statistics: \n{summary_stats}")

        storage_config = InstallationChecksStorageConfig(
            run_config_name=run_config_name,
            assume_user=True,
            product_name=product,
            install_folder=install_folder,
        )
        self.save(checks, summary_stats, storage_config, profiler_config.summary_stats_file)

    def run_for_patterns(
        self,
        patterns: list[str],
        profiler_config: ProfilerConfig,
        checks_location: str,
        install_folder: str,
        max_parallelism: int,
    ) -> None:
        """
        Run the DQX profiler for the given table patterns and save the generated checks and profile summary stats.

        Args:
            patterns: List of table patterns to profile (e.g. ["catalog.schema.table*"]).
            profiler_config: Profiler configuration.
            checks_location: Delta table to save the generated checks,
                otherwise checks are saved under checks/{table_name}.yml.
            install_folder: Installation folder path (used in storage config).
            max_parallelism: Maximum number of parallel threads to use for profiling.
        """
        options = [
            {
                "table": "*",  # Matches all tables
                "options": {
                    "sample_fraction": profiler_config.sample_fraction,
                    "sample_seed": profiler_config.sample_seed,
                    "limit": profiler_config.limit,
                },
            }
        ]
        results = self.profiler.profile_tables(patterns=patterns, options=options, max_parallelism=max_parallelism)
        storage_handler_factory = ChecksStorageHandlerFactory(self.ws, self.spark)

        for table, (summary_stats, profiles) in results.items():
            checks = self.generator.generate_dq_rules(profiles)  # use default criticality level "error"
            logger.info(f"Generated checks: \n{checks}")
            logger.info(f"Generated summary statistics: \n{summary_stats}")

            checks_location_resolved = (
                # for file based checks expecting a file per table
                checks_location
                if TABLE_PATTERN.match(checks_location)
                else f"{install_folder}/checks/{table}.yml"
            )

            _, storage_config = storage_handler_factory.create_for_location(checks_location_resolved, table)
            self.save(checks, summary_stats, storage_config, profiler_config.summary_stats_file)

    def save(
        self,
        checks: list[dict],
        summary_stats: dict[str, Any],
        storage_config: BaseChecksStorageConfig,
        profile_summary_stats_file: str,
    ) -> None:
        """
        Save the generated checks and profile summary statistics to the specified files.

        Args:
            checks: The generated checks.
            summary_stats: The profile summary statistics.
            storage_config: Configuration for where to save the checks.
            profile_summary_stats_file: The file to save the profile summary statistics to.
        """
        self.dq_engine.save_checks(checks, storage_config)
        self._save_summary_stats(profile_summary_stats_file, summary_stats)

    def _save_summary_stats(self, profile_summary_stats_file: str, summary_stats: dict[str, Any]) -> None:
        install_folder = self.installation.install_folder()
        summary_stats_file = f"{install_folder}/{profile_summary_stats_file}"

        logger.info(f"Uploading profile summary stats to {summary_stats_file}")
        content = yaml.safe_dump(summary_stats).encode("utf-8")
        self.ws.workspace.upload(summary_stats_file, content, format=ImportFormat.AUTO, overwrite=True)
