from typing import Any
import logging
import yaml
from pyspark.sql import SparkSession
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat

from databricks.labs.dqx.config import InputConfig, ProfilerConfig, BaseChecksStorageConfig
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
        input_config: InputConfig,
        profiler_config: ProfilerConfig,
    ) -> tuple[list[dict], dict[str, Any]]:
        """
        Run the DQX profiler on the input data and return the generated checks and profile summary stats.

        Args:
            input_config: Input data configuration (e.g. table name or file location, read options).
            profiler_config: Profiler configuration.

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
        logger.info(f"Generated checks:\n{checks}")
        logger.info(f"Generated summary statistics:\n{summary_stats}")
        return checks, summary_stats

    def save(
        self,
        checks: list[dict],
        summary_stats: dict[str, Any],
        storage_config: BaseChecksStorageConfig,
        profile_summary_stats_file: str | None,
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

    def _save_summary_stats(self, profile_summary_stats_file, summary_stats):
        install_folder = self.installation.install_folder()
        summary_stats_file = f"{install_folder}/{profile_summary_stats_file}"

        logger.info(f"Uploading profile summary stats to {summary_stats_file}")
        content = yaml.safe_dump(summary_stats).encode("utf-8")
        self.ws.workspace.upload(summary_stats_file, content, format=ImportFormat.AUTO, overwrite=True)
