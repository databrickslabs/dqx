from typing import Any
import logging
import yaml
from pyspark.sql import SparkSession

from databricks.labs.dqx.utils import read_input_data
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.sdk import WorkspaceClient
from databricks.labs.blueprint.installation import Installation


logger = logging.getLogger(__name__)


class ProfilerRunner:
    """Runs the DQX profiler on the input data and saves the generated checks and profile summary stats."""

    def __init__(
        self,
        ws: WorkspaceClient,
        spark: SparkSession,
        installation_name: str = "dqx",
        profiler: DQProfiler | None = None,
        generator: DQGenerator | None = None,
        installation: Installation | None = None,
    ):
        self.spark = spark
        self.ws = ws
        self.installation_name = installation_name
        if not profiler:
            self.profiler = DQProfiler(ws)
        if not generator:
            self.generator = DQGenerator(ws)
        if not installation:
            self.installation = Installation(ws, installation_name)

    def run(
        self,
        input_location: str | None,
        input_format: str | None,
    ) -> tuple[list[dict], dict[str, Any]]:
        """
        Run the DQX profiler on the input data and return the generated checks and profile summary stats.

        :param input_location: The location of the input data.
        :param input_format: The format of the input data.
        :return: A tuple containing the generated checks and profile summary statistics.
        """
        df = read_input_data(self.spark, input_location, input_format)
        summary_stats, profiles = self.profiler.profile(df)
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