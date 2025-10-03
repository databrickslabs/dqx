import logging
import os

from pyspark.sql import SparkSession

from databricks.labs.dqx.config import RunConfig
from databricks.labs.dqx.engine import DQEngine

logger = logging.getLogger(__name__)


class QualityCheckerRunner:
    """Runs the DQX data quality on the input data and saves the generated results to delta table(s)."""

    def __init__(self, spark: SparkSession, dq_engine: DQEngine):
        self.spark = spark
        self.dq_engine = dq_engine

    def run(self, run_configs: list[RunConfig], max_parallelism: int | None = os.cpu_count()) -> None:
        """
        Run the DQX data quality job for the provided run configs.

        Args:
            run_configs: List of RunConfig objects containing the configuration for each run.
            max_parallelism: Maximum number of parallel runs. Defaults to the number of CPU cores.
        """
        logger.info("Data quality checker started.")
        self.dq_engine.apply_checks_and_save_in_tables(run_configs=run_configs, max_parallelism=max_parallelism)
        logger.info("Data quality checker completed.")

    def run_for_patterns(
        self,
        patterns: list[str],
        exclude_patterns: list[str],
        run_config_template: RunConfig,
        checks_location: str,
        output_table_suffix: str,
        quarantine_table_suffix: str,
        max_parallelism: int,
    ) -> None:
        """
        Run the DQX data quality job for the provided location patterns using a run config template.

        Args:
            patterns: List of location patterns (with wildcards) to apply the data quality checks to.
            exclude_patterns: List of table patterns to exclude from profiling (e.g. ["*output", "*quarantine"]).
            run_config_template: A RunConfig object to be used as a template for each pattern, except location.
            checks_location: Location to read the checks from.
            output_table_suffix: Suffix to append to the output table names.
            quarantine_table_suffix: Suffix to append to the quarantine table names.
            max_parallelism: Maximum number of parallel runs. Defaults to the number of CPU cores.
        """
        logger.info(f"Data quality checker started for patterns {patterns} and checks {checks_location}")
        self.dq_engine.apply_checks_and_save_in_tables_for_patterns(
            patterns=patterns,
            exclude_patterns=exclude_patterns,
            checks_location=checks_location,
            run_config_template=run_config_template,
            max_parallelism=max_parallelism,
            output_table_suffix=output_table_suffix,
            quarantine_table_suffix=quarantine_table_suffix,
        )
        logger.info(f"Data quality checker completed for {patterns} and checks {checks_location}")
