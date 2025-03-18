from dataclasses import dataclass

from databricks.sdk.core import Config

__all__ = ["WorkspaceConfig", "RunConfig"]


@dataclass
class RunConfig:
    """Configuration class for the data quality checks"""

    name: str = "default"  # name of the run configuration
    input_location: str | None = None  # input data path or a table
    input_format: str | None = "delta"  # input data format
    output_table: str | None = None  # output data table
    quarantine_table: str | None = None  # quarantined data table
    checks_file: str | None = "checks.yml"  # file containing quality rules / checks
    profile_summary_stats_file: str | None = "profile_summary_stats.yml"  # file containing profile summary statistics
    override_clusters: dict[str, str] | None = None  # cluster configuration for jobs
    spark_conf: dict[str, str] | None = None  # extra spark configs
    warehouse_id: str | None = None  # warehouse id to use in the dashboard
    profiler_sample_fraction: float | None = 0.3  # fraction of data to sample (30%)
    profiler_sample_seed: int | None = None  # seed for sampling
    profiler_limit: int | None = 1000  # limit the number of records to profile


@dataclass
class WorkspaceConfig:
    """Configuration class for the workspace"""

    __file__ = "config.yml"
    __version__ = 1

    run_configs: list[RunConfig]
    log_level: str | None = "INFO"
    connect: Config | None = None

    def get_run_config(self, run_config_name: str | None = "default") -> RunConfig:
        """Get the run configuration for a given run name, or the default configuration if no run name is provided.
        :param run_config_name: The name of the run configuration to get.
        :return: The run configuration.
        :raises ValueError: If no run configurations are available or if the specified run configuration name is
        not found.
        """
        if not self.run_configs:
            raise ValueError("No run configurations available")

        if not run_config_name:
            return self.run_configs[0]

        for run in self.run_configs:
            if run.name == run_config_name:
                return run

        raise ValueError("No run configurations available")
