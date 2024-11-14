from dataclasses import dataclass

from databricks.sdk.core import Config

__all__ = ["WorkspaceConfig", "RunConfig"]


@dataclass
class RunConfig:
    """Configuration class for the data quality checks"""

    name: str = "default"  # name of the run configuration
    input_locations: str | None = None  # input data path or a table
    output_table: str | None = None  # output data table
    quarantine_table: str | None = None  # quarantined data table
    curated_table: str | None = None  # curated data table
    checks_file: str | None = "checks.yml"  # name of the file containing quality rules / checks
    profile_summary_stats_file: str | None = "checks.yml"  # name of the file containing profile summary statistics


@dataclass
class WorkspaceConfig:
    """Configuration class for the workspace"""

    __file__ = "config.yml"
    __version__ = 2

    run_configs: list[RunConfig]
    log_level: str | None = "INFO"
    connect: Config | None = None

    def get_run_config(self, run_config_name: str | None) -> RunConfig:
        """Get the run configuration for a given run name, or the default configuration if no run name is provided.
        :param run_config_name: The name of the run configuration to get.
        :return: The run configuration.
        """
        if not self.run_configs:
            raise ValueError("No run configurations available")

        if not run_config_name:
            return self.run_configs[0]

        for run in self.run_configs:
            if run.name == run_config_name:
                return run

        return self.run_configs[0]
