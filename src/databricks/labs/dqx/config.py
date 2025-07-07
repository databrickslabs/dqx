from dataclasses import dataclass, field

from databricks.sdk.core import Config

__all__ = ["WorkspaceConfig", "RunConfig", "InputConfig", "OutputConfig", "ProfilerConfig"]


@dataclass
class InputConfig:
    """Configuration class for input data sources (e.g. tables or files)."""

    location: str
    format: str = "delta"
    is_streaming: bool = False
    schema: str | None = None
    options: dict[str, str] = field(default_factory=dict)


@dataclass
class OutputConfig:
    """Configuration class for output data sinks (e.g. tables or files)."""

    location: str
    format: str = "delta"
    mode: str = "append"
    options: dict[str, str] = field(default_factory=dict)
    trigger: dict[str, bool | str] = field(default_factory=dict)


@dataclass
class ProfilerConfig:
    """Configuration class for profiler."""

    summary_stats_file: str = "profile_summary_stats.yml"  # file containing profile summary statistics
    sample_fraction: float = 0.3  # fraction of data to sample (30%)
    sample_seed: int | None = None  # seed for sampling
    limit: int = 1000  # limit the number of records to profile


@dataclass
class RunConfig:
    """Configuration class for the data quality checks"""

    name: str = "default"  # name of the run configuration
    input_config: InputConfig | None = None
    output_config: OutputConfig | None = None
    quarantine_config: OutputConfig | None = None  # quarantined data table
    checks_file: str | None = "checks.yml"  # file containing quality rules / checks
    checks_table: str | None = None  # table containing quality rules / checks
    warehouse_id: str | None = None  # warehouse id to use in the dashboard
    profiler_config: ProfilerConfig = field(default_factory=ProfilerConfig)


@dataclass
class WorkspaceConfig:
    """Configuration class for the workspace"""

    __file__ = "config.yml"
    __version__ = 1

    run_configs: list[RunConfig]
    log_level: str | None = "INFO"
    connect: Config | None = None

    # cluster configuration for the profiler job, global config since there should be one profiler instance only
    profiler_override_clusters: dict[str, str] | None = field(default_factory=dict)
    # extra spark config for the profiler job, global config since there should be one profiler instance only
    profiler_spark_conf: dict[str, str] | None = field(default_factory=dict)

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
