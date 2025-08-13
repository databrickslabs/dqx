import abc
from dataclasses import dataclass, field
from databricks.sdk.core import Config

__all__ = [
    "WorkspaceConfig",
    "RunConfig",
    "InputConfig",
    "OutputConfig",
    "ProfilerConfig",
    "BaseChecksStorageConfig",
    "FileChecksStorageConfig",
    "WorkspaceFileChecksStorageConfig",
    "TableChecksStorageConfig",
    "InstallationChecksStorageConfig",
    "VolumeFileChecksStorageConfig",
]


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
    checks_location: str = "checks.yml"  # relative workspace file path or table containing quality rules / checks
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


@dataclass
class BaseChecksStorageConfig(abc.ABC):
    """Marker base class for storage configuration."""


@dataclass
class FileChecksStorageConfig(BaseChecksStorageConfig):
    """
    Configuration class for storing checks in a file.

    :param location: The file path where the checks are stored.
    """

    location: str

    def __post_init__(self):
        if not self.location:
            raise ValueError("The file path ('location' field) must not be empty or None.")


@dataclass
class WorkspaceFileChecksStorageConfig(BaseChecksStorageConfig):
    """
    Configuration class for storing checks in a workspace file.

    :param location: The workspace file path where the checks are stored.
    """

    location: str

    def __post_init__(self):
        if not self.location:
            raise ValueError("The workspace file path ('location' field) must not be empty or None.")


@dataclass
class TableChecksStorageConfig(BaseChecksStorageConfig):
    """
    Configuration class for storing checks in a table.

    :param location: The table name where the checks are stored.
    :param run_config_name: The name of the run configuration to use for checks (default is 'default').
    :param mode: The mode for writing checks to a table (e.g., 'append' or 'overwrite').
    The `overwrite` mode will only replace checks for the specific run config and not all checks in the table.
    """

    location: str
    run_config_name: str = "default"  # to filter checks by run config
    mode: str = "overwrite"

    def __post_init__(self):
        if not self.location:
            raise ValueError("The table name ('location' field) must not be empty or None.")


@dataclass
class VolumeFileChecksStorageConfig(BaseChecksStorageConfig):
    """
    Configuration class for storing checks in a Unity Catalog volume file.

    :param location: The Unity Catalog volume file path where the checks are stored.
    """

    location: str

    def __post_init__(self):
        if not self.location:
            raise ValueError("The Unity Catalog volume file path ('location' field) must not be empty or None.")


@dataclass
class InstallationChecksStorageConfig(
    WorkspaceFileChecksStorageConfig, TableChecksStorageConfig, VolumeFileChecksStorageConfig
):
    """
    Configuration class for storing checks in an installation.

    :param location: The installation path where the checks are stored (e.g., table name, file path).
    Not used when using installation method, as it is retrieved from the installation config.
    :param run_config_name: The name of the run configuration to use for checks (default is 'default').
    :param product_name: The product name for retrieving checks from the installation (default is 'dqx').
    :param assume_user: Whether to assume the user is the owner of the checks (default is True).
    """

    location: str = "installation"  # retrieved from the installation config
    run_config_name: str = "default"  # to retrieve run config
    product_name: str = "dqx"
    assume_user: bool = True
