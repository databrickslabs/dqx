import abc
from datetime import datetime, timezone
from dataclasses import dataclass, field
from urllib.parse import urlparse, unquote

__all__ = [
    "WorkspaceConfig",
    "RunConfig",
    "InputConfig",
    "OutputConfig",
    "ExtraParams",
    "ProfilerConfig",
    "BaseChecksStorageConfig",
    "FileChecksStorageConfig",
    "WorkspaceFileChecksStorageConfig",
    "TableChecksStorageConfig",
    "LakebaseChecksStorageConfig",
    "InstallationChecksStorageConfig",
    "VolumeFileChecksStorageConfig",
]

LAKEBASE_DEFAULT_PORT = 5432


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
    checks_location: str = (
        "checks.yml"  # absolute or relative workspace file path or table containing quality rules / checks
    )
    warehouse_id: str | None = None  # warehouse id to use in the dashboard
    profiler_config: ProfilerConfig = field(default_factory=ProfilerConfig)
    reference_tables: dict[str, InputConfig] = field(default_factory=dict)  # reference tables to use in the checks
    # mapping of fully qualified custom check function (e.g. my_func) to the module location in the workspace
    # (e.g. {"my_func": "/Workspace/my_repo/my_module.py"})
    custom_check_functions: dict[str, str] = field(default_factory=dict)


def _default_run_time() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class ExtraParams:
    """Class to represent extra parameters for DQEngine."""

    result_column_names: dict[str, str] = field(default_factory=dict)
    run_time: str = field(default_factory=_default_run_time)
    user_metadata: dict[str, str] = field(default_factory=dict)


@dataclass
class WorkspaceConfig:
    """Configuration class for the workspace"""

    __file__ = "config.yml"
    __version__ = 1

    run_configs: list[RunConfig]
    log_level: str | None = "INFO"

    # whether to use serverless clusters for the jobs, only used during workspace installation
    serverless_clusters: bool = True
    extra_params: ExtraParams | None = None  # extra parameters to pass to the jobs, e.g. run_time

    # cluster configuration for the jobs (applicable for non-serverless clusters only)
    profiler_override_clusters: dict[str, str] | None = field(default_factory=dict)
    quality_checker_override_clusters: dict[str, str] | None = field(default_factory=dict)
    e2e_override_clusters: dict[str, str] | None = field(default_factory=dict)

    # extra spark config for jobs (applicable for non-serverless clusters only)
    profiler_spark_conf: dict[str, str] | None = field(default_factory=dict)
    quality_checker_spark_conf: dict[str, str] | None = field(default_factory=dict)
    e2e_spark_conf: dict[str, str] | None = field(default_factory=dict)

    def get_run_config(self, run_config_name: str | None = "default") -> RunConfig:
        """Get the run configuration for a given run name, or the default configuration if no run name is provided.

        Args:
            run_config_name: The name of the run configuration to get.

        Returns:
            The run configuration.

        Raises:
            ValueError: If no run configurations are available or if the specified run configuration name is
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

    Args:
        location: The file path where the checks are stored.
    """

    location: str

    def __post_init__(self):
        if not self.location:
            raise ValueError("The file path ('location' field) must not be empty or None.")


@dataclass
class WorkspaceFileChecksStorageConfig(BaseChecksStorageConfig):
    """
    Configuration class for storing checks in a workspace file.

    Args:
        location: The workspace file path where the checks are stored.
    """

    location: str

    def __post_init__(self):
        if not self.location:
            raise ValueError("The workspace file path ('location' field) must not be empty or None.")


@dataclass
class TableChecksStorageConfig(BaseChecksStorageConfig):
    """
    Configuration class for storing checks in a table.

    Args:
        location: The table name where the checks are stored.
        run_config_name: The name of the run configuration to use for checks (default is 'default').
        mode: The mode for writing checks to a table (e.g., 'append' or 'overwrite').
            The *overwrite* mode will only replace checks for the specific run config and not all checks in the table.
    """

    location: str
    run_config_name: str = "default"  # to filter checks by run config
    mode: str = "overwrite"

    def __post_init__(self):
        if not self.location:
            raise ValueError("The table name ('location' field) must not be empty or None.")


@dataclass
class LakebaseConnectionConfig:
    """
    Configuration class for a Lakebase connection.

    Note:
        The connection string format includes a placeholder for a password,
        e.g., postgresql://user:password@host:port/database), but the password 
        is not stored or parsed by this class.

    Args:
        instance_name: The Lakebase instance name (hostname).
        database: The Lakebase database name.
        user: The Lakebase username.
        port: The Lakebase port (default is '5432').
    """

    instance_name: str
    database: str
    user: str
    port: int = LAKEBASE_DEFAULT_PORT

    @staticmethod
    def _parse_connection_string(connection_string: str) -> "LakebaseConnectionConfig":
        """
        Parse PostgreSQL connection string to extract connection parameters.

        Expected format: postgresql://user:password@instance_name:port/database?params.

        Args:
            connection_string: Lakebase (SQLAlchemy) connection string.

        Returns:
            Instance of LakebaseConnectionConfig with extracted parameters.

        Raises:
            ValueError: If the URL format is invalid or required components are missing.
        """
        if not connection_string:
            raise ValueError("Connection string cannot be empty or None.")

        try:
            parsed = urlparse(connection_string)
        except Exception as e:
            raise ValueError(f"Failed to parse URL '{connection_string}': {e}") from e

        if parsed.scheme != "postgresql":
            raise ValueError(f"Invalid URL scheme '{parsed.scheme}'. Expected 'postgresql' for Lakebase connections.")

        try:
            user = unquote(parsed.username) if parsed.username else None
        except Exception as e:
            raise ValueError(f"Failed to decode username from URL: {e}") from e

        if not user:
            raise ValueError(f"Missing username in URL: {connection_string}")

        instance_name = parsed.hostname
        if not instance_name:
            raise ValueError(f"Missing hostname in URL: {connection_string}")

        port = LAKEBASE_DEFAULT_PORT
        if parsed.port:
            try:
                port = int(parsed.port)
            except (ValueError, TypeError) as e:
                raise ValueError(f"Invalid port '{parsed.port}' in URL: {e}") from e

        database = None
        if parsed.path:
            try:
                database = parsed.path.lstrip("/")
                if not database:
                    raise ValueError("Database name is missing")
            except Exception as e:
                raise ValueError(f"Failed to extract database name from connection string '{parsed.path}': {e}") from e

        if not database:
            raise ValueError(f"Missing required database name in connection string: {connection_string}")

        return LakebaseConnectionConfig(
            instance_name=instance_name,
            database=database,
            user=user,
            port=port,
        )


@dataclass
class LakebaseChecksStorageConfig(BaseChecksStorageConfig):
    """
    Configuration class for storing checks in a Lakebase table.

    Note: 
        The `schema` and `table` fields are not parsed from the connection string, but 
        must be provided in the `location` field in the format 'database.schema.table'.

    Args:
        location: Fully qualified Lakebase table name in the format 'database.schema.table'.
        connection_string: (SQLAlchemy) Lakebase connection string, e.g., postgresql://user:password@host:port/database.
        run_config_name: Name of the run configuration to use for checks (default is 'default').
        mode: The mode for writing checks to a table (e.g., 'append' or 'overwrite'). The *overwrite* mode
        only replaces checks for the specific run config and not all checks in the table (default is 'overwrite').
    """

    location: str
    connection_string: str
    run_config_name: str = "default"
    mode: str = "overwrite"

    def __post_init__(self):
        if not self.location:
            raise ValueError("The location ('location' field) must not be empty or None.")

        if not self.connection_string:
            raise ValueError("The connection string ('connection_string' field) must not be empty or None.")

        if self.connection_string and self.connection_string.startswith("postgresql://"):
            try:
                LakebaseConnectionConfig._parse_connection_string(self.connection_string)
            except Exception as e:
                raise ValueError(f"Failed to parse connection string '{self.connection_string}': {e}") from e


@dataclass
class VolumeFileChecksStorageConfig(BaseChecksStorageConfig):
    """
    Configuration class for storing checks in a Unity Catalog volume file.

    Args:
        location: The Unity Catalog volume file path where the checks are stored.
    """

    location: str

    def __post_init__(self):
        if not self.location:
            raise ValueError("The Unity Catalog volume file path ('location' field) must not be empty or None.")


@dataclass
class InstallationChecksStorageConfig(
    WorkspaceFileChecksStorageConfig,
    TableChecksStorageConfig,
    VolumeFileChecksStorageConfig,
    LakebaseChecksStorageConfig,
):
    """
    Configuration class for storing checks in an installation.

    Args:
        location: The installation path where the checks are stored (e.g., table name, file path).
            Not used when using installation method, as it is retrieved from the installation config.
        run_config_name: The name of the run configuration to use for checks (default is 'default').
        product_name: The product name for retrieving checks from the installation (default is 'dqx').
        assume_user: Whether to assume the user is the owner of the checks (default is True).
        install_folder: The installation folder where DQX is installed.
        DQX will be installed in a default directory if no custom folder is provided:
        * User's home directory: "/Users/<your_user>/.dqx"
        * Global directory if `DQX_FORCE_INSTALL=global`: "/Applications/dqx"
    """

    location: str = "installation"  # retrieved from the installation config
    run_config_name: str = "default"  # to retrieve run config
    product_name: str = "dqx"
    assume_user: bool = True
    install_folder: str | None = None
