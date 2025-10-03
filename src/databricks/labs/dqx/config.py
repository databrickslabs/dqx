import abc
from functools import cached_property
from datetime import datetime, timezone
from dataclasses import dataclass, field
from urllib.parse import urlparse, unquote
from databricks.labs.dqx.errors import InvalidConfigError, InvalidParameterError

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

LAKEBASE_DEFAULT_PORT = "5432"


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
    filter: str | None = None  # filter to apply to the data before profiling


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
    connection_string: str | None = None  # connection string for Lakebase instances
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

    profiler_max_parallelism: int = 4  # max parallelism for profiling multiple tables
    quality_checker_max_parallelism: int = 4  # max parallelism for quality checking multiple tables

    def get_run_config(self, run_config_name: str | None = "default") -> RunConfig:
        """Get the run configuration for a given run name, or the default configuration if no run name is provided.

        Args:
            run_config_name: The name of the run configuration to get.

        Returns:
            The run configuration.

        Raises:
            InvalidConfigError: If no run configurations are available or if the specified run configuration name is
            not found.
        """
        if not self.run_configs:
            raise InvalidConfigError("No run configurations available")

        if not run_config_name:
            return self.run_configs[0]

        for run in self.run_configs:
            if run.name == run_config_name:
                return run

        raise InvalidConfigError("No run configurations available")


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
            raise InvalidConfigError("The file path ('location' field) must not be empty or None.")


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
            raise InvalidConfigError("The workspace file path ('location' field) must not be empty or None.")


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
            raise InvalidConfigError("The table name ('location' field) must not be empty or None.")


@dataclass
class LakebaseConnectionConfig:
    """
    Configuration class for a Lakebase connection.

    Note:
        The connection string format includes a placeholder for a password,
        e.g., postgresql://user:password@host:port/database, but the password
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
    port: str = LAKEBASE_DEFAULT_PORT

    @staticmethod
    def create(connection_string: str) -> "LakebaseConnectionConfig":
        if not connection_string:
            raise InvalidParameterError("Connection string cannot be empty or None.")

        parsed = urlparse(connection_string)

        if parsed.scheme != "postgresql":
            raise InvalidParameterError(f"Invalid URL scheme '{parsed.scheme}'. Expected 'postgresql'.")

        user = unquote(parsed.username) if parsed.username else None
        if not user:
            raise InvalidParameterError(f"Missing username in connection string: {connection_string}")

        instance_name = parsed.hostname
        if not instance_name:
            raise InvalidParameterError(f"Missing hostname in connection string: {connection_string}")

        port = str(parsed.port) if parsed.port else LAKEBASE_DEFAULT_PORT

        database = parsed.path.lstrip("/") if parsed.path else None
        if not database:
            raise InvalidParameterError(f"Missing database name in connection string: {connection_string}")

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
            raise InvalidParameterError("The 'location' field must not be empty or None.")

        # Eager validation of connection string
        try:
            self._connection_config = LakebaseConnectionConfig.create(self.connection_string)
        except Exception as e:
            raise InvalidParameterError(f"Failed to parse connection string '{self.connection_string}': {e}") from e

        if len(self.location.split(".")) != 3:
            raise InvalidConfigError(
                f"Invalid Lakebase table name '{self.location}'. " "Must be in the format 'database.schema.table'."
            )

        if self.mode not in ("append", "overwrite"):
            raise InvalidConfigError(f"Invalid mode '{self.mode}'. Must be 'append' or 'overwrite'.")

    @cached_property
    def connection_config(self) -> LakebaseConnectionConfig:
        """Parses and validates the Lakebase connection string."""
        return LakebaseConnectionConfig.create(self.connection_string)

    @cached_property
    def database_name(self) -> str:
        return self._split_location()[0]

    @cached_property
    def schema_name(self) -> str:
        return self._split_location()[1]

    @cached_property
    def table_name(self) -> str:
        return self._split_location()[2]

    def _split_location(self) -> tuple[str, ...]:
        """Splits 'database.schema.table' into components."""
        return tuple(self.location.split("."))


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
            raise InvalidConfigError("The Unity Catalog volume file path ('location' field) must not be empty or None.")


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
            Not used when using installation method, as it is retrieved from the installation config,
            unless overwrite_location is enabled.
        run_config_name: The name of the run configuration to use for checks (default is 'default').
        product_name: The product name for retrieving checks from the installation (default is 'dqx').
        assume_user: Whether to assume the user is the owner of the checks (default is True).
        install_folder: The installation folder where DQX is installed.
        DQX will be installed in a default directory if no custom folder is provided:
        * User's home directory: "/Users/<your_user>/.dqx"
        * Global directory if `DQX_FORCE_INSTALL=global`: "/Applications/dqx"
        overwrite_location: Whether to overwrite the location from run config if provided (default is False).
    """

    location: str = "installation"  # retrieved from the installation config
    connection_string: str = "installation"  # (optional) retrieved from the installation config for database connection
    run_config_name: str = "default"  # to retrieve run config
    product_name: str = "dqx"
    assume_user: bool = True
    install_folder: str | None = None
    overwrite_location: bool = False
