import abc
import ipaddress
import re
from dataclasses import asdict, dataclass, field
from functools import cached_property
from urllib.parse import urlparse

from databricks.labs.dqx.checks_serializer import SerializerFactory
from databricks.labs.dqx.errors import InvalidConfigError, InvalidParameterError
from databricks.labs.dqx.utils import VariableValue

__all__ = [
    "WorkspaceConfig",
    "RunConfig",
    "InputConfig",
    "OutputConfig",
    "ExtraParams",
    "ProfilerConfig",
    "LLMModelConfig",
    "LLMConfig",
    "AnomalyConfig",
    "AnomalyParams",
    "IsolationForestConfig",
    "FeatureEngineeringConfig",
    "TemporalAnomalyConfig",
    "BaseChecksStorageConfig",
    "FileChecksStorageConfig",
    "WorkspaceFileChecksStorageConfig",
    "TableChecksStorageConfig",
    "LakebaseChecksStorageConfig",
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
    trigger: dict[str, str | bool] = field(default_factory=dict)
    partition_by: list[str] = field(default_factory=list)
    cluster_by: list[str] = field(default_factory=list)

    def __post_init__(self):
        """
        Normalize trigger configuration by converting string boolean representations to actual booleans.
        This is required due to the limitation of the config deserializer.
        """
        if self.partition_by and self.cluster_by:
            raise InvalidParameterError("Only one of partition_by or cluster_by is allowed")
        # Convert string representations of booleans to actual booleans
        for key, value in list(self.trigger.items()):
            if isinstance(value, str):
                if value.lower() == 'true':
                    self.trigger[key] = True
                elif value.lower() == 'false':
                    self.trigger[key] = False
                # Otherwise keep it as a string (e.g., "10 seconds" for processingTime)


@dataclass
class ProfilerConfig:
    """Configuration class for profiler."""

    summary_stats_file: str = "profile_summary_stats.yml"  # file containing profile summary statistics
    sample_fraction: float = 0.3  # fraction of data to sample (30%)
    sample_seed: int | None = None  # seed for sampling
    limit: int = 1000  # limit the number of records to profile
    filter: str | None = None  # filter to apply to the data before profiling
    criticality: str = "error"  # default criticality for generated rules ("error" or "warn")
    llm_primary_key_detection: bool = (
        False  # whether to use LLM for primary key detection to generate uniqueness checks
    )
    # Override profiler default thresholds
    max_null_ratio: float | None = None
    max_empty_ratio: float | None = None


@dataclass
class IsolationForestConfig:
    """Algorithm parameters for Spark ML IsolationForest."""

    contamination: float | None = None
    num_trees: int = 200
    max_depth: int | None = None
    subsampling_rate: float | None = None
    random_seed: int = 42


@dataclass
class TemporalAnomalyConfig:
    """Configuration for temporal feature extraction."""

    timestamp_column: str
    temporal_features: list[str] = field(default_factory=lambda: ["hour", "day_of_week", "month"])


@dataclass
class FeatureEngineeringConfig:
    """Configuration for multi-type feature engineering in anomaly detection."""

    max_input_columns: int = 25  # Soft limit - warns but proceeds if exceeded
    max_engineered_features: int = 50  # Soft limit on total engineered features
    categorical_cardinality_threshold: int = 20  # OneHot if <=20, Frequency if >20
    # Datetime features (always 7 per column: hour_sin, hour_cos, dow_sin, dow_cos, month_sin, month_cos, is_weekend)
    datetime_features: list[str] = field(
        default_factory=lambda: ["hour_sin", "hour_cos", "dow_sin", "dow_cos", "month_sin", "month_cos", "is_weekend"]
    )
    enable_categorical: bool = True
    enable_datetime: bool = True
    enable_boolean: bool = True


@dataclass
class AnomalyParams:
    """Optional tuning parameters for row anomaly detection.

    Attributes:
        sample_fraction: Fraction of data to sample for training (default 0.3).
        max_rows: Maximum rows to use for training (default 1,000,000).
        train_ratio: Train/validation split ratio (default 0.8).
        ensemble_size: Number of models in ensemble (default 3). Set to None for single model.
            Ensemble models provide:
            - More robust anomaly scores (averaged across models)
            - Confidence scores via standard deviation
            - Better generalization
            Performance: Optimized ensemble scoring makes this negligible overhead.
        algorithm_config: Isolation Forest parameters (contamination, num_trees, seed).
        feature_engineering: Feature engineering parameters (temporal features, scaling, etc.).
    """

    sample_fraction: float = 0.3
    max_rows: int = 1_000_000
    train_ratio: float = 0.8
    ensemble_size: int | None = 3  # Default 3-model ensemble for robustness, tie-breaking, and confidence scores
    algorithm_config: IsolationForestConfig = field(default_factory=IsolationForestConfig)
    feature_engineering: FeatureEngineeringConfig = field(default_factory=FeatureEngineeringConfig)


@dataclass
class AnomalyConfig:
    """Configuration for row anomaly detection."""

    columns: list[str] | None = None  # Auto-discovered if omitted
    segment_by: list[str] | None = None  # Auto-discovered if omitted (when columns also omitted)
    model_name: str | None = None  # Optional in workflows; defaults to dqx_anomaly_<run_config.name>
    registry_table: str | None = None


@dataclass
class RunConfig:
    """Configuration class for the data quality checks"""

    name: str = "default"  # name of the run configuration
    input_config: InputConfig | None = None
    output_config: OutputConfig | None = None
    quarantine_config: OutputConfig | None = None  # quarantined data table
    metrics_config: OutputConfig | None = None  # summary metrics table
    profiler_config: ProfilerConfig = field(default_factory=ProfilerConfig)
    checks_user_requirements: str | None = None  # user input for AI-assisted rule generation

    checks_location: str = (
        "checks.yml"  # absolute or relative workspace file path or table containing quality rules / checks
    )

    warehouse_id: str | None = None  # warehouse id to use in the dashboard

    reference_tables: dict[str, InputConfig] = field(default_factory=dict)  # reference tables to use in the checks
    # mapping of fully qualified custom check function (e.g. my_func) to the module location in the workspace
    # (e.g. {"my_func": "/Workspace/my_repo/my_module.py"})
    custom_check_functions: dict[str, str] = field(default_factory=dict)

    anomaly_config: AnomalyConfig | None = None  # optional anomaly detection configuration

    # Lakebase connection parameters, if wanting to store checks in lakebase database
    lakebase_instance_name: str | None = None
    lakebase_client_id: str | None = None
    lakebase_port: str | None = None


_DEFAULT_API_BASE_ALLOWED_HOSTS: tuple[str, ...] = (
    ".databricks.com",
    ".cloud.databricks.com",
    ".azuredatabricks.net",
    ".gcp.databricks.com",
    ".openai.com",
    ".anthropic.com",
)

# Matches a Databricks secret reference of the form 'scope/key': exactly one '/', no whitespace,
# no scheme, both sides non-empty. URLs and bare hostnames must NOT pass this check.
_SECRET_REF_RE = re.compile(r"^[^\s/:]+/[^\s/:]+$")


def _normalize_host(host: str) -> str:
    """Lower-case, strip IPv6 brackets and a trailing FQDN dot, IDNA-encode for unicode/punycode parity."""
    host = host.lower().strip("[]").rstrip(".")
    if not host:
        return host
    try:
        return host.encode("idna").decode("ascii")
    except UnicodeError:
        return host


def _is_ip_literal(host: str) -> bool:
    """True if *host* parses as an IPv4 or IPv6 literal."""
    try:
        ipaddress.ip_address(host.strip("[]"))
        return True
    except ValueError:
        return False


@dataclass
class LLMModelConfig:
    """Configuration for LLM model"""

    # The model to use for the DSPy language model
    model_name: str = "databricks/databricks-claude-sonnet-4-5"
    # Optional API key for the model as text or secret scope/key. Not required by foundational models.
    api_key: str = ""  # when used with Profiler Workflow, this should be a secret: secret_scope/secret_key
    # Optional API base URL for the model. Not required by foundational models. When set as a URL,
    # must use https and resolve to a host whose suffix is in the built-in allowlist (Databricks
    # AWS/Azure/GCP, OpenAI, Anthropic) or *api_base_allowed_hosts* (e.g. AI Gateway hosts).
    # When used with Profiler Workflow, this can also be a secret reference of the strict form
    # 'secret_scope/secret_key' (no scheme, no whitespace) — resolved + revalidated downstream.
    api_base: str = ""
    # Per-call output token cap. Bounds cost and latency for pathological prompts (OWASP LLM04).
    max_tokens: int = 1000
    # Sampling temperature. 0.0 is deterministic; raise for more creative narratives.
    temperature: float = 0.0
    # Per-call wall-clock timeout in seconds.
    timeout: float = 30.0
    # Additional host suffixes to allow for *api_base*, on top of the built-in allowlist. Use this
    # to permit explicitly approved AI Gateway hosts. Each entry is matched as a case-insensitive
    # suffix of the URL host (e.g. ".gateway.corp.example"). IP literals must match exactly.
    api_base_allowed_hosts: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.api_base:
            return
        if _SECRET_REF_RE.match(self.api_base):
            return  # secret reference — resolved + revalidated by the workflow before use
        if "://" not in self.api_base:
            raise InvalidParameterError("api_base must be either an https URL or a 'secret_scope/secret_key' reference")
        self._validate_api_base(self.api_base)

    def _validate_api_base(self, api_base: str) -> None:
        """Reject api_base URLs that don't pass scheme + host-suffix allowlist checks (CWE-918)."""
        parsed = urlparse(api_base)
        if parsed.scheme.lower() != "https":
            raise InvalidParameterError(f"api_base must use https scheme, got {parsed.scheme or '<empty>'!r}")
        raw_host = parsed.hostname or ""
        host = _normalize_host(raw_host)
        if not host:
            raise InvalidParameterError("api_base must include a host")

        host_is_ip = _is_ip_literal(host)
        merged = (*_DEFAULT_API_BASE_ALLOWED_HOSTS, *self.api_base_allowed_hosts)
        for entry in merged:
            if not entry or not entry.strip():
                continue  # ignore empty entries — they would otherwise match any host
            normalized = _normalize_host(entry.lstrip("."))
            if not normalized:
                continue
            entry_is_ip = _is_ip_literal(normalized)
            # IP literals: exact match. Hostnames: exact match or strict subdomain (host endswith
            # ".suffix"). Blocks "attacker.10.0.0.1" or "attackerexample.com" style bypasses.
            if entry_is_ip or host_is_ip:
                if host == normalized:
                    return
                continue
            if host == normalized or host.endswith(f".{normalized}"):
                return

        raise InvalidParameterError(
            f"api_base host {raw_host!r} is not in the allowed-hosts list. "
            f"Extend LLMModelConfig.api_base_allowed_hosts to permit it."
        )


@dataclass(frozen=True)
class LLMConfig:
    """Configuration for LLM usage"""

    model: LLMModelConfig = field(default_factory=LLMModelConfig)


@dataclass(frozen=True)
class ExtraParams:
    """Class to represent extra parameters for DQEngine."""

    result_column_names: dict[str, str] = field(default_factory=dict)
    user_metadata: dict[str, str] = field(default_factory=dict)
    run_time_overwrite: str | None = None
    run_id_overwrite: str | None = None
    suppress_skipped: bool = False
    variables: dict[str, VariableValue] = field(default_factory=dict)


@dataclass
class WorkspaceConfig:
    """Configuration class for the workspace"""

    __file__ = "config.yml"
    __version__ = 1

    run_configs: list[RunConfig]
    log_level: str | None = "INFO"

    # whether to use serverless clusters for the jobs, only used during workspace installation
    serverless_clusters: bool = True
    upload_dependencies: bool = (
        False  # whether to upload dependencies to the workspace during installation to enable DQX in restricted (no-internet) environments
    )
    extra_params: ExtraParams | None = None  # extra parameters to pass to the jobs, e.g. result_column_names

    # cluster configuration for the jobs (applicable for non-serverless clusters only)
    profiler_override_clusters: dict[str, str] | None = field(default_factory=dict)
    quality_checker_override_clusters: dict[str, str] | None = field(default_factory=dict)
    e2e_override_clusters: dict[str, str] | None = field(default_factory=dict)
    anomaly_override_clusters: dict[str, str] | None = field(default_factory=dict)

    # extra spark config for jobs (applicable for non-serverless clusters only)
    profiler_spark_conf: dict[str, str] | None = field(default_factory=dict)
    quality_checker_spark_conf: dict[str, str] | None = field(default_factory=dict)
    e2e_spark_conf: dict[str, str] | None = field(default_factory=dict)
    anomaly_spark_conf: dict[str, str] | None = field(default_factory=dict)

    profiler_max_parallelism: int = 4  # max parallelism for profiling multiple tables
    quality_checker_max_parallelism: int = 4  # max parallelism for quality checking multiple tables
    custom_metrics: list[str] | None = None  # custom summary metrics tracked by the observer when applying checks

    llm_config: LLMConfig = field(default_factory=LLMConfig)

    def as_dict(self) -> dict:
        """
        Convert the WorkspaceConfig to a dictionary for serialization.
        This method ensures that all fields, including boolean False values, are properly serialized.
        Used by blueprint's installation when saving the config (Installation.save()).

        Returns:
            A dictionary representation of the WorkspaceConfig.
        """
        return asdict(self)

    def get_run_config(self, run_config_name: str | None = "default") -> RunConfig:
        """Get the run configuration for a given run name, or the default configuration if no run name is provided.

        Args:
            run_config_name: The name of the run configuration to get, e.g. input table or job name (use "default" if not provided).

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
    """Marker base class for storage configuration.

    Args:
        location: The file path or table name where checks are stored.
    """

    location: str


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
        run_config_name: The name of the run configuration to use for checks, e.g. input table or job name (use "default" if not provided).
        mode: The mode for writing checks to a table ('append' or 'overwrite', default 'append').
            - **overwrite**: Replaces all rows for this run_config_name when the fingerprint differs.
              Skips write when the fingerprint already exists.
            - **append**: Adds new rows when the fingerprint differs; multiple versions can coexist.
              Skips write when the fingerprint already exists.
        rule_set_fingerprint: Optional SHA-256 fingerprint of the rule set to load.
            When provided, loads rules matching this specific fingerprint instead of the latest batch.
            When None (default), loads the latest batch.
    """

    location: str
    run_config_name: str = "default"  # to filter checks by run config
    mode: str = "append"
    rule_set_fingerprint: str | None = None  # to filter checks by rule set fingerprint

    def __post_init__(self):
        if not self.location:
            raise InvalidConfigError("The table name ('location' field) must not be empty or None.")


@dataclass
class LakebaseChecksStorageConfig(BaseChecksStorageConfig):
    """
    Configuration class for storing checks in a Lakebase table.

    Args:
        location: Fully qualified name of the Lakebase table to store checks in the format 'database.schema.table'.
        instance_name: Name of the Lakebase instance.
        client_id: ID of the Databricks service principal to use for the Lakebase connection.
        port: The Lakebase port (default is '5432').
        run_config_name: Name of the run configuration to use for checks (default is 'default').
        mode: The mode for writing checks to a table ('append' or 'overwrite', default 'append').
            - **overwrite**: Replaces all rows for this run_config_name when the fingerprint differs.
              Skips write when the fingerprint already exists.
            - **append**: Adds new rows when the fingerprint differs; multiple versions can coexist.
              Skips write when the fingerprint already exists.
        rule_set_fingerprint: Optional SHA-256 fingerprint of the rule set to load.
            When provided, loads rules matching this specific fingerprint instead of the latest batch.
            When None (default), loads the latest batch.
    """

    location: str
    instance_name: str | None = None
    client_id: str | None = None
    port: str = "5432"
    run_config_name: str = "default"
    mode: str = "append"
    rule_set_fingerprint: str | None = None

    def __post_init__(self):
        if not self.location or self.location == "":
            raise InvalidParameterError("Location must not be empty or None.")

        if not self.instance_name or self.instance_name == "":
            raise InvalidParameterError("Instance name must not be empty or None.")

        if len(self.location.split(".")) != 3:
            raise InvalidConfigError(
                f"Invalid Lakebase table name '{self.location}'. Must be in the format 'database.schema.table'."
            )

        if self.mode not in ("append", "overwrite"):
            raise InvalidConfigError(f"Invalid mode '{self.mode}'. Must be 'append' or 'overwrite'.")

    def _split_location(self) -> tuple[str, ...]:
        """Splits 'database.schema.table' into components."""
        if not self.location:
            raise InvalidConfigError("location must be set before accessing database components.")
        return tuple(self.location.split("."))

    @cached_property
    def database_name(self) -> str:
        return self._split_location()[0]

    @cached_property
    def schema_name(self) -> str:
        return self._split_location()[1]

    @cached_property
    def table_name(self) -> str:
        return self._split_location()[2]


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
            raise InvalidParameterError(
                "The Unity Catalog volume file path ('location' field) must not be empty or None."
            )

        # Expected format: /Volumes/{catalog}/{schema}/{volume}/{path/to/file}
        if not self.location.startswith("/Volumes/"):
            raise InvalidParameterError("The volume path must start with '/Volumes/'.")

        parts = self.location.split("/")
        # After split need at least: ['', 'Volumes', 'catalog', 'schema', 'volume', optional 'dir', 'file']
        if len(parts) < 3 or not parts[2]:
            raise InvalidParameterError("Invalid path: Path is missing a catalog name")
        if len(parts) < 4 or not parts[3]:
            raise InvalidParameterError("Invalid path: Path is missing a schema name")
        if len(parts) < 5 or not parts[4]:
            raise InvalidParameterError("Invalid path: Path is missing a volume name")
        if len(parts) < 6 or not parts[-1].lower().endswith(SerializerFactory.get_supported_extensions()):
            raise InvalidParameterError("Invalid path: Path must include a file name after the volume")


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
        run_config_name: The name of the run configuration to use for checks, e.g. input table or job name (use "default" if not provided).
        product_name: The product name for retrieving checks from the installation (default is 'dqx').
        assume_user: Whether to assume the user is the owner of the checks (default is True).
        install_folder: The installation folder where DQX is installed.
        DQX will be installed in a default directory if no custom folder is provided:
        * User's home directory: "/Users/<your_user>/.dqx"
        * Global directory if `DQX_FORCE_INSTALL=global`: "/Applications/dqx"
        overwrite_location: Whether to overwrite the location from run config if provided (default is False).
    """

    location: str = "installation"  # retrieved from the installation config
    run_config_name: str = "default"  # to retrieve run config
    product_name: str = "dqx"
    assume_user: bool = True
    install_folder: str | None = None
    overwrite_location: bool = False
