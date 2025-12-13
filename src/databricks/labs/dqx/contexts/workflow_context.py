from functools import cached_property
from pathlib import Path
from pyspark.sql import SparkSession

from databricks.labs.blueprint.wheels import ProductInfo
from databricks.labs.blueprint.installation import Installation
from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.checks_storage import is_table_location
from databricks.labs.dqx.contexts.global_context import GlobalContext
from databricks.labs.dqx.config import WorkspaceConfig, RunConfig
from databricks.labs.dqx.__about__ import __version__
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.metrics_observer import DQMetricsObserver
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.profiler_runner import ProfilerRunner
from databricks.labs.dqx.quality_checker.quality_checker_runner import QualityCheckerRunner
from databricks.labs.dqx.telemetry import log_telemetry
from databricks.labs.dqx.errors import InvalidConfigError
from databricks.labs.dqx.utils import safe_strip_file_from_path


class WorkflowContext(GlobalContext):
    """
    WorkflowContext class that provides a context for workflows, including workspace configuration,
    """

    @cached_property
    def config(self) -> WorkspaceConfig:
        """Loads and returns the workspace configuration."""
        return Installation.load_local(WorkspaceConfig, self._config_path)

    @cached_property
    def _config_path(self) -> Path:
        config = self.named_parameters.get("config")
        if not config:
            raise InvalidConfigError("config flag is required")
        return Path(config)

    @cached_property
    def spark(self) -> SparkSession:
        """Returns spark session."""
        return SparkSession.builder.getOrCreate()

    @cached_property
    def run_config_name(self) -> str | None:
        """Returns run configuration name."""
        return self.named_parameters.get("run_config_name")

    @cached_property
    def runnable_for_patterns(self) -> bool:
        """Returns run configuration name."""
        return bool(self.patterns and self.run_config_name)

    @cached_property
    def runnable_for_run_config(self) -> bool:
        """Returns run configuration name."""
        return bool(self.run_config_name)

    @cached_property
    def patterns(self) -> str | None:
        """Returns semicolon delimited list of location patterns to use."""
        return self.named_parameters.get("patterns")

    @cached_property
    def exclude_patterns(self) -> str | None:
        """Returns semicolon delimited list of location patterns to exclude."""
        return self.named_parameters.get("exclude_patterns")

    @cached_property
    def output_table_suffix(self) -> str:
        """Returns suffix to use for output tables."""
        return self.named_parameters.get("output_table_suffix", "_dq_output")

    @cached_property
    def quarantine_table_suffix(self) -> str:
        """Returns suffix to use for quarantine tables."""
        return self.named_parameters.get("quarantine_table_suffix", "_dq_quarantine")

    @cached_property
    def run_config(self) -> RunConfig:
        """Loads and returns the run configuration."""
        run_config_name = self.run_config_name
        if not run_config_name:
            raise InvalidConfigError("Run config flag is required")
        raw_run_config = self.config.get_run_config(run_config_name)
        return self.prepare_run_config(raw_run_config)

    @cached_property
    def product_info(self) -> ProductInfo:
        """Returns the ProductInfo instance for the runtime.
        If `product_name` is provided in `named_parameters`, it overrides the default product name.
        This is useful for testing or when the product name needs to be dynamically set at runtime.
        """
        product_info = super().product_info
        if runtime_product_name := self.named_parameters.get("product_name"):
            setattr(product_info, '_product_name', runtime_product_name)
        return product_info

    @cached_property
    def workspace_client(self) -> WorkspaceClient:
        """Returns the WorkspaceClient instance."""
        return WorkspaceClient(product=self.product_info.product_name(), product_version=__version__)

    @cached_property
    def installation(self) -> Installation:
        """Returns the installation instance for the runtime."""
        install_folder = self._config_path.parent.as_posix().removeprefix("/Workspace")
        return Installation(self.workspace_client, self.product_info.product_name(), install_folder=install_folder)

    @cached_property
    def resolved_patterns(self) -> tuple[list[str], list[str]]:
        """Returns a tuple of patterns and exclude patterns lists."""
        patterns: list[str] = []
        exclude_patterns: list[str] = []

        if self.patterns:
            patterns = [pattern.strip() for pattern in self.patterns.split(';')]

            exclude_patterns = []
            for pattern in patterns:
                # Exclude output and quarantine tables by default to avoid profiling them
                if self.output_table_suffix:
                    exclude_patterns.append(pattern + self.output_table_suffix)
                if self.quarantine_table_suffix:
                    exclude_patterns.append(pattern + self.quarantine_table_suffix)

            if self.exclude_patterns:
                exclude_patterns.extend([pattern.strip() for pattern in self.exclude_patterns.split(';')])

        return patterns, exclude_patterns

    @cached_property
    def profiler(self) -> ProfilerRunner:
        """Returns the ProfilerRunner instance."""
        profiler = DQProfiler(self.workspace_client)
        dq_engine = DQEngine(
            workspace_client=self.workspace_client, spark=self.spark, extra_params=self.config.extra_params
        )
        log_telemetry(self.workspace_client, "workflow", "profiler")
        return ProfilerRunner(
            self.workspace_client,
            self.spark,
            dq_engine,
            installation=self.installation,
            profiler=profiler,
        )

    @cached_property
    def quality_checker(self) -> QualityCheckerRunner:
        """Returns the QualityCheckerRunner instance."""
        observer = DQMetricsObserver(custom_metrics=self.config.custom_metrics)
        dq_engine = DQEngine(
            workspace_client=self.workspace_client,
            spark=self.spark,
            extra_params=self.config.extra_params,
            observer=observer,
        )
        log_telemetry(self.workspace_client, "workflow", "quality_checker")
        return QualityCheckerRunner(self.spark, dq_engine)

    def prepare_run_config(self, run_config: RunConfig) -> RunConfig:
        """
        Apply common path prefixing to a run configuration in-place and return it.
        Ensures custom check function paths and checks location are absolute in the Databricks Workspace.

        Args:
            run_config: The run configuration to prepare.

        Returns:
            The prepared run configuration.
        """
        if not run_config.input_config:
            raise InvalidConfigError("No input data source configured during installation")

        run_config.custom_check_functions = self._get_resolved_custom_check_function(run_config.custom_check_functions)
        run_config.checks_location = self._get_resolved_checks_location(run_config.checks_location)

        return run_config

    def _get_resolved_checks_location(self, checks_location: str) -> str:
        """
        Prefixes the checks location with the installation folder if it is not an absolute path.

        Args:
            checks_location: The original checks location.

            The resolved checks location.
        Returns:
        """
        if is_table_location(checks_location):
            return checks_location

        if self.runnable_for_patterns:  # don't need file name for pattern based execution
            checks_location = safe_strip_file_from_path(checks_location)

        if checks_location.startswith("/") or checks_location.startswith("/Volumes/"):
            return checks_location

        return f"{self.installation.install_folder()}/{checks_location}"

    def _get_resolved_custom_check_function(self, custom_check_functions: dict[str, str]):
        """
        Prefixes custom check function paths with the installation folder if they are not absolute paths.

        Args:
            custom_check_functions: A mapping where each key is the name of a function (e.g., "my_func")
                and each value is the file path to the Python module that defines it. The path can be absolute
                or relative to the installation folder, and may refer to a local filesystem location, a
                Databricks workspace path (e.g. /Workspace/my_repo/my_module.py), or a Unity Catalog volume
                (e.g. /Volumes/catalog/schema/volume/my_module.py).

        Returns:
            A dictionary with function names as keys and prefixed paths as values.
        """
        if custom_check_functions:
            return {
                func_name: path if path.startswith("/") else f"/Workspace{self.installation.install_folder()}/{path}"
                for func_name, path in custom_check_functions.items()
            }
        return custom_check_functions
