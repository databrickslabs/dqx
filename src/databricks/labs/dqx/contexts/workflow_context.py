from functools import cached_property
from pathlib import Path
from pyspark.sql import SparkSession

from databricks.labs.blueprint.wheels import ProductInfo
from databricks.labs.blueprint.installation import Installation
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.contexts.global_context import GlobalContext
from databricks.labs.dqx.config import WorkspaceConfig, RunConfig
from databricks.labs.dqx.__about__ import __version__
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.profiler_runner import ProfilerRunner
from databricks.labs.dqx.quality_checker.quality_checker_runner import QualityCheckerRunner


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
            raise ValueError("config flag is required")
        return Path(config)

    @cached_property
    def spark(self) -> SparkSession:
        """Returns spark session."""
        return SparkSession.builder.getOrCreate()

    @cached_property
    def run_config(self) -> RunConfig:
        """Loads and returns the run configuration."""
        run_config_name = self.named_parameters.get("run_config_name")
        if not run_config_name:
            raise ValueError("Run config flag is required")
        return self.config.get_run_config(run_config_name)

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
    def profiler(self) -> ProfilerRunner:
        """Returns the ProfilerRunner instance."""
        profiler = DQProfiler(self.workspace_client)
        generator = DQGenerator(self.workspace_client)
        dq_engine = DQEngine(
            workspace_client=self.workspace_client, spark=self.spark, extra_params=self.config.extra_params
        )

        return ProfilerRunner(
            self.workspace_client,
            self.spark,
            dq_engine,
            installation=self.installation,
            profiler=profiler,
            generator=generator,
        )

    @cached_property
    def quality_checker(self) -> QualityCheckerRunner:
        """Returns the QualityCheckerRunner instance."""
        dq_engine = DQEngine(
            workspace_client=self.workspace_client, spark=self.spark, extra_params=self.config.extra_params
        )
        return QualityCheckerRunner(self.spark, dq_engine)
