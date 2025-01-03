from functools import cached_property
from pathlib import Path
from pyspark.sql import SparkSession

from databricks.labs.blueprint.installation import Installation
from databricks.sdk import WorkspaceClient, core
from databricks.labs.dqx.contexts.application import GlobalContext
from databricks.labs.dqx.config import WorkspaceConfig, RunConfig
from databricks.labs.dqx.__about__ import __version__
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.runner import ProfilerRunner


class RuntimeContext(GlobalContext):

    @cached_property
    def _config_path(self) -> Path:
        config = self.named_parameters.get("config")
        if not config:
            raise ValueError("config flag is required")
        return Path(config)

    @cached_property
    def config(self) -> WorkspaceConfig:
        """Loads and returns the workspace configuration."""
        return Installation.load_local(WorkspaceConfig, self._config_path)

    @cached_property
    def run_config(self) -> RunConfig:
        """Loads and returns the run configuration."""
        run_config_name = self.named_parameters.get("run_config_name")
        if not run_config_name:
            raise ValueError("Run config flag is required")
        return self.config.get_run_config(run_config_name)

    @cached_property
    def connect_config(self) -> core.Config:
        """
        Returns the connection configuration.

        :return: The core.Config instance.
        :raises AssertionError: If the connect configuration is not provided.
        """
        connect = self.config.connect
        assert connect, "connect is required"
        return connect

    @cached_property
    def workspace_client(self) -> WorkspaceClient:
        """Returns the WorkspaceClient instance."""
        return WorkspaceClient(
            config=self.connect_config, product=self.product_info.product_name(), product_version=__version__
        )

    @cached_property
    def installation(self) -> Installation:
        """Returns the installation instance for the runtime."""
        install_folder = self._config_path.parent.as_posix().removeprefix("/Workspace")
        return Installation(self.workspace_client, self.product_info.product_name(), install_folder=install_folder)

    @cached_property
    def workspace_id(self) -> int:
        """Returns the workspace ID."""
        return self.workspace_client.get_workspace_id()

    @cached_property
    def parent_run_id(self) -> int:
        """Returns the parent run ID."""
        return int(self.named_parameters["parent_run_id"])

    @cached_property
    def profiler(self) -> ProfilerRunner:
        """Returns the ProfilerRunner instance."""
        spark_session = SparkSession.builder.getOrCreate()
        profiler = DQProfiler(self.workspace_client)
        generator = DQGenerator(self.workspace_client)

        return ProfilerRunner(
            self.workspace_client, spark_session, installation=self.installation, profiler=profiler, generator=generator
        )
