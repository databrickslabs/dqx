import logging
import os
from pathlib import Path
from abc import ABC, abstractmethod
import yaml

from pyspark.sql import SparkSession
from databricks.sdk.errors import NotFound
from databricks.sdk.service.workspace import ImportFormat
from databricks.labs.blueprint.installation import Installation
from databricks.labs.dqx.config import ChecksStorageConfig
from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.builder import build_checks_from_dataframe, build_dataframe_from_checks
from databricks.labs.dqx.config_loader import RunConfigLoader
from databricks.labs.dqx.utils import deserialize_dicts


logger = logging.getLogger(__name__)


class BaseChecksStorageHandler(ABC):
    """
    Abstract base class for handling storage of quality rules (checks).
    """

    @abstractmethod
    def load(self, config: ChecksStorageConfig) -> list[dict]:
        """
        Load quality rules from the source.
        The returned checks can be used as input for `apply_checks_by_metadata` or
        `apply_checks_by_metadata_and_split` functions.

        :param config: configuration for loading checks, including the table location and run configuration name.
        :return: list of dq rules or raise an error if checks file is missing or is invalid.
        """

    @abstractmethod
    def save(self, checks: list[dict], config: ChecksStorageConfig) -> None:
        """Save quality rules to the target."""


class ChecksStorageHandler(BaseChecksStorageHandler):
    """
    Abstract class for handling storage of quality rules (checks) that require workspace client and spark session.
    """

    def __init__(self, ws: WorkspaceClient, spark: SparkSession | None = None):
        self.ws = ws
        self.spark = SparkSession.builder.getOrCreate() if spark is None else spark

    @abstractmethod
    def load(self, config: ChecksStorageConfig) -> list[dict]:
        """
        Load quality rules from the source.
        The returned checks can be used as input for `apply_checks_by_metadata` or
        `apply_checks_by_metadata_and_split` functions.

        :param config: configuration for loading checks, including the table location and run configuration name.
        :return: list of dq rules or raise an error if checks file is missing or is invalid.
        """

    @abstractmethod
    def save(self, checks: list[dict], config: ChecksStorageConfig) -> None:
        """Save quality rules to the target."""


class TableChecksStorageHandler(ChecksStorageHandler):
    """
    Handler for storing quality rules (checks) in a Delta table in the workspace.
    """

    def load(self, config: ChecksStorageConfig) -> list[dict]:
        """
        Load checks (dq rules) from a Delta table in the workspace.

        :param config: configuration for loading checks, including the table location and run configuration name.
        :return: list of dq rules or raise an error if checks table is missing or is invalid.
        """
        logger.info(f"Loading quality rules (checks) from table {config.location}")
        if not self.ws.tables.exists(config.location).table_exists:
            raise NotFound(f"Table {config.location} does not exist in the workspace")
        return self._load_checks_from_table(config.location, config.run_config_name)

    def save(self, checks: list[dict], config: ChecksStorageConfig) -> None:
        """
        Save checks to a Delta table in the workspace.

        :param checks: list of dq rules to save
        :param config: configuration for saving checks, including the table location and run configuration name.
        :raises ValueError: if the table name is not provided
        """
        logger.info(f"Saving quality rules (checks) to table {config.location}")
        self._save_checks_in_table(checks, config.location, config.run_config_name, config.mode)

    def _load_checks_from_table(self, table_name: str, run_config_name: str) -> list[dict]:
        rules_df = self.spark.read.table(table_name)
        return build_checks_from_dataframe(rules_df, run_config_name=run_config_name)

    def _save_checks_in_table(self, checks: list[dict], table_name: str, run_config_name: str, mode: str):
        rules_df = build_dataframe_from_checks(self.spark, checks, run_config_name=run_config_name)
        rules_df.write.option("replaceWhere", f"run_config_name = '{run_config_name}'").saveAsTable(
            table_name, mode=mode
        )


class WorkspaceFileChecksStorageHandler(ChecksStorageHandler):
    """
    Handler for storing quality rules (checks) in a file (json or yaml) in the workspace.
    """

    def load(self, config: ChecksStorageConfig) -> list[dict]:
        """Load checks (dq rules) from a file (json or yaml) in the workspace.
        This does not require installation of DQX in the workspace.

        :param config: configuration for loading checks, including the file location and storage type.
        :return: list of dq rules or raise an error if checks file is missing or is invalid.
        """
        workspace_dir = os.path.dirname(config.location)
        filename = os.path.basename(config.location)
        installation = Installation(self.ws, config.product_name, install_folder=workspace_dir)

        logger.info(f"Loading quality rules (checks) from {config.location} in the workspace.")
        parsed_checks = self._load_checks_from_file(installation, filename)
        if not parsed_checks:
            raise ValueError(f"Invalid or no checks in workspace file: {config.location}")
        return parsed_checks

    def save(self, checks: list[dict], config: ChecksStorageConfig) -> None:
        """Save checks (dq rules) to yaml file in the workspace.
        This does not require installation of DQX in the workspace.

        :param checks: list of dq rules to save
        :param config: configuration for saving checks, including the file location and storage type.
        """
        workspace_dir = os.path.dirname(config.location)

        logger.info(f"Saving quality rules (checks) to {config.location} in the workspace.")
        self.ws.workspace.mkdirs(workspace_dir)
        self.ws.workspace.upload(
            config.location, yaml.safe_dump(checks).encode('utf-8'), format=ImportFormat.AUTO, overwrite=True
        )

    @staticmethod
    def _load_checks_from_file(installation: Installation, filename: str) -> list[dict]:
        try:
            checks = installation.load(list[dict[str, str]], filename=filename)
            return deserialize_dicts(checks)
        except NotFound:
            msg = f"Checks file {filename} missing"
            raise NotFound(msg) from None


class FileChecksStorageHandler(BaseChecksStorageHandler):
    """
    Handler for storing quality rules (checks) in a file (json or yaml) in the local filesystem.
    """

    def load(self, config: ChecksStorageConfig) -> list[dict]:
        """
        Load checks (dq rules) from a file (json or yaml) in the local filesystem.

        :param config: configuration for loading checks, including the file location.
        :return: list of dq rules or raise an error if checks file is missing or is invalid.
        :raises ValueError: if the file path is not provided
        """
        parsed_checks = self._load_checks_from_local_file(config.location)
        if not parsed_checks:
            raise ValueError(f"Invalid or no checks in file: {config.location}")
        return parsed_checks

    def save(self, checks: list[dict], config: ChecksStorageConfig) -> None:
        """
        Save checks (dq rules) to yaml file in the local filesystem.

        :param checks: list of dq rules to save
        :param config: configuration for saving checks, including the file location.
        :raises ValueError: if the file path is not provided
        :raises FileNotFoundError: if the file path does not exist
        """
        if not config.location:
            raise ValueError("filepath must be provided")

        try:
            with open(config.location, 'w', encoding="utf-8") as file:
                yaml.safe_dump(checks, file)
        except FileNotFoundError:
            msg = f"Checks file {config.location} missing"
            raise FileNotFoundError(msg) from None

    @staticmethod
    def _load_checks_from_local_file(filepath: str) -> list[dict]:
        if not filepath:
            raise ValueError("filepath must be provided")

        try:
            checks = Installation.load_local(list[dict[str, str]], Path(filepath))
            return deserialize_dicts(checks)
        except FileNotFoundError:
            msg = f"Checks file {filepath} missing"
            raise FileNotFoundError(msg) from None


class InstallationChecksStorageHandler(ChecksStorageHandler):
    """
    Handler for storing quality rules (checks) defined in the installation configuration.
    """

    def __init__(
        self, ws: WorkspaceClient, spark: SparkSession | None = None, run_config_loader: RunConfigLoader | None = None
    ):
        super().__init__(ws, spark)
        self._run_config_loader = run_config_loader or RunConfigLoader(self.ws)
        self.workspace_file_handler = WorkspaceFileChecksStorageHandler(ws, spark)
        self.table_handler = TableChecksStorageHandler(ws, spark)

    def load(self, config: ChecksStorageConfig) -> list[dict]:
        """
        Load checks (dq rules) from the installation configuration.

        :param config: configuration for loading checks, including the run configuration name and method.
        :return: list of dq rules or raise an error if checks file is missing or is invalid.
        """
        run_config = self._run_config_loader.load_run_config(
            config.run_config_name, config.assume_user, config.product_name
        )
        installation = self._run_config_loader.get_installation(config.assume_user, config.product_name)

        if run_config.checks_table:
            config.location = run_config.checks_table
            return self.table_handler.load(config)

        workspace_path = f"{installation.install_folder()}/{run_config.checks_file}"
        logger.info(f"Loading quality rules (checks) from {workspace_path} in the workspace.")
        config.location = workspace_path

        return self.workspace_file_handler.load(config)

    def save(self, checks: list[dict], config: ChecksStorageConfig) -> None:
        """
        Save checks (dq rules) to yaml file or table in the installation folder.
        This will overwrite existing checks file or table.

        :param checks: list of dq rules to save
        :param config: configuration for saving checks, including the run configuration name, method, and table location.
        """
        run_config = self._run_config_loader.load_run_config(
            config.run_config_name, config.assume_user, config.product_name
        )
        installation = self._run_config_loader.get_installation(config.assume_user, config.product_name)

        if run_config.checks_table:
            config.location = run_config.checks_table
            return self.table_handler.save(checks, config)

        workspace_path = f"{installation.install_folder()}/{run_config.checks_file}"
        logger.info(f"Loading quality rules (checks) from {workspace_path} in the workspace.")
        config.location = workspace_path

        return self.workspace_file_handler.save(checks, config)
