import json
import logging
import os
from io import StringIO
from pathlib import Path
from abc import ABC, abstractmethod
from typing import Generic, TypeVar

import yaml
from pyspark.sql import SparkSession
from databricks.sdk.errors import NotFound
from databricks.sdk.service.workspace import ImportFormat

from databricks.labs.dqx.config import (
    TableChecksStorageConfig,
    FileChecksStorageConfig,
    WorkspaceFileChecksStorageConfig,
    InstallationChecksStorageConfig,
    BaseChecksStorageConfig,
)
from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.checks_serializer import (
    serialize_checks_from_dataframe,
    deserialize_checks_to_dataframe,
    serialize_checks_to_bytes,
    get_file_deserializer,
)
from databricks.labs.dqx.config_loader import RunConfigLoader


logger = logging.getLogger(__name__)
T = TypeVar("T", bound=BaseChecksStorageConfig)


class ChecksStorageHandler(ABC, Generic[T]):
    """
    Abstract base class for handling storage of quality rules (checks).
    """

    @abstractmethod
    def load(self, config: T) -> list[dict]:
        """
        Load quality rules from the source.
        The returned checks can be used as input for `apply_checks_by_metadata` or
        `apply_checks_by_metadata_and_split` functions.

        :param config: configuration for loading checks, including the table location and run configuration name.
        :return: list of dq rules or raise an error if checks file is missing or is invalid.
        """

    @abstractmethod
    def save(self, checks: list[dict], config: T) -> None:
        """Save quality rules to the target."""


class TableChecksStorageHandler(ChecksStorageHandler[TableChecksStorageConfig]):
    """
    Handler for storing quality rules (checks) in a Delta table in the workspace.
    """

    def __init__(self, ws: WorkspaceClient, spark: SparkSession):
        self.ws = ws
        self.spark = spark

    def load(self, config: TableChecksStorageConfig) -> list[dict]:
        """
        Load checks (dq rules) from a Delta table in the workspace.

        :param config: configuration for loading checks, including the table location and run configuration name.
        :return: list of dq rules or raise an error if checks table is missing or is invalid.
        """
        logger.info(f"Loading quality rules (checks) from table '{config.location}'")
        if not self.ws.tables.exists(config.location).table_exists:
            raise NotFound(f"Table {config.location} does not exist in the workspace")
        rules_df = self.spark.read.table(config.location)
        return serialize_checks_from_dataframe(rules_df, run_config_name=config.run_config_name) or []

    def save(self, checks: list[dict], config: TableChecksStorageConfig) -> None:
        """
        Save checks to a Delta table in the workspace.

        :param checks: list of dq rules to save
        :param config: configuration for saving checks, including the table location and run configuration name.
        :raises ValueError: if the table name is not provided
        """
        logger.info(f"Saving quality rules (checks) to table '{config.location}'")
        rules_df = deserialize_checks_to_dataframe(self.spark, checks, run_config_name=config.run_config_name)
        rules_df.write.option("replaceWhere", f"run_config_name = '{config.run_config_name}'").saveAsTable(
            config.location, mode=config.mode
        )


class WorkspaceFileChecksStorageHandler(ChecksStorageHandler[WorkspaceFileChecksStorageConfig]):
    """
    Handler for storing quality rules (checks) in a file (json or yaml) in the workspace.
    """

    def __init__(self, ws: WorkspaceClient):
        self.ws = ws

    def load(self, config: WorkspaceFileChecksStorageConfig) -> list[dict]:
        """Load checks (dq rules) from a file (json or yaml) in the workspace.
        This does not require installation of DQX in the workspace.

        :param config: configuration for loading checks, including the file location and storage type.
        :return: list of dq rules or raise an error if checks file is missing or is invalid.
        """
        file_path = config.location
        logger.info(f"Loading quality rules (checks) from '{file_path}' in the workspace.")

        deserializer = get_file_deserializer(file_path)

        try:
            file_bytes = self.ws.workspace.download(file_path).read()
            file_content = file_bytes.decode("utf-8")
        except NotFound as e:
            raise NotFound(f"Checks file {file_path} missing: {e}") from e

        try:
            return deserializer(StringIO(file_content)) or []
        except (yaml.YAMLError, json.JSONDecodeError) as e:
            raise ValueError(f"Invalid checks in file: {file_path}: {e}") from e

    def save(self, checks: list[dict], config: WorkspaceFileChecksStorageConfig) -> None:
        """Save checks (dq rules) to yaml file in the workspace.
        This does not require installation of DQX in the workspace.

        :param checks: list of dq rules to save
        :param config: configuration for saving checks, including the file location and storage type.
        """
        logger.info(f"Saving quality rules (checks) to '{config.location}' in the workspace.")
        file_path = Path(config.location)
        workspace_dir = str(file_path.parent)
        self.ws.workspace.mkdirs(workspace_dir)

        content = serialize_checks_to_bytes(checks, file_path)
        self.ws.workspace.upload(config.location, content, format=ImportFormat.AUTO, overwrite=True)


class FileChecksStorageHandler(ChecksStorageHandler[FileChecksStorageConfig]):
    """
    Handler for storing quality rules (checks) in a file (json or yaml) in the local filesystem.
    """

    def load(self, config: FileChecksStorageConfig) -> list[dict]:
        """
        Load checks (dq rules) from a file (json or yaml) in the local filesystem.

        :param config: configuration for loading checks, including the file location.
        :return: list of dq rules or raise an error if checks file is missing or is invalid.
        :raises ValueError: if the file path is not provided
        :raises FileNotFoundError: if the file path does not exist
        """
        file_path = config.location
        logger.info(f"Loading quality rules (checks) from '{file_path}'.")

        deserializer = get_file_deserializer(file_path)

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return deserializer(f) or []
        except FileNotFoundError as e:
            raise FileNotFoundError(f"Checks file {file_path} missing: {e}") from e
        except (yaml.YAMLError, json.JSONDecodeError) as e:
            raise ValueError(f"Invalid checks in file: {file_path}: {e}") from e

    def save(self, checks: list[dict], config: FileChecksStorageConfig) -> None:
        """
        Save checks (dq rules) to a file (json or yaml) in the local filesystem.

        :param checks: list of dq rules to save
        :param config: configuration for saving checks, including the file location.
        :raises ValueError: if the file path is not provided
        :raises FileNotFoundError: if the file path does not exist
        """
        logger.info(f"Saving quality rules (checks) to '{config.location}'.")
        file_path = Path(config.location)
        os.makedirs(file_path.parent, exist_ok=True)

        try:
            content = serialize_checks_to_bytes(checks, file_path)
            with open(file_path, "wb") as file:
                file.write(content)
        except FileNotFoundError:
            msg = f"Checks file {config.location} missing"
            raise FileNotFoundError(msg) from None


class InstallationChecksStorageHandler(ChecksStorageHandler[InstallationChecksStorageConfig]):
    """
    Handler for storing quality rules (checks) defined in the installation configuration.
    """

    def __init__(self, ws: WorkspaceClient, spark: SparkSession, run_config_loader: RunConfigLoader | None = None):
        self._run_config_loader = run_config_loader or RunConfigLoader(ws)
        self.workspace_file_handler = WorkspaceFileChecksStorageHandler(ws)
        self.table_handler = TableChecksStorageHandler(ws, spark)

    def load(self, config: InstallationChecksStorageConfig) -> list[dict]:
        """
        Load checks (dq rules) from the installation configuration.

        :param config: configuration for loading checks, including the run configuration name and method.
        :return: list of dq rules or raise an error if checks file is missing or is invalid.
        :raises NotFound: if the checks file or table is not found in the installation.
        """
        run_config = self._run_config_loader.load_run_config(
            config.run_config_name, config.assume_user, config.product_name
        )
        installation = self._run_config_loader.get_installation(config.assume_user, config.product_name)

        if run_config.checks_table:
            config.location = run_config.checks_table
            return self.table_handler.load(config)

        workspace_path = f"{installation.install_folder()}/{run_config.checks_file}"
        config.location = workspace_path

        return self.workspace_file_handler.load(config)

    def save(self, checks: list[dict], config: InstallationChecksStorageConfig) -> None:
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
        config.location = workspace_path

        return self.workspace_file_handler.save(checks, config)


class BaseChecksStorageHandlerFactory(ABC):
    """
    Abstract base class for factories that create storage handlers for checks.
    """

    @abstractmethod
    def create(self, config: BaseChecksStorageConfig) -> ChecksStorageHandler:
        """
        Abstract method to create a handler based on the type of the provided configuration object.

        :param config: Configuration object for loading or saving checks.
        :return: An instance of the corresponding BaseChecksStorageHandler.
        """


class ChecksStorageHandlerFactory(BaseChecksStorageHandlerFactory):
    def __init__(self, workspace_client: WorkspaceClient, spark: SparkSession):
        self.workspace_client = workspace_client
        self.spark = spark

    def create(self, config: BaseChecksStorageConfig) -> ChecksStorageHandler:
        """
        Factory method to create a handler based on the type of the provided configuration object.

        :param config: Configuration object for loading or saving checks.
        :return: An instance of the corresponding BaseChecksStorageHandler.
        :raises ValueError: If the configuration type is unsupported.
        """
        if isinstance(config, FileChecksStorageConfig):
            return FileChecksStorageHandler()
        if isinstance(config, InstallationChecksStorageConfig):
            return InstallationChecksStorageHandler(self.workspace_client, self.spark)
        if isinstance(config, WorkspaceFileChecksStorageConfig):
            return WorkspaceFileChecksStorageHandler(self.workspace_client)
        if isinstance(config, TableChecksStorageConfig):
            return TableChecksStorageHandler(self.workspace_client, self.spark)

        raise ValueError(f"Unsupported storage config type: {type(config).__name__}")
