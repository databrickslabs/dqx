import json
import logging
import os
from io import StringIO, BytesIO
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
    VolumeFileChecksStorageConfig,
)
from databricks.labs.dqx.errors import InvalidCheckError, InvalidConfigError, CheckDownloadError
from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.checks_serializer import (
    serialize_checks_from_dataframe,
    deserialize_checks_to_dataframe,
    serialize_checks_to_bytes,
    get_file_deserializer,
    FILE_SERIALIZERS,
)
from databricks.labs.dqx.config_loader import RunConfigLoader
from databricks.labs.dqx.io import TABLE_PATTERN
from databricks.labs.dqx.telemetry import telemetry_logger

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
        The returned checks can be used as input for *apply_checks_by_metadata* or
        *apply_checks_by_metadata_and_split* functions.

        Args:
            config: configuration for loading checks, including the table location and run configuration name.

        Returns:
            list of dq rules or raise an error if checks file is missing or is invalid.
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

    @telemetry_logger("load_checks", "table")
    def load(self, config: TableChecksStorageConfig) -> list[dict]:
        """
        Load checks (dq rules) from a Delta table in the workspace.

        Args:
            config: configuration for loading checks, including the table location and run configuration name.

        Returns:
            list of dq rules or raise an error if checks table is missing or is invalid.

        Raises:
            NotFound: if the table does not exist in the workspace
        """
        logger.info(f"Loading quality rules (checks) from table '{config.location}'")
        if not self.ws.tables.exists(config.location).table_exists:
            raise NotFound(f"Checks table {config.location} does not exist in the workspace")
        rules_df = self.spark.read.table(config.location)
        return serialize_checks_from_dataframe(rules_df, run_config_name=config.run_config_name) or []

    @telemetry_logger("save_checks", "table")
    def save(self, checks: list[dict], config: TableChecksStorageConfig) -> None:
        """
        Save checks to a Delta table in the workspace.

        Args:
            checks: list of dq rules to save
            config: configuration for saving checks, including the table location and run configuration name.

        Raises:
            InvalidCheckError: If any check is invalid or unsupported.
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

    @telemetry_logger("load_checks", "workspace_file")
    def load(self, config: WorkspaceFileChecksStorageConfig) -> list[dict]:
        """Load checks (dq rules) from a file (json or yaml) in the workspace.
        This does not require installation of DQX in the workspace.

        Args:
            config: configuration for loading checks, including the file location and storage type.

        Returns:
            list of dq rules or raise an error if checks file is missing or is invalid.

        Raises:
            NotFound: if the checks file is not found in the workspace.
            InvalidCheckError: if the checks file cannot be parsed.
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
            raise InvalidCheckError(f"Invalid checks in file: {file_path}: {e}") from e

    @telemetry_logger("save_checks", "workspace_file")
    def save(self, checks: list[dict], config: WorkspaceFileChecksStorageConfig) -> None:
        """Save checks (dq rules) to yaml file in the workspace.
        This does not require installation of DQX in the workspace.

        Args:
            checks: list of dq rules to save
            config: configuration for saving checks, including the file location and storage type.
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

        Args:
            config: configuration for loading checks, including the file location.

        Returns:
            list of dq rules or raise an error if checks file is missing or is invalid.

        Raises:
            FileNotFoundError: if the file path does not exist
            InvalidCheckError: if the checks file cannot be parsed
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
            raise InvalidCheckError(f"Invalid checks in file: {file_path}: {e}") from e

    def save(self, checks: list[dict], config: FileChecksStorageConfig) -> None:
        """
        Save checks (dq rules) to a file (json or yaml) in the local filesystem.

        Args:
            checks: list of dq rules to save
            config: configuration for saving checks, including the file location.

        Raises:
            FileNotFoundError: if the file path does not exist
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
        self.ws = ws
        self._run_config_loader = run_config_loader or RunConfigLoader(ws)
        self.workspace_file_handler = WorkspaceFileChecksStorageHandler(ws)
        self.table_handler = TableChecksStorageHandler(ws, spark)
        self.volume_handler = VolumeFileChecksStorageHandler(ws)

    @telemetry_logger("load_checks", "installation")
    def load(self, config: InstallationChecksStorageConfig) -> list[dict]:
        """
        Load checks (dq rules) from the installation configuration.

        Args:
            config: configuration for loading checks, including the run configuration name and method.

        Returns:
            list of dq rules or raise an error if checks file is missing or is invalid.

        Raises:
            NotFound: if the checks file or table is not found in the installation.
            InvalidCheckError: if the checks file cannot be parsed.
        """
        handler, config = self._get_storage_handler_and_config(config)
        return handler.load(config)

    @telemetry_logger("save_checks", "installation")
    def save(self, checks: list[dict], config: InstallationChecksStorageConfig) -> None:
        """
        Save checks (dq rules) to yaml file or table in the installation folder.
        This will overwrite existing checks file or table.

        Args:
            checks: list of dq rules to save
            config: configuration for saving checks, including the run configuration name, method, and table location.
        """
        handler, config = self._get_storage_handler_and_config(config)
        return handler.save(checks, config)

    def _get_storage_handler_and_config(
        self, config: InstallationChecksStorageConfig
    ) -> tuple[ChecksStorageHandler, InstallationChecksStorageConfig]:
        if config.overwrite_location:
            checks_location = config.location
        else:
            run_config = self._run_config_loader.load_run_config(
                run_config_name=config.run_config_name,
                assume_user=config.assume_user,
                product_name=config.product_name,
                install_folder=config.install_folder,
            )
            checks_location = run_config.checks_location

        installation = self._run_config_loader.get_installation(
            config.assume_user, config.product_name, config.install_folder
        )

        config.location = checks_location

        if is_table_location(config.location):
            return self.table_handler, config

        if config.location.startswith("/Volumes/"):
            return self.volume_handler, config

        if not config.location.startswith("/"):
            # if absolute path is not provided, the location should be set relative to the installation folder
            config.location = f"{installation.install_folder()}/{config.location}"

        return self.workspace_file_handler, config


class VolumeFileChecksStorageHandler(ChecksStorageHandler[VolumeFileChecksStorageConfig]):
    """
    Handler for storing quality rules (checks) in a file (json or yaml) in a Unity Catalog volume.
    """

    def __init__(self, ws: WorkspaceClient):
        self.ws = ws

    @telemetry_logger("load_checks", "volume")
    def load(self, config: VolumeFileChecksStorageConfig) -> list[dict]:
        """Load checks (dq rules) from a file (json or yaml) in a Unity Catalog volume.

        Args:
            config: configuration for loading checks, including the file location and storage type.

        Returns:
            list of dq rules or raise an error if checks file is missing or is invalid.

        Raises:
            NotFound: if the checks file is not found in the workspace.
            InvalidCheckError: if the checks file cannot be parsed.
            CheckDownloadError: if there is an error downloading the file from the volume.
        """
        file_path = config.location
        logger.info(f"Loading quality rules (checks) from '{file_path}' in a volume.")

        deserializer = get_file_deserializer(file_path)

        try:
            file_download = self.ws.files.download(file_path)
            if not file_download.contents:
                raise CheckDownloadError(f"File download failed at Unity Catalog volume path: {file_path}")
            file_bytes: bytes = file_download.contents.read()
            if not file_bytes:
                raise NotFound(f"No contents at Unity Catalog volume path: {file_path}")
            file_content: str = file_bytes.decode("utf-8")

        except NotFound as e:
            raise NotFound(f"Checks file {file_path} missing: {e}") from e

        try:
            return deserializer(StringIO(file_content)) or []
        except (yaml.YAMLError, json.JSONDecodeError) as e:
            raise InvalidCheckError(f"Invalid checks in file: {file_path}: {e}") from e

    @telemetry_logger("save_checks", "volume")
    def save(self, checks: list[dict], config: VolumeFileChecksStorageConfig) -> None:
        """Save checks (dq rules) to yaml file in a Unity Catalog volume.
        This does not require installation of DQX in a Unity Catalog volume.

        Args:
            checks: list of dq rules to save
            config: configuration for saving checks, including the file location and storage type.
        """
        logger.info(f"Saving quality rules (checks) to '{config.location}' in a Unity Catalog volume.")
        file_path = Path(config.location)
        volume_dir = str(file_path.parent)
        self.ws.files.create_directory(volume_dir)

        content = serialize_checks_to_bytes(checks, file_path)
        binary_data = BytesIO(content)
        self.ws.files.upload(config.location, binary_data, overwrite=True)


class BaseChecksStorageHandlerFactory(ABC):
    """
    Abstract base class for factories that create storage handlers for checks.
    """

    @abstractmethod
    def create(self, config: BaseChecksStorageConfig) -> ChecksStorageHandler:
        """
        Abstract method to create a handler based on the type of the provided configuration object.

        Args:
            config: Configuration object for loading or saving checks.

        Returns:
            An instance of the corresponding BaseChecksStorageHandler.
        """

    @abstractmethod
    def create_for_location(
        self, location: str, run_config_name: str = "default"
    ) -> tuple[ChecksStorageHandler, BaseChecksStorageConfig]:
        """
        Abstract method to create a handler and config based on checks location.

        Args:
            location: location of the checks (file path, table name, volume, etc.)
            run_config_name: the name of the run configuration to use for checks (default is 'default').

        Returns:
            An instance of the corresponding BaseChecksStorageHandler.
        """


class ChecksStorageHandlerFactory(BaseChecksStorageHandlerFactory):
    def __init__(self, workspace_client: WorkspaceClient, spark: SparkSession):
        self.workspace_client = workspace_client
        self.spark = spark

    def create(self, config: BaseChecksStorageConfig) -> ChecksStorageHandler:
        """
        Factory method to create a handler based on the type of the provided configuration object.

        Args:
            config: Configuration object for loading or saving checks.

        Returns:
            An instance of the corresponding BaseChecksStorageHandler.

        Raises:
            InvalidConfigError: If the configuration type is unsupported.
        """
        if isinstance(config, FileChecksStorageConfig):
            return FileChecksStorageHandler()
        if isinstance(config, InstallationChecksStorageConfig):
            return InstallationChecksStorageHandler(self.workspace_client, self.spark)
        if isinstance(config, WorkspaceFileChecksStorageConfig):
            return WorkspaceFileChecksStorageHandler(self.workspace_client)
        if isinstance(config, TableChecksStorageConfig):
            return TableChecksStorageHandler(self.workspace_client, self.spark)
        if isinstance(config, VolumeFileChecksStorageConfig):
            return VolumeFileChecksStorageHandler(self.workspace_client)

        raise InvalidConfigError(f"Unsupported storage config type: {type(config).__name__}")

    def create_for_location(
        self, location: str, run_config_name: str = "default"
    ) -> tuple[ChecksStorageHandler, BaseChecksStorageConfig]:
        if is_table_location(location):
            return (
                TableChecksStorageHandler(self.workspace_client, self.spark),
                TableChecksStorageConfig(location=location, run_config_name=run_config_name),
            )
        if location.startswith("/Volumes/"):
            return (
                VolumeFileChecksStorageHandler(self.workspace_client),
                VolumeFileChecksStorageConfig(location=location),
            )
        if location.startswith("/"):
            return (
                WorkspaceFileChecksStorageHandler(self.workspace_client),
                WorkspaceFileChecksStorageConfig(location=location),
            )

        return FileChecksStorageHandler(), FileChecksStorageConfig(location=location)


def is_table_location(location: str) -> bool:
    """
    True if location points to a Delta table (catalog.schema.table) and is not a file path
    with a known checks serializer extension.

    Args:
        location (str): The checks location to validate.

    Returns:
        bool: True if the location is a valid table name and not a file path, False otherwise.
    """
    return bool(TABLE_PATTERN.match(location)) and not location.lower().endswith(tuple(FILE_SERIALIZERS.keys()))
