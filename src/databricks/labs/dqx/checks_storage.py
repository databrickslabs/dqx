import json
import logging
import uuid
import os
from io import StringIO, BytesIO
from pathlib import Path
from abc import ABC, abstractmethod
from typing import Generic, TypeVar
from psycopg2.pool import ThreadedConnectionPool

import yaml
from pyspark.sql import SparkSession
from databricks.sdk.errors import NotFound
from databricks.sdk.service.workspace import ImportFormat

from databricks.labs.dqx.config import (
    TableChecksStorageConfig,
    LakebaseChecksStorageConfig,
    FileChecksStorageConfig,
    WorkspaceFileChecksStorageConfig,
    InstallationChecksStorageConfig,
    BaseChecksStorageConfig,
    VolumeFileChecksStorageConfig,
)
from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.checks_serializer import (
    serialize_checks_from_dataframe,
    deserialize_checks_to_dataframe,
    serialize_checks_to_bytes,
    get_file_deserializer,
)
from databricks.labs.dqx.config_loader import RunConfigLoader
from databricks.labs.dqx.utils import TABLE_PATTERN
from databricks.labs.dqx.checks_serializer import FILE_SERIALIZERS


logger = logging.getLogger(__name__)
T = TypeVar("T", bound=BaseChecksStorageConfig)
connection_pool: ThreadedConnectionPool | None = None


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

    def load(self, config: TableChecksStorageConfig) -> list[dict]:
        """
        Load checks (dq rules) from a Delta table in the workspace.

        Args:
            config: configuration for loading checks, including the table location and run configuration name.

        Returns:
            list of dq rules or raise an error if checks table is missing or is invalid.
        """
        logger.info(f"Loading quality rules (checks) from table '{config.location}'")
        if not self.ws.tables.exists(config.location).table_exists:
            raise NotFound(f"Table {config.location} does not exist in the workspace")
        rules_df = self.spark.read.table(config.location)
        return serialize_checks_from_dataframe(rules_df, run_config_name=config.run_config_name) or []

    def save(self, checks: list[dict], config: TableChecksStorageConfig) -> None:
        """
        Save checks to a Delta table in the workspace.

        Args:
            checks: list of dq rules to save
            config: configuration for saving checks, including the table location and run configuration name.

        Raises:
            ValueError: if the table name is not provided
        """
        logger.info(f"Saving quality rules (checks) to table '{config.location}'")
        rules_df = deserialize_checks_to_dataframe(self.spark, checks, run_config_name=config.run_config_name)
        rules_df.write.option("replaceWhere", f"run_config_name = '{config.run_config_name}'").saveAsTable(
            config.location, mode=config.mode
        )


class LakebaseChecksStorageHandler(ChecksStorageHandler[LakebaseChecksStorageConfig]):
    """
    Handler for storing quality rules (checks) in a Lakebase table in the workspace.
    """

    def __init__(self, ws: WorkspaceClient, spark: SparkSession):
        self.ws = ws
        self.spark = spark

    def get_connection_pool(self, config: LakebaseChecksStorageConfig) -> ThreadedConnectionPool:
        """
        Get a connection from a threaded connection pool for a Lakebase instance.

        Args:
            config: configuration for loading checks, including the table location, instance name, and run configuration name.

        Returns:
            A connection pool.
        """
        global connection_pool

        if not connection_pool:
            instance = self.ws.database.get_database_instance(config.instance_name)
            cred = self.ws.database.generate_database_credential(
                request_id=str(uuid.uuid4()), instance_names=[config.instance_name]
            )

            user = self.ws.current_user.me().user_name
            password = cred.token

            connection_pool = ThreadedConnectionPool(
                minconn=1,
                maxconn=10,
                user=user,
                password=password,
                host=instance.read_write_dns,
                port="5432",
                database=config.location,
            )

            logger.info(f"Successfully created connection pool for instance {config.instance_name}")

        return connection_pool

    def load(self, config: LakebaseChecksStorageConfig) -> list[dict]:
        """
        Load checks from a Lakebase table.

        Args:
            config: configuration for loading checks, including the table location and run configuration name.

        Returns:
            list of dq rules or raise an error if checks table is missing or is invalid.
        """
        logger.info(f"Loading quality rules (checks) from Lakebase table '{config.location}'")
        connection_pool = self.get_connection_pool(config)
        connection = None
        cursor = None

        try:
            connection = connection_pool.getconn()
            if not connection:
                raise RuntimeError(f"Failed to get connection from pool for instance {config.instance_name}")

            cursor = connection.cursor()

            check_table_query = f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = '{config.location}' 
                    AND table_name = 'checks'
                );
            """
            cursor.execute(check_table_query)
            table_exists = cursor.fetchone()[0]

            if not table_exists:
                logger.warning(f"Table {config.location}.checks does not exist")
                return []

            select_query = f"""
                SELECT name, criticality, "check", filter, run_config_name, user_metadata
                FROM {config.location}.checks
                WHERE run_config_name = %s
            """
            cursor.execute(select_query, (config.run_config_name,))
            rows = cursor.fetchall()

            checks = []
            for row in rows:
                field_name, field_criticality, field_check, field_filter, field_run_config_name, field_user_metadata = (
                    row
                )

                check_dict = {
                    "name": field_name,
                    "criticality": field_criticality,
                    "check": field_check,
                    "filter": field_filter,
                    "run_config_name": field_run_config_name,
                    "user_metadata": field_user_metadata,
                }

                checks.append(check_dict)

            logger.info(f"Successfully loaded checks from {config.location}")
            return checks
        except Exception as e:
            logger.error(f"Failed to load checks from {config.instance_name}: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection_pool.putconn(connection)

    def save(self, checks: list[dict], config: LakebaseChecksStorageConfig):
        """
        Save checks to a Lakebase table.

        Args:
            config: configuration for loading checks, including the table location, instance name, and run configuration name.

        Returns:

        """
        connection_pool = self.get_connection_pool(config)
        connection = None
        cursor = None

        try:
            connection = connection_pool.getconn()
            if not connection:
                raise RuntimeError(f"Failed to get connection from pool for instance {config.instance_name}")

            cursor = connection.cursor()

            create_dataset_query = f"CREATE SCHEMA IF NOT EXISTS {config.location};"
            cursor.execute(create_dataset_query)

            create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {config.location}.checks (
                    name VARCHAR(255), 
                    criticality VARCHAR(50), 
                    "check" JSONB, 
                    filter TEXT, 
                    run_config_name VARCHAR(255), 
                    user_metadata JSONB
                )
            """
            cursor.execute(create_table_query)

            if config.mode == "overwrite":
                delete_query = f"DELETE FROM {config.location}.checks WHERE run_config_name = %s"
                cursor.execute(delete_query, (config.run_config_name,))

            values = []
            placeholders = []

            for check in checks:
                field_name = check.get("name")
                field_criticality = check.get("criticality", "error")
                field_filter = check.get("filter")
                field_run_config_name = check.get("run_config_name", config.run_config_name)
                field_check = check.get("check")
                field_user_metadata = check.get("user_metadata")

                field_check = json.dumps(field_check) if field_check is not None else None
                field_user_metadata = json.dumps(field_user_metadata) if field_user_metadata is not None else None

                placeholders.append("(%s, %s, %s, %s, %s, %s)")
                values.extend(
                    [
                        field_name,
                        field_criticality,
                        field_check,
                        field_filter,
                        field_run_config_name,
                        field_user_metadata,
                    ]
                )

            if values:
                insert_values_query = f"""
                    INSERT INTO {config.location}.checks (name, criticality, "check", filter, run_config_name, user_metadata)
                    VALUES {", ".join(placeholders)}
                """
                cursor.execute(insert_values_query, values)

            connection.commit()
            logger.info(f"Successfully saved checks to {config.location}")
        except Exception as e:
            logger.error(f"Failed to save checks to {config.instance_name}: {e}")
            if connection:
                connection.rollback()
            raise
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection_pool.putconn(connection)


class WorkspaceFileChecksStorageHandler(ChecksStorageHandler[WorkspaceFileChecksStorageConfig]):
    """
    Handler for storing quality rules (checks) in a file (json or yaml) in the workspace.
    """

    def __init__(self, ws: WorkspaceClient):
        self.ws = ws

    def load(self, config: WorkspaceFileChecksStorageConfig) -> list[dict]:
        """Load checks (dq rules) from a file (json or yaml) in the workspace.
        This does not require installation of DQX in the workspace.

        Args:
            config: configuration for loading checks, including the file location and storage type.

        Returns:
            list of dq rules or raise an error if checks file is missing or is invalid.
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
            ValueError: if the file path is not provided
            FileNotFoundError: if the file path does not exist
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

        Args:
            checks: list of dq rules to save
            config: configuration for saving checks, including the file location.

        Raises:
            ValueError: if the file path is not provided
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
        self._run_config_loader = run_config_loader or RunConfigLoader(ws)
        self.workspace_file_handler = WorkspaceFileChecksStorageHandler(ws)
        self.table_handler = TableChecksStorageHandler(ws, spark)
        self.volume_handler = VolumeFileChecksStorageHandler(ws)

    def load(self, config: InstallationChecksStorageConfig) -> list[dict]:
        """
        Load checks (dq rules) from the installation configuration.

        Args:
            config: configuration for loading checks, including the run configuration name and method.

        Returns:
            list of dq rules or raise an error if checks file is missing or is invalid.

        Raises:
            NotFound: if the checks file or table is not found in the installation.
        """
        handler, config = self._get_storage_handler_and_config(config)
        return handler.load(config)

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
        run_config = self._run_config_loader.load_run_config(
            config.run_config_name, config.assume_user, config.product_name
        )
        installation = self._run_config_loader.get_installation(config.assume_user, config.product_name)

        config.location = run_config.checks_location

        if TABLE_PATTERN.match(config.location) and not config.location.lower().endswith(
            tuple(FILE_SERIALIZERS.keys())
        ):
            return self.table_handler, config
        if config.location.startswith("/Volumes/"):
            return self.volume_handler, config

        if not config.location.startswith("/"):
            # if absolute path is not provided, the location should be set relative to the installation folder
            workspace_path = f"{installation.install_folder()}/{run_config.checks_location}"
        else:
            workspace_path = run_config.checks_location

        config.location = workspace_path
        return self.workspace_file_handler, config


class VolumeFileChecksStorageHandler(ChecksStorageHandler[VolumeFileChecksStorageConfig]):
    """
    Handler for storing quality rules (checks) in a file (json or yaml) in a Unity Catalog volume.
    """

    def __init__(self, ws: WorkspaceClient):
        self.ws = ws

    def load(self, config: VolumeFileChecksStorageConfig) -> list[dict]:
        """Load checks (dq rules) from a file (json or yaml) in a Unity Catalog volume.

        Args:
            config: configuration for loading checks, including the file location and storage type.

        Returns:
            list of dq rules or raise an error if checks file is missing or is invalid.
        """
        file_path = config.location
        logger.info(f"Loading quality rules (checks) from '{file_path}' in a volume.")

        deserializer = get_file_deserializer(file_path)

        try:
            file_download = self.ws.files.download(file_path)
            if not file_download.contents:
                raise ValueError(f"File download failed at Unity Catalog volume path: {file_path}")
            file_bytes: bytes = file_download.contents.read()
            if not file_bytes:
                raise NotFound(f"No contents at Unity Catalog volume path: {file_path}")
            file_content: str = file_bytes.decode("utf-8")

        except NotFound as e:
            raise NotFound(f"Checks file {file_path} missing: {e}") from e

        try:
            return deserializer(StringIO(file_content)) or []
        except (yaml.YAMLError, json.JSONDecodeError) as e:
            raise ValueError(f"Invalid checks in file: {file_path}: {e}") from e

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
            ValueError: If the configuration type is unsupported.
        """
        if isinstance(config, FileChecksStorageConfig):
            return FileChecksStorageHandler()
        if isinstance(config, InstallationChecksStorageConfig):
            return InstallationChecksStorageHandler(self.workspace_client, self.spark)
        if isinstance(config, WorkspaceFileChecksStorageConfig):
            return WorkspaceFileChecksStorageHandler(self.workspace_client)
        if isinstance(config, TableChecksStorageConfig):
            return TableChecksStorageHandler(self.workspace_client, self.spark)
        if isinstance(config, LakebaseChecksStorageConfig):
            return LakebaseChecksStorageHandler(self.workspace_client, self.spark)
        if isinstance(config, VolumeFileChecksStorageConfig):
            return VolumeFileChecksStorageHandler(self.workspace_client)

        raise ValueError(f"Unsupported storage config type: {type(config).__name__}")
