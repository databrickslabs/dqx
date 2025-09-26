import json
import logging
import uuid
import os
from io import StringIO, BytesIO
from pathlib import Path
from abc import ABC, abstractmethod
from typing import Dict, Generic, List, TypeVar
from sqlalchemy import (
    Engine,
    create_engine,
    MetaData,
    Table,
    Column,
    String,
    Text,
    insert,
    select,
    delete,
)
from sqlalchemy.schema import CreateSchema
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import OperationalError, DatabaseError, ProgrammingError

import yaml
from pyspark.sql import SparkSession
from databricks.sdk.errors import NotFound
from databricks.sdk.service.workspace import ImportFormat

from databricks.labs.dqx.config import (
    LakebaseConnectionConfig,
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
from databricks.labs.dqx.telemetry import telemetry_logger
from databricks.labs.dqx.utils import TABLE_PATTERN
from databricks.labs.dqx.checks_serializer import FILE_SERIALIZERS


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
        """
        logger.info(f"Loading quality rules (checks) from table '{config.location}'")
        if not self.ws.tables.exists(config.location).table_exists:
            raise NotFound(f"Table {config.location} does not exist in the workspace")
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
            ValueError: if the table name is not provided
        """
        logger.info(f"Saving quality rules (checks) to table '{config.location}'")
        rules_df = deserialize_checks_to_dataframe(self.spark, checks, run_config_name=config.run_config_name)
        rules_df.write.option("replaceWhere", f"run_config_name = '{config.run_config_name}'").saveAsTable(
            config.location, mode=config.mode
        )


class LakebaseChecksStorageHandler(ChecksStorageHandler[LakebaseChecksStorageConfig]):
    """
    Handler for storing dq rules (checks) in a Lakebase table.
    """

    def __init__(self, ws: WorkspaceClient, spark: SparkSession, engine: Engine | None = None):
        self.ws = ws
        self.spark = spark
        self.engine = engine

    def _get_connection_url(self, connection_config: LakebaseConnectionConfig) -> str:
        """
        Generate a Lakebase connection URL.

        Args:
            config: Configuration for saving and loading checks to Lakebase.

        Returns:
            Lakebase connection URL.
        """
        instance = self.ws.database.get_database_instance(connection_config.instance_name)
        cred = self.ws.database.generate_database_credential(
            request_id=str(uuid.uuid4()), instance_names=[connection_config.instance_name]
        )
        host = instance.read_write_dns
        password = cred.token

        return f"postgresql://{connection_config.user}:{password}@{host}:{connection_config.port}/{connection_config.database}?sslmode=require"

    def _get_engine(self, connection_config: LakebaseConnectionConfig) -> Engine:
        """
        Create a SQLAlchemy engine for the Lakebase instance.

        Args:
            config: Configuration for saving and loading checks to Lakebase.

        Returns:
            SQLAlchemy engine for the Lakebase instance.
        """
        connection_url = self._get_connection_url(connection_config)
        return create_engine(connection_url)

    def _get_schema_and_table_name(self, config: LakebaseChecksStorageConfig) -> tuple[str, str]:
        """
        Extract the schema and table name from the fully qualified table name.

        Args:
            config: Configuration for saving and loading checks to Lakebase.

        Returns:
            A tuple containing the schema and table name.

        Raises:
            ValueError: If the fully qualified table name is not in the correct format.
        """
        location_components = config.location.split(".")

        if len(location_components) != 3:
            raise ValueError(
                f"Invalid Lakebase table name '{config.location}'. Must be in the format 'database.schema.table'."
            )

        _, schema_name, table_name = location_components
        return schema_name, table_name

    def _get_table_definition(self, schema_name: str, table_name: str) -> Table:
        """
        Create a table definition for consistency between the load and save methods.

        Args:
            schema: The schema name where the checks table is located.
            table: The table name where the checks are stored.

        Returns:
            SQLAlchemy table definition for the Lakebase instance.
        """
        return Table(
            table_name,
            MetaData(schema=schema_name),
            Column("name", String(255)),
            Column("criticality", String(50), server_default="error"),
            Column("check", JSONB),
            Column("filter", Text),
            Column("run_config_name", String(255), server_default="default"),
            Column("user_metadata", JSONB),
        )

    @telemetry_logger("load_checks", "lakebase")
    def load(self, config: LakebaseChecksStorageConfig) -> List[Dict]:
        """
        Load dq rules (checks) from a Lakebase table.

        Args:
            config: Configuration for saving and loading checks to Lakebase.

        Returns:
            List of dq rules or error if loading checks fails.

        Raises:
            OperationalError: If connection to Lakebase instance fails.
            DatabaseError: If loading checks fails.
            ProgrammingError: If loading checks fails.
        """
        connection_config = LakebaseConnectionConfig._parse_connection_string(config.connection_string)

        engine_created_internally = False
        if not self.engine:
            engine = self._get_engine(connection_config)
            engine_created_internally = True
        else:
            engine = self.engine

        try:
            logger.info(f"Loading checks from Lakebase instance {connection_config.instance_name}")

            schema_name, table_name = self._get_schema_and_table_name(config)
            table = self._get_table_definition(schema_name, table_name)
            stmt = select(table)

            if config.run_config_name:
                stmt = stmt.where(table.c.run_config_name == config.run_config_name)

            with engine.connect() as conn:
                result = conn.execute(stmt)
                checks = result.mappings().all()
                logger.info(f"Successfully loaded {len(checks)} checks from {schema_name}.{table_name}")
                return [dict(check) for check in checks]
        except (OperationalError, DatabaseError, ProgrammingError) as e:
            logger.error(f"Failed to load checks from Lakebase instance {connection_config.instance_name}: {e}")
            raise
        finally:
            if engine_created_internally:
                engine.dispose()

    @telemetry_logger("save_checks", "lakebase")
    def save(self, checks: List[Dict], config: LakebaseChecksStorageConfig) -> None:
        """
        Save dq rules (checks) to a Lakebase table.

        Args:
            checks: List of dq rules (checks) to save.
            config: Configuration for saving and loading checks to Lakebase.

        Returns:
            None

        Raises:
            OperationalError: If connection to Lakebase instance fails.
            DatabaseError: If saving checks fails.
            ProgrammingError: If saving checks fails.
            ValueError: If invalid mode is specified.
        """
        if not checks:
            logger.warning("No checks provided to save.")
            return

        if config.mode not in ("append", "overwrite"):
            raise ValueError(f"Invalid mode '{config.mode}'. Must be 'append' or 'overwrite'.")

        connection_config = LakebaseConnectionConfig._parse_connection_string(config.connection_string)

        engine_created_internally = False
        if not self.engine:
            engine = self._get_engine(connection_config)
            engine_created_internally = True
        else:
            engine = self.engine

        schema_name, table_name = self._get_schema_and_table_name(config)

        try:
            logger.info(f"Saving {len(checks)} checks to Lakebase instance {connection_config.instance_name}")

            with engine.begin() as conn:
                if not conn.dialect.has_schema(conn, schema_name):
                    conn.execute(CreateSchema(schema_name))
                    logger.info(
                        f"Successfully created schema '{schema_name}' in database '{connection_config.database}'."
                    )

                table = self._get_table_definition(schema_name, table_name)
                table.metadata.create_all(engine, checkfirst=True)

                if config.mode == "overwrite":
                    delete_stmt = delete(table).where(table.c.run_config_name == config.run_config_name)
                    result = conn.execute(delete_stmt)
                    logger.info(
                        f"Deleted {result.rowcount} existing checks for run_config_name '{config.run_config_name}'"
                    )

                insert_stmt = insert(table)
                conn.execute(insert_stmt, checks)
                logger.info(f"Inserted {len(checks)} new checks into {schema_name}.{table_name}")
        except (OperationalError, DatabaseError, ProgrammingError) as e:
            logger.error(f"Failed to save checks to Lakebase: {e}")
            raise
        finally:
            if engine_created_internally:
                engine.dispose()


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
        self.ws = ws
        self._run_config_loader = run_config_loader or RunConfigLoader(ws)
        self.workspace_file_handler = WorkspaceFileChecksStorageHandler(ws)
        self.table_handler = TableChecksStorageHandler(ws, spark)
        self.volume_handler = VolumeFileChecksStorageHandler(ws)
        self.lakebase_handler = LakebaseChecksStorageHandler(ws, spark, None)

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
        run_config = self._run_config_loader.load_run_config(
            run_config_name=config.run_config_name,
            assume_user=config.assume_user,
            product_name=config.product_name,
            install_folder=config.install_folder,
        )
        installation = self._run_config_loader.get_installation(
            config.assume_user, config.product_name, config.install_folder
        )

        config.location = run_config.checks_location

        matches_table_pattern = TABLE_PATTERN.match(config.location) and not config.location.lower().endswith(
            tuple(FILE_SERIALIZERS.keys())
        )
        matches_lakebase_pattern = config.connection_string and config.connection_string.startswith("postgresql://")

        if matches_table_pattern and matches_lakebase_pattern:
            return self.lakebase_handler, config

        if matches_table_pattern:
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

    @telemetry_logger("load_checks", "volume")
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
            return LakebaseChecksStorageHandler(self.workspace_client, self.spark, None)
        if isinstance(config, VolumeFileChecksStorageConfig):
            return VolumeFileChecksStorageHandler(self.workspace_client)

        raise ValueError(f"Unsupported storage config type: {type(config).__name__}")
