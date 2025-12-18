import json
import logging
import uuid
import os
from io import StringIO, BytesIO
from pathlib import Path
from abc import ABC, abstractmethod
from typing import Generic, TypeVar, NoReturn
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
    null,
)
from sqlalchemy.schema import CreateSchema
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import DatabaseError, ProgrammingError, OperationalError, IntegrityError


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
    RunConfig,
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
from databricks.labs.dqx.config_serializer import ConfigSerializer
from databricks.labs.dqx.installer.mixins import InstallationMixin
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


class LakebaseChecksStorageHandler(ChecksStorageHandler[LakebaseChecksStorageConfig]):
    """
    Handler for storing dq rules (checks) in a Lakebase table.
    """

    def __init__(self, ws: WorkspaceClient, spark: SparkSession, engine: Engine | None = None):
        self.ws = ws
        self.spark = spark
        self.engine = engine

    def _get_connection_url(self, config: LakebaseChecksStorageConfig) -> str:
        """
        Generate a Lakebase connection URL.

        Args:
            config: Configuration for saving and loading checks to Lakebase.

        Returns:
            Lakebase connection URL.
        """
        if not config.instance_name:
            raise InvalidConfigError("instance_name must be provided for Lakebase storage")
        if not config.user:
            raise InvalidConfigError("user must be provided for Lakebase storage")
        if not config.location:
            raise InvalidConfigError("location must be provided for Lakebase storage")

        instance = self.ws.database.get_database_instance(config.instance_name)
        cred = self.ws.database.generate_database_credential(
            request_id=str(uuid.uuid4()), instance_names=[config.instance_name]
        )
        host = instance.read_write_dns
        password = cred.token

        return f"postgresql://{config.user}:{password}@{host}:{config.port}/{config.database_name}?sslmode=require"

    def _get_engine(self, config: LakebaseChecksStorageConfig) -> Engine:
        """
        Create a SQLAlchemy engine for the Lakebase instance.

        Args:
            config: Configuration for saving and loading checks to Lakebase.

        Returns:
            SQLAlchemy engine for the Lakebase instance.
        """
        connection_url = self._get_connection_url(config)
        return create_engine(connection_url)

    @staticmethod
    def get_table_definition(schema_name: str, table_name: str) -> Table:
        """
        Create a SQLAlchemy table definition for storing DQ rules (checks) in Lakebase.

        Args:
            schema_name: The schema where the checks table is located.
            table_name: The table where the checks are stored.

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

    @staticmethod
    def _normalize_checks(checks: list[dict], config: LakebaseChecksStorageConfig) -> list[dict]:
        """
        Normalize the checks to be compatible with the Lakebase table.

        Args:
            checks: List of dq rules (checks) to normalize.
            config: Configuration for saving and loading checks to Lakebase.

        Returns:
            List of normalized dq rules (checks).
        """
        normalized_checks = []
        for check in checks:
            user_metadata = check.get("user_metadata")
            normalized_check = {
                "name": check.get("name"),
                "criticality": check.get("criticality", "error"),
                "check": check.get("check"),
                "filter": check.get("filter"),
                "run_config_name": check.get("run_config_name", config.run_config_name),
                "user_metadata": null() if user_metadata is None else user_metadata,
            }
            normalized_checks.append(normalized_check)
        return normalized_checks

    def _save_checks_to_lakebase(self, checks: list[dict], config: LakebaseChecksStorageConfig, engine: Engine) -> None:
        """
        Save dq rules (checks) to a Lakebase table.

        Args:
            checks: List of dq rules (checks) to save.
            config: Configuration for saving and loading checks to Lakebase.
            engine: SQLAlchemy engine for the Lakebase instance.

        Returns:
            None

        Raises:
            OperationalError: If connecting to the database fails.
        """
        try:
            with engine.connect() as conn:
                pass
        except OperationalError as e:
            logger.error(
                f"Failed to connect to database '{config.database_name}'. "
                f"Please verify the database exists and the user has access: {e}"
            )
            raise

        with engine.begin() as conn:
            if not conn.dialect.has_schema(conn, config.schema_name):
                conn.execute(CreateSchema(config.schema_name))
                logger.info(f"Successfully created schema '{config.schema_name}'.")

        with engine.begin() as conn:
            table = self.get_table_definition(config.schema_name, config.table_name)
            table.metadata.create_all(engine, checkfirst=True)
            logger.info(
                f"Successfully created or verified table '{config.database_name}.{config.schema_name}.{config.table_name}'."
            )

            if config.mode == "overwrite":
                delete_stmt = delete(table).where(table.c.run_config_name == config.run_config_name)
                result = conn.execute(delete_stmt)
                logger.info(f"Deleted {result.rowcount} existing checks for run_config_name '{config.run_config_name}'")

            normalized_checks = self._normalize_checks(checks, config)
            insert_stmt = insert(table)
            conn.execute(insert_stmt, normalized_checks)
            logger.info(
                f"Inserted {len(normalized_checks)} checks to {config.database_name}.{config.schema_name}.{config.table_name} "
                f"with run_config_name='{config.run_config_name}'"
            )

    def _load_checks_from_lakebase(self, config: LakebaseChecksStorageConfig, engine: Engine) -> list[dict]:
        """
        Load dq rules (checks) from a Lakebase table.

        Args:
            config: Configuration for saving and loading checks to Lakebase.
            engine: SQLAlchemy engine for the Lakebase instance.

        Returns:
            List of dq rules.
        """
        table = self.get_table_definition(config.schema_name, config.table_name)

        stmt = select(table)
        if config.run_config_name:
            logger.info(f"Filtering checks by run_config_name='{config.run_config_name}'")
            stmt = stmt.where(table.c.run_config_name == config.run_config_name)
        else:
            logger.info("Loading all checks (no run_config_name filter)")

        with engine.connect() as conn:
            result = conn.execute(stmt)
            checks = result.mappings().all()
            logger.info(
                f"Successfully loaded {len(checks)} checks from {config.database_name}.{config.schema_name}.{config.table_name} "
                f"for run_config_name='{config.run_config_name}'"
            )
            if len(checks) == 0:
                logger.warning(
                    f"No checks found in {config.database_name}.{config.schema_name}.{config.table_name} "
                    f"for run_config_name='{config.run_config_name}'. "
                    f"Make sure the profiler has run successfully and saved checks to this location."
                )
            return [dict(check) for check in checks]

    def _check_for_undefined_table_error(self, e: ProgrammingError, config: LakebaseChecksStorageConfig) -> NoReturn:
        """
        Check if the error is an undefined table error and raise an appropriate exception.

        This method always raises an exception and never returns normally.

        Args:
            e: Programming error to check.
            config: Configuration for saving and loading checks to Lakebase.

        Raises:
            NotFound: If the table does not exist in the Lakebase instance (pgcode 42P01).
            ProgrammingError: Re-raises the original error if it's not an undefined table error.
        """
        pgcode = getattr(getattr(e, 'orig', None), 'pgcode', None)
        postgres_undefined_table_error = '42P01'
        if pgcode == postgres_undefined_table_error:
            raise NotFound(f"Table '{config.location}' does not exist in the Lakebase instance") from e
        raise e

    @telemetry_logger("load_checks", "lakebase")
    def load(self, config: LakebaseChecksStorageConfig) -> list[dict]:
        """
        Load dq rules (checks) from a Lakebase table.

        Args:
            config: Configuration for saving and loading checks to Lakebase.

        Returns:
            List of dq rules or error if loading checks fails.

        Raises:
            NotFound: If the table does not exist in the Lakebase instance.
            ProgrammingError: If SQL syntax errors or missing objects (converted to NotFound for missing tables).
            DatabaseError: If other database operations fail (includes OperationalError, IntegrityError, etc.).
        """
        engine = self.engine
        engine_created_internally = False
        if not engine:
            engine = self._get_engine(config)
            engine_created_internally = True

        try:
            return self._load_checks_from_lakebase(config, engine)

        except ProgrammingError as e:
            logger.error(f"Programming error while loading checks from Lakebase: {e}")
            self._check_for_undefined_table_error(e, config)

        except DatabaseError as e:
            logger.error(f"Database error while loading checks from Lakebase: {e}")
            raise

        finally:
            if engine_created_internally:
                engine.dispose()

    @telemetry_logger("save_checks", "lakebase")
    def save(self, checks: list[dict], config: LakebaseChecksStorageConfig) -> None:
        """
        Save dq rules (checks) to a Lakebase table.

        Args:
            checks: List of dq rules (checks) to save.
            config: Configuration for saving and loading checks to Lakebase.

        Returns:
            None

        Raises:
            InvalidCheckError: If any check is invalid or unsupported.
            IntegrityError: If constraint violations occur (e.g., duplicate keys).
            ProgrammingError: If SQL syntax errors or missing objects.
            DatabaseError: If other database operations fail (includes OperationalError, DataError, etc.).
        """
        if not checks:
            raise InvalidCheckError("Checks cannot be empty or None.")

        engine = self.engine
        engine_created_internally = False
        if not engine:
            engine = self._get_engine(config)
            engine_created_internally = True

        try:
            self._save_checks_to_lakebase(checks, config, engine)
            logger.info(f"Successfully saved {len(checks)} checks to Lakebase.")

        except IntegrityError as e:
            logger.error(f"Integrity constraint violation while saving checks to Lakebase: {e}")
            raise

        except ProgrammingError as e:
            logger.error(f"Programming error while saving checks to Lakebase: {e}")
            raise

        except DatabaseError as e:
            logger.error(f"Database error while saving checks to Lakebase: {e}")
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


class InstallationChecksStorageHandler(ChecksStorageHandler[InstallationChecksStorageConfig], InstallationMixin):
    """
    Handler for storing quality rules (checks) defined in the installation configuration.
    """

    def __init__(
        self,
        ws: WorkspaceClient,
        spark: SparkSession,
        config_serializer: ConfigSerializer | None = None,
    ):
        self.ws = ws
        self._config_serializer = config_serializer or ConfigSerializer(ws)
        self.workspace_file_handler = WorkspaceFileChecksStorageHandler(ws)
        self.table_handler = TableChecksStorageHandler(ws, spark)
        self.volume_handler = VolumeFileChecksStorageHandler(ws)
        self.lakebase_handler = LakebaseChecksStorageHandler(ws, spark, None)
        super().__init__(ws)

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
        # Overwrite location if overwrite_location is set
        if config.overwrite_location:
            checks_location = config.location
        else:
            run_config = self._config_serializer.load_run_config(
                run_config_name=config.run_config_name,
                assume_user=config.assume_user,
                product_name=config.product_name,
                install_folder=config.install_folder,
            )

            checks_location = run_config.checks_location

            # transfer lakebase fields from run config to storage config if not already set
            if run_config.lakebase_instance_name and not config.instance_name:
                config.instance_name = run_config.lakebase_instance_name
            if run_config.lakebase_user and not config.user:
                config.user = run_config.lakebase_user
            # replace port if non-default is specified in the run config
            if run_config.lakebase_port and config.port != run_config.lakebase_port:
                config.port = run_config.lakebase_port

        installation = self._get_installation(
            product_name=config.product_name, assume_user=config.assume_user, install_folder=config.install_folder
        )

        config.location = checks_location

        matches_table_pattern = is_table_location(config.location)
        is_lakebase_storage = config.instance_name is not None

        if matches_table_pattern and is_lakebase_storage:
            logger.debug(f"Using LakebaseChecksStorageHandler for location '{config.location}'")
            return self.lakebase_handler, config

        if matches_table_pattern:
            logger.debug(f"Using TableChecksStorageHandler for location '{config.location}'")
            return self.table_handler, config

        if config.location.startswith("/Volumes/"):
            logger.debug(f"Using VolumeChecksStorageHandler for location '{config.location}'")
            return self.volume_handler, config

        if not config.location.startswith("/"):
            # if absolute path is not provided, the location should be set relative to the installation folder
            config.location = f"{installation.install_folder()}/{config.location}"

        logger.debug(f"Using WorkspaceFileChecksStorageHandler for location '{config.location}'")
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
            location: location of the checks (file path, table name, volume, etc.).
            run_config_name: the name of the run configuration to use for checks, e.g. input table or job name (use "default" if not provided).

        Returns:
            An instance of the corresponding BaseChecksStorageHandler.
        """

    @abstractmethod
    def create_for_run_config(self, run_config: RunConfig) -> tuple[ChecksStorageHandler, BaseChecksStorageConfig]:
        """
        Abstract method to create a handler and config based on a RunConfig.

        This method inspects the RunConfig to determine the appropriate storage handler.
        If Lakebase connection parameters are present (lakebase_instance_name), it creates
        a LakebaseChecksStorageHandler. Otherwise, it delegates to create_for_location
        to infer the handler from the checks location string.

        Args:
            run_config: RunConfig containing checks location and optional Lakebase parameters.

        Returns:
            A tuple of (ChecksStorageHandler, BaseChecksStorageConfig).
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
        if isinstance(config, LakebaseChecksStorageConfig):
            return LakebaseChecksStorageHandler(self.workspace_client, self.spark, None)
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

    def create_for_run_config(self, run_config: RunConfig) -> tuple[ChecksStorageHandler, BaseChecksStorageConfig]:
        """
        Factory method to create a handler and config based on a RunConfig.

        This method inspects the RunConfig to determine the appropriate storage handler.
        If Lakebase connection parameters are present (lakebase_instance_name), it creates
        a LakebaseChecksStorageHandler. Otherwise, it delegates to create_for_location
        to infer the handler from the checks location string.

        Args:
            run_config: RunConfig containing checks location and optional Lakebase parameters.

        Returns:
            A tuple of (ChecksStorageHandler, BaseChecksStorageConfig).

        Raises:
            InvalidConfigError: If the configuration is invalid or unsupported.
        """
        if run_config.lakebase_instance_name:
            if not run_config.lakebase_user:
                raise InvalidConfigError(
                    f"Lakebase user must be specified in run config '{run_config.name}' when "
                    f"lakebase_instance_name is set. Please add 'lakebase_user' to your run configuration."
                )

            if not run_config.checks_location:
                raise InvalidConfigError(
                    f"checks_location must be specified in run config '{run_config.name}' when using Lakebase. "
                    f"Expected format: 'database.schema.table'."
                )

            if len(run_config.checks_location.split(".")) != 3:
                raise InvalidConfigError(
                    f"Invalid Lakebase table name '{run_config.checks_location}' in run config '{run_config.name}'. "
                    f"Must be in the format 'database.schema.table'. "
                    f"Example: 'my_database.my_schema.my_table'."
                )

            return (
                LakebaseChecksStorageHandler(self.workspace_client, self.spark, None),
                LakebaseChecksStorageConfig(
                    location=run_config.checks_location,
                    instance_name=run_config.lakebase_instance_name,
                    user=run_config.lakebase_user,
                    port=run_config.lakebase_port or "5432",
                    run_config_name=run_config.name,
                ),
            )

        return self.create_for_location(run_config.checks_location, run_config.name)


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
