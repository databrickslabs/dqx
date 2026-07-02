"""Storage handlers for persisting *DQAction* definitions.

This module provides:

- *ActionsStorageHandler* — abstract base class with *save* and *load* methods.
- *TableActionsStorageHandler* — persists actions to a Unity Catalog Delta table
  via Spark.
- *LakebaseActionsStorageHandler* — persists actions to a Lakebase (PostgreSQL)
  table via SQLAlchemy.
- *ActionsStorageHandlerFactory* — selects the right handler from config type.

Schema
------
Both handlers store each *DQAction* as a serialized JSON string alongside the
*run_config_name* and a *created_at* timestamp:

- *action_json* — JSON string produced by *ActionSerializer.to_dict*.
- *run_config_name* — run configuration name for multi-run isolation.
- *created_at* — timestamp at write time.

Security
--------
User-supplied values are sanitized before appearing in log messages (CWE-117).
"""

import json
import logging
import re
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Generic, TypeVar, overload

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from sqlalchemy import Column, DateTime, Engine, MetaData, String, Table, Text, delete, insert
from sqlalchemy import inspect as sa_inspect
from sqlalchemy import select
from sqlalchemy.schema import CreateSchema

from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.actions.dq_action import DQAction
from databricks.labs.dqx.actions.log_sanitize import sanitize_for_log as _sanitize
from databricks.labs.dqx.actions.serializer import ActionSerializer
from databricks.labs.dqx.config import LakebaseActionsStorageConfig, TableActionsStorageConfig
from databricks.labs.dqx.errors import UnsafeSqlQueryError
from databricks.labs.dqx.lakebase_engine import LakebaseConnectionMixin

logger = logging.getLogger(__name__)


T = TypeVar("T", TableActionsStorageConfig, LakebaseActionsStorageConfig)

ACTIONS_TABLE_SCHEMA = "action_json STRING, run_config_name STRING, created_at TIMESTAMP"


def build_replace_where_predicate(run_config_name: str) -> str:
    """Build a safe Delta *replaceWhere* predicate for *run_config_name*.

    Validates that *run_config_name* contains only characters in the set
    *[A-Za-z0-9_.-]* to prevent SQL injection (CWE-89) in the predicate
    string.  If the name contains any other character, *UnsafeSqlQueryError*
    is raised before the predicate is constructed.

    This mirrors the identical guard in *TableChecksStorageHandler.save* in
    *checks_storage.py*.

    Args:
        run_config_name: The run configuration name to embed in the predicate.

    Returns:
        A SQL predicate string that compares *run_config_name* against its
        validated value, safe for use as a Delta *replaceWhere* option.

    Raises:
        UnsafeSqlQueryError: If *run_config_name* contains characters outside
            *[A-Za-z0-9_.-]*.
    """
    if not re.fullmatch(r"[\w.\-]+", run_config_name):
        raise UnsafeSqlQueryError(
            f"run_config_name must not contain unsafe SQL characters. "
            f"Only word characters (a-z, A-Z, 0-9, _), '.', and '-' are allowed; got '{run_config_name}'."
        )
    return f"run_config_name = '{run_config_name}'"


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------


class ActionsStorageHandler(ABC, Generic[T]):
    """Abstract base class for DQAction definition storage handlers.

    Subclasses implement persistence to a specific backend (Delta table or
    Lakebase PostgreSQL).

    Args:
        T: The config type that parameterizes this handler.
    """

    @abstractmethod
    def save(self, actions: list[DQAction], config: T) -> None:
        """Persist *actions* to storage.

        Args:
            actions: List of *DQAction* definitions to persist.
            config: Backend-specific configuration.
        """

    @abstractmethod
    def load(self, config: T) -> list[DQAction]:
        """Load *DQAction* definitions from storage.

        Args:
            config: Backend-specific configuration.

        Returns:
            List of *DQAction* instances loaded from storage.
        """


# ---------------------------------------------------------------------------
# Table (UC Delta) handler
# ---------------------------------------------------------------------------


class TableActionsStorageHandler(ActionsStorageHandler[TableActionsStorageConfig]):
    """Persists *DQAction* definitions to a Unity Catalog Delta table via Spark.

    Each *DQAction* is serialized to JSON via *ActionSerializer.to_dict* and
    stored as a single row alongside the *run_config_name* and a
    *created_at* timestamp.

    On *save*, the write mode from *config.mode* controls whether existing
    rows for the *run_config_name* are replaced (*"overwrite"*) or kept
    (*"append"*).

    On *load*, all rows matching *config.run_config_name* are read and
    deserialized.

    Args:
        spark: Active *SparkSession* for Spark-based read/write.
        ws: Authenticated *WorkspaceClient* (reserved for future use such
            as table-existence checks).
    """

    def __init__(self, spark: SparkSession, ws: WorkspaceClient) -> None:
        self._spark = spark
        self._ws = ws

    def save(self, actions: list[DQAction], config: TableActionsStorageConfig) -> None:
        """Serialize and write *actions* to the configured Delta table.

        Args:
            actions: List of *DQAction* definitions to persist.
            config: *TableActionsStorageConfig* with target table, mode, and
                run config name.

        Raises:
            UnsafeSqlQueryError: If *run_config_name* contains characters outside
                *[A-Za-z0-9_.-]* when *mode* is *"overwrite"*, raised by
                *_build_replace_where* to prevent SQL injection in the Delta
                *replaceWhere* predicate (CWE-89).
        """
        if not actions:
            logger.info("No actions provided; skipping save.")
            return

        safe_location = _sanitize(config.location)
        logger.info(f"Saving {len(actions)} action(s) to table '{safe_location}'.")

        created_at = datetime.now(timezone.utc)
        rows = []
        for action in actions:
            action_dict = ActionSerializer.to_dict(action)
            rows.append((json.dumps(action_dict), config.run_config_name, created_at))

        df = self._spark.createDataFrame(rows, schema=ACTIONS_TABLE_SCHEMA)

        writer = df.write.format("delta")
        if config.mode == "overwrite":
            predicate = build_replace_where_predicate(config.run_config_name)
            writer = writer.option("replaceWhere", predicate)
            writer.mode("overwrite").saveAsTable(config.location)
        else:
            writer.mode("append").saveAsTable(config.location)

        logger.info(f"Saved {len(actions)} action(s) to '{safe_location}' (mode={config.mode}).")

    def load(self, config: TableActionsStorageConfig) -> list[DQAction]:
        """Read and deserialize *DQAction* definitions from the Delta table.

        Returns an empty list when the table does not exist.

        Args:
            config: *TableActionsStorageConfig* with source table and run
                config name.

        Returns:
            List of *DQAction* instances.
        """
        safe_location = _sanitize(config.location)
        safe_run_config = _sanitize(config.run_config_name)

        if not self._spark.catalog.tableExists(config.location):
            logger.info(f"Table '{safe_location}' does not exist; returning empty list.")
            return []

        df = self._spark.read.table(config.location).filter(F.col("run_config_name") == config.run_config_name)

        result: list[DQAction] = []
        for row in df.collect():
            try:
                action_dict = json.loads(row["action_json"])
                result.append(ActionSerializer.from_dict(action_dict))
            except Exception as exc:  # broad catch: isolate deserialization errors
                logger.warning(f"Failed to deserialize action from table '{safe_location}': {_sanitize(str(exc))}")

        logger.info(f"Loaded {len(result)} action(s) from '{safe_location}' (run_config={safe_run_config!r}).")
        return result


# ---------------------------------------------------------------------------
# Lakebase (PostgreSQL / SQLAlchemy) handler
# ---------------------------------------------------------------------------


class LakebaseActionsStorageHandler(LakebaseConnectionMixin, ActionsStorageHandler[LakebaseActionsStorageConfig]):
    """Persists *DQAction* definitions to a Lakebase (PostgreSQL) table via SQLAlchemy.

    Inherits engine lifecycle management from *LakebaseConnectionMixin*: a lazily
    created, cached engine with a *do_connect* listener that refreshes the
    Databricks-generated credential token before each connection, and schema /
    table bootstrap on first use.

    The handler accepts an optional pre-built *Engine* for testability — pass
    an in-memory or test engine via the *engine* constructor parameter to avoid
    needing a real Lakebase instance in unit tests.

    Args:
        spark: Active *SparkSession* (kept for interface symmetry; not used for
            PostgreSQL queries).
        ws: Authenticated *WorkspaceClient* used to retrieve the Lakebase DNS
            and generate short-lived credentials.
        config: *LakebaseActionsStorageConfig* with instance and table details.
        engine: Optional pre-built SQLAlchemy *Engine* (useful for testing).
    """

    def __init__(
        self,
        spark: SparkSession,
        ws: WorkspaceClient,
        config: LakebaseActionsStorageConfig,
        engine: Engine | None = None,
    ) -> None:
        super().__init__(spark=spark, ws=ws, config=config, engine=engine)

    @staticmethod
    def _get_table_definition(schema_name: str, table_name: str) -> Table:
        """Build a SQLAlchemy *Table* definition for the actions table.

        Args:
            schema_name: PostgreSQL schema to contain the table.
            table_name: Name of the actions table.

        Returns:
            A SQLAlchemy *Table* object.
        """
        return Table(
            table_name,
            MetaData(schema=schema_name),
            Column("action_json", Text),
            Column("run_config_name", String(255)),
            Column("created_at", DateTime(timezone=True)),
        )

    def _bootstrap(self, engine: Engine) -> None:
        """Ensure schema and table exist, creating them if necessary.

        Args:
            engine: SQLAlchemy *Engine* to use for DDL.
        """
        with engine.begin() as conn:
            if not conn.dialect.has_schema(conn, self._config.schema_name):
                conn.execute(CreateSchema(self._config.schema_name))
                logger.info(f"Created schema '{_sanitize(self._config.schema_name)}'.")

        table = self._get_table_definition(self._config.schema_name, self._config.table_name)
        table.metadata.create_all(engine, checkfirst=True)
        logger.info(f"Bootstrapped Lakebase table '{_sanitize(self._config.location)}'.")

    def save(self, actions: list[DQAction], config: LakebaseActionsStorageConfig) -> None:
        """Serialize and write *actions* to the Lakebase table.

        Bootstraps the schema and table on first use.  When *config.mode* is
        *"overwrite"*, all existing rows for *config.run_config_name* are
        deleted before inserting the new rows.

        Args:
            actions: List of *DQAction* definitions to persist.
            config: *LakebaseActionsStorageConfig* with instance and table details.
        """
        if not actions:
            logger.info("No actions provided; skipping Lakebase save.")
            return

        safe_location = _sanitize(config.location)
        logger.info(f"Saving {len(actions)} action(s) to Lakebase table '{safe_location}'.")

        engine = self._get_engine()
        self._bootstrap(engine)
        table = self._get_table_definition(config.schema_name, config.table_name)

        created_at = datetime.now(timezone.utc)
        rows = [
            {
                "action_json": json.dumps(ActionSerializer.to_dict(action)),
                "run_config_name": config.run_config_name,
                "created_at": created_at,
            }
            for action in actions
        ]

        with engine.begin() as conn:
            if config.mode == "overwrite":
                conn.execute(delete(table).where(table.c.run_config_name == config.run_config_name))
            conn.execute(insert(table), rows)

        logger.info(f"Saved {len(actions)} action(s) to Lakebase '{safe_location}' (mode={config.mode}).")

    def load(self, config: LakebaseActionsStorageConfig) -> list[DQAction]:
        """Read and deserialize *DQAction* definitions from the Lakebase table.

        Returns an empty list when the table does not exist.

        Args:
            config: *LakebaseActionsStorageConfig* with instance and table details.

        Returns:
            List of *DQAction* instances.
        """
        safe_location = _sanitize(config.location)
        safe_run_config = _sanitize(config.run_config_name)

        engine = self._get_engine()
        inspector = sa_inspect(engine)
        if not inspector.has_table(config.table_name, schema=config.schema_name):
            logger.info(f"Table '{safe_location}' not found in Lakebase; returning empty list.")
            return []

        table = self._get_table_definition(config.schema_name, config.table_name)
        stmt = select(table).where(table.c.run_config_name == config.run_config_name)

        result: list[DQAction] = []
        with engine.connect() as conn:
            rows = conn.execute(stmt).mappings().all()

        for row in rows:
            try:
                action_dict = json.loads(row["action_json"])
                result.append(ActionSerializer.from_dict(action_dict))
            except Exception as exc:  # broad catch: isolate deserialization errors
                logger.warning(
                    f"Failed to deserialize action from Lakebase table '{safe_location}': {_sanitize(str(exc))}"
                )

        logger.info(f"Loaded {len(result)} action(s) from Lakebase '{safe_location}' (run_config={safe_run_config!r}).")
        return result


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


class ActionsStorageHandlerFactory:
    """Creates the appropriate *ActionsStorageHandler* for a given config type.

    Selection logic:

    - *LakebaseActionsStorageConfig* → *LakebaseActionsStorageHandler*
    - *TableActionsStorageConfig* → *TableActionsStorageHandler*

    Args:
        config: Storage configuration; either *TableActionsStorageConfig* or
            *LakebaseActionsStorageConfig*.
        spark: Active *SparkSession*.
        ws: Authenticated *WorkspaceClient*.

    Returns:
        A concrete *ActionsStorageHandler* instance.
    """

    @staticmethod
    @overload
    def create(
        config: LakebaseActionsStorageConfig,
        spark: SparkSession,
        ws: WorkspaceClient,
    ) -> ActionsStorageHandler[LakebaseActionsStorageConfig]: ...

    @staticmethod
    @overload
    def create(
        config: TableActionsStorageConfig,
        spark: SparkSession,
        ws: WorkspaceClient,
    ) -> ActionsStorageHandler[TableActionsStorageConfig]: ...

    @staticmethod
    def create(
        config: TableActionsStorageConfig | LakebaseActionsStorageConfig,
        spark: SparkSession,
        ws: WorkspaceClient,
    ) -> ActionsStorageHandler[TableActionsStorageConfig] | ActionsStorageHandler[LakebaseActionsStorageConfig]:
        """Instantiate the correct storage handler for *config*.

        Args:
            config: Storage configuration.
            spark: Active *SparkSession*.
            ws: Authenticated *WorkspaceClient*.

        Returns:
            *LakebaseActionsStorageHandler* when *config* is a
            *LakebaseActionsStorageConfig*; *TableActionsStorageHandler*
            otherwise.
        """
        if isinstance(config, LakebaseActionsStorageConfig):
            return LakebaseActionsStorageHandler(spark=spark, ws=ws, config=config)
        return TableActionsStorageHandler(spark=spark, ws=ws)

    @staticmethod
    def save(
        actions: list[DQAction],
        config: TableActionsStorageConfig | LakebaseActionsStorageConfig,
        spark: SparkSession,
        ws: WorkspaceClient,
    ) -> None:
        """Create the appropriate handler and persist *actions* to storage.

        Args:
            actions: List of *DQAction* definitions to persist.
            config: Backend-specific configuration.
            spark: Active *SparkSession*.
            ws: Authenticated *WorkspaceClient*.
        """
        if isinstance(config, LakebaseActionsStorageConfig):
            LakebaseActionsStorageHandler(spark=spark, ws=ws, config=config).save(actions, config)
        else:
            TableActionsStorageHandler(spark=spark, ws=ws).save(actions, config)

    @staticmethod
    def load(
        config: TableActionsStorageConfig | LakebaseActionsStorageConfig,
        spark: SparkSession,
        ws: WorkspaceClient,
    ) -> list[DQAction]:
        """Create the appropriate handler and load *DQAction* definitions from storage.

        Args:
            config: Backend-specific configuration.
            spark: Active *SparkSession*.
            ws: Authenticated *WorkspaceClient*.

        Returns:
            List of *DQAction* instances loaded from storage.
        """
        if isinstance(config, LakebaseActionsStorageConfig):
            return LakebaseActionsStorageHandler(spark=spark, ws=ws, config=config).load(config)
        return TableActionsStorageHandler(spark=spark, ws=ws).load(config)


__all__ = [
    "ACTIONS_TABLE_SCHEMA",
    "ActionsStorageHandler",
    "ActionsStorageHandlerFactory",
    "LakebaseActionsStorageHandler",
    "TableActionsStorageHandler",
    "build_replace_where_predicate",
]
