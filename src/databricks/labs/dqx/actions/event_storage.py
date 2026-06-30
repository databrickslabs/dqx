"""Concrete event store implementations for the DQX actions & alerting subsystem.

This module provides:

- *ACTION_EVENT_TABLE_SCHEMA* — Spark DDL schema string for the events Delta table.
- *TableActionEventStore* — appends *AlertEvent* records to a Unity Catalog Delta
  table via Spark and reads back the latest event per action using a window function.
- *LakebaseActionEventStore* — mirrors the pattern from *LakebaseChecksStorageHandler*
  to persist and load events via SQLAlchemy / PostgreSQL (Databricks Lakebase).
- *ActionEventStoreFactory* — selects the appropriate store based on
  *ActionEventsConfig* fields.
"""

from __future__ import annotations

import logging
import uuid

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Engine,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
    event,
    inspect,
    insert,
    select,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.schema import CreateSchema

from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.actions.state import ActionEventStore, AlertEvent
from databricks.labs.dqx.actions.base import ActionStatus
from databricks.labs.dqx.config import ActionEventsConfig, LakebaseActionsStorageConfig
from databricks.labs.dqx.io import save_dataframe_as_table
from databricks.labs.dqx.config import OutputConfig

logger = logging.getLogger(__name__)

ACTION_EVENT_TABLE_SCHEMA = (
    "action_name STRING, condition STRING, fired BOOLEAN, status STRING, "
    "observed_metrics MAP<STRING,STRING>, run_id STRING, run_time TIMESTAMP, "
    "input_location STRING, destinations ARRAY<STRING>, delivery_errors ARRAY<STRING>"
)


# ---------------------------------------------------------------------------
# TableActionEventStore
# ---------------------------------------------------------------------------


class TableActionEventStore(ActionEventStore):
    """Persists *AlertEvent* records to a Unity Catalog Delta table via Spark.

    Events are appended to a Delta table with the schema defined in
    *ACTION_EVENT_TABLE_SCHEMA*.  Loading the latest event per action uses a
    window function partitioned by *action_name* and ordered by *run_time*
    descending, selecting rank == 1.

    Args:
        spark: Active *SparkSession*.
        ws: Authenticated *WorkspaceClient* (reserved for future use such as
            table-existence checks).
        config: *ActionEventsConfig* carrying the target table name and write mode.
    """

    def __init__(self, spark: SparkSession, ws: WorkspaceClient, config: ActionEventsConfig) -> None:
        self._spark = spark
        self._ws = ws
        self._config = config

    def append(self, events: list[AlertEvent]) -> None:
        """Convert *events* to rows and append them to the configured Delta table.

        *observed_metrics* values are coerced to *str* because the Delta schema
        stores them as *MAP<STRING,STRING>*.

        Args:
            events: One or more *AlertEvent* records to persist.
        """
        if not events:
            return

        rows = [
            (
                e.action_name,
                e.condition,
                e.fired,
                e.status.value,
                {k: str(v) for k, v in e.observed_metrics.items()},
                e.run_id,
                e.run_time,
                e.input_location,
                e.destinations,
                e.delivery_errors,
            )
            for e in events
        ]

        df = self._spark.createDataFrame(rows, schema=ACTION_EVENT_TABLE_SCHEMA)
        output_config = OutputConfig(location=self._config.location, mode=self._config.mode)
        save_dataframe_as_table(df, output_config)
        logger.info(f"Appended {len(events)} event(s) to '{self._config.location}'.")

    def load_latest_per_action(self) -> dict[str, AlertEvent]:
        """Read the Delta table and return the most recent *AlertEvent* per action.

        Returns an empty dict when the table does not exist or contains no rows.

        Returns:
            Mapping of *action_name* to its latest *AlertEvent*.
        """
        if not self._spark.catalog.tableExists(self._config.location):
            logger.info(f"Table '{self._config.location}' does not exist; returning empty state.")
            return {}

        df = self._spark.read.table(self._config.location)

        window_spec = Window.partitionBy("action_name").orderBy(F.desc("run_time"))
        ranked = df.withColumn("_rank", F.row_number().over(window_spec)).filter(F.col("_rank") == 1).drop("_rank")

        result: dict[str, AlertEvent] = {}
        for row in ranked.collect():
            alert_event = AlertEvent(
                action_name=row["action_name"],
                condition=row["condition"],
                fired=row["fired"],
                status=ActionStatus(row["status"]),
                observed_metrics=dict(row["observed_metrics"]) if row["observed_metrics"] else {},
                run_id=row["run_id"],
                run_time=row["run_time"],
                input_location=row["input_location"],
                destinations=list(row["destinations"]) if row["destinations"] else [],
                delivery_errors=list(row["delivery_errors"]) if row["delivery_errors"] else [],
            )
            result[row["action_name"]] = alert_event

        logger.info(f"Loaded latest events for {len(result)} action(s) from '{self._config.location}'.")
        return result


# ---------------------------------------------------------------------------
# LakebaseActionEventStore
# ---------------------------------------------------------------------------


class LakebaseActionEventStore(ActionEventStore):
    """Persists *AlertEvent* records to a Lakebase (PostgreSQL) table via SQLAlchemy.

    Mirrors the pattern from *LakebaseChecksStorageHandler* in
    *databricks.labs.dqx.checks_storage*: engine creation with a
    *do_connect* listener that refreshes the Databricks-generated credential
    token before each connection, and schema / table bootstrap on first use.

    The *observed_metrics* dict is serialized to a PostgreSQL *JSONB* column;
    values are stored as JSON-compatible objects (coerced to *str* on write
    and returned as *dict[str, object]* on read).

    Args:
        spark: Active *SparkSession* (kept for interface symmetry; not used for
            PostgreSQL queries but may be used for future cross-engine queries).
        ws: Authenticated *WorkspaceClient* used to retrieve the Lakebase
            instance DNS and generate short-lived credentials.
        config: *LakebaseActionsStorageConfig* with instance and table details.
        engine: Optional pre-built SQLAlchemy *Engine* (useful for testing
            without a real Lakebase instance).
    """

    def __init__(
        self,
        spark: SparkSession,
        ws: WorkspaceClient,
        config: LakebaseActionsStorageConfig,
        engine: Engine | None = None,
    ) -> None:
        self._spark = spark
        self._ws = ws
        self._config = config
        self._engine = engine

    def _get_engine(self) -> Engine:
        """Return the cached SQLAlchemy engine, creating it on first call.

        Returns:
            SQLAlchemy *Engine* configured for Lakebase (PostgreSQL).
        """
        if self._engine is None:
            self._engine = self._create_engine()
        return self._engine

    def _create_engine(self) -> Engine:
        """Build a SQLAlchemy engine with a credential-refresh listener.

        Returns:
            A newly created SQLAlchemy *Engine*.
        """
        url = self._connection_url()
        engine = create_engine(
            url,
            pool_recycle=45 * 60,
            connect_args={"sslmode": "require"},
            pool_size=4,
        )
        event.listen(engine, "do_connect", self._before_connect_listener())
        return engine

    def _connection_url(self) -> str:
        """Construct a *postgresql+psycopg2* connection URL for the Lakebase instance.

        Returns:
            Connection URL string.
        """
        instance = self._ws.database.get_database_instance(self._config.instance_name)
        host = instance.read_write_dns
        user = self._config.client_id if self._config.client_id else self._ws.current_user.me().user_name
        return f"postgresql+psycopg2://{user}@{host}:{self._config.port}/{self._config.database_name}"

    def _before_connect_listener(self):
        """Return a *do_connect* event listener that injects a fresh credential token.

        Returns:
            A callable suitable for *event.listen(engine, "do_connect", …)*.
        """
        instance_name = self._config.instance_name

        def _before_connect(_dialect, _conn_rec, _cargs, cparams) -> None:
            cred = self._ws.database.generate_database_credential(
                request_id=str(uuid.uuid4()), instance_names=[instance_name]
            )
            cparams["password"] = cred.token

        return _before_connect

    @staticmethod
    def _get_table_definition(schema_name: str, table_name: str) -> Table:
        """Build a SQLAlchemy *Table* definition for the events table.

        Args:
            schema_name: PostgreSQL schema to contain the table.
            table_name: Name of the events table.

        Returns:
            A SQLAlchemy *Table* object.
        """
        return Table(
            table_name,
            MetaData(schema=schema_name),
            Column("action_name", String(255)),
            Column("condition", Text),
            Column("fired", Boolean),
            Column("status", String(50)),
            Column("observed_metrics", JSONB),
            Column("run_id", String(255)),
            Column("run_time", DateTime(timezone=True)),
            Column("input_location", Text),
            Column("destinations", JSONB),
            Column("delivery_errors", JSONB),
        )

    def _bootstrap(self, engine: Engine) -> None:
        """Ensure schema and table exist, creating them if necessary.

        Args:
            engine: SQLAlchemy *Engine* to use for DDL.
        """
        with engine.begin() as conn:
            if not conn.dialect.has_schema(conn, self._config.schema_name):
                conn.execute(CreateSchema(self._config.schema_name))
                logger.info(f"Created schema '{self._config.schema_name}'.")

        table = self._get_table_definition(self._config.schema_name, self._config.table_name)
        table.metadata.create_all(engine, checkfirst=True)
        logger.info(f"Bootstrapped table '{self._config.location}'.")

    def append(self, events: list[AlertEvent]) -> None:
        """Persist *events* to the Lakebase table.

        Bootstraps the schema and table on first use.  *observed_metrics*
        values are coerced to *str* before serialization into JSONB.

        Args:
            events: One or more *AlertEvent* records to persist.
        """
        if not events:
            return

        engine = self._get_engine()
        self._bootstrap(engine)

        table = self._get_table_definition(self._config.schema_name, self._config.table_name)
        rows = [
            {
                "action_name": e.action_name,
                "condition": e.condition,
                "fired": e.fired,
                "status": e.status.value,
                "observed_metrics": {k: str(v) for k, v in e.observed_metrics.items()},
                "run_id": e.run_id,
                "run_time": e.run_time,
                "input_location": e.input_location,
                "destinations": e.destinations,
                "delivery_errors": e.delivery_errors,
            }
            for e in events
        ]

        with engine.begin() as conn:
            conn.execute(insert(table), rows)
        logger.info(f"Appended {len(events)} event(s) to Lakebase table '{self._config.location}'.")

    def load_latest_per_action(self) -> dict[str, AlertEvent]:
        """Read the Lakebase table and return the most recent *AlertEvent* per action.

        Returns an empty dict when the table does not exist or contains no rows.

        Returns:
            Mapping of *action_name* to its latest *AlertEvent*.
        """
        engine = self._get_engine()
        inspector = inspect(engine)
        if not inspector.has_table(self._config.table_name, schema=self._config.schema_name):
            logger.info(f"Table '{self._config.location}' not found in Lakebase; returning empty state.")
            return {}

        table = self._get_table_definition(self._config.schema_name, self._config.table_name)

        # Subquery: rank rows per action_name by run_time descending.
        # Use a raw approach: load all and deduplicate in Python for portability.
        stmt = select(table).order_by(table.c.run_time.desc())

        result: dict[str, AlertEvent] = {}
        with engine.connect() as conn:
            rows = conn.execute(stmt).mappings().all()

        for row in rows:
            action_name = row["action_name"]
            if action_name in result:
                continue  # already have the most recent (ordered by run_time desc)
            metrics_raw = row["observed_metrics"] or {}
            result[action_name] = AlertEvent(
                action_name=action_name,
                condition=row["condition"],
                fired=row["fired"],
                status=ActionStatus(row["status"]),
                observed_metrics=dict(metrics_raw),
                run_id=row["run_id"],
                run_time=row["run_time"],
                input_location=row["input_location"],
                destinations=list(row["destinations"]) if row["destinations"] else [],
                delivery_errors=list(row["delivery_errors"]) if row["delivery_errors"] else [],
            )

        logger.info(f"Loaded latest events for {len(result)} action(s) from Lakebase '{self._config.location}'.")
        return result


# ---------------------------------------------------------------------------
# ActionEventStoreFactory
# ---------------------------------------------------------------------------


class ActionEventStoreFactory:
    """Creates the appropriate *ActionEventStore* implementation.

    Selection logic:

    - If *config* is a *LakebaseActionsStorageConfig* (has an *instance_name*
      field), return a *LakebaseActionEventStore*.
    - Otherwise (plain *ActionEventsConfig*), return a *TableActionEventStore*.

    Args:
        config: Storage configuration; either *ActionEventsConfig* or
            *LakebaseActionsStorageConfig*.
        spark: Active *SparkSession*.
        ws: Authenticated *WorkspaceClient*.

    Returns:
        A concrete *ActionEventStore* instance.
    """

    @staticmethod
    def create(
        config: ActionEventsConfig | LakebaseActionsStorageConfig,
        spark: SparkSession,
        ws: WorkspaceClient,
    ) -> ActionEventStore:
        """Instantiate the correct event store for *config*.

        Args:
            config: Storage configuration.
            spark: Active *SparkSession*.
            ws: Authenticated *WorkspaceClient*.

        Returns:
            *LakebaseActionEventStore* when *config* is a
            *LakebaseActionsStorageConfig*; *TableActionEventStore* otherwise.
        """
        if isinstance(config, LakebaseActionsStorageConfig):
            return LakebaseActionEventStore(spark=spark, ws=ws, config=config)
        return TableActionEventStore(spark=spark, ws=ws, config=config)


__all__ = [
    "ACTION_EVENT_TABLE_SCHEMA",
    "ActionEventStoreFactory",
    "LakebaseActionEventStore",
    "TableActionEventStore",
]
