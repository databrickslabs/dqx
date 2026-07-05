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

import logging
from datetime import datetime

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
    inspect,
    insert,
    select,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.schema import CreateSchema

from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.actions.state import ActionEventStore, AlertEvent
from databricks.labs.dqx.actions.base import ActionStatus
from databricks.labs.dqx.actions.log_sanitize import sanitize_for_log
from databricks.labs.dqx.config import ActionEventsConfig, LakebaseActionsStorageConfig
from databricks.labs.dqx.io import save_dataframe_as_table
from databricks.labs.dqx.config import OutputConfig
from databricks.labs.dqx.lakebase_engine import LakebaseConnectionMixin
from databricks.labs.dqx.utils import to_utc

logger = logging.getLogger(__name__)

ACTION_EVENT_TABLE_SCHEMA = (
    "action_name STRING, condition STRING, fired BOOLEAN, status STRING, "
    "observed_metrics MAP<STRING,STRING>, run_id STRING, run_time TIMESTAMP, "
    "input_location STRING, destinations ARRAY<STRING>, delivery_errors ARRAY<STRING>, "
    "run_config_name STRING"
)


# ---------------------------------------------------------------------------
# TableActionEventStore
# ---------------------------------------------------------------------------


class TableActionEventStore(ActionEventStore):
    """Persists *AlertEvent* records to a Unity Catalog Delta table via Spark.

    Events are appended to a Delta table with the schema defined in
    *ACTION_EVENT_TABLE_SCHEMA*.  Every row is stamped with the store's
    *run_config_name*, and loading the latest event per action first filters to
    that *run_config_name* — so several run configs can share one events table
    without their alert suppression interfering.  Loading uses a window function
    partitioned by *action_name* and ordered by *run_time* descending, selecting
    rank == 1.

    Args:
        spark: Active *SparkSession*.
        ws: Authenticated *WorkspaceClient* (reserved for future use such as
            table-existence checks).
        config: *ActionEventsConfig* carrying the target table name and the *run_config_name* the
            events are scoped to.
    """

    def __init__(self, spark: SparkSession, ws: WorkspaceClient, config: ActionEventsConfig) -> None:
        self._spark = spark
        self._ws = ws
        self._config = config

    def append(self, events: list[AlertEvent]) -> None:
        """Convert *events* to rows and append them to the configured Delta table.

        *observed_metrics* values are coerced to *str* because the Delta schema
        stores them as a Spark *MAP* of string to string.

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
                self._config.run_config_name,
            )
            for e in events
        ]

        df = self._spark.createDataFrame(rows, schema=ACTION_EVENT_TABLE_SCHEMA)
        # Action events are an append-only audit log: record() appends one event at a time and several
        # run configs may share one table, so the events table is always appended to (never overwritten,
        # which would wipe prior history). This matches the Lakebase store, which always inserts.
        output_config = OutputConfig(location=self._config.location, mode="append")
        save_dataframe_as_table(df, output_config)
        logger.info(f"Appended {len(events)} event(s) to '{sanitize_for_log(self._config.location)}'.")

    def load_latest_per_action(self) -> dict[str, AlertEvent]:
        """Read the Delta table and return the most recent *AlertEvent* per action.

        Returns an empty dict when the table does not exist or contains no rows.

        Returns:
            Mapping of *action_name* to its latest *AlertEvent*.
        """
        safe_location = sanitize_for_log(self._config.location)
        if not self._spark.catalog.tableExists(self._config.location):
            logger.info(f"Table '{safe_location}' does not exist; returning empty state.")
            return {}

        df = self._spark.read.table(self._config.location).filter(
            F.col("run_config_name") == self._config.run_config_name
        )

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
                run_time=to_utc(row["run_time"]),
                input_location=row["input_location"],
                destinations=list(row["destinations"]) if row["destinations"] else [],
                delivery_errors=list(row["delivery_errors"]) if row["delivery_errors"] else [],
                run_config_name=row["run_config_name"],
            )
            result[row["action_name"]] = alert_event

        logger.info(f"Loaded latest events for {len(result)} action(s) from '{safe_location}'.")
        return result

    def load_last_fired_per_action(self) -> dict[str, datetime]:
        """Return the *run_time* of the most recent fired event per action for this run config.

        Returns an empty dict when the table does not exist or has no fired events.

        Returns:
            Mapping of *action_name* to the latest fired *run_time*.
        """
        if not self._spark.catalog.tableExists(self._config.location):
            return {}

        fired = self._spark.read.table(self._config.location).filter(
            (F.col("run_config_name") == self._config.run_config_name) & (F.col("fired"))
        )
        grouped = fired.groupBy("action_name").agg(F.max("run_time").alias("last_fired"))
        return {row["action_name"]: to_utc(row["last_fired"]) for row in grouped.collect()}


# ---------------------------------------------------------------------------
# LakebaseActionEventStore
# ---------------------------------------------------------------------------


class LakebaseActionEventStore(LakebaseConnectionMixin, ActionEventStore):
    """Persists *AlertEvent* records to a Lakebase (PostgreSQL) table via SQLAlchemy.

    Inherits engine lifecycle management from *LakebaseConnectionMixin*: a lazily
    created, cached engine with a *do_connect* listener that refreshes the
    Databricks-generated credential token before each connection, and schema /
    table bootstrap on first use.

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
        super().__init__(spark=spark, ws=ws, config=config, engine=engine)

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
            Column("run_config_name", String(255)),
        )

    def _bootstrap(self, engine: Engine) -> None:
        """Ensure schema and table exist, creating them if necessary.

        Args:
            engine: SQLAlchemy *Engine* to use for DDL.
        """
        with engine.begin() as conn:
            if not conn.dialect.has_schema(conn, self._config.schema_name):
                conn.execute(CreateSchema(self._config.schema_name))
                logger.info(f"Created schema '{sanitize_for_log(self._config.schema_name)}'.")

        table = self._get_table_definition(self._config.schema_name, self._config.table_name)
        table.metadata.create_all(engine, checkfirst=True)
        logger.info(f"Bootstrapped table '{sanitize_for_log(self._config.location)}'.")

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
                "run_config_name": self._config.run_config_name,
            }
            for e in events
        ]

        with engine.begin() as conn:
            conn.execute(insert(table), rows)
        logger.info(f"Appended {len(events)} event(s) to Lakebase table '{sanitize_for_log(self._config.location)}'.")

    def load_latest_per_action(self) -> dict[str, AlertEvent]:
        """Read the Lakebase table and return the most recent *AlertEvent* per action.

        Returns an empty dict when the table does not exist or contains no rows.

        Returns:
            Mapping of *action_name* to its latest *AlertEvent*.
        """
        engine = self._get_engine()
        inspector = inspect(engine)
        if not inspector.has_table(self._config.table_name, schema=self._config.schema_name):
            logger.info(
                f"Table '{sanitize_for_log(self._config.location)}' not found in Lakebase; returning empty state."
            )
            return {}

        table = self._get_table_definition(self._config.schema_name, self._config.table_name)

        # Subquery: rank rows per action_name by run_time descending.
        # Use a raw approach: load all (scoped to this run config) and deduplicate in Python for portability.
        stmt = (
            select(table)
            .where(table.c.run_config_name == self._config.run_config_name)
            .order_by(table.c.run_time.desc())
        )

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
                run_time=to_utc(row["run_time"]),
                input_location=row["input_location"],
                destinations=list(row["destinations"]) if row["destinations"] else [],
                delivery_errors=list(row["delivery_errors"]) if row["delivery_errors"] else [],
                run_config_name=row["run_config_name"],
            )

        logger.info(
            f"Loaded latest events for {len(result)} action(s) from Lakebase '{sanitize_for_log(self._config.location)}'."
        )
        return result

    def load_last_fired_per_action(self) -> dict[str, datetime]:
        """Return the *run_time* of the most recent fired event per action for this run config.

        Returns an empty dict when the table does not exist or has no fired events.

        Returns:
            Mapping of *action_name* to the latest fired *run_time*.
        """
        engine = self._get_engine()
        inspector = inspect(engine)
        if not inspector.has_table(self._config.table_name, schema=self._config.schema_name):
            return {}

        table = self._get_table_definition(self._config.schema_name, self._config.table_name)
        stmt = (
            select(table.c.action_name, table.c.run_time)
            .where((table.c.run_config_name == self._config.run_config_name) & (table.c.fired.is_(True)))
            .order_by(table.c.run_time.desc())
        )

        last_fired: dict[str, datetime] = {}
        with engine.connect() as conn:
            rows = conn.execute(stmt).mappings().all()
        for row in rows:
            action_name = row["action_name"]
            if action_name not in last_fired:  # rows are run_time-descending, so first seen is latest
                last_fired[action_name] = to_utc(row["run_time"])
        return last_fired


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
