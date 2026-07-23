"""Unit tests for databricks.labs.dqx.actions.event_storage.

Tests cover observable behaviour of:

- *TableActionEventStore.append* — empty-list early-return, DataFrame creation from events,
  append-only *OutputConfig* mode, run_config_name stamping, observed_metrics coercion to str.
- *TableActionEventStore.load_latest_per_action* — missing table returns empty dict,
  window de-duplication, row-to-AlertEvent mapping, None-safe fields.
- *TableActionEventStore.load_last_fired_per_action* — missing table returns empty dict,
  groupBy result mapping.
- *LakebaseActionEventStore.append* — empty-list early-return, bootstrap invoked before
  insert, rows built correctly, run_config_name stamping, observed_metrics coercion.
- *LakebaseActionEventStore.load_latest_per_action* — missing table returns empty dict,
  latest-per-action deduplication in Python, None-safe fields.
- *LakebaseActionEventStore.load_last_fired_per_action* — missing table returns empty dict,
  first-seen-wins deduplication, result mapping.
- *ActionEventStoreFactory.create* — returns *LakebaseActionEventStore* for
  *LakebaseActionsStorageConfig*, *TableActionEventStore* for plain *ActionEventsConfig*.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, create_autospec, patch

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.catalog import Catalog
from sqlalchemy import Engine
from sqlalchemy.engine import Connection, Inspector
from sqlalchemy.engine.interfaces import Dialect

import databricks.labs.dqx.actions.event_storage as event_storage_mod
from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.actions.base import ActionStatus
from databricks.labs.dqx.actions.event_storage import (
    ACTION_EVENT_TABLE_SCHEMA,
    ActionEventStoreFactory,
    LakebaseActionEventStore,
    TableActionEventStore,
)
from databricks.labs.dqx.actions.state import AlertEvent
from databricks.labs.dqx.config import ActionEventsConfig, LakebaseActionsStorageConfig


# ---------------------------------------------------------------------------
# Spark / SQLAlchemy mock factories
# ---------------------------------------------------------------------------


def _make_spark(table_exists: bool = False) -> MagicMock:
    """Return a *SparkSession* autospec with a fully spec'd *catalog* attribute.

    *SparkSession.catalog* is a *cached_property*, so *create_autospec* cannot
    set attributes on it directly. This helper replaces the catalog with a
    dedicated *Catalog* autospec so that *tableExists* can be configured.

    Returning *MagicMock* (rather than *SparkSession*) lets mypy understand that
    mock-specific attributes such as *call_args* and *assert_called_once_with*
    are available on the returned object.
    """
    spark = create_autospec(SparkSession)
    catalog = create_autospec(Catalog)
    catalog.tableExists.return_value = table_exists
    spark.catalog = catalog
    return spark  # type: ignore[return-value]


def _make_conn(schema_exists: bool = True) -> MagicMock:
    """Return a *Connection* autospec with a spec'd *dialect* that reports *schema_exists*.

    Returning *MagicMock* lets mypy recognise mock-specific attributes
    (*call_args*, *call_count*, etc.) on the returned object.
    """
    conn = create_autospec(Connection, instance=True)
    dialect = create_autospec(Dialect, instance=True)
    dialect.has_schema.return_value = schema_exists
    conn.dialect = dialect  # type: ignore[assignment]
    return conn


# ---------------------------------------------------------------------------
# Shared constants and helpers
# ---------------------------------------------------------------------------

_NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)

_TABLE_CONFIG = ActionEventsConfig(location="catalog.schema.events", run_config_name="prod")
_LAKEBASE_CONFIG = LakebaseActionsStorageConfig(
    location="mydb.myschema.events",
    instance_name="my-lakebase",
    run_config_name="prod",
)


def _make_event(
    action_name: str = "action-a",
    fired: bool = True,
    status: ActionStatus = ActionStatus.UNHEALTHY,
    run_time: datetime | None = None,
    observed_metrics: dict[str, object] | None = None,
) -> AlertEvent:
    return AlertEvent(
        action_name=action_name,
        condition="error_count > 0",
        fired=fired,
        status=status,
        observed_metrics=observed_metrics if observed_metrics is not None else {"error_count": 3},
        run_id="run-001",
        run_time=run_time or _NOW,
        input_location="catalog.schema.input",
        destinations=["slack"],
        delivery_errors=[],
        run_config_name="prod",
    )


class _Row:
    """Minimal subscript-access test double for a Spark/DB Row."""

    def __init__(self, data: dict[str, object]) -> None:
        self._data = data

    def __getitem__(self, key: str) -> object:
        return self._data[key]


def _make_spark_row(
    action_name: str = "action-a",
    fired: bool = True,
    status: str = "unhealthy",
    run_time: datetime | None = None,
    observed_metrics: dict[str, str] | None = None,
    destinations: list[str] | None = None,
    delivery_errors: list[str] | None = None,
    run_config_name: str = "prod",
) -> _Row:
    """Return a subscript-accessible test double for a Spark Row."""
    return _Row(
        {
            "action_name": action_name,
            "condition": "error_count > 0",
            "fired": fired,
            "status": status,
            "observed_metrics": observed_metrics,
            "run_id": "run-001",
            "run_time": run_time or _NOW,
            "input_location": "catalog.schema.input",
            "destinations": destinations,
            "delivery_errors": delivery_errors,
            "run_config_name": run_config_name,
        }
    )


def _make_db_row(
    action_name: str = "action-a",
    fired: bool = True,
    status: str = "unhealthy",
    run_time: datetime | None = None,
    observed_metrics: dict[str, object] | None = None,
    destinations: list[str] | None = None,
    delivery_errors: list[str] | None = None,
    run_config_name: str = "prod",
) -> _Row:
    """Return a subscript-accessible test double for a database mapping row."""
    return _Row(
        {
            "action_name": action_name,
            "condition": "error_count > 0",
            "fired": fired,
            "status": status,
            "observed_metrics": observed_metrics,
            "run_id": "run-001",
            "run_time": run_time or _NOW,
            "input_location": "catalog.schema.input",
            "destinations": destinations,
            "delivery_errors": delivery_errors,
            "run_config_name": run_config_name,
        }
    )


# ---------------------------------------------------------------------------
# TableActionEventStore — append
# ---------------------------------------------------------------------------


def test_table_store_append_empty_list_is_noop() -> None:
    """append([]) must return immediately without calling Spark."""
    spark = create_autospec(SparkSession)
    ws = create_autospec(WorkspaceClient, instance=True)
    store = TableActionEventStore(spark=spark, ws=ws, config=_TABLE_CONFIG)

    store.append([])

    spark.createDataFrame.assert_not_called()


def test_table_store_append_calls_create_dataframe_and_saves() -> None:
    """append(events) creates a DataFrame with the correct schema and saves it in append mode."""
    spark = create_autospec(SparkSession)
    ws = create_autospec(WorkspaceClient, instance=True)
    store = TableActionEventStore(spark=spark, ws=ws, config=_TABLE_CONFIG)
    event = _make_event()
    spark.createDataFrame.return_value = create_autospec(SparkSession)

    with patch.object(event_storage_mod, "save_dataframe_as_table") as mock_save:
        store.append([event])

    spark.createDataFrame.assert_called_once()
    rows = spark.createDataFrame.call_args.args[0]
    schema = spark.createDataFrame.call_args.kwargs["schema"]
    assert schema == ACTION_EVENT_TABLE_SCHEMA
    assert len(rows) == 1
    row = rows[0]
    assert row[0] == event.action_name
    assert row[2] == event.fired
    assert row[3] == event.status.value
    # observed_metrics values are coerced to str
    assert row[4] == {"error_count": "3"}
    # run_config_name stamped from config
    assert row[10] == "prod"
    mock_save.assert_called_once()
    saved_config = mock_save.call_args[0][1]
    assert saved_config.mode == "append"
    assert saved_config.location == "catalog.schema.events"


def test_table_store_append_metrics_coerced_to_str() -> None:
    """append coerces observed_metrics values to str before writing."""
    spark = create_autospec(SparkSession)
    ws = create_autospec(WorkspaceClient, instance=True)
    store = TableActionEventStore(spark=spark, ws=ws, config=_TABLE_CONFIG)
    event = _make_event(observed_metrics={"count": 42, "rate": 0.5})
    spark.createDataFrame.return_value = create_autospec(SparkSession)

    with patch.object(event_storage_mod, "save_dataframe_as_table"):
        store.append([event])

    rows = spark.createDataFrame.call_args.args[0]
    assert rows[0][4] == {"count": "42", "rate": "0.5"}


def test_table_store_append_run_config_stamp_from_config() -> None:
    """The run_config_name in the row comes from the store config, not the event."""
    spark = create_autospec(SparkSession)
    ws = create_autospec(WorkspaceClient, instance=True)
    config = ActionEventsConfig(location="catalog.schema.events", run_config_name="my-run-config")
    store = TableActionEventStore(spark=spark, ws=ws, config=config)
    event = _make_event()
    spark.createDataFrame.return_value = create_autospec(SparkSession)

    with patch.object(event_storage_mod, "save_dataframe_as_table"):
        store.append([event])

    rows = spark.createDataFrame.call_args.args[0]
    assert rows[0][10] == "my-run-config"


# ---------------------------------------------------------------------------
# TableActionEventStore — load_latest_per_action
# ---------------------------------------------------------------------------


def test_table_store_load_latest_returns_empty_when_table_missing() -> None:
    """load_latest_per_action returns {} when the table does not exist."""
    spark = _make_spark(table_exists=False)
    ws = create_autospec(WorkspaceClient, instance=True)
    store = TableActionEventStore(spark=spark, ws=ws, config=_TABLE_CONFIG)

    result = store.load_latest_per_action()

    assert not result
    spark.catalog.tableExists.assert_called_once_with("catalog.schema.events")


def test_table_store_load_latest_returns_events_per_action() -> None:
    """load_latest_per_action reads the table, applies window, and maps rows to AlertEvents."""
    spark = _make_spark(table_exists=True)
    ws = create_autospec(WorkspaceClient, instance=True)

    row = _make_spark_row(
        action_name="action-a",
        fired=True,
        status="unhealthy",
        run_time=_NOW,
        observed_metrics={"error_count": "3"},
        destinations=["slack"],
        delivery_errors=[],
        run_config_name="prod",
    )

    # Build the DataFrame chain: read.table().filter().withColumn().filter().drop().collect()
    mock_dropped = create_autospec(DataFrame)
    mock_dropped.collect.return_value = [row]
    mock_filtered2 = create_autospec(DataFrame)
    mock_filtered2.drop.return_value = mock_dropped
    mock_with_col = create_autospec(DataFrame)
    mock_with_col.filter.return_value = mock_filtered2
    mock_filtered1 = create_autospec(DataFrame)
    mock_filtered1.withColumn.return_value = mock_with_col
    mock_table_df = create_autospec(DataFrame)
    mock_table_df.filter.return_value = mock_filtered1
    spark.read.table.return_value = mock_table_df

    store = TableActionEventStore(spark=spark, ws=ws, config=_TABLE_CONFIG)

    with patch.object(event_storage_mod, "Window"), patch.object(event_storage_mod, "F"):
        result = store.load_latest_per_action()

    assert "action-a" in result
    evt = result["action-a"]
    assert isinstance(evt, AlertEvent)
    assert evt.action_name == "action-a"
    assert evt.fired is True
    assert evt.status == ActionStatus.UNHEALTHY
    assert evt.observed_metrics == {"error_count": "3"}
    assert evt.destinations == ["slack"]
    assert evt.delivery_errors == []
    assert evt.run_config_name == "prod"


def test_table_store_load_latest_none_safe_fields() -> None:
    """load_latest_per_action handles None observed_metrics, destinations, delivery_errors."""
    spark = _make_spark(table_exists=True)
    ws = create_autospec(WorkspaceClient, instance=True)

    row = _make_spark_row(
        action_name="action-b",
        fired=False,
        status="healthy",
        observed_metrics=None,
        destinations=None,
        delivery_errors=None,
        run_config_name="prod",
    )

    mock_dropped = create_autospec(DataFrame)
    mock_dropped.collect.return_value = [row]
    mock_filtered2 = create_autospec(DataFrame)
    mock_filtered2.drop.return_value = mock_dropped
    mock_with_col = create_autospec(DataFrame)
    mock_with_col.filter.return_value = mock_filtered2
    mock_filtered1 = create_autospec(DataFrame)
    mock_filtered1.withColumn.return_value = mock_with_col
    mock_table_df = create_autospec(DataFrame)
    mock_table_df.filter.return_value = mock_filtered1
    spark.read.table.return_value = mock_table_df

    store = TableActionEventStore(spark=spark, ws=ws, config=_TABLE_CONFIG)

    with patch.object(event_storage_mod, "Window"), patch.object(event_storage_mod, "F"):
        result = store.load_latest_per_action()

    evt = result["action-b"]
    assert evt.observed_metrics == {}
    assert evt.destinations == []
    assert evt.delivery_errors == []


# ---------------------------------------------------------------------------
# TableActionEventStore — load_last_fired_per_action
# ---------------------------------------------------------------------------


def test_table_store_load_last_fired_returns_empty_when_table_missing() -> None:
    """load_last_fired_per_action returns {} when the table does not exist."""
    spark = _make_spark(table_exists=False)
    ws = create_autospec(WorkspaceClient, instance=True)
    store = TableActionEventStore(spark=spark, ws=ws, config=_TABLE_CONFIG)

    result = store.load_last_fired_per_action()

    assert not result


def test_table_store_load_last_fired_returns_mapping() -> None:
    """load_last_fired_per_action returns action_name -> run_time for fired events."""
    spark = _make_spark(table_exists=True)
    ws = create_autospec(WorkspaceClient, instance=True)

    agg_row = _Row({"action_name": "action-x", "last_fired": _NOW})

    mock_agg = create_autospec(DataFrame)
    mock_agg.collect.return_value = [agg_row]
    mock_grouped = create_autospec(DataFrame)
    mock_grouped.agg.return_value = mock_agg
    mock_filtered = create_autospec(DataFrame)
    mock_filtered.groupBy.return_value = mock_grouped
    mock_table_df = create_autospec(DataFrame)
    mock_table_df.filter.return_value = mock_filtered
    spark.read.table.return_value = mock_table_df

    store = TableActionEventStore(spark=spark, ws=ws, config=_TABLE_CONFIG)

    with patch.object(event_storage_mod, "F"):
        result = store.load_last_fired_per_action()

    assert result == {"action-x": _NOW}


# ---------------------------------------------------------------------------
# LakebaseActionEventStore — append
# ---------------------------------------------------------------------------


def test_lakebase_append_empty_list_is_noop() -> None:
    """append([]) returns without touching the engine."""
    spark = create_autospec(SparkSession)
    ws = create_autospec(WorkspaceClient, instance=True)
    engine = create_autospec(Engine, instance=True)
    store = LakebaseActionEventStore(spark=spark, ws=ws, config=_LAKEBASE_CONFIG, engine=engine)

    store.append([])

    engine.begin.assert_not_called()


def test_lakebase_append_bootstraps_then_inserts() -> None:
    """append(events) calls bootstrap (schema check + DDL) then executes INSERT."""
    spark = create_autospec(SparkSession)
    ws = create_autospec(WorkspaceClient, instance=True)
    engine = create_autospec(Engine, instance=True)

    conn = _make_conn(schema_exists=True)
    engine.begin.return_value.__enter__ = lambda s: conn
    engine.begin.return_value.__exit__ = lambda s, *a: None

    store = LakebaseActionEventStore(spark=spark, ws=ws, config=_LAKEBASE_CONFIG, engine=engine)
    event = _make_event()

    store.append([event])

    # engine.begin() must have been called (bootstrap + insert both use engine.begin)
    assert engine.begin.called
    # The INSERT was executed on the connection: conn.execute called with stmt + rows
    conn.execute.assert_called()
    execute_args = conn.execute.call_args[0]
    assert len(execute_args) == 2
    inserted_rows = execute_args[1]
    assert len(inserted_rows) == 1
    inserted = inserted_rows[0]
    assert inserted["action_name"] == event.action_name
    assert inserted["fired"] == event.fired
    assert inserted["status"] == event.status.value
    assert inserted["observed_metrics"] == {"error_count": "3"}
    assert inserted["run_config_name"] == "prod"


def test_lakebase_append_creates_schema_when_absent() -> None:
    """append creates the PostgreSQL schema when it does not yet exist."""
    spark = create_autospec(SparkSession)
    ws = create_autospec(WorkspaceClient, instance=True)
    engine = create_autospec(Engine, instance=True)

    conn = _make_conn(schema_exists=False)
    engine.begin.return_value.__enter__ = lambda s: conn
    engine.begin.return_value.__exit__ = lambda s, *a: None

    store = LakebaseActionEventStore(spark=spark, ws=ws, config=_LAKEBASE_CONFIG, engine=engine)
    event = _make_event()

    store.append([event])

    # conn.execute must have been called at least twice:
    # once with CreateSchema (bootstrap) and once with the INSERT stmt
    assert conn.execute.call_count >= 2


def test_lakebase_append_metrics_coerced_to_str() -> None:
    """append coerces observed_metrics values to str."""
    spark = create_autospec(SparkSession)
    ws = create_autospec(WorkspaceClient, instance=True)
    engine = create_autospec(Engine, instance=True)

    conn = _make_conn(schema_exists=True)
    engine.begin.return_value.__enter__ = lambda s: conn
    engine.begin.return_value.__exit__ = lambda s, *a: None

    store = LakebaseActionEventStore(spark=spark, ws=ws, config=_LAKEBASE_CONFIG, engine=engine)
    event = _make_event(observed_metrics={"count": 99, "score": 0.75})

    store.append([event])

    rows_arg = conn.execute.call_args[0][1]
    assert rows_arg[0]["observed_metrics"] == {"count": "99", "score": "0.75"}


def test_lakebase_append_run_config_stamped_from_config() -> None:
    """append stamps run_config_name from the store config, ignoring event.run_config_name."""
    spark = create_autospec(SparkSession)
    ws = create_autospec(WorkspaceClient, instance=True)
    engine = create_autospec(Engine, instance=True)

    conn = _make_conn(schema_exists=True)
    engine.begin.return_value.__enter__ = lambda s: conn
    engine.begin.return_value.__exit__ = lambda s, *a: None

    config = LakebaseActionsStorageConfig(
        location="mydb.myschema.events",
        instance_name="my-lakebase",
        run_config_name="overriding-config",
    )
    store = LakebaseActionEventStore(spark=spark, ws=ws, config=config, engine=engine)
    event = _make_event()  # event.run_config_name = "prod"

    store.append([event])

    rows_arg = conn.execute.call_args[0][1]
    assert rows_arg[0]["run_config_name"] == "overriding-config"


def test_lakebase_append_row_has_expected_columns() -> None:
    """append inserts rows with all required column names."""
    spark = create_autospec(SparkSession)
    ws = create_autospec(WorkspaceClient, instance=True)
    engine = create_autospec(Engine, instance=True)

    conn = _make_conn(schema_exists=True)
    engine.begin.return_value.__enter__ = lambda s: conn
    engine.begin.return_value.__exit__ = lambda s, *a: None

    store = LakebaseActionEventStore(spark=spark, ws=ws, config=_LAKEBASE_CONFIG, engine=engine)
    event = _make_event()

    store.append([event])

    rows_arg = conn.execute.call_args[0][1]
    row = rows_arg[0]
    expected_keys = {
        "action_name",
        "condition",
        "fired",
        "status",
        "observed_metrics",
        "run_id",
        "run_time",
        "input_location",
        "destinations",
        "delivery_errors",
        "run_config_name",
    }
    assert expected_keys == set(row.keys())


# ---------------------------------------------------------------------------
# LakebaseActionEventStore — load_latest_per_action
# ---------------------------------------------------------------------------


def test_lakebase_load_latest_returns_empty_when_table_missing() -> None:
    """load_latest_per_action returns {} when the table does not exist in Lakebase."""
    spark = create_autospec(SparkSession)
    ws = create_autospec(WorkspaceClient, instance=True)
    engine = create_autospec(Engine, instance=True)

    mock_inspector = create_autospec(Inspector, instance=True)
    mock_inspector.has_table.return_value = False

    store = LakebaseActionEventStore(spark=spark, ws=ws, config=_LAKEBASE_CONFIG, engine=engine)

    with patch.object(event_storage_mod, "inspect", return_value=mock_inspector):
        result = store.load_latest_per_action()

    assert not result
    mock_inspector.has_table.assert_called_once_with("events", schema="myschema")


def test_lakebase_load_latest_returns_events_per_action() -> None:
    """load_latest_per_action maps database rows to AlertEvent objects."""
    spark = create_autospec(SparkSession)
    ws = create_autospec(WorkspaceClient, instance=True)
    engine = create_autospec(Engine, instance=True)

    row = _make_db_row(
        action_name="action-a",
        fired=True,
        status="unhealthy",
        observed_metrics={"error_count": "3"},
        destinations=["slack"],
        delivery_errors=[],
    )

    mock_conn = create_autospec(Connection, instance=True)
    mock_conn.execute.return_value.mappings.return_value.all.return_value = [row]
    engine.connect.return_value.__enter__ = lambda s: mock_conn
    engine.connect.return_value.__exit__ = lambda s, *a: None

    mock_inspector = create_autospec(Inspector, instance=True)
    mock_inspector.has_table.return_value = True

    store = LakebaseActionEventStore(spark=spark, ws=ws, config=_LAKEBASE_CONFIG, engine=engine)

    with patch.object(event_storage_mod, "inspect", return_value=mock_inspector):
        result = store.load_latest_per_action()

    assert "action-a" in result
    evt = result["action-a"]
    assert isinstance(evt, AlertEvent)
    assert evt.action_name == "action-a"
    assert evt.status == ActionStatus.UNHEALTHY
    assert evt.observed_metrics == {"error_count": "3"}
    assert evt.destinations == ["slack"]
    assert evt.delivery_errors == []


def test_lakebase_load_latest_deduplicates_keeping_first() -> None:
    """load_latest_per_action keeps only the first row per action (run_time-desc order)."""
    spark = create_autospec(SparkSession)
    ws = create_autospec(WorkspaceClient, instance=True)
    engine = create_autospec(Engine, instance=True)

    newer = _make_db_row(action_name="action-a", fired=True, status="unhealthy", run_time=_NOW)
    older = _make_db_row(
        action_name="action-a",
        fired=False,
        status="healthy",
        run_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )

    mock_conn = create_autospec(Connection, instance=True)
    mock_conn.execute.return_value.mappings.return_value.all.return_value = [newer, older]
    engine.connect.return_value.__enter__ = lambda s: mock_conn
    engine.connect.return_value.__exit__ = lambda s, *a: None

    mock_inspector = create_autospec(Inspector, instance=True)
    mock_inspector.has_table.return_value = True

    store = LakebaseActionEventStore(spark=spark, ws=ws, config=_LAKEBASE_CONFIG, engine=engine)

    with patch.object(event_storage_mod, "inspect", return_value=mock_inspector):
        result = store.load_latest_per_action()

    assert len(result) == 1
    assert result["action-a"].fired is True
    assert result["action-a"].status == ActionStatus.UNHEALTHY


def test_lakebase_load_latest_none_safe_fields() -> None:
    """load_latest_per_action handles None observed_metrics, destinations, delivery_errors."""
    spark = create_autospec(SparkSession)
    ws = create_autospec(WorkspaceClient, instance=True)
    engine = create_autospec(Engine, instance=True)

    row = _make_db_row(
        action_name="action-b",
        fired=False,
        status="healthy",
        observed_metrics=None,
        destinations=None,
        delivery_errors=None,
    )

    mock_conn = create_autospec(Connection, instance=True)
    mock_conn.execute.return_value.mappings.return_value.all.return_value = [row]
    engine.connect.return_value.__enter__ = lambda s: mock_conn
    engine.connect.return_value.__exit__ = lambda s, *a: None

    mock_inspector = create_autospec(Inspector, instance=True)
    mock_inspector.has_table.return_value = True

    store = LakebaseActionEventStore(spark=spark, ws=ws, config=_LAKEBASE_CONFIG, engine=engine)

    with patch.object(event_storage_mod, "inspect", return_value=mock_inspector):
        result = store.load_latest_per_action()

    evt = result["action-b"]
    assert evt.observed_metrics == {}
    assert evt.destinations == []
    assert evt.delivery_errors == []


# ---------------------------------------------------------------------------
# LakebaseActionEventStore — load_last_fired_per_action
# ---------------------------------------------------------------------------


def test_lakebase_load_last_fired_returns_empty_when_table_missing() -> None:
    """load_last_fired_per_action returns {} when the table does not exist."""
    spark = create_autospec(SparkSession)
    ws = create_autospec(WorkspaceClient, instance=True)
    engine = create_autospec(Engine, instance=True)

    mock_inspector = create_autospec(Inspector, instance=True)
    mock_inspector.has_table.return_value = False

    store = LakebaseActionEventStore(spark=spark, ws=ws, config=_LAKEBASE_CONFIG, engine=engine)

    with patch.object(event_storage_mod, "inspect", return_value=mock_inspector):
        result = store.load_last_fired_per_action()

    assert not result


def test_lakebase_load_last_fired_returns_mapping() -> None:
    """load_last_fired_per_action maps action_name to run_time for fired events."""
    spark = create_autospec(SparkSession)
    ws = create_autospec(WorkspaceClient, instance=True)
    engine = create_autospec(Engine, instance=True)

    fired_row = _Row({"action_name": "action-x", "run_time": _NOW})

    mock_conn = create_autospec(Connection, instance=True)
    mock_conn.execute.return_value.mappings.return_value.all.return_value = [fired_row]
    engine.connect.return_value.__enter__ = lambda s: mock_conn
    engine.connect.return_value.__exit__ = lambda s, *a: None

    mock_inspector = create_autospec(Inspector, instance=True)
    mock_inspector.has_table.return_value = True

    store = LakebaseActionEventStore(spark=spark, ws=ws, config=_LAKEBASE_CONFIG, engine=engine)

    with patch.object(event_storage_mod, "inspect", return_value=mock_inspector):
        result = store.load_last_fired_per_action()

    assert result == {"action-x": _NOW}


def test_lakebase_load_last_fired_deduplicates_keeping_first() -> None:
    """load_last_fired_per_action keeps the first (most recent) row per action."""
    spark = create_autospec(SparkSession)
    ws = create_autospec(WorkspaceClient, instance=True)
    engine = create_autospec(Engine, instance=True)

    old_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
    recent = _Row({"action_name": "action-x", "run_time": _NOW})
    old = _Row({"action_name": "action-x", "run_time": old_time})

    mock_conn = create_autospec(Connection, instance=True)
    mock_conn.execute.return_value.mappings.return_value.all.return_value = [recent, old]
    engine.connect.return_value.__enter__ = lambda s: mock_conn
    engine.connect.return_value.__exit__ = lambda s, *a: None

    mock_inspector = create_autospec(Inspector, instance=True)
    mock_inspector.has_table.return_value = True

    store = LakebaseActionEventStore(spark=spark, ws=ws, config=_LAKEBASE_CONFIG, engine=engine)

    with patch.object(event_storage_mod, "inspect", return_value=mock_inspector):
        result = store.load_last_fired_per_action()

    assert result == {"action-x": _NOW}


# ---------------------------------------------------------------------------
# ActionEventStoreFactory — type dispatch
# ---------------------------------------------------------------------------


def test_factory_returns_table_store_for_action_events_config() -> None:
    """ActionEventStoreFactory.create returns TableActionEventStore for ActionEventsConfig."""
    spark = create_autospec(SparkSession)
    ws = create_autospec(WorkspaceClient, instance=True)
    config = ActionEventsConfig(location="catalog.schema.events")

    store = ActionEventStoreFactory.create(config=config, spark=spark, ws=ws)

    assert isinstance(store, TableActionEventStore)


def test_factory_returns_lakebase_store_for_lakebase_config() -> None:
    """ActionEventStoreFactory.create returns LakebaseActionEventStore for LakebaseActionsStorageConfig."""
    spark = create_autospec(SparkSession)
    ws = create_autospec(WorkspaceClient, instance=True)
    config = LakebaseActionsStorageConfig(
        location="mydb.myschema.events",
        instance_name="my-lakebase",
    )

    store = ActionEventStoreFactory.create(config=config, spark=spark, ws=ws)

    assert isinstance(store, LakebaseActionEventStore)


# ---------------------------------------------------------------------------
# LakebaseActionEventStore — table schema (verified through insert row keys)
# ---------------------------------------------------------------------------


def test_lakebase_append_table_definition_has_all_expected_columns() -> None:
    """The table definition used by append contains all expected column names.

    Verified indirectly: the inserted row dict must include every column that
    *ACTION_EVENT_TABLE_SCHEMA* defines for the Spark store, ensuring both stores
    are schema-consistent.
    """
    spark = create_autospec(SparkSession)
    ws = create_autospec(WorkspaceClient, instance=True)
    engine = create_autospec(Engine, instance=True)

    conn = _make_conn(schema_exists=True)
    engine.begin.return_value.__enter__ = lambda s: conn
    engine.begin.return_value.__exit__ = lambda s, *a: None

    store = LakebaseActionEventStore(spark=spark, ws=ws, config=_LAKEBASE_CONFIG, engine=engine)
    store.append([_make_event()])

    rows = conn.execute.call_args[0][1]
    assert set(rows[0].keys()) == {
        "action_name",
        "condition",
        "fired",
        "status",
        "observed_metrics",
        "run_id",
        "run_time",
        "input_location",
        "destinations",
        "delivery_errors",
        "run_config_name",
    }
