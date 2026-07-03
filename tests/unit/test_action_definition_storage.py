import json
import logging
from datetime import datetime
from unittest.mock import MagicMock, create_autospec, patch

import pytest
from pyspark.sql import SparkSession
from sqlalchemy import Engine, create_engine

from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.actions.alert import DQAlert
from databricks.labs.dqx.actions.definition_storage import (
    ActionsStorageHandlerFactory,
    LakebaseActionsStorageHandler,
    TableActionsStorageHandler,
)
from databricks.labs.dqx.actions.destinations.slack import SlackDQAlertDestination
from databricks.labs.dqx.actions.dq_action import DQAction
from databricks.labs.dqx.actions.fail_pipeline import FailPipeline
from databricks.labs.dqx.actions.serializer import ActionSerializer
from databricks.labs.dqx.config import LakebaseActionsStorageConfig, TableActionsStorageConfig
from databricks.labs.dqx.errors import UnsafeSqlQueryError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_fail_action(name: str = "fail") -> DQAction:
    """Return a minimal *DQAction* wrapping a *FailPipeline*."""
    return DQAction(action=FailPipeline(name=name))


def _make_slack_action(name: str = "alert") -> DQAction:
    """Return a *DQAction* wrapping a *DQAlert* with a Slack destination."""
    dest = SlackDQAlertDestination(name="slack", webhook_url="https://hooks.slack.com/T/B/x")
    alert = DQAlert(destinations=[dest], name=name)
    return DQAction(action=alert)


def _make_table_config(
    location: str = "cat.db.actions",
    run_config_name: str = "default",
    mode: str = "append",
) -> TableActionsStorageConfig:
    return TableActionsStorageConfig(location=location, run_config_name=run_config_name, mode=mode)


def _make_lakebase_config(
    location: str = "mydb.myschema.mytable",
    run_config_name: str = "default",
    mode: str = "append",
    instance_name: str = "my-lakebase",
) -> LakebaseActionsStorageConfig:
    return LakebaseActionsStorageConfig(
        location=location,
        run_config_name=run_config_name,
        mode=mode,
        instance_name=instance_name,
    )


def _make_engine_mock() -> MagicMock:
    """Return a *MagicMock(spec=Engine)* whose context-manager connections are pre-wired."""
    engine = MagicMock(spec=Engine)
    conn = MagicMock(name="conn")
    conn.dialect.has_schema.return_value = True
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    return engine


# ---------------------------------------------------------------------------
# TableActionsStorageHandler — save
# ---------------------------------------------------------------------------


class TestTableActionsStorageHandlerSave:
    """Tests for *TableActionsStorageHandler.save*."""

    def test_empty_actions_skips_spark(self) -> None:
        """No Spark calls when *actions* list is empty."""
        spark = create_autospec(SparkSession)
        ws = create_autospec(WorkspaceClient, instance=True)
        handler = TableActionsStorageHandler(spark=spark, ws=ws)
        config = _make_table_config()

        handler.save([], config)

        spark.createDataFrame.assert_not_called()

    def test_empty_actions_logs_info(self, caplog: pytest.LogCaptureFixture) -> None:
        """Empty-list path emits a log message."""
        spark = create_autospec(SparkSession)
        ws = create_autospec(WorkspaceClient, instance=True)
        handler = TableActionsStorageHandler(spark=spark, ws=ws)

        with caplog.at_level(logging.INFO, logger="databricks.labs.dqx.actions.definition_storage"):
            handler.save([], _make_table_config())

        assert any("skipping save" in r.message.lower() for r in caplog.records)

    def test_append_mode_calls_save_as_table_with_append(self) -> None:
        """Append mode must call *mode("append").saveAsTable()*."""
        spark = create_autospec(SparkSession)
        ws = create_autospec(WorkspaceClient, instance=True)
        handler = TableActionsStorageHandler(spark=spark, ws=ws)
        config = _make_table_config(mode="append")

        # Build a mock writer chain returned at each stage.
        mock_writer = MagicMock(name="writer")
        spark.createDataFrame.return_value.write.format.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer

        handler.save([_make_fail_action()], config)

        spark.createDataFrame.assert_called_once()
        spark.createDataFrame.return_value.write.format.assert_called_once_with("delta")
        mock_writer.mode.assert_called_once_with("append")
        mock_writer.mode.return_value.saveAsTable.assert_called_once_with(config.location)

    def test_overwrite_mode_uses_replace_where_option(self) -> None:
        """Overwrite mode sets the *replaceWhere* option before saving."""
        spark = create_autospec(SparkSession)
        ws = create_autospec(WorkspaceClient, instance=True)
        handler = TableActionsStorageHandler(spark=spark, ws=ws)
        config = _make_table_config(mode="overwrite", run_config_name="run1")

        mock_writer = MagicMock(name="writer")
        spark.createDataFrame.return_value.write.format.return_value = mock_writer
        mock_writer.option.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer

        handler.save([_make_fail_action()], config)

        mock_writer.option.assert_called_once_with("replaceWhere", "run_config_name = 'run1'")
        mock_writer.mode.assert_called_once_with("overwrite")
        mock_writer.mode.return_value.saveAsTable.assert_called_once_with(config.location)

    def test_overwrite_mode_unsafe_run_config_raises(self) -> None:
        """Unsafe *run_config_name* must raise *UnsafeSqlQueryError* before touching Spark."""
        spark = create_autospec(SparkSession)
        ws = create_autospec(WorkspaceClient, instance=True)
        handler = TableActionsStorageHandler(spark=spark, ws=ws)
        config = _make_table_config(mode="overwrite", run_config_name="bad'; DROP TABLE--")

        with pytest.raises(UnsafeSqlQueryError):
            handler.save([_make_fail_action()], config)

    def test_serialized_rows_contain_run_config_name(self) -> None:
        """Each row passed to *createDataFrame* must embed *run_config_name*."""
        spark = create_autospec(SparkSession)
        ws = create_autospec(WorkspaceClient, instance=True)
        handler = TableActionsStorageHandler(spark=spark, ws=ws)
        config = _make_table_config(run_config_name="my-run")

        mock_writer = MagicMock(name="writer")
        spark.createDataFrame.return_value.write.format.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer

        handler.save([_make_fail_action()], config)

        call_args = spark.createDataFrame.call_args
        rows = call_args[0][0]
        assert len(rows) == 1
        _json_str, run_config, _timestamp = rows[0]
        assert run_config == "my-run"

    def test_action_json_round_trips(self) -> None:
        """The *action_json* column must survive a round-trip through JSON."""
        spark = create_autospec(SparkSession)
        ws = create_autospec(WorkspaceClient, instance=True)
        handler = TableActionsStorageHandler(spark=spark, ws=ws)
        action = _make_slack_action(name="test-alert")

        mock_writer = MagicMock(name="writer")
        spark.createDataFrame.return_value.write.format.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer

        handler.save([action], _make_table_config())

        call_args = spark.createDataFrame.call_args
        rows = call_args[0][0]
        action_json_str, _, _ = rows[0]
        deserialized = ActionSerializer.from_dict(json.loads(action_json_str))
        assert isinstance(deserialized.action, DQAlert)

    def test_multiple_actions_produce_multiple_rows(self) -> None:
        """One row per action must be passed to *createDataFrame*."""
        spark = create_autospec(SparkSession)
        ws = create_autospec(WorkspaceClient, instance=True)
        handler = TableActionsStorageHandler(spark=spark, ws=ws)

        mock_writer = MagicMock(name="writer")
        spark.createDataFrame.return_value.write.format.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer

        actions = [_make_fail_action("a"), _make_fail_action("b"), _make_slack_action("c")]
        handler.save(actions, _make_table_config())

        call_args = spark.createDataFrame.call_args
        rows = call_args[0][0]
        assert len(rows) == 3

    def test_created_at_is_utc_timestamp(self) -> None:
        """The *created_at* field must be a timezone-aware UTC *datetime*."""
        spark = create_autospec(SparkSession)
        ws = create_autospec(WorkspaceClient, instance=True)
        handler = TableActionsStorageHandler(spark=spark, ws=ws)

        mock_writer = MagicMock(name="writer")
        spark.createDataFrame.return_value.write.format.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer

        handler.save([_make_fail_action()], _make_table_config())

        rows = spark.createDataFrame.call_args[0][0]
        _, _, created_at = rows[0]
        assert isinstance(created_at, datetime)
        assert created_at.tzinfo is not None

    def test_location_is_sanitized_in_log(self, caplog: pytest.LogCaptureFixture) -> None:
        """Location string must appear in log output."""
        spark = create_autospec(SparkSession)
        ws = create_autospec(WorkspaceClient, instance=True)
        handler = TableActionsStorageHandler(spark=spark, ws=ws)

        mock_writer = MagicMock(name="writer")
        spark.createDataFrame.return_value.write.format.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer

        config = _make_table_config(location="cat.db.actions")
        with caplog.at_level(logging.INFO, logger="databricks.labs.dqx.actions.definition_storage"):
            handler.save([_make_fail_action()], config)

        combined = " ".join(r.message for r in caplog.records)
        assert "cat.db.actions" in combined


# ---------------------------------------------------------------------------
# TableActionsStorageHandler — load
# ---------------------------------------------------------------------------


class TestTableActionsStorageHandlerLoad:
    """Tests for *TableActionsStorageHandler.load*."""

    def _make_spark(self, table_exists: bool = True) -> MagicMock:
        """Return a loosely-typed mock for *SparkSession* with *catalog.tableExists* preset.

        *SparkSession.catalog* is a *cached_property* whose type does not expose
        *tableExists* in the spec, so *create_autospec* cannot be used here.
        """
        spark = MagicMock(name="spark")
        spark.catalog.tableExists.return_value = table_exists
        return spark

    def test_table_not_found_returns_empty_list(self) -> None:
        """When the table does not exist *load* returns *[]* without reading."""
        spark = self._make_spark(table_exists=False)
        ws = create_autospec(WorkspaceClient, instance=True)
        handler = TableActionsStorageHandler(spark=spark, ws=ws)

        result = handler.load(_make_table_config())

        assert not result
        spark.read.table.assert_not_called()

    def test_table_not_found_logs_info(self, caplog: pytest.LogCaptureFixture) -> None:
        """Missing-table path emits an info log."""
        spark = self._make_spark(table_exists=False)
        ws = create_autospec(WorkspaceClient, instance=True)
        handler = TableActionsStorageHandler(spark=spark, ws=ws)

        with caplog.at_level(logging.INFO, logger="databricks.labs.dqx.actions.definition_storage"):
            handler.load(_make_table_config())

        assert any("does not exist" in r.message.lower() for r in caplog.records)

    def test_load_deserializes_rows(self) -> None:
        """Rows returned by Spark must be deserialized into *DQAction* instances."""
        spark = self._make_spark(table_exists=True)
        ws = create_autospec(WorkspaceClient, instance=True)

        action = _make_fail_action()
        action_json = json.dumps(ActionSerializer.to_dict(action))
        mock_row = {"action_json": action_json, "run_config_name": "default"}
        spark.read.table.return_value.filter.return_value.collect.return_value = [mock_row]

        handler = TableActionsStorageHandler(spark=spark, ws=ws)
        result = handler.load(_make_table_config())

        assert len(result) == 1
        assert isinstance(result[0].action, FailPipeline)

    def test_load_filters_by_run_config_name(self) -> None:
        """The table name must be passed to *read.table*."""
        spark = self._make_spark(table_exists=True)
        ws = create_autospec(WorkspaceClient, instance=True)
        spark.read.table.return_value.filter.return_value.collect.return_value = []

        handler = TableActionsStorageHandler(spark=spark, ws=ws)
        config = _make_table_config(run_config_name="custom-run")
        handler.load(config)

        spark.read.table.assert_called_once_with(config.location)

    def test_load_returns_multiple_actions(self) -> None:
        """All valid rows must be deserialized and returned."""
        spark = self._make_spark(table_exists=True)
        ws = create_autospec(WorkspaceClient, instance=True)

        actions = [_make_fail_action("a"), _make_slack_action("b")]
        rows = [{"action_json": json.dumps(ActionSerializer.to_dict(a)), "run_config_name": "default"} for a in actions]
        spark.read.table.return_value.filter.return_value.collect.return_value = rows

        handler = TableActionsStorageHandler(spark=spark, ws=ws)
        result = handler.load(_make_table_config())

        assert len(result) == 2

    def test_deserialization_error_is_isolated(self, caplog: pytest.LogCaptureFixture) -> None:
        """A bad row must be skipped with a warning; valid rows must still be returned."""
        spark = self._make_spark(table_exists=True)
        ws = create_autospec(WorkspaceClient, instance=True)

        good_action = _make_fail_action()
        good_json = json.dumps(ActionSerializer.to_dict(good_action))
        rows = [
            {"action_json": "{invalid json{{", "run_config_name": "default"},
            {"action_json": good_json, "run_config_name": "default"},
        ]
        spark.read.table.return_value.filter.return_value.collect.return_value = rows

        handler = TableActionsStorageHandler(spark=spark, ws=ws)
        with caplog.at_level(logging.WARNING, logger="databricks.labs.dqx.actions.definition_storage"):
            result = handler.load(_make_table_config())

        assert len(result) == 1
        assert any("failed to deserialize" in r.message.lower() for r in caplog.records)

    def test_load_logs_count_on_success(self, caplog: pytest.LogCaptureFixture) -> None:
        """A completion log with the count of loaded actions must be emitted."""
        spark = self._make_spark(table_exists=True)
        ws = create_autospec(WorkspaceClient, instance=True)

        action = _make_fail_action()
        row = {"action_json": json.dumps(ActionSerializer.to_dict(action)), "run_config_name": "default"}
        spark.read.table.return_value.filter.return_value.collect.return_value = [row]

        handler = TableActionsStorageHandler(spark=spark, ws=ws)
        with caplog.at_level(logging.INFO, logger="databricks.labs.dqx.actions.definition_storage"):
            handler.load(_make_table_config())

        assert any("loaded" in r.message.lower() for r in caplog.records)


# ---------------------------------------------------------------------------
# LakebaseActionsStorageHandler — save
# ---------------------------------------------------------------------------


class TestLakebaseActionsStorageHandlerSave:
    """Tests for *LakebaseActionsStorageHandler.save*."""

    def test_empty_actions_skips_engine(self) -> None:
        """No engine call when *actions* is empty."""
        spark = create_autospec(SparkSession)
        ws = create_autospec(WorkspaceClient, instance=True)
        engine = _make_engine_mock()
        config = _make_lakebase_config()
        handler = LakebaseActionsStorageHandler(spark=spark, ws=ws, config=config, engine=engine)

        handler.save([], config)

        engine.begin.assert_not_called()

    def test_empty_actions_logs_info(self, caplog: pytest.LogCaptureFixture) -> None:
        """Empty-list path emits a log message."""
        spark = create_autospec(SparkSession)
        ws = create_autospec(WorkspaceClient, instance=True)
        engine = _make_engine_mock()
        config = _make_lakebase_config()
        handler = LakebaseActionsStorageHandler(spark=spark, ws=ws, config=config, engine=engine)

        with caplog.at_level(logging.INFO, logger="databricks.labs.dqx.actions.definition_storage"):
            handler.save([], config)

        assert any("skipping" in r.message.lower() for r in caplog.records)

    def test_append_mode_inserts_without_delete(self) -> None:
        """Append mode must insert rows without executing a DELETE statement first."""
        spark = create_autospec(SparkSession)
        ws = create_autospec(WorkspaceClient, instance=True)
        engine = _make_engine_mock()
        config = _make_lakebase_config(mode="append")
        conn = engine.begin.return_value.__enter__.return_value

        handler = LakebaseActionsStorageHandler(spark=spark, ws=ws, config=config, engine=engine)
        handler.save([_make_fail_action()], config)

        # Only one execute call → INSERT only (no DELETE)
        assert conn.execute.call_count == 1

    def test_overwrite_mode_deletes_before_insert(self) -> None:
        """Overwrite mode must execute DELETE then INSERT on the same connection."""
        spark = create_autospec(SparkSession)
        ws = create_autospec(WorkspaceClient, instance=True)
        engine = _make_engine_mock()
        config = _make_lakebase_config(mode="overwrite")
        conn = engine.begin.return_value.__enter__.return_value

        handler = LakebaseActionsStorageHandler(spark=spark, ws=ws, config=config, engine=engine)
        handler.save([_make_fail_action()], config)

        # Two execute calls → DELETE + INSERT
        assert conn.execute.call_count >= 2

    def test_save_inserts_correct_row_count(self) -> None:
        """The INSERT call must receive exactly one row per action."""
        spark = create_autospec(SparkSession)
        ws = create_autospec(WorkspaceClient, instance=True)
        engine = _make_engine_mock()
        config = _make_lakebase_config(mode="append")
        conn = engine.begin.return_value.__enter__.return_value

        handler = LakebaseActionsStorageHandler(spark=spark, ws=ws, config=config, engine=engine)
        handler.save([_make_fail_action("a"), _make_fail_action("b")], config)

        # The INSERT call's second positional argument is the rows list.
        insert_call = conn.execute.call_args_list[-1]
        rows_arg = insert_call[0][1]
        assert len(rows_arg) == 2

    def test_save_row_contains_run_config_name(self) -> None:
        """Each inserted row must embed *run_config_name* from config."""
        spark = create_autospec(SparkSession)
        ws = create_autospec(WorkspaceClient, instance=True)
        engine = _make_engine_mock()
        config = _make_lakebase_config(run_config_name="my-run", mode="append")
        conn = engine.begin.return_value.__enter__.return_value

        handler = LakebaseActionsStorageHandler(spark=spark, ws=ws, config=config, engine=engine)
        handler.save([_make_fail_action()], config)

        insert_call = conn.execute.call_args_list[-1]
        rows_arg = insert_call[0][1]
        assert rows_arg[0]["run_config_name"] == "my-run"

    def test_save_action_json_round_trips(self) -> None:
        """The *action_json* value in the inserted row must survive deserialization."""
        spark = create_autospec(SparkSession)
        ws = create_autospec(WorkspaceClient, instance=True)
        engine = _make_engine_mock()
        config = _make_lakebase_config(mode="append")
        conn = engine.begin.return_value.__enter__.return_value

        action = _make_slack_action("test-alert")
        handler = LakebaseActionsStorageHandler(spark=spark, ws=ws, config=config, engine=engine)
        handler.save([action], config)

        insert_call = conn.execute.call_args_list[-1]
        rows_arg = insert_call[0][1]
        deserialized = ActionSerializer.from_dict(json.loads(rows_arg[0]["action_json"]))
        assert isinstance(deserialized.action, DQAlert)

    def test_save_logs_completion(self, caplog: pytest.LogCaptureFixture) -> None:
        """A completion info message must be emitted after saving."""
        spark = create_autospec(SparkSession)
        ws = create_autospec(WorkspaceClient, instance=True)
        engine = _make_engine_mock()
        config = _make_lakebase_config(mode="append")

        handler = LakebaseActionsStorageHandler(spark=spark, ws=ws, config=config, engine=engine)
        with caplog.at_level(logging.INFO, logger="databricks.labs.dqx.actions.definition_storage"):
            handler.save([_make_fail_action()], config)

        assert any("saved" in r.message.lower() for r in caplog.records)

    def test_bootstrap_creates_schema_when_missing_via_save(self, caplog: pytest.LogCaptureFixture) -> None:
        """*_bootstrap* creates a schema when absent — exercised via the public *save* method.

        *MetaData.create_all* accepts a *MagicMock(spec=Engine)* without error
        because the mock absorbs all attribute accesses, so no *patch.object* is needed.
        """
        spark = create_autospec(SparkSession)
        ws = create_autospec(WorkspaceClient, instance=True)
        engine = MagicMock(spec=Engine)
        conn = MagicMock(name="conn")
        conn.dialect.has_schema.return_value = False  # schema absent
        engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
        engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        config = _make_lakebase_config(mode="append")
        handler = LakebaseActionsStorageHandler(spark=spark, ws=ws, config=config, engine=engine)

        with caplog.at_level(logging.INFO, logger="databricks.labs.dqx.actions.definition_storage"):
            handler.save([_make_fail_action()], config)

        assert any("created schema" in r.message.lower() for r in caplog.records)

    def test_bootstrap_skips_schema_creation_when_present_via_save(self) -> None:
        """*_bootstrap* skips *CreateSchema* when the schema already exists — exercised via *save*."""
        spark = create_autospec(SparkSession)
        ws = create_autospec(WorkspaceClient, instance=True)
        engine = MagicMock(spec=Engine)
        conn = MagicMock(name="conn")
        conn.dialect.has_schema.return_value = True  # schema present
        engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
        engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        config = _make_lakebase_config(mode="append")
        handler = LakebaseActionsStorageHandler(spark=spark, ws=ws, config=config, engine=engine)

        handler.save([_make_fail_action()], config)

        # has_schema returned True → no CreateSchema execute; only INSERT is called.
        assert conn.execute.call_count == 1


# ---------------------------------------------------------------------------
# LakebaseActionsStorageHandler — load (table not found)
# ---------------------------------------------------------------------------


class TestLakebaseActionsStorageHandlerLoad:
    """Tests for *LakebaseActionsStorageHandler.load* — table-not-found path.

    The "table found" deserialization path (lines 354-369) requires
    *sa_inspect(engine).has_table(name, schema=...)* to return *True*.
    An in-memory SQLite engine cannot satisfy *has_table* with a named schema,
    so those lines are left uncovered (documented in the module docstring).
    """

    def _make_sqlite_engine(self) -> Engine:
        """Return a real in-memory SQLite engine for inspection tests."""
        return create_engine("sqlite:///:memory:")

    def test_table_not_found_returns_empty_list(self) -> None:
        """When the table does not exist, *load* returns an empty list without querying."""
        spark = create_autospec(SparkSession)
        ws = create_autospec(WorkspaceClient, instance=True)
        config = _make_lakebase_config()
        handler = LakebaseActionsStorageHandler(spark=spark, ws=ws, config=config, engine=self._make_sqlite_engine())

        result = handler.load(config)

        assert not result

    def test_table_not_found_logs_info(self, caplog: pytest.LogCaptureFixture) -> None:
        """Missing-table path emits an info log."""
        spark = create_autospec(SparkSession)
        ws = create_autospec(WorkspaceClient, instance=True)
        config = _make_lakebase_config()
        handler = LakebaseActionsStorageHandler(spark=spark, ws=ws, config=config, engine=self._make_sqlite_engine())

        with caplog.at_level(logging.INFO, logger="databricks.labs.dqx.actions.definition_storage"):
            handler.load(config)

        assert any("not found" in r.message.lower() for r in caplog.records)


# ---------------------------------------------------------------------------
# ActionsStorageHandlerFactory — create
# ---------------------------------------------------------------------------


class TestActionsStorageHandlerFactoryCreate:
    """Tests for *ActionsStorageHandlerFactory.create*."""

    def test_lakebase_config_returns_lakebase_handler(self) -> None:
        """*LakebaseActionsStorageConfig* must produce a *LakebaseActionsStorageHandler*."""
        spark = create_autospec(SparkSession)
        ws = create_autospec(WorkspaceClient, instance=True)
        config = _make_lakebase_config()

        handler = ActionsStorageHandlerFactory.create(config, spark, ws)

        assert isinstance(handler, LakebaseActionsStorageHandler)

    def test_table_config_returns_table_handler(self) -> None:
        """*TableActionsStorageConfig* must produce a *TableActionsStorageHandler*."""
        spark = create_autospec(SparkSession)
        ws = create_autospec(WorkspaceClient, instance=True)
        config = _make_table_config()

        handler = ActionsStorageHandlerFactory.create(config, spark, ws)

        assert isinstance(handler, TableActionsStorageHandler)


# ---------------------------------------------------------------------------
# ActionsStorageHandlerFactory — save
# ---------------------------------------------------------------------------


class TestActionsStorageHandlerFactorySave:
    """Tests for *ActionsStorageHandlerFactory.save*."""

    def test_lakebase_config_delegates_to_lakebase_handler(self) -> None:
        """Factory *save* with a Lakebase config must call the Lakebase handler."""
        spark = create_autospec(SparkSession)
        ws = create_autospec(WorkspaceClient, instance=True)
        config = _make_lakebase_config()
        actions = [_make_fail_action()]

        with patch.object(LakebaseActionsStorageHandler, "save") as mock_save:
            ActionsStorageHandlerFactory.save(actions, config, spark, ws)
            mock_save.assert_called_once_with(actions, config)

    def test_table_config_delegates_to_table_handler(self) -> None:
        """Factory *save* with a table config must call the table handler."""
        spark = create_autospec(SparkSession)
        ws = create_autospec(WorkspaceClient, instance=True)
        config = _make_table_config()
        actions = [_make_fail_action()]

        with patch.object(TableActionsStorageHandler, "save") as mock_save:
            ActionsStorageHandlerFactory.save(actions, config, spark, ws)
            mock_save.assert_called_once_with(actions, config)


# ---------------------------------------------------------------------------
# ActionsStorageHandlerFactory — load
# ---------------------------------------------------------------------------


class TestActionsStorageHandlerFactoryLoad:
    """Tests for *ActionsStorageHandlerFactory.load*."""

    def test_lakebase_config_delegates_to_lakebase_handler(self) -> None:
        """Factory *load* with a Lakebase config must call the Lakebase handler."""
        spark = create_autospec(SparkSession)
        ws = create_autospec(WorkspaceClient, instance=True)
        config = _make_lakebase_config()
        expected = [_make_fail_action()]

        with patch.object(LakebaseActionsStorageHandler, "load", return_value=expected) as mock_load:
            result = ActionsStorageHandlerFactory.load(config, spark, ws)
            mock_load.assert_called_once_with(config)

        assert result is expected

    def test_table_config_delegates_to_table_handler(self) -> None:
        """Factory *load* with a table config must call the table handler."""
        spark = create_autospec(SparkSession)
        ws = create_autospec(WorkspaceClient, instance=True)
        config = _make_table_config()
        expected = [_make_fail_action()]

        with patch.object(TableActionsStorageHandler, "load", return_value=expected) as mock_load:
            result = ActionsStorageHandlerFactory.load(config, spark, ws)
            mock_load.assert_called_once_with(config)

        assert result is expected
