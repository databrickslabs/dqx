import json
from pathlib import Path
from unittest.mock import MagicMock, create_autospec, patch

import pytest
from pyspark.sql import SparkSession

from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.actions.alert import DQAlert
from databricks.labs.dqx.actions.definition_storage import ActionsStorageHandlerFactory
from databricks.labs.dqx.actions.destinations.slack import SlackDQAlertDestination
from databricks.labs.dqx.actions.dq_action import DQAction
from databricks.labs.dqx.actions.fail_pipeline import FailPipeline
from databricks.labs.dqx.actions.manager import DQActionManager
from databricks.labs.dqx.config import DQSecret, TableActionsStorageConfig
from databricks.labs.dqx.errors import InvalidConfigError, InvalidParameterError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_fail_action() -> DQAction:
    """Return a minimal *DQAction* wrapping *FailPipeline*."""
    return DQAction(action=FailPipeline(), condition="error_row_count > 0")


def _make_alert_action(condition: str | None = None) -> DQAction:
    """Return a *DQAction* wrapping a *DQAlert* with a *DQSecret* Slack destination."""
    dest = SlackDQAlertDestination(
        name="ops-channel",
        webhook_url=DQSecret(scope="my_scope", key="slack_webhook"),
    )
    return DQAction(action=DQAlert(destinations=[dest]), condition=condition)


def _make_table_config() -> TableActionsStorageConfig:
    """Return a minimal *TableActionsStorageConfig*."""
    return TableActionsStorageConfig(location="catalog.schema.dqx_actions")


# ---------------------------------------------------------------------------
# save_actions / load_actions delegate to the factory (public behaviour)
# ---------------------------------------------------------------------------


def test_save_actions_delegates_to_factory_with_injected_spark() -> None:
    """*save_actions* forwards the injected spark and ws to *ActionsStorageHandlerFactory.save*."""
    ws = MagicMock(spec=WorkspaceClient)
    spark = create_autospec(SparkSession, instance=True)
    manager = DQActionManager(ws=ws, spark=spark)

    actions = [_make_fail_action()]
    config = _make_table_config()

    with patch.object(ActionsStorageHandlerFactory, "save") as mock_save:
        manager.save_actions(actions, config)

    mock_save.assert_called_once_with(actions, config, spark, ws)


def test_load_actions_delegates_to_factory_with_injected_spark() -> None:
    """*load_actions* forwards the injected spark and ws and returns the factory result."""
    ws = MagicMock(spec=WorkspaceClient)
    spark = create_autospec(SparkSession, instance=True)
    manager = DQActionManager(ws=ws, spark=spark)

    config = _make_table_config()
    expected: list[DQAction] = [_make_fail_action()]

    with patch.object(ActionsStorageHandlerFactory, "load", return_value=expected) as mock_load:
        result = manager.load_actions(config)

    mock_load.assert_called_once_with(config, spark, ws)
    assert result is expected


# ---------------------------------------------------------------------------
# Spark auto-resolution observed through public methods
# ---------------------------------------------------------------------------


def test_save_actions_uses_active_session_when_spark_not_injected() -> None:
    """With *spark=None*, *save_actions* resolves the active *SparkSession* and forwards it."""
    ws = MagicMock(spec=WorkspaceClient)
    active_spark = create_autospec(SparkSession, instance=True)
    manager = DQActionManager(ws=ws)

    config = _make_table_config()

    with patch.object(SparkSession, "getActiveSession", return_value=active_spark):
        with patch.object(ActionsStorageHandlerFactory, "save") as mock_save:
            manager.save_actions([_make_fail_action()], config)

    _, _, spark_arg, ws_arg = mock_save.call_args[0]
    assert spark_arg is active_spark
    assert ws_arg is ws


def test_save_actions_uses_builder_when_no_active_session() -> None:
    """With *spark=None* and no active session, *save_actions* builds a session and forwards it."""
    ws = MagicMock(spec=WorkspaceClient)
    built_spark = create_autospec(SparkSession, instance=True)
    manager = DQActionManager(ws=ws)

    config = _make_table_config()

    with patch.object(SparkSession, "getActiveSession", return_value=None):
        with patch.object(SparkSession, "builder") as mock_builder:
            mock_builder.getOrCreate.return_value = built_spark
            with patch.object(ActionsStorageHandlerFactory, "save") as mock_save:
                manager.save_actions([_make_fail_action()], config)

    mock_builder.getOrCreate.assert_called_once()
    spark_arg = mock_save.call_args[0][2]
    assert spark_arg is built_spark


def test_load_actions_uses_active_session_when_spark_not_injected() -> None:
    """With *spark=None*, *load_actions* resolves the active *SparkSession* and forwards it."""
    ws = MagicMock(spec=WorkspaceClient)
    active_spark = create_autospec(SparkSession, instance=True)
    manager = DQActionManager(ws=ws)

    config = _make_table_config()

    with patch.object(SparkSession, "getActiveSession", return_value=active_spark):
        with patch.object(ActionsStorageHandlerFactory, "load", return_value=[]) as mock_load:
            manager.load_actions(config)

    _, spark_arg, ws_arg = mock_load.call_args[0]
    assert spark_arg is active_spark
    assert ws_arg is ws


def test_load_actions_uses_builder_when_no_active_session() -> None:
    """With *spark=None* and no active session, *load_actions* builds a session and forwards it."""
    ws = MagicMock(spec=WorkspaceClient)
    built_spark = create_autospec(SparkSession, instance=True)
    manager = DQActionManager(ws=ws)

    config = _make_table_config()

    with patch.object(SparkSession, "getActiveSession", return_value=None):
        with patch.object(SparkSession, "builder") as mock_builder:
            mock_builder.getOrCreate.return_value = built_spark
            with patch.object(ActionsStorageHandlerFactory, "load", return_value=[]) as mock_load:
                manager.load_actions(config)

    mock_builder.getOrCreate.assert_called_once()
    spark_arg = mock_load.call_args[0][1]
    assert spark_arg is built_spark


def test_injected_spark_takes_precedence_over_active_session() -> None:
    """When a spark session is injected, the active-session lookup is not consulted."""
    ws = MagicMock(spec=WorkspaceClient)
    injected_spark = create_autospec(SparkSession, instance=True)
    manager = DQActionManager(ws=ws, spark=injected_spark)

    config = _make_table_config()

    with patch.object(SparkSession, "getActiveSession") as mock_get_active:
        with patch.object(ActionsStorageHandlerFactory, "save") as mock_save:
            manager.save_actions([_make_fail_action()], config)

    mock_get_active.assert_not_called()
    assert mock_save.call_args[0][2] is injected_spark


# ---------------------------------------------------------------------------
# load_actions_from_local_file — OSError branch
# ---------------------------------------------------------------------------


def test_load_actions_from_local_file_raises_invalid_config_on_os_error(tmp_path: Path) -> None:
    """When the file exists but cannot be read, *InvalidConfigError* is raised."""
    filepath = str(tmp_path / "actions.yml")
    # Create the file so the existence check passes.
    (tmp_path / "actions.yml").write_text("[]")

    with patch("builtins.open", side_effect=OSError("permission denied")):
        with pytest.raises(InvalidConfigError, match="Cannot read actions file"):
            DQActionManager.load_actions_from_local_file(filepath)


# ---------------------------------------------------------------------------
# load_actions_from_local_file — parse-error branch
# ---------------------------------------------------------------------------


def test_load_actions_from_local_file_raises_invalid_config_on_yaml_parse_error(tmp_path: Path) -> None:
    """Malformed YAML in a *.yml* file raises *InvalidConfigError*."""
    filepath = tmp_path / "actions.yml"
    # Unclosed flow sequence forces a YAML parse error.
    filepath.write_text("key: [unclosed\n")

    with pytest.raises(InvalidConfigError, match="Failed to parse actions file"):
        DQActionManager.load_actions_from_local_file(str(filepath))


def test_load_actions_from_local_file_raises_invalid_config_on_json_parse_error(tmp_path: Path) -> None:
    """Malformed JSON in a *.json* file raises *InvalidConfigError*."""
    filepath = tmp_path / "actions.json"
    filepath.write_text("{not valid json}")

    with pytest.raises(InvalidConfigError, match="Failed to parse actions file"):
        DQActionManager.load_actions_from_local_file(str(filepath))


# ---------------------------------------------------------------------------
# load_actions_from_local_file — non-list content
# ---------------------------------------------------------------------------


def test_load_actions_from_local_file_raises_invalid_config_when_yaml_not_list(tmp_path: Path) -> None:
    """A YAML file whose top-level value is not a list raises *InvalidConfigError*."""
    filepath = tmp_path / "actions.yml"
    filepath.write_text("key: value\n")

    with pytest.raises(InvalidConfigError, match="must contain a top-level list"):
        DQActionManager.load_actions_from_local_file(str(filepath))


def test_load_actions_from_local_file_raises_invalid_config_when_json_not_list(tmp_path: Path) -> None:
    """A JSON file whose top-level value is not a list raises *InvalidConfigError*."""
    filepath = tmp_path / "actions.json"
    filepath.write_text(json.dumps({"action": "fail_pipeline"}))

    with pytest.raises(InvalidConfigError, match="must contain a top-level list"):
        DQActionManager.load_actions_from_local_file(str(filepath))


# ---------------------------------------------------------------------------
# load_actions_from_local_file — missing file and unsupported extension
# ---------------------------------------------------------------------------


def test_load_actions_from_local_file_missing_raises_invalid_parameter_error() -> None:
    """Missing file raises *InvalidParameterError*."""
    with pytest.raises(InvalidParameterError, match="Actions file not found"):
        DQActionManager.load_actions_from_local_file("/nonexistent/path/actions.yml")


def test_load_actions_from_local_file_unsupported_extension_raises(tmp_path: Path) -> None:
    """An existing file with an unsupported extension raises *InvalidParameterError*."""
    filepath = tmp_path / "actions.txt"
    filepath.write_text("[]")

    with pytest.raises(InvalidParameterError, match="Unsupported file extension"):
        DQActionManager.load_actions_from_local_file(str(filepath))


# ---------------------------------------------------------------------------
# save_actions_in_local_file — OSError and unsupported extension branches
# ---------------------------------------------------------------------------


def test_save_actions_in_local_file_raises_invalid_config_on_os_error(tmp_path: Path) -> None:
    """When the file cannot be written, *InvalidConfigError* is raised."""
    filepath = str(tmp_path / "actions.yml")
    actions = [_make_fail_action()]

    with patch("builtins.open", side_effect=OSError("disk full")):
        with pytest.raises(InvalidConfigError, match="Cannot write actions file"):
            DQActionManager.save_actions_in_local_file(actions, filepath)


def test_save_actions_in_local_file_unsupported_extension_raises(tmp_path: Path) -> None:
    """Saving to an unsupported extension raises *InvalidParameterError*."""
    filepath = str(tmp_path / "actions.txt")

    with pytest.raises(InvalidParameterError, match="Unsupported file extension"):
        DQActionManager.save_actions_in_local_file([_make_fail_action()], filepath)


# ---------------------------------------------------------------------------
# File round-trips (public happy path)
# ---------------------------------------------------------------------------


def test_save_load_yml_round_trip(tmp_path: Path) -> None:
    """Saving and loading a YAML file preserves action types and conditions."""
    filepath = str(tmp_path / "actions.yml")
    actions = [_make_alert_action("error_row_count > 0"), _make_fail_action()]

    DQActionManager.save_actions_in_local_file(actions, filepath)
    loaded = DQActionManager.load_actions_from_local_file(filepath)

    assert len(loaded) == 2
    assert isinstance(loaded[0].action, DQAlert)
    assert isinstance(loaded[1].action, FailPipeline)
    assert loaded[0].condition == "error_row_count > 0"


def test_save_load_json_round_trip(tmp_path: Path) -> None:
    """Saving and loading a JSON file preserves action types and conditions."""
    filepath = str(tmp_path / "actions.json")
    actions = [_make_alert_action("error_row_count > 0"), _make_fail_action()]

    DQActionManager.save_actions_in_local_file(actions, filepath)
    loaded = DQActionManager.load_actions_from_local_file(filepath)

    assert len(loaded) == 2
    assert isinstance(loaded[0].action, DQAlert)
    assert isinstance(loaded[1].action, FailPipeline)
