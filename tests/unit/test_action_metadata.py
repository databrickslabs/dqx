"""Unit tests for the action metadata (dict/YAML) API.

Covers:
- Round-trip serialization: list[DQAction] -> list[dict] -> list[DQAction]
- Error cases in deserialize_actions
- File round-trips for YAML and JSON via DQActionManager
- DQEngine normalization: dict entries are deserialized to DQAction at construction time
"""

from __future__ import annotations

import collections.abc
import json
from pathlib import Path
from typing import cast
from unittest.mock import create_autospec

import pytest
from pyspark.sql import SparkSession

from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.actions.alert import DQAlert
from databricks.labs.dqx.actions.base import ActionResult, ActionStatus
from databricks.labs.dqx.actions.dq_action import DQAction
from databricks.labs.dqx.actions.destinations.slack import SlackDQAlertDestination
from databricks.labs.dqx.actions.evaluator import ActionEvaluator
from databricks.labs.dqx.actions.fail_pipeline import FailPipeline
from databricks.labs.dqx.actions.manager import DQActionManager
from databricks.labs.dqx.actions.serializer import deserialize_actions, serialize_actions
from databricks.labs.dqx.config import DQSecret
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.errors import InvalidActionError, InvalidParameterError
from databricks.labs.dqx.metrics_observer import DQMetricsObserver


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_alert_action(condition: str | None = None) -> DQAction:
    """Build a *DQAction* wrapping a *DQAlert* with a *DQSecret* Slack destination."""
    dest = SlackDQAlertDestination(
        name="ops-channel",
        webhook_url=DQSecret(scope="my_scope", key="slack_webhook"),
    )
    alert = DQAlert(destinations=[dest])
    return DQAction(action=alert, condition=condition)


def _make_fail_action(condition: str | None = None) -> DQAction:
    """Build a *DQAction* wrapping a *FailPipeline* action."""
    return DQAction(action=FailPipeline(), condition=condition)


def _make_observer() -> DQMetricsObserver:
    return DQMetricsObserver()


def _make_fake_evaluator_factory() -> (
    tuple[ActionEvaluator, list[DQAction], collections.abc.Callable[[list[DQAction]], ActionEvaluator]]
):
    """Return a fake evaluator, a capture list, and a factory that populates it.

    When the returned factory is passed to *DQEngine* as *action_evaluator_factory*,
    every call to *evaluate_actions* will append the normalized *DQAction* list
    received by the factory into the capture list so tests can inspect it.
    """
    captured: list[DQAction] = []
    fake_evaluator = create_autospec(ActionEvaluator)
    fake_evaluator.evaluate.return_value = [ActionResult(action_name="test", fired=True, status=ActionStatus.UNHEALTHY)]

    def factory(actions: list[DQAction]) -> ActionEvaluator:
        captured.extend(actions)
        return fake_evaluator

    return fake_evaluator, captured, factory


# ---------------------------------------------------------------------------
# Round-trip tests
# ---------------------------------------------------------------------------


def test_serialize_deserialize_round_trip() -> None:
    """Round-trip: list of mixed actions to dicts and back asserts structural equality."""
    alert_action = _make_alert_action(condition="error_row_count > 0")
    fail_action = _make_fail_action(condition="error_row_count > 10")

    serialized = serialize_actions([alert_action, fail_action])

    assert isinstance(serialized, list)
    assert len(serialized) == 2

    deserialized = deserialize_actions(serialized)

    assert len(deserialized) == 2

    # Check types are preserved
    assert isinstance(deserialized[0].action, DQAlert)
    assert isinstance(deserialized[1].action, FailPipeline)

    # Check conditions are preserved
    assert deserialized[0].condition == "error_row_count > 0"
    assert deserialized[1].condition == "error_row_count > 10"

    # Check DQSecret round-trips correctly
    alert_dest = deserialized[0].action.destinations[0]
    assert isinstance(alert_dest, SlackDQAlertDestination)
    assert isinstance(alert_dest.webhook_url, DQSecret)
    assert alert_dest.webhook_url.scope == "my_scope"
    assert alert_dest.webhook_url.key == "slack_webhook"


def test_serialize_actions_empty_list() -> None:
    """serialize_actions on an empty list returns an empty list."""
    assert serialize_actions([]) == []


def test_deserialize_actions_empty_list() -> None:
    """deserialize_actions on an empty list returns an empty list."""
    assert not deserialize_actions([])


# ---------------------------------------------------------------------------
# Error cases for deserialize_actions
# ---------------------------------------------------------------------------


def test_deserialize_raises_on_non_dict() -> None:
    """deserialize_actions raises *InvalidActionError* on non-dict element."""
    bad = cast(list[dict[str, object]], [{"action": {"type": "fail_pipeline"}}, "not_a_dict"])
    with pytest.raises(InvalidActionError, match="index 1"):
        deserialize_actions(bad)


def test_deserialize_raises_on_unknown_type() -> None:
    """deserialize_actions raises *InvalidActionError* for unknown action type."""
    with pytest.raises(InvalidActionError):
        deserialize_actions([{"action": {"type": "unknown_action_type"}, "condition": None}])


# ---------------------------------------------------------------------------
# File round-trip tests
# ---------------------------------------------------------------------------


def test_save_load_yml_round_trip(tmp_path: Path) -> None:
    """Saving and loading a YAML file preserves action types and conditions."""
    filepath = str(tmp_path / "actions.yml")
    actions = [_make_alert_action("error_row_count > 0"), _make_fail_action("error_row_count > 10")]

    DQActionManager.save_actions_in_local_file(actions, filepath)
    loaded = DQActionManager.load_actions_from_local_file(filepath)

    assert len(loaded) == 2
    assert isinstance(loaded[0].action, DQAlert)
    assert isinstance(loaded[1].action, FailPipeline)
    assert loaded[0].condition == "error_row_count > 0"
    assert loaded[1].condition == "error_row_count > 10"


def test_save_load_yaml_extension_round_trip(tmp_path: Path) -> None:
    """Saving and loading a .yaml (not .yml) file also works."""
    filepath = str(tmp_path / "actions.yaml")
    actions = [_make_fail_action("error_row_count > 5")]

    DQActionManager.save_actions_in_local_file(actions, filepath)
    loaded = DQActionManager.load_actions_from_local_file(filepath)

    assert len(loaded) == 1
    assert isinstance(loaded[0].action, FailPipeline)
    assert loaded[0].condition == "error_row_count > 5"


def test_save_load_json_round_trip(tmp_path: Path) -> None:
    """Saving and loading a JSON file preserves action types and conditions."""
    filepath = str(tmp_path / "actions.json")
    actions = [_make_alert_action("error_row_count > 0"), _make_fail_action("error_row_count > 10")]

    DQActionManager.save_actions_in_local_file(actions, filepath)
    loaded = DQActionManager.load_actions_from_local_file(filepath)

    assert len(loaded) == 2
    assert isinstance(loaded[0].action, DQAlert)
    assert isinstance(loaded[1].action, FailPipeline)
    assert loaded[0].condition == "error_row_count > 0"
    assert loaded[1].condition == "error_row_count > 10"


def test_json_file_is_valid_json(tmp_path: Path) -> None:
    """The JSON file produced by save_actions_in_local_file is valid JSON."""
    filepath = str(tmp_path / "actions.json")
    actions = [_make_fail_action()]

    DQActionManager.save_actions_in_local_file(actions, filepath)

    with open(filepath, encoding="utf-8") as file:
        parsed = json.load(file)

    assert isinstance(parsed, list)
    assert len(parsed) == 1


def test_load_unsupported_extension_raises(tmp_path: Path) -> None:
    """load_actions_from_local_file raises *InvalidParameterError* for unsupported extension."""
    filepath = str(tmp_path / "actions.txt")
    filepath_path = tmp_path / "actions.txt"
    filepath_path.write_text("[]")

    with pytest.raises(InvalidParameterError, match="Unsupported file extension"):
        DQActionManager.load_actions_from_local_file(filepath)


def test_save_unsupported_extension_raises(tmp_path: Path) -> None:
    """save_actions_in_local_file raises *InvalidParameterError* for unsupported extension."""
    filepath = str(tmp_path / "actions.txt")

    with pytest.raises(InvalidParameterError, match="Unsupported file extension"):
        DQActionManager.save_actions_in_local_file([_make_fail_action()], filepath)


def test_load_missing_file_raises() -> None:
    """load_actions_from_local_file raises *InvalidParameterError* when the file does not exist."""
    with pytest.raises(InvalidParameterError, match="Actions file not found"):
        DQActionManager.load_actions_from_local_file("/nonexistent/path/actions.yml")


# ---------------------------------------------------------------------------
# Engine metadata acceptance tests
# ---------------------------------------------------------------------------


def test_engine_accepts_action_dicts(mock_workspace_client: WorkspaceClient) -> None:
    """DQEngine normalizes action dicts to *DQAction* at construction time.

    Verified by constructing the engine (no error means normalization succeeded)
    and then calling evaluate_actions which invokes the injected evaluator — the
    evaluator factory receives the already-normalized *DQAction* list.
    """
    spark = create_autospec(SparkSession)
    observer = _make_observer()

    action_dict: dict[str, object] = {
        "action": {"type": "fail_pipeline"},
        "condition": "error_row_count > 0",
    }

    _, captured, factory = _make_fake_evaluator_factory()

    engine = DQEngine(
        mock_workspace_client,
        spark=spark,
        observer=observer,
        actions=cast(list[DQAction | dict[str, object]], [action_dict]),
        action_evaluator_factory=factory,
    )

    # Trigger evaluation so the factory is called and captures the normalized actions
    engine.evaluate_actions({"error_row_count": 1})

    assert len(captured) == 1
    assert isinstance(captured[0], DQAction)
    assert isinstance(captured[0].action, FailPipeline)
    assert captured[0].condition == "error_row_count > 0"


def test_engine_accepts_mixed_list(mock_workspace_client: WorkspaceClient) -> None:
    """DQEngine handles a mixed list of *DQAction* instances and dicts.

    The evaluator factory receives only fully normalized *DQAction* instances —
    no raw dicts remain after construction.
    """
    spark = create_autospec(SparkSession)
    observer = _make_observer()

    dq_action = _make_fail_action("error_row_count > 5")
    action_dict: dict[str, object] = {
        "action": {"type": "fail_pipeline"},
        "condition": "error_row_count > 10",
    }

    _, captured, factory = _make_fake_evaluator_factory()

    engine = DQEngine(
        mock_workspace_client,
        spark=spark,
        observer=observer,
        actions=cast(list[DQAction | dict[str, object]], [dq_action, action_dict]),
        action_evaluator_factory=factory,
    )

    engine.evaluate_actions({"error_row_count": 1})

    assert len(captured) == 2
    assert all(isinstance(action, DQAction) for action in captured)
    assert captured[0].condition == "error_row_count > 5"
    assert captured[1].condition == "error_row_count > 10"


def test_engine_bad_action_dict_raises(mock_workspace_client: WorkspaceClient) -> None:
    """DQEngine raises *InvalidActionError* for a bad action dict at construction time."""
    spark = create_autospec(SparkSession)
    observer = _make_observer()

    bad_dict: dict[str, object] = {"type": "unknown_action_xyz"}

    # InvalidActionError is raised at construction — bad dicts are deserialized eagerly
    with pytest.raises(InvalidActionError):
        DQEngine(
            mock_workspace_client,
            spark=spark,
            observer=observer,
            actions=cast(list[DQAction | dict[str, object]], [bad_dict]),
        )
