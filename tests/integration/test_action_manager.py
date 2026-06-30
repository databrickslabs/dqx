"""Integration tests for *DQActionManager* — requires a live Databricks workspace and Spark session.

These tests verify that *DQActionManager.save_actions* and *load_actions* round-trip a list
of *DQAction* instances through a Unity Catalog Delta table.

Prerequisites:
- A Databricks workspace accessible via *WorkspaceClient* (``ws`` fixture).
- An active *SparkSession* (``spark`` fixture from pytester).
- A UC catalog named ``dqx`` (``TEST_CATALOG`` constant).

Run with::

    .venv/bin/pytest tests/integration/test_action_manager.py -v
"""

from __future__ import annotations

import pytest
from databricks.labs.pytester.fixtures.baseline import factory
from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.actions.alert import DQAlert, DQAlertFrequency, NotifyOn
from databricks.labs.dqx.actions.base import DQAction
from databricks.labs.dqx.actions.destinations.slack import SlackDQAlertDestination
from databricks.labs.dqx.actions.destinations.webhook import WebhookDQAlertDestination
from databricks.labs.dqx.actions.fail_pipeline import FailPipeline
from databricks.labs.dqx.actions.manager import DQActionManager
from databricks.labs.dqx.config import TableActionsStorageConfig

from tests.constants import TEST_CATALOG


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def make_actions_table(ws: WorkspaceClient, make_schema):
    """Factory fixture that creates a unique UC table name and cleans it up after the test."""

    def create(**kwargs) -> str:
        schema = make_schema(catalog_name=TEST_CATALOG)
        table_name = kwargs.get("table_name", "dqx_actions_test")
        full_name = f"{schema.full_name}.{table_name}"
        return full_name

    def delete(full_name: str) -> None:
        ws.tables.delete(full_name)

    yield from factory("actions_table", create, delete)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_test_actions() -> list[DQAction]:
    slack_dest = SlackDQAlertDestination(
        name="slack-dest",
        webhook_url="https://hooks.slack.com/T/B/xtest",
    )
    webhook_dest = WebhookDQAlertDestination(
        name="webhook-dest",
        webhook_url="https://example.com/hook",
    )
    alert = DQAlert(
        destinations=[slack_dest, webhook_dest],
        name="test-alert",
        alert_frequency=DQAlertFrequency.DAILY,
        notify_on=NotifyOn.STATUS_CHANGE,
        severity="warning",
    )
    fail_action = FailPipeline(message="test failure")

    return [
        DQAction(action=alert, condition="error_row_count > 0", name="alert-action"),
        DQAction(action=fail_action, name="fail-action"),
    ]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestDQActionManagerTableRoundTrip:
    def test_save_and_load_round_trips_actions(self, ws: WorkspaceClient, spark, make_actions_table) -> None:
        table_name = make_actions_table()
        config = TableActionsStorageConfig(location=table_name, run_config_name="test-run")
        manager = DQActionManager(ws=ws, spark=spark)

        actions = _build_test_actions()
        manager.save_actions(actions, config)
        loaded = manager.load_actions(config)

        assert len(loaded) == len(actions)

        # Verify the alert action
        alert_actions = [a for a in loaded if isinstance(a.action, DQAlert)]
        fail_actions = [a for a in loaded if isinstance(a.action, FailPipeline)]
        assert len(alert_actions) == 1
        assert len(fail_actions) == 1

        loaded_alert_action = alert_actions[0]
        assert loaded_alert_action.name == "alert-action"
        assert loaded_alert_action.condition == "error_row_count > 0"
        alert = loaded_alert_action.action
        assert isinstance(alert, DQAlert)
        assert alert.name == "test-alert"
        assert alert.alert_frequency == DQAlertFrequency.DAILY
        assert alert.notify_on == NotifyOn.STATUS_CHANGE
        assert alert.severity == "warning"
        assert len(alert.destinations) == 2

        dest_types = {type(dest).__name__ for dest in alert.destinations}
        assert "SlackDQAlertDestination" in dest_types
        assert "WebhookDQAlertDestination" in dest_types

        loaded_fail_action = fail_actions[0]
        assert loaded_fail_action.name == "fail-action"
        fail_pipe = loaded_fail_action.action
        assert isinstance(fail_pipe, FailPipeline)
        assert fail_pipe.message == "test failure"

    def test_load_from_nonexistent_table_returns_empty_list(self, ws: WorkspaceClient, spark) -> None:
        config = TableActionsStorageConfig(
            location=f"{TEST_CATALOG}.nonexistent_schema.nonexistent_actions_table",
            run_config_name="default",
        )
        manager = DQActionManager(ws=ws, spark=spark)
        result = manager.load_actions(config)
        assert not result

    def test_run_config_name_isolation(self, ws: WorkspaceClient, spark, make_actions_table) -> None:
        """Actions saved under one run_config_name must not appear under another."""
        table_name = make_actions_table()
        manager = DQActionManager(ws=ws, spark=spark)

        config_a = TableActionsStorageConfig(location=table_name, run_config_name="run-a")
        config_b = TableActionsStorageConfig(location=table_name, run_config_name="run-b")

        slack_dest = SlackDQAlertDestination(name="slack", webhook_url="https://hooks.slack.com/T/B/x")
        alert = DQAlert(destinations=[slack_dest])
        actions_a = [DQAction(action=alert, name="action-for-a")]
        manager.save_actions(actions_a, config_a)

        loaded_b = manager.load_actions(config_b)
        assert not loaded_b

        loaded_a = manager.load_actions(config_a)
        assert len(loaded_a) == 1

    def test_overwrite_mode_replaces_existing_rows(self, ws: WorkspaceClient, spark, make_actions_table) -> None:
        """Saving with mode=overwrite replaces existing rows for the same run_config_name."""
        table_name = make_actions_table()
        config = TableActionsStorageConfig(location=table_name, run_config_name="run-ow", mode="overwrite")
        manager = DQActionManager(ws=ws, spark=spark)

        slack_dest = SlackDQAlertDestination(name="slack", webhook_url="https://hooks.slack.com/T/B/x")
        alert1 = DQAlert(destinations=[slack_dest], name="first-alert")
        manager.save_actions([DQAction(action=alert1, name="first")], config)

        alert2 = DQAlert(destinations=[slack_dest], name="second-alert")
        manager.save_actions([DQAction(action=alert2, name="second")], config)

        loaded = manager.load_actions(config)
        assert len(loaded) == 1
        assert isinstance(loaded[0].action, DQAlert)
        assert loaded[0].action.name == "second-alert"
