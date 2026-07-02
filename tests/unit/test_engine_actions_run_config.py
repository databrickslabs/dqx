"""Unit tests for per-run-config action wiring in DQEngine.

Covers the pure resolution helpers and the scoped-engine decision logic that back
auto-loading of RunConfig.actions_location / action_events_location in the workflow
runner. Actual loading + end-to-end firing is covered by integration tests.
"""

from __future__ import annotations

from unittest.mock import create_autospec

import pytest
from pyspark.sql import SparkSession

from databricks.labs.dqx.actions import DQAction, FailPipeline
from databricks.labs.dqx.config import (
    ActionEventsConfig,
    LakebaseActionsStorageConfig,
    RunConfig,
    TableActionsStorageConfig,
)
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.errors import InvalidConfigError
from databricks.labs.dqx.metrics_observer import DQMetricsObserver


def _engine(mock_workspace_client, observer: DQMetricsObserver | None = None) -> DQEngine:
    spark = create_autospec(SparkSession)
    return DQEngine(mock_workspace_client, spark=spark, observer=observer)


# ---------------------------------------------------------------------------
# _run_config_actions_storage_config
# ---------------------------------------------------------------------------


def test_actions_storage_config_none_when_location_absent() -> None:
    assert DQEngine._run_config_actions_storage_config(RunConfig(name="rc")) is None


def test_actions_storage_config_table_location() -> None:
    run_config = RunConfig(name="rc", actions_location="cat.sch.dqx_actions")
    config = DQEngine._run_config_actions_storage_config(run_config)
    assert isinstance(config, TableActionsStorageConfig)
    assert config.location == "cat.sch.dqx_actions"
    assert config.run_config_name == "rc"


def test_actions_storage_config_file_location_returns_none() -> None:
    # A file path is loaded via load_actions_from_local_file, not a table backend.
    run_config = RunConfig(name="rc", actions_location="/Workspace/dqx/actions.yml")
    assert DQEngine._run_config_actions_storage_config(run_config) is None


def test_actions_storage_config_file_with_lakebase_still_uses_file_loader() -> None:
    # A file path is a file even when Lakebase params are set: it must not be wrapped in a table config.
    run_config = RunConfig(
        name="rc",
        actions_location="/Volumes/cat/sch/vol/actions.yml",
        lakebase_instance_name="my-instance",
    )
    assert DQEngine._run_config_actions_storage_config(run_config) is None


def test_actions_storage_config_lakebase() -> None:
    run_config = RunConfig(
        name="rc",
        actions_location="db.sch.actions",
        lakebase_instance_name="my-instance",
        lakebase_client_id="client-1",
        lakebase_port="6432",
    )
    config = DQEngine._run_config_actions_storage_config(run_config)
    assert isinstance(config, LakebaseActionsStorageConfig)
    assert config.location == "db.sch.actions"
    assert config.instance_name == "my-instance"
    assert config.client_id == "client-1"
    assert config.port == "6432"
    assert config.run_config_name == "rc"


# ---------------------------------------------------------------------------
# _run_config_action_events_config
# ---------------------------------------------------------------------------


def test_events_config_none_when_location_absent() -> None:
    assert DQEngine._run_config_action_events_config(RunConfig(name="rc")) is None


def test_events_config_table_location() -> None:
    run_config = RunConfig(name="rc", action_events_location="cat.sch.dqx_action_events")
    config = DQEngine._run_config_action_events_config(run_config)
    assert isinstance(config, ActionEventsConfig)
    assert config.location == "cat.sch.dqx_action_events"
    # Events are scoped to the run config so a shared events table stays independent.
    assert config.run_config_name == "rc"


def test_events_config_rejects_file_location() -> None:
    run_config = RunConfig(name="rc", action_events_location="/Volumes/cat/sch/vol/events.json")
    with pytest.raises(InvalidConfigError, match="action_events_location must be"):
        DQEngine._run_config_action_events_config(run_config)


def test_events_config_lakebase() -> None:
    run_config = RunConfig(
        name="rc",
        action_events_location="db.sch.events",
        lakebase_instance_name="my-instance",
    )
    config = DQEngine._run_config_action_events_config(run_config)
    assert isinstance(config, LakebaseActionsStorageConfig)
    assert config.location == "db.sch.events"
    assert config.instance_name == "my-instance"


# ---------------------------------------------------------------------------
# _engine_for_run_config / _build_scoped_engine
# ---------------------------------------------------------------------------


def test_engine_for_run_config_returns_self_without_actions_location(mock_workspace_client) -> None:
    engine = _engine(mock_workspace_client)
    run_config = RunConfig(name="rc")  # no actions_location
    assert engine._engine_for_run_config(run_config) is engine


def test_build_scoped_engine_returns_self_when_no_actions(mock_workspace_client) -> None:
    engine = _engine(mock_workspace_client)
    run_config = RunConfig(name="rc", actions_location="cat.sch.dqx_actions")
    assert engine._build_scoped_engine(run_config, []) is engine


def test_build_scoped_engine_carries_actions_observer_and_events(mock_workspace_client) -> None:
    base_observer = DQMetricsObserver(custom_metrics=["error_row_count as e"])
    engine = _engine(mock_workspace_client, observer=base_observer)

    action = DQAction(action=FailPipeline(), name="fail_on_errors")
    run_config = RunConfig(
        name="rc",
        actions_location="cat.sch.dqx_actions",
        action_events_location="cat.sch.dqx_action_events",
    )

    scoped = engine._build_scoped_engine(run_config, [action])

    assert scoped is not engine
    assert scoped._actions == [action]
    # A fresh observer is created for isolation, seeded with the base engine's custom metrics.
    assert scoped._engine.observer is not None
    assert scoped._engine.observer is not base_observer
    assert scoped._engine.observer.custom_metrics == ["error_row_count as e"]
    # The events location is wired to the scoped engine's event store config.
    assert isinstance(scoped._action_events_config, ActionEventsConfig)
    assert scoped._action_events_config.location == "cat.sch.dqx_action_events"


def test_build_scoped_engine_without_events_location_has_no_event_store(mock_workspace_client) -> None:
    engine = _engine(mock_workspace_client, observer=DQMetricsObserver())
    action = DQAction(action=FailPipeline(), name="fail_on_errors")
    run_config = RunConfig(name="rc", actions_location="cat.sch.dqx_actions")

    scoped = engine._build_scoped_engine(run_config, [action])

    assert scoped._actions == [action]
    assert scoped._action_events_config is None


def test_load_actions_returns_empty_without_location(mock_workspace_client) -> None:
    engine = _engine(mock_workspace_client)
    assert engine._load_actions_for_run_config(RunConfig(name="rc")) == []
