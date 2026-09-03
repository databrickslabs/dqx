from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from databricks.labs.dqx.anomaly import anomaly_workflow
from databricks.labs.dqx.config import AnomalyConfig, InputConfig, RunConfig
from databricks.labs.dqx.errors import InvalidConfigError


def test_anomaly_workflow_requires_model_name(monkeypatch):
    class FakeEngine:
        def __init__(self, _ws, _spark):
            pass

        def train(self, **_kwargs):
            return None

    monkeypatch.setattr(
        "databricks.labs.dqx.anomaly.anomaly_engine.AnomalyEngine",
        FakeEngine,
    )
    monkeypatch.setattr(anomaly_workflow, "read_input_data", lambda _spark, _input_config: Mock())

    run_config = RunConfig(
        name="Orders Daily",
        input_config=InputConfig(location="catalog.schema.orders"),
        anomaly_config=AnomalyConfig(),
    )
    ctx = SimpleNamespace(run_config=run_config, spark=Mock(), workspace_client=Mock())

    workflow = anomaly_workflow.AnomalyTrainerWorkflow()
    with pytest.raises(InvalidConfigError, match="model_name is required"):
        workflow.train_model(ctx)


def test_anomaly_workflow_requires_registry_table(monkeypatch):
    """Test that registry_table is required and validated before training."""

    class FakeEngine:
        def __init__(self, _ws, _spark):
            pass

        def train(self, **_kwargs):
            return None

    monkeypatch.setattr(
        "databricks.labs.dqx.anomaly.anomaly_engine.AnomalyEngine",
        FakeEngine,
    )
    monkeypatch.setattr(anomaly_workflow, "read_input_data", lambda _spark, _input_config: Mock())

    # Provide model_name but no registry_table
    run_config = RunConfig(
        name="Orders Daily",
        input_config=InputConfig(location="catalog.schema.orders"),
        anomaly_config=AnomalyConfig(model_name="catalog.schema.my_model"),
    )
    ctx = SimpleNamespace(run_config=run_config, spark=Mock(), workspace_client=Mock())

    workflow = anomaly_workflow.AnomalyTrainerWorkflow()
    with pytest.raises(InvalidConfigError, match="registry_table is required"):
        workflow.train_model(ctx)


def test_anomaly_workflow_validates_empty_registry_table(monkeypatch):
    """Test that empty string registry_table is rejected."""

    class FakeEngine:
        def __init__(self, _ws, _spark):
            pass

        def train(self, **_kwargs):
            return None

    monkeypatch.setattr(
        "databricks.labs.dqx.anomaly.anomaly_engine.AnomalyEngine",
        FakeEngine,
    )
    monkeypatch.setattr(anomaly_workflow, "read_input_data", lambda _spark, _input_config: Mock())

    # Provide model_name but empty registry_table
    run_config = RunConfig(
        name="Orders Daily",
        input_config=InputConfig(location="catalog.schema.orders"),
        anomaly_config=AnomalyConfig(
            model_name="catalog.schema.my_model",
            registry_table="",  # Empty string should be rejected
        ),
    )
    ctx = SimpleNamespace(run_config=run_config, spark=Mock(), workspace_client=Mock())

    workflow = anomaly_workflow.AnomalyTrainerWorkflow()
    with pytest.raises(InvalidConfigError, match="registry_table is required"):
        workflow.train_model(ctx)


def test_anomaly_workflow_missing_anomaly_config_raises():
    """anomaly_config=None raises before any external call — no monkeypatch needed."""
    run_config = RunConfig(
        name="test",
        input_config=InputConfig(location="catalog.schema.table"),
        anomaly_config=None,
    )
    ctx = SimpleNamespace(run_config=run_config)

    with pytest.raises(InvalidConfigError, match="anomaly_config is required"):
        anomaly_workflow.AnomalyTrainerWorkflow().train_model(ctx)


def test_anomaly_workflow_missing_input_config_raises():
    """input_config=None raises before any external call — no monkeypatch needed."""
    run_config = RunConfig(
        name="test",
        input_config=None,
        anomaly_config=AnomalyConfig(
            model_name="catalog.schema.model",
            registry_table="catalog.schema.registry",
        ),
    )
    ctx = SimpleNamespace(run_config=run_config)

    with pytest.raises(InvalidConfigError, match="input_config is required"):
        anomaly_workflow.AnomalyTrainerWorkflow().train_model(ctx)


def test_anomaly_workflow_succeeds_with_valid_config(monkeypatch):
    """Test that workflow proceeds when both model_name and registry_table are provided."""
    train_called = {"called": False, "kwargs": {}}

    class FakeEngine:
        def __init__(self, _ws, _spark):
            pass

        def train(self, **kwargs):
            train_called["called"] = True
            train_called["kwargs"] = kwargs
            return "catalog.schema.my_model"

    monkeypatch.setattr(
        "databricks.labs.dqx.anomaly.anomaly_engine.AnomalyEngine",
        FakeEngine,
    )
    monkeypatch.setattr(anomaly_workflow, "read_input_data", lambda _spark, _input_config: Mock())

    run_config = RunConfig(
        name="Orders Daily",
        input_config=InputConfig(location="catalog.schema.orders"),
        anomaly_config=AnomalyConfig(
            model_name="catalog.schema.my_model",
            registry_table="catalog.schema.my_registry",
        ),
    )
    ctx = SimpleNamespace(run_config=run_config, spark=Mock(), workspace_client=Mock())

    workflow = anomaly_workflow.AnomalyTrainerWorkflow()
    workflow.train_model(ctx)

    # Verify train was called with correct parameters
    assert train_called["called"] is True
    assert train_called["kwargs"]["model_name"] == "catalog.schema.my_model"
    assert train_called["kwargs"]["registry_table"] == "catalog.schema.my_registry"
    # Unset in the run config, so the detector default is resolved downstream rather than here.
    assert train_called["kwargs"]["profile"] is None


def test_anomaly_workflow_passes_the_configured_profile(monkeypatch):
    """A profile named in the run config reaches ``train()``.

    Scheduled retraining is the whole point of the workflow, so a detector choice that cannot be
    expressed in a run config is not really a configurable option. This is the assertion that the
    parameter is genuinely reachable from YAML and not merely from Python.
    """
    train_called: dict = {"kwargs": {}}

    class FakeEngine:
        def __init__(self, _ws, _spark):
            pass

        def train(self, **kwargs):
            train_called["kwargs"] = kwargs
            return "catalog.schema.my_model"

    monkeypatch.setattr("databricks.labs.dqx.anomaly.anomaly_engine.AnomalyEngine", FakeEngine)
    monkeypatch.setattr(anomaly_workflow, "read_input_data", lambda _spark, _input_config: Mock())

    run_config = RunConfig(
        name="Fleet Telemetry",
        input_config=InputConfig(location="catalog.schema.telemetry"),
        anomaly_config=AnomalyConfig(
            model_name="catalog.schema.my_model",
            registry_table="catalog.schema.my_registry",
            baseline_by=["machine_id"],
            profile="timeseries",
            baseline_over_time="reading_ts",
        ),
    )
    ctx = SimpleNamespace(run_config=run_config, spark=Mock(), workspace_client=Mock())

    anomaly_workflow.AnomalyTrainerWorkflow().train_model(ctx)

    assert train_called["kwargs"]["profile"] == "timeseries"
    assert train_called["kwargs"]["baseline_by"] == ["machine_id"]
    # All three comparison bases, because a scheduled retrain that silently drops one produces a different
    # model from the YAML it was given, and nothing downstream would report the discrepancy.
    assert train_called["kwargs"]["baseline_over_time"] == "reading_ts"
