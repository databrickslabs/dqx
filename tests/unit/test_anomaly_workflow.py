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

    monkeypatch.setattr(anomaly_workflow, "AnomalyEngine", FakeEngine)
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

    monkeypatch.setattr(anomaly_workflow, "AnomalyEngine", FakeEngine)
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

    monkeypatch.setattr(anomaly_workflow, "AnomalyEngine", FakeEngine)
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

    monkeypatch.setattr(anomaly_workflow, "AnomalyEngine", FakeEngine)
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
