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

    monkeypatch.setattr(anomaly_workflow, "ANOMALY_ENABLED", True)
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
