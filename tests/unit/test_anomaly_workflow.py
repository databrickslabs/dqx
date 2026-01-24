from types import SimpleNamespace
from unittest.mock import Mock

from databricks.labs.dqx.anomaly import anomaly_workflow
from databricks.labs.dqx.config import AnomalyConfig, InputConfig, RunConfig


def test_anomaly_workflow_default_model_name(monkeypatch):
    captured = {}

    class FakeEngine:
        def __init__(self, _ws, _spark):
            pass

        def train(self, **kwargs):
            captured.update(kwargs)

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
    workflow.train_model(ctx)

    assert captured["model_name"] == "dqx_anomaly_orders_daily"
