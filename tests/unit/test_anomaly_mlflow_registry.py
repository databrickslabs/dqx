"""Unit tests for MLflow anomaly model registry compatibility."""

from types import SimpleNamespace

import mlflow

from databricks.labs.dqx.anomaly.mlflow_registry import log_sklearn_model_compatible


class _DummyModel:
    def predict(self, _data):
        return [0]


class _DummySignature:
    inputs = None
    outputs = None


def test_log_model_uses_name_when_supported(monkeypatch):
    captured = {}

    def fake_log_model(*, sk_model, name, registered_model_name, signature):
        captured["kwargs"] = {
            "sk_model": sk_model,
            "name": name,
            "registered_model_name": registered_model_name,
            "signature": signature,
        }
        return SimpleNamespace(registered_model_version="1")

    monkeypatch.setattr(mlflow.sklearn, "log_model", fake_log_model)

    info = log_sklearn_model_compatible(
        model=_DummyModel(),
        model_name="catalog.schema.model",
        signature=_DummySignature(),
    )

    assert info.registered_model_version == "1"
    assert captured["kwargs"]["name"] == "model"
    assert captured["kwargs"]["registered_model_name"] == "catalog.schema.model"


def test_log_model_uses_artifact_path_when_name_not_supported(monkeypatch):
    captured = {}

    def fake_log_model(*, sk_model, artifact_path, registered_model_name, signature):
        captured["kwargs"] = {
            "sk_model": sk_model,
            "artifact_path": artifact_path,
            "registered_model_name": registered_model_name,
            "signature": signature,
        }
        return SimpleNamespace(registered_model_version="2")

    monkeypatch.setattr(mlflow.sklearn, "log_model", fake_log_model)

    info = log_sklearn_model_compatible(
        model=_DummyModel(),
        model_name="catalog.schema.model",
        signature=_DummySignature(),
    )

    assert info.registered_model_version == "2"
    assert captured["kwargs"]["artifact_path"] == "model"
    assert captured["kwargs"]["registered_model_name"] == "catalog.schema.model"
