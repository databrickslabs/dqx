"""Unit tests for MLflow anomaly model registry compatibility.

``mlflow.sklearn`` is imported explicitly, and that import is load-bearing rather than tidiness. On
MLflow 3 ``mlflow.sklearn`` is a ``LazyLoader`` until something touches it, and patching an attribute on
the loader materialises the real module underneath, so the patch lands on the loader while the code under
test reads the freshly materialised module and calls the real ``log_model`` -- which then tries to reach
a tracking server. Importing the submodule first means the patch target and the call target are the same
object.
"""

from types import SimpleNamespace

import mlflow
import mlflow.sklearn  # noqa: F401  -- see below

from databricks.labs.dqx.anomaly.mlflow_registry import (
    SKLEARN_SERIALIZATION_FORMAT,
    log_sklearn_model_compatible,
)


class _DummyModel:
    def predict(self, _data):
        return [0]


class _DummySignature:
    inputs = None
    outputs = None


def test_log_model_uses_name_when_supported(monkeypatch):
    captured = {}

    def fake_log_model(*, sk_model, name, registered_model_name, signature, serialization_format):
        captured["kwargs"] = {
            "sk_model": sk_model,
            "name": name,
            "registered_model_name": registered_model_name,
            "signature": signature,
            "serialization_format": serialization_format,
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
    # Named rather than defaulted, and asserted on both branches: MLflow 3 validates a saved sklearn
    # model against skops' trusted types and refuses MahalanobisDetector, which is DQX's own class, so
    # omitting this breaks profile="timeseries" at registration while every other test still passes.
    assert captured["kwargs"]["serialization_format"] == SKLEARN_SERIALIZATION_FORMAT


def test_log_model_uses_artifact_path_when_name_not_supported(monkeypatch):
    captured = {}

    def fake_log_model(*, sk_model, artifact_path, registered_model_name, signature, serialization_format):
        captured["kwargs"] = {
            "sk_model": sk_model,
            "artifact_path": artifact_path,
            "registered_model_name": registered_model_name,
            "signature": signature,
            "serialization_format": serialization_format,
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
    assert captured["kwargs"]["serialization_format"] == SKLEARN_SERIALIZATION_FORMAT
