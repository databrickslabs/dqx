import builtins
import importlib
import sys
import types

import pytest

from databricks.labs.dqx.anomaly import explainer as explainer_mod
from databricks.labs.dqx.anomaly.explainer import add_top_contributors_to_message
from databricks.labs.dqx.errors import InvalidParameterError


def test_compute_feature_contributions_requires_shap(spark):
    """Missing SHAP should raise ImportError after pandas/numpy check."""
    shap_saved = explainer_mod.shap
    try:
        explainer_mod.shap = None
        df = spark.createDataFrame([(1.0, 2.0)], "amount double, quantity double")
        with pytest.raises(ImportError, match="include_contributions=True"):
            explainer_mod.compute_feature_contributions("models:/dummy", df, ["amount", "quantity"])
    finally:
        explainer_mod.shap = shap_saved


def test_add_top_contributors_requires_severity_column(spark):
    """Missing severity_percentile/_dq_info should raise InvalidParameterError."""
    df = spark.createDataFrame(
        [(0.7, {"amount": 80.0, "quantity": 20.0})],
        "anomaly_score double, anomaly_contributions map<string,double>",
    )
    with pytest.raises(InvalidParameterError, match="severity_percentile is required"):
        add_top_contributors_to_message(df, threshold=60.0)


def test_compute_contributions_zero_total_uses_equal_weights():
    """Zero-sum SHAP values fall back to equal weights."""
    if explainer_mod.np is None or explainer_mod.pd is None or explainer_mod.Pipeline is None:
        pytest.skip("Explainability dependencies not available")

    class _FakeExplainer:
        def shap_values(self, matrix):
            return [explainer_mod.np.zeros((1, matrix.shape[1]))]

    shap_saved = explainer_mod.shap
    try:
        explainer_mod.shap = types.SimpleNamespace(TreeExplainer=lambda _model: _FakeExplainer())
        feature_matrix = explainer_mod.np.array([[1.0, 2.0]])
        contributions = explainer_mod.compute_contributions_for_matrix(object(), feature_matrix, ["a", "b"])
        assert contributions == [{"a": 0.5, "b": 0.5}]
    finally:
        explainer_mod.shap = shap_saved


def test_optional_imports_fallback(monkeypatch):
    """Import error branches set optional dependencies to None."""
    module_name = explainer_mod.__name__
    original_import = builtins.__import__

    def _fake_import(name, *args, **kwargs):
        if name in {"numpy", "pandas", "shap", "mlflow.sklearn", "sklearn.pipeline"}:
            raise ImportError("missing optional dependency")
        if name.startswith("mlflow.sklearn") or name.startswith("sklearn.pipeline"):
            raise ImportError("missing optional dependency")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _fake_import)
    try:
        sys.modules.pop(module_name, None)
        reloaded = importlib.import_module(module_name)
        assert reloaded.pd is None
        assert reloaded.np is None
        assert reloaded.shap is None
        assert reloaded.mlflow_sklearn is None
        assert reloaded.Pipeline is None
    finally:
        monkeypatch.setattr(builtins, "__import__", original_import)
        sys.modules[module_name] = explainer_mod
        importlib.reload(explainer_mod)
