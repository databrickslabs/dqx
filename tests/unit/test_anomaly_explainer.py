import types
from typing import Any

import numpy as np
import pytest

from databricks.labs.dqx.anomaly import explainer as explainer_mod
from databricks.labs.dqx.anomaly.explainer import add_top_contributors_to_message
from databricks.labs.dqx.errors import InvalidParameterError


def test_add_top_contributors_requires_severity_column():
    """Missing severity_percentile/_dq_info should raise InvalidParameterError."""

    with pytest.raises(InvalidParameterError, match="severity_percentile is required"):
        add_top_contributors_to_message(types.SimpleNamespace(columns=[]), threshold=60.0)


def test_compute_contributions_zero_total_uses_equal_weights():
    """Zero-sum SHAP values fall back to equal weights."""

    class _FakeExplainer:
        def shap_values(self, matrix: Any) -> list[Any]:
            return [np.zeros((1, matrix.shape[1]))]

    shap_saved = explainer_mod.shap
    try:
        explainer_mod.shap = types.SimpleNamespace(TreeExplainer=lambda _model: _FakeExplainer())
        feature_matrix = np.array([[1.0, 2.0]])
        contributions = explainer_mod.compute_contributions_for_matrix(object(), feature_matrix, ["a", "b"])
        assert contributions == [{"a": 0.5, "b": 0.5}]
    finally:
        explainer_mod.shap = shap_saved
