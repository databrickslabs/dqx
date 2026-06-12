"""Unit tests for severity-gated SHAP contribution computation (no Spark required)."""

import numpy as np
import pandas as pd
import pytest
from sklearn.ensemble import IsolationForest

from databricks.labs.dqx.anomaly.explainability import (
    compute_gated_shap_contributions,
    severity_from_scores,
)

QUANTILE_POINTS = [(10.0, 1.0), (50.0, 2.0), (90.0, 4.0)]


def test_severity_from_scores_interpolates_between_points():
    severity = severity_from_scores(np.array([1.5, 3.0]), QUANTILE_POINTS)
    assert severity[0] == pytest.approx(30.0)
    assert severity[1] == pytest.approx(70.0)


def test_severity_from_scores_clamps_at_both_ends():
    severity = severity_from_scores(np.array([0.1, 99.0]), QUANTILE_POINTS)
    assert severity[0] == pytest.approx(10.0)
    assert severity[1] == pytest.approx(90.0)


@pytest.fixture(name="fitted_model_and_features")
def fitted_model_and_features_fixture():
    rng = np.random.default_rng(42)
    train = pd.DataFrame({"amount": rng.normal(100, 5, 200), "quantity": rng.normal(2, 0.5, 200)})
    model = IsolationForest(random_state=42).fit(train)
    features = pd.DataFrame({"amount": [100.0, 101.0, 99.0, 9999.0], "quantity": [2.0, 2.1, 1.9, 1.0]})
    return model, features


def test_gated_contributions_computed_only_for_anomalous_rows(fitted_model_and_features):
    model, features = fitted_model_and_features
    # Scores are passed in explicitly; only the last row's severity reaches the threshold.
    scores = np.array([1.0, 1.2, 1.1, 10.0])
    contributions = compute_gated_shap_contributions(
        model, features, ["amount", "quantity"], scores, QUANTILE_POINTS, threshold=85.0
    )
    assert contributions[:3] == [None, None, None]
    assert isinstance(contributions[3], dict)
    assert set(contributions[3]) == {"amount", "quantity"}
    total = sum(v for v in contributions[3].values() if v is not None)
    assert total == pytest.approx(100.0, abs=0.5)


def test_gated_contributions_fall_back_to_all_rows_without_quantile_points(fitted_model_and_features):
    model, features = fitted_model_and_features
    scores = np.array([1.0, 1.2, 1.1, 10.0])
    contributions = compute_gated_shap_contributions(
        model, features, ["amount", "quantity"], scores, quantile_points=None, threshold=85.0
    )
    assert all(isinstance(c, dict) for c in contributions)


def test_gated_contributions_epsilon_includes_threshold_boundary(fitted_model_and_features):
    model, features = fitted_model_and_features
    # Severity of score 4.0 is exactly 90; with threshold 90 the boundary row must be included.
    scores = np.array([1.0, 1.0, 1.0, 4.0])
    contributions = compute_gated_shap_contributions(
        model, features, ["amount", "quantity"], scores, QUANTILE_POINTS, threshold=90.0
    )
    assert contributions[:3] == [None, None, None]
    assert isinstance(contributions[3], dict)
