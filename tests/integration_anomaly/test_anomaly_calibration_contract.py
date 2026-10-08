"""Calibration returns explicit state without modifying the fitted transformations."""

import pytest

from databricks.labs.dqx.anomaly.core import (
    SCORE_QUANTILE_KEYS,
    compute_baseline_score_quantiles,
    compute_score_quantiles,
    compute_score_quantiles_ensemble,
    fit_isolation_forest,
)
from databricks.labs.dqx.config import AnomalyParams, IsolationForestConfig


@pytest.mark.parametrize("ensemble", [False, True])
def test_calibration_is_explicit_and_leaves_feature_metadata_unchanged(grouped_categorical_model, ensemble):
    df, model, metadata = grouped_categorical_model
    before = metadata.to_json()
    if ensemble:
        calibration = compute_score_quantiles_ensemble([model, model], df, ["status", "enabled"], metadata)
    else:
        calibration = compute_score_quantiles(model, df, ["status", "enabled"], metadata)

    assert metadata.to_json() == before
    assert set(calibration.global_quantiles) == set(SCORE_QUANTILE_KEYS)
    assert len(calibration.known_group_keys) == 2
    assert set(calibration.group_quantiles) == set(calibration.known_group_keys)
    assert all(set(quantiles) == set(SCORE_QUANTILE_KEYS) for quantiles in calibration.group_quantiles.values())
    assert not metadata.baseline_medians


def test_incomplete_group_calibration_preserves_known_membership(spark):
    df = spark.createDataFrame(
        [("North", 0.2), ("North", 0.5), ("South", None), (None, 0.3)], "region string, anomaly_score double"
    )
    calibration = compute_baseline_score_quantiles(df, ["region"])
    assert len(calibration.known_group_keys) == 3
    assert len(calibration.group_quantiles) == 2
    assert set(calibration.group_quantiles).issubset(calibration.known_group_keys)


@pytest.mark.parametrize("ensemble", [False, True])
def test_calibration_accepts_features_named_like_scoring_outputs(spark, ensemble: bool):
    """Calibration must read its computed scores, not collide with a same-named training feature."""
    df = spark.createDataFrame([(float(i), float(i % 7)) for i in range(80)], "anomaly_score double, prediction double")
    columns = ["anomaly_score", "prediction"]
    params = AnomalyParams(algorithm_config=IsolationForestConfig(num_trees=10, random_seed=42))
    model, _, metadata = fit_isolation_forest(df, columns, params)
    before = metadata.to_json()

    calibration = (
        compute_score_quantiles_ensemble([model, model], df, columns, metadata)
        if ensemble
        else compute_score_quantiles(model, df, columns, metadata)
    )

    assert metadata.to_json() == before
    assert set(calibration.global_quantiles) == set(SCORE_QUANTILE_KEYS)
    assert not calibration.group_quantiles
    assert not calibration.known_group_keys
    assert df.columns == columns
