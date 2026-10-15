"""Calibration returns explicit state without modifying the fitted transformations."""

from collections import Counter
import math

import pytest

from databricks.labs.dqx.anomaly.core import (
    SCORE_QUANTILE_KEYS,
    compute_baseline_score_quantiles,
    compute_score_quantiles,
    compute_score_quantiles_ensemble,
    fit_isolation_forest,
)
from databricks.labs.dqx.config import AnomalyParams, IsolationForestConfig
from databricks.labs.dqx.anomaly.segment_utils import build_baseline_key
from databricks.labs.dqx.anomaly.model_registry import AnomalyModelRegistry
from databricks.labs.dqx.anomaly.transformers import SparkFeatureMetadata
from databricks.labs.dqx.engine import DQEngine
from tests.integration_anomaly.conftest import create_anomaly_check_rule


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
        [("North", 0.2), ("North", 0.5)] * 15 + [("South", None), (None, 0.3)],
        "region string, anomaly_score double",
    )
    calibration = compute_baseline_score_quantiles(df, ["region"])
    assert len(calibration.known_group_keys) == 3
    assert set(calibration.group_quantiles) == {build_baseline_key({"region": "North"})}
    assert set(calibration.group_quantiles).issubset(calibration.known_group_keys)


def test_group_calibration_requires_thirty_finite_scores(spark):
    rows = [(name, 0.25) for name, count in (("below", 29), ("at", 30), ("above", 31)) for _ in range(count)]
    rows += [("below", value) for value in (None, float("nan"), float("inf"), float("-inf"))]
    rows += [("missing", None)] * 40
    df = spark.createDataFrame(rows, "region string, anomaly_score double")

    calibration = compute_baseline_score_quantiles(df, ["region"])

    keys = {name: build_baseline_key({"region": name}) for name in ("below", "at", "above", "missing")}
    assert set(calibration.known_group_keys) == set(keys.values())
    assert set(calibration.group_quantiles) == {keys["at"], keys["above"]}
    assert all(set(quantiles.values()) == {0.25} for quantiles in calibration.group_quantiles.values())


def test_small_known_groups_remain_scored_after_registry_reload(ws, spark, quick_model_factory):
    model, registry, _ = quick_model_factory(
        spark,
        columns=["amount"],
        baseline_by=["region"],
        train_data=[("large", 10.0 + (i % 3)) for i in range(100)] + [("small", 1000.0)] * 29,
        train_schema="region string, amount double",
        params=AnomalyParams(
            sample_fraction=1.0,
            train_ratio=1.0,
            algorithm_config=IsolationForestConfig(num_trees=10, random_seed=42),
        ),
    )
    record = AnomalyModelRegistry(spark).get_active_model(registry, model)
    assert record is not None and record.features.feature_metadata is not None
    metadata = SparkFeatureMetadata.from_json(record.features.feature_metadata)
    known_key = build_baseline_key({"region": "small"})
    assert known_key in metadata.baseline_group_keys
    assert known_key not in metadata.baseline_score_quantiles
    assert metadata.baseline_medians["amount"][known_key] == metadata.global_medians["amount"]

    values = [("small", 1000.0), ("small", 1000.0), ("large", 10.0), ("unseen", 1000.0)]
    source = spark.createDataFrame(values, "region string, amount double")
    checked = DQEngine(ws, spark).apply_checks(
        source,
        [create_anomaly_check_rule(model, registry, enable_contributions=False, enable_ai_explanation=False)],
    )
    rows = checked.collect()
    assert Counter((row.region, row.amount) for row in rows) == Counter(values)
    for row in rows:
        info = row["_dq_info"][0]["anomaly"]
        assert info["is_new_baseline"] is (row.region == "unseen")
        if row.region == "unseen":
            assert info["score"] is None and info["severity_percentile"] is None
        else:
            assert math.isfinite(info["score"]) and math.isfinite(info["severity_percentile"])


def test_distinct_group_keys_survive_registry_reload(ws, spark, quick_model_factory):
    groups = [("x\x1fy", "z"), ("x", "y\x1fz"), (None, "z"), ("\x00", "z"), ("", "z"), ("-1:", "z")]
    model, registry, _ = quick_model_factory(
        spark,
        columns=["amount"],
        baseline_by=["country", "product"],
        train_data=[
            (*group, float(100 * (index + 1) + offset % 3))
            for index, group in enumerate(groups)
            for offset in range(40)
        ],
        train_schema="country string, product string, amount double",
        params=AnomalyParams(
            sample_fraction=1.0,
            train_ratio=1.0,
            algorithm_config=IsolationForestConfig(num_trees=10, random_seed=42),
        ),
    )
    record = AnomalyModelRegistry(spark).get_active_model(registry, model)
    assert record is not None and record.features.feature_metadata is not None
    metadata = SparkFeatureMetadata.from_json(record.features.feature_metadata)
    keys = [build_baseline_key({"country": country, "product": product}) for country, product in groups]
    assert len(keys) == len(set(keys)) == len(groups)
    assert set(metadata.baseline_group_keys) == set(keys)
    assert set(metadata.baseline_score_quantiles) == set(keys)
    for index, key in enumerate(keys):
        assert metadata.baseline_medians["amount"][key] == 100 * (index + 1) + 1

    values = [(*group, float(100 * (index + 1) + 1)) for index, group in enumerate(groups)]
    values += [values[0], ("unseen", "z", 101.0)]
    source = spark.createDataFrame(values, "country string, product string, amount double")
    result = DQEngine(ws, spark).apply_checks(source, [create_anomaly_check_rule(model, registry)])
    assert [result.schema[name] for name in source.columns] == list(source.schema)
    assert set(result.columns) == {*source.columns, "_errors", "_warnings", "_dq_info"}
    rows = result.collect()
    assert Counter((row.country, row.product, row.amount) for row in rows) == Counter(values)
    for row in rows:
        info = row["_dq_info"][0]["anomaly"]
        unseen = row.country == "unseen"
        assert info["is_new_baseline"] is unseen
        if unseen:
            assert info["score"] is None and info["severity_percentile"] is None
        else:
            assert math.isfinite(info["score"]) and math.isfinite(info["severity_percentile"])


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
