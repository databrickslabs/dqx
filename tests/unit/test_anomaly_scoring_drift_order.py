from datetime import datetime
from typing import cast

import pytest
from pyspark.sql import DataFrame

from databricks.labs.dqx.anomaly import check_funcs
from databricks.labs.dqx.anomaly.check_funcs import ScoringConfig
from databricks.labs.dqx.anomaly.model_registry import (
    AnomalyModelRecord,
    FeatureEngineering,
    ModelIdentity,
    SegmentationConfig,
    TrainingMetadata,
    compute_config_hash,
)
from databricks.labs.dqx.errors import InvalidParameterError


def test_score_global_model_uses_filtered_df_for_drift(monkeypatch) -> None:
    sentinel_df = cast(DataFrame, object())
    filtered_df = cast(DataFrame, object())
    captured: dict[str, object] = {}

    def fake_prepare(df, row_filter):
        captured["prepare"] = (df, row_filter)
        return filtered_df

    def fake_check(df, *_args, **_kwargs):
        captured["drift_df"] = df
        raise RuntimeError("stop")

    monkeypatch.setattr(check_funcs, "_prepare_scoring_dataframe", fake_prepare)
    monkeypatch.setattr(check_funcs, "_check_and_warn_drift", fake_check)

    columns = ["amount"]
    config = ScoringConfig(
        columns=columns,
        model_name="catalog.schema.model",
        registry_table="catalog.schema.registry",
        threshold=0.5,
        merge_columns=[],
        row_filter="amount > 0",
        drift_threshold=0.1,
        driver_only=True,
    )
    record = AnomalyModelRecord(
        identity=ModelIdentity(
            model_name="catalog.schema.model",
            model_uri="models:/catalog.schema.model/1",
            algorithm="IsolationForest",
            mlflow_run_id="run123",
        ),
        training=TrainingMetadata(
            columns=columns,
            hyperparameters={},
            training_rows=10,
            training_time=datetime.now(),
        ),
        features=FeatureEngineering(feature_metadata="{}"),
        segmentation=SegmentationConfig(config_hash=compute_config_hash(columns, None)),
    )

    score_global_model = getattr(check_funcs, "_score_global_model")
    with pytest.raises(RuntimeError, match="stop"):
        score_global_model(sentinel_df, record, config)

    assert captured["prepare"] == (sentinel_df, config.row_filter)
    assert captured["drift_df"] is filtered_df


def test_resolve_internal_columns_hashes_on_double_collision() -> None:
    class DummyDf:
        columns = [
            "anomaly_score",
            "_dq_anomaly_score",
            "anomaly_score_std",
            "_dq_anomaly_score_std",
            "anomaly_contributions",
            "_dq_anomaly_contributions",
            "severity_percentile",
            "_dq_severity_percentile",
        ]

    resolve_internal_columns = getattr(check_funcs, "_resolve_internal_columns")
    score_col, score_std_col, contributions_col, severity_col = resolve_internal_columns(cast(DataFrame, DummyDf()))

    assert score_col.startswith("_dq_anomaly_score_")
    assert score_std_col.startswith("_dq_anomaly_score_std_")
    assert contributions_col.startswith("_dq_anomaly_contributions_")
    assert severity_col.startswith("_dq_severity_percentile_")


def test_resolve_internal_columns_raises_on_hashed_collision(monkeypatch) -> None:
    base_columns = [
        "anomaly_score",
        "_dq_anomaly_score",
        "anomaly_score_std",
        "_dq_anomaly_score_std",
        "anomaly_contributions",
        "_dq_anomaly_contributions",
        "severity_percentile",
        "_dq_severity_percentile",
    ]

    class DummyDf:
        columns = base_columns

    resolve_internal_columns = getattr(check_funcs, "_resolve_internal_columns")
    score_col, _, _, _ = resolve_internal_columns(cast(DataFrame, DummyDf()))

    class FakeHash:
        def hexdigest(self) -> str:
            return "deadbeef" * 5

    monkeypatch.setattr(check_funcs.hashlib, "sha1", lambda *_args, **_kwargs: FakeHash())

    class DummyDfCollision:
        columns = base_columns + [score_col, "_dq_anomaly_score_deadbeef"]

    with pytest.raises(InvalidParameterError):
        resolve_internal_columns(cast(DataFrame, DummyDfCollision()))
