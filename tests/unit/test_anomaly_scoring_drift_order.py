from datetime import datetime
from typing import cast

import pytest
from pyspark.sql import DataFrame

from databricks.labs.dqx.anomaly import check_funcs
from databricks.labs.dqx.anomaly.check_funcs import ScoringConfig
from databricks.labs.dqx.anomaly.model_config import compute_config_hash
from databricks.labs.dqx.anomaly.model_registry import (
    AnomalyModelRecord,
    FeatureEngineering,
    ModelIdentity,
    SegmentationConfig,
    TrainingMetadata,
)


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
