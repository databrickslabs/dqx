"""Persist actual validation metrics through the production registry schema."""

from datetime import datetime

import pytest
from pyspark.sql import SparkSession

from databricks.labs.dqx.anomaly.core import (
    compute_validation_metrics,
    fit_sklearn_model,
    prepare_training_features,
)
from databricks.labs.dqx.anomaly.model_config import (
    AnomalyModelRecord,
    FeatureEngineering,
    GroupingConfig,
    ModelIdentity,
    TrainingMetadata,
)
from databricks.labs.dqx.anomaly.model_registry import AnomalyModelRegistry
from databricks.labs.dqx.config import AnomalyParams, IsolationForestConfig


@pytest.mark.parametrize("validation_rows", [0, 3])
def test_single_model_validation_metrics_fit_registry_schema(spark: SparkSession, validation_rows: int) -> None:
    training = spark.createDataFrame([(float(index),) for index in range(12)], "amount double")
    params = AnomalyParams(ensemble_size=None, algorithm_config=IsolationForestConfig(num_trees=5, random_seed=42))
    features, metadata = prepare_training_features(training, ["amount"], params)
    model, _ = fit_sklearn_model(features, params)
    metrics = compute_validation_metrics(model, training.limit(validation_rows), ["amount"], metadata)
    record = AnomalyModelRecord(
        identity=ModelIdentity("validation_model", "runs:/validation/model", "isolation_forest", "validation"),
        training=TrainingMetadata(
            columns=["amount"],
            hyperparameters={},
            training_rows=12,
            training_time=datetime(2026, 1, 1),
            metrics=metrics,
        ),
        features=FeatureEngineering(feature_metadata=metadata.to_json()),
        grouping=GroupingConfig(),
    )

    rows = AnomalyModelRegistry.build_model_df(spark, record).select("training.metrics").collect()

    assert len(rows) == 1
    assert rows[0].metrics == metrics
    assert metrics["validation_rows"] == float(validation_rows)
    assert all(isinstance(value, float) for value in metrics.values())
