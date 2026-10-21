"""Validation metric values must satisfy the registry double-map contract."""

from unittest.mock import create_autospec

from pyspark.sql import DataFrame
from sklearn.pipeline import Pipeline

from databricks.labs.dqx.anomaly.core import compute_validation_metrics
from databricks.labs.dqx.anomaly.transformers import SparkFeatureMetadata


def test_empty_validation_count_is_a_double() -> None:
    empty = create_autospec(DataFrame, instance=True)
    empty.take.return_value = []
    model = create_autospec(Pipeline, instance=True)
    metadata = create_autospec(SparkFeatureMetadata, instance=True)
    metrics = compute_validation_metrics(model, empty, ["amount"], metadata)
    assert metrics == {"validation_rows": 0.0}
    assert isinstance(metrics["validation_rows"], float)
