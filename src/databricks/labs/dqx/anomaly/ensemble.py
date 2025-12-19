"""
Ensemble model wrapper for anomaly detection.

Combines multiple IsolationForest models trained with different seeds
to provide more robust anomaly scores with confidence intervals.
"""

from __future__ import annotations


from pyspark.ml import Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol, Param, Params
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.sql import DataFrame
import pyspark.sql.functions as F


class EnsembleAnomalyTransformer(Transformer, HasInputCol, HasOutputCol, DefaultParamsReadable, DefaultParamsWritable):
    """
    Ensemble transformer that loads multiple models and averages their scores.

    Provides both mean anomaly score and standard deviation (confidence).
    """

    model_uris: Param = Param(
        Params._dummy(),
        "model_uris",
        "List of MLflow model URIs to load and ensemble",
    )

    def __init__(self, model_uris: list[str] | None = None):
        super().__init__()
        if model_uris:
            self._set(model_uris=model_uris)
        self._models = None

    def setModelUris(self, value: list[str]) -> EnsembleAnomalyTransformer:
        """Set the model URIs."""
        return self._set(model_uris=value)

    def getModelUris(self) -> list[str]:
        """Get the model URIs."""
        return self.getOrDefault(self.model_uris)

    def _load_models(self):
        """Lazy load all models."""
        if self._models is None:
            # Lazy import to avoid circular import issues with MLflow
            import mlflow.spark

            self._models = [mlflow.spark.load_model(uri) for uri in self.getModelUris()]
        return self._models

    def _transform(self, dataset: DataFrame) -> DataFrame:
        """
        Transform dataset by scoring with all models and averaging.

        Args:
            dataset: Input DataFrame.

        Returns:
            DataFrame with anomaly_score (mean), anomaly_score_std (confidence).
        """
        models = self._load_models()

        # Score with each model
        scored_dfs = []
        for i, model in enumerate(models):
            scored = model.transform(dataset)
            scored = scored.withColumnRenamed("anomaly_score", f"_score_{i}")
            scored_dfs.append(scored.select("*", f"_score_{i}"))

        # Combine scores
        result = scored_dfs[0]
        for i in range(1, len(scored_dfs)):
            result = result.crossJoin(scored_dfs[i].select(f"_score_{i}"))

        # Compute mean and std
        score_cols = [f"_score_{i}" for i in range(len(models))]

        # Mean score
        result = result.withColumn("anomaly_score", sum(F.col(col) for col in score_cols) / F.lit(len(models)))

        # Standard deviation (confidence)
        if len(models) > 1:
            mean_col = F.col("anomaly_score")
            variance = sum((F.col(col) - mean_col) ** 2 for col in score_cols) / F.lit(len(models) - 1)
            result = result.withColumn("anomaly_score_std", F.sqrt(variance))
        else:
            result = result.withColumn("anomaly_score_std", F.lit(0.0))

        # Drop intermediate columns
        for col in score_cols:
            result = result.drop(col)

        return result
