"""Ensemble trainer for anomaly detection models.

Encapsulates the logic for training multiple models with different random seeds
to create a robust ensemble.
"""

from copy import deepcopy
from typing import Any

import pandas as pd
from pyspark.sql import DataFrame
from sklearn.pipeline import Pipeline
from mlflow.models import infer_signature

from databricks.labs.dqx.anomaly.core import (
    aggregate_ensemble_metrics,
    compute_score_quantiles_ensemble,
    compute_validation_metrics,
    fit_sklearn_model,
    prepare_training_features,
)
from databricks.labs.dqx.anomaly.mlflow_registry import ModelRegistryBase, get_default_registry
from databricks.labs.dqx.anomaly.types import EnsembleTrainingResult
from databricks.labs.dqx.anomaly.transformers import SparkFeatureMetadata
from databricks.labs.dqx.config import AnomalyParams


class EnsembleTrainer:
    """Trains ensemble of anomaly detection models with different random seeds.

    Responsibilities:
    - Feature engineering (done once, reused for all models)
    - Training multiple models with varied seeds
    - Computing validation metrics for each model
    - Registering models to the registry
    - Aggregating ensemble metrics

    Uses dependency injection for the model registry, enabling testing with mocks.
    """

    def __init__(self, registry: ModelRegistryBase | None = None) -> None:
        """Initialize ensemble trainer.

        Args:
            registry: Model registry to use. Defaults to MLflow/Unity Catalog.
        """
        self._registry = registry or get_default_registry()

    def train(
        self,
        train_df: DataFrame,
        val_df: DataFrame,
        columns: list[str],
        params: AnomalyParams,
        ensemble_size: int,
        model_name: str,
    ) -> EnsembleTrainingResult:
        """Train an ensemble of models.

        Args:
            train_df: Training DataFrame
            val_df: Validation DataFrame
            columns: Feature columns to use
            params: Training parameters
            ensemble_size: Number of models in ensemble
            model_name: Base name for registered models

        Returns:
            EnsembleTrainingResult with model URIs, metrics, and metadata
        """
        self._registry.ensure_registry_configured()

        base_params = params or AnomalyParams()
        train_pandas, feature_metadata = self._prepare_features(train_df, columns, base_params)
        models, hyperparams, signature = self._train_models(train_pandas, base_params, ensemble_size)
        all_metrics = self._compute_metrics(models, val_df, columns, feature_metadata)
        model_uris = self._register_models(models, model_name, ensemble_size, hyperparams, all_metrics, signature)
        aggregated_metrics = aggregate_ensemble_metrics(all_metrics)
        score_quantiles = compute_score_quantiles_ensemble(models, train_df, columns, feature_metadata)

        return EnsembleTrainingResult(
            model_uris=model_uris,
            models=models,
            hyperparams=hyperparams,
            aggregated_metrics=aggregated_metrics,
            score_quantiles=score_quantiles,
            feature_metadata=feature_metadata,
        )

    def _prepare_features(
        self, train_df: DataFrame, columns: list[str], params: AnomalyParams
    ) -> tuple[pd.DataFrame, SparkFeatureMetadata]:
        """Prepare training features (done once for all ensemble members)."""
        return prepare_training_features(train_df, columns, params)

    def _train_models(
        self, train_pandas: pd.DataFrame, base_params: AnomalyParams, ensemble_size: int
    ) -> tuple[list[Pipeline], dict[str, Any], Any]:
        """Train all ensemble members with different random seeds."""
        models: list[Pipeline] = []
        hyperparams: dict[str, Any] = {}
        signature = None

        for i in range(ensemble_size):
            modified_params = deepcopy(base_params)
            modified_params.algorithm_config.random_seed += i

            model, model_hyperparams = fit_sklearn_model(train_pandas, modified_params)
            models.append(model)

            if i == 0:
                hyperparams = model_hyperparams
                predictions = model.predict(train_pandas)
                signature = infer_signature(train_pandas, predictions)

        return models, hyperparams, signature

    def _compute_metrics(
        self,
        models: list[Pipeline],
        val_df: DataFrame,
        columns: list[str],
        feature_metadata: SparkFeatureMetadata,
    ) -> list[dict[str, float]]:
        """Compute validation metrics for all models."""
        return [compute_validation_metrics(model, val_df, columns, feature_metadata) for model in models]

    def _register_models(
        self,
        models: list[Pipeline],
        model_name: str,
        ensemble_size: int,
        hyperparams: dict[str, Any],
        all_metrics: list[dict[str, float]],
        signature: Any,
    ) -> list[str]:
        """Register all ensemble members to the registry."""
        model_uris: list[str] = []
        for i, (model, metrics) in enumerate(zip(models, all_metrics, strict=True)):
            ensemble_model_name = f"{model_name}_ensemble_{i}"
            model_uri = self._registry.register_model(
                model=model,
                model_name=ensemble_model_name,
                signature=signature,
                hyperparams=hyperparams,
                metrics=metrics,
                tags={"ensemble_index": i, "ensemble_size": ensemble_size},
            )
            model_uris.append(model_uri)
        return model_uris


def train_ensemble(
    train_df: DataFrame,
    val_df: DataFrame,
    columns: list[str],
    params: AnomalyParams,
    ensemble_size: int,
    model_name: str,
) -> tuple[list[str], dict[str, Any], dict[str, float], dict[str, float], SparkFeatureMetadata]:
    """Train ensemble of models with different random seeds.

    This function delegates to EnsembleTrainer for the actual implementation.
    Kept for backward compatibility with existing code.
    """
    trainer = EnsembleTrainer()
    result = trainer.train(train_df, val_df, columns, params, ensemble_size, model_name)
    return (
        result.model_uris,
        result.hyperparams,
        result.aggregated_metrics,
        result.score_quantiles,
        result.feature_metadata,
    )
