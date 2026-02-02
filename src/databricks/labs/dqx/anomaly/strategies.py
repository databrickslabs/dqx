"""Training strategy pattern for anomaly detection.

Enables different anomaly detection algorithms through a common interface.
Currently implements IsolationForest, but designed for extensibility.

Uses dependency injection for the model registry, enabling:
- Consistent registration path with EnsembleTrainer
- Easy mocking/testing
- Potential for alternative backends
"""

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame

from databricks.labs.dqx.anomaly.core import (
    compute_score_quantiles,
    compute_validation_metrics,
    fit_isolation_forest,
    prepare_engineered_pandas,
)
from databricks.labs.dqx.anomaly.ensemble import train_ensemble
from databricks.labs.dqx.anomaly.mlflow_registry import ModelRegistryBase, get_default_registry
from databricks.labs.dqx.anomaly.types import TrainingResult
from databricks.labs.dqx.config import AnomalyParams


class AnomalyTrainingStrategy(ABC):
    """Training strategy interface for anomaly models.

    Implement this interface to add new anomaly detection algorithms.
    Uses dependency injection for the model registry.
    """

    name: str

    def __init__(self, registry: ModelRegistryBase | None = None) -> None:
        """Initialize strategy with optional registry.

        Args:
            registry: Model registry to use. Defaults to MLflow/Unity Catalog.
        """
        self._registry = registry or get_default_registry()

    @abstractmethod
    def train(
        self,
        train_df: DataFrame,
        val_df: DataFrame,
        columns: list[str],
        params: AnomalyParams,
        model_name: str,
        *,
        allow_ensemble: bool,
    ) -> TrainingResult:
        """Train an anomaly detection model.

        Args:
            train_df: Training DataFrame
            val_df: Validation DataFrame
            columns: Feature columns to use
            params: Training parameters
            model_name: Name for registered model
            allow_ensemble: Whether to allow ensemble training

        Returns:
            TrainingResult with model URI, metrics, and metadata
        """


class IsolationForestTrainingStrategy(AnomalyTrainingStrategy):
    """IsolationForest training strategy (default).

    Uses sklearn's IsolationForest algorithm with optional ensemble training.
    Both single-model and ensemble paths use the same ModelRegistryBase abstraction.
    """

    name = "isolation_forest"

    def train(
        self,
        train_df: DataFrame,
        val_df: DataFrame,
        columns: list[str],
        params: AnomalyParams,
        model_name: str,
        *,
        allow_ensemble: bool,
    ) -> TrainingResult:
        """Train IsolationForest model(s).

        If allow_ensemble and params.ensemble_size > 1, trains an ensemble.
        Otherwise trains a single model using the registry abstraction.
        """
        ensemble_size = params.ensemble_size if allow_ensemble and params.ensemble_size else 1

        if ensemble_size > 1:
            model_uris, hyperparams, validation_metrics, score_quantiles, feature_metadata = train_ensemble(
                train_df, val_df, columns, params, ensemble_size, model_name
            )
            model_uri = ",".join(model_uris)
            run_id = "ensemble"
        else:
            model, hyperparams, feature_metadata = fit_isolation_forest(train_df, columns, params)
            validation_metrics = compute_validation_metrics(model, val_df, columns, feature_metadata)
            score_quantiles = compute_score_quantiles(model, train_df, columns, feature_metadata)

            self._registry.ensure_registry_configured()
            train_pandas = prepare_engineered_pandas(train_df, feature_metadata)
            model_uri, run_id = self._registry.register_model_with_signature_inference(
                model, model_name, train_pandas, hyperparams, validation_metrics
            )

        algorithm = f"IsolationForest_Ensemble_{ensemble_size}" if ensemble_size > 1 else "IsolationForest"
        return TrainingResult(
            model_uri=model_uri,
            run_id=run_id,
            hyperparams=hyperparams,
            validation_metrics=validation_metrics,
            score_quantiles=score_quantiles,
            feature_metadata=feature_metadata,
            ensemble_size=ensemble_size,
            algorithm=algorithm,
        )
