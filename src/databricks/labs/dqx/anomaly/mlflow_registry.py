"""Model registry abstraction for anomaly detection.

Provides an abstract interface for model registration, with MLflow/Unity Catalog
as the default implementation. This abstraction enables:
- Unit testing with mock registries
- Potential support for alternative backends
- Clean separation of concerns
"""

import os
import inspect
from abc import ABC, abstractmethod
from typing import Any

import pandas as pd
import mlflow
from mlflow.models import infer_signature

from databricks.labs.dqx.anomaly.types import MLflowSignature, TrainedModel


class ModelRegistryBase(ABC):
    """Abstract base for model registration backends.

    Implementations should handle model persistence and versioning.
    """

    @abstractmethod
    def register_model(
        self,
        model: TrainedModel,
        model_name: str,
        signature: MLflowSignature,
        hyperparams: dict[str, Any],
        metrics: dict[str, float],
        tags: dict[str, Any] | None = None,
    ) -> str:
        """Register a model and return its URI.

        Args:
            model: Trained sklearn-compatible model
            model_name: Fully qualified model name (catalog.schema.model)
            signature: MLflow signature for input/output schema
            hyperparams: Model hyperparameters to log
            metrics: Validation metrics to log
            tags: Additional metadata tags

        Returns:
            Model URI in format models:/<name>/<version>
        """

    @abstractmethod
    def register_model_with_signature_inference(
        self,
        model: TrainedModel,
        model_name: str,
        train_pandas: pd.DataFrame,
        hyperparams: dict[str, Any],
        metrics: dict[str, float],
    ) -> tuple[str, str]:
        """Register a model, inferring signature from training data.

        Args:
            model: Trained sklearn-compatible model
            model_name: Fully qualified model name (catalog.schema.model)
            train_pandas: Training data for signature inference
            hyperparams: Model hyperparameters to log
            metrics: Validation metrics to log

        Returns:
            Tuple of (model_uri, run_id)
        """

    @abstractmethod
    def ensure_registry_configured(self) -> None:
        """Ensure the registry is properly configured for the environment."""


class MLflowModelRegistry(ModelRegistryBase):
    """MLflow/Unity Catalog implementation of model registry.

    Uses MLflow's sklearn integration for model logging and Unity Catalog
    for model versioning and governance.
    """

    def ensure_registry_configured(self) -> None:
        """Configure MLflow for Unity Catalog.

        Sets registry URI to 'databricks-uc' (or MLFLOW_REGISTRY_URI env var).
        Also sets tracking URI if MLFLOW_TRACKING_URI is set.
        """
        registry_uri = os.environ.get("MLFLOW_REGISTRY_URI", "databricks-uc")
        mlflow.set_registry_uri(registry_uri)
        tracking_uri = os.environ.get("MLFLOW_TRACKING_URI")
        if tracking_uri:
            mlflow.set_tracking_uri(tracking_uri)

    def register_model(
        self,
        model: TrainedModel,
        model_name: str,
        signature: MLflowSignature,
        hyperparams: dict[str, Any],
        metrics: dict[str, float],
        tags: dict[str, Any] | None = None,
    ) -> str:
        """Register model to MLflow/Unity Catalog.

        Creates a new MLflow run, logs the model with signature, hyperparameters,
        and metrics, then registers it to Unity Catalog.
        """
        with mlflow.start_run(run_name=model_name):
            model_info = log_sklearn_model_compatible(
                model=model,
                model_name=model_name,
                signature=signature,
            )
            mlflow.log_params(_flatten_hyperparams(hyperparams))
            mlflow.log_metrics(metrics)
            if tags:
                for key, value in tags.items():
                    mlflow.log_param(key, value)
            return f"models:/{model_name}/{model_info.registered_model_version}"

    def register_model_with_signature_inference(
        self,
        model: TrainedModel,
        model_name: str,
        train_pandas: pd.DataFrame,
        hyperparams: dict[str, Any],
        metrics: dict[str, float],
    ) -> tuple[str, str]:
        """Register model, inferring signature from training data.

        Creates a new MLflow run, infers the model signature from training data
        and predictions, logs the model with hyperparameters and metrics.
        """
        with mlflow.start_run() as run:
            predictions = model.predict(train_pandas)
            signature = infer_signature(train_pandas, predictions)

            model_info = log_sklearn_model_compatible(
                model=model,
                model_name=model_name,
                signature=signature,
            )

            mlflow.log_params(_flatten_hyperparams(hyperparams))
            mlflow.log_metrics(metrics)

            model_uri = f"models:/{model_name}/{model_info.registered_model_version}"
            return model_uri, run.info.run_id


def _flatten_hyperparams(hyperparams: dict[str, Any]) -> dict[str, Any]:
    """Flatten hyperparams dict for MLflow logging with prefix."""
    return {f"hyperparam_{k}": v for k, v in hyperparams.items() if v is not None}


def log_sklearn_model_compatible(
    *,
    model: TrainedModel,
    model_name: str,
    signature: MLflowSignature,
):
    """Log sklearn model with compatibility across MLflow API variants.

    Some runtimes accept `name=...` while others require `artifact_path=...`.
    """
    log_model_params = inspect.signature(mlflow.sklearn.log_model).parameters
    if "name" in log_model_params:
        return mlflow.sklearn.log_model(
            sk_model=model,
            name="model",
            registered_model_name=model_name,
            signature=signature,
        )
    return mlflow.sklearn.log_model(
        sk_model=model,
        artifact_path="model",
        registered_model_name=model_name,
        signature=signature,
    )


class _RegistryHolder:
    """Holder for the default registry instance. Avoids global statement."""

    _instance: ModelRegistryBase | None = None

    @classmethod
    def get(cls) -> ModelRegistryBase:
        """Get the default registry, creating if needed."""
        if cls._instance is None:
            cls._instance = MLflowModelRegistry()
        return cls._instance

    @classmethod
    def set(cls, registry: ModelRegistryBase) -> None:
        """Set the default registry (useful for testing)."""
        cls._instance = registry


def get_default_registry() -> ModelRegistryBase:
    """Get the default model registry instance."""
    return _RegistryHolder.get()


def set_default_registry(registry: ModelRegistryBase) -> None:
    """Set the default model registry (useful for testing).

    Args:
        registry: ModelRegistryBase implementation to use as default
    """
    _RegistryHolder.set(registry)
