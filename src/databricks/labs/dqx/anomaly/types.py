"""Type definitions for anomaly detection module.

Contains:
- Type protocols for duck typing (TrainedModel, MLflowSignature)
- Immutable data classes for training results and context
"""

from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from sklearn.pipeline import Pipeline

from databricks.labs.dqx.anomaly.transformers import SparkFeatureMetadata
from databricks.labs.dqx.config import AnomalyParams


# =============================================================================
# Type Protocols - Structural interfaces for duck typing
# =============================================================================


@runtime_checkable
class TrainedModel(Protocol):
    """Protocol for trained sklearn-compatible models.

    Any object with predict, decision_function, and fit methods satisfies this protocol.
    Uses sklearn naming convention where input features are passed as 'data'.
    """

    def predict(self, data: pd.DataFrame) -> np.ndarray:
        """Predict anomaly labels (-1 for anomaly, 1 for normal)."""

    def decision_function(self, data: pd.DataFrame) -> np.ndarray:
        """Return anomaly scores (lower = more anomalous)."""

    def fit(self, data: pd.DataFrame) -> "TrainedModel":
        """Fit the model on training data."""


class MLflowSignature(Protocol):
    """Protocol for MLflow model signatures."""

    inputs: Any
    outputs: Any


# =============================================================================
# Data Classes - Immutable value objects for training
# =============================================================================


@dataclass(frozen=True)
class TrainingResult:
    """Result of single model training."""

    model_uri: str
    run_id: str | None
    hyperparams: dict[str, Any]
    validation_metrics: dict[str, float]
    score_quantiles: dict[str, float]
    feature_metadata: SparkFeatureMetadata
    ensemble_size: int
    algorithm: str


@dataclass(frozen=True)
class EnsembleTrainingResult:
    """Result of ensemble training."""

    model_uris: list[str]
    models: list[Pipeline]
    hyperparams: dict[str, Any]
    aggregated_metrics: dict[str, float]
    score_quantiles: dict[str, float]
    feature_metadata: SparkFeatureMetadata


@dataclass(frozen=True)
class AnomalyTrainingContext:
    """Context containing all inputs needed for training."""

    spark: SparkSession
    df: DataFrame
    df_filtered: DataFrame
    model_name: str
    registry_table: str
    columns: list[str]
    segment_by: list[str] | None
    params: AnomalyParams
    expected_anomaly_rate: float
    exclude_columns: list[str] | None
    auto_discovery_used: bool


@dataclass(frozen=True)
class TrainingArtifacts:
    """Artifacts produced by training a single model or segment."""

    model_name: str
    model_uri: str
    run_id: str | None
    ensemble_size: int
    feature_metadata: SparkFeatureMetadata
    hyperparams: dict[str, Any]
    training_rows: int
    validation_metrics: dict[str, float]
    score_quantiles: dict[str, float]
    baseline_stats: dict[str, dict[str, float]]
    algorithm: str
    segment_values: dict[str, Any] | None = None
