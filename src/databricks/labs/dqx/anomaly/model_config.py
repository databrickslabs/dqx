"""Model configuration and record structure for row anomaly detection.

Single responsibility: model record structure (dataclasses) and config identity
(compute_config_hash). Persistence lives in model_registry.
"""

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime


@dataclass
class ModelIdentity:
    """Core model identification (5 fields)."""

    model_name: str
    model_uri: str
    algorithm: str
    mlflow_run_id: str
    status: str = "active"

    @property
    def model_uris(self) -> list[str]:
        # Ensemble records store comma-separated MLflow URIs; single-model records store one URI.
        # Comma is always the list separator (set via ",".join(...) in training_strategies); MLflow
        # URIs do not legitimately contain commas.
        return self.model_uri.split(",")

    @property
    def is_ensemble(self) -> bool:
        return len(self.model_uris) > 1


@dataclass
class TrainingMetadata:
    """Training configuration and metrics (7 fields)."""

    columns: list[str]
    hyperparameters: dict[str, str]
    training_rows: int
    training_time: datetime
    metrics: dict[str, float] | None = None
    score_quantiles: dict[str, float] | None = None
    baseline_stats: dict[str, dict[str, float]] | None = None


@dataclass
class FeatureEngineering:
    """Feature engineering metadata (5 fields)."""

    mode: str = "spark"
    column_types: dict[str, str] | None = None
    feature_metadata: str | None = None
    feature_importance: dict[str, float] | None = None
    temporal_config: dict[str, str] | None = None


@dataclass
class SegmentationConfig:
    """Segmentation configuration (5 fields)."""

    segment_by: list[str] | None = None
    segment_values: dict[str, str] | None = None
    is_global_model: bool = True
    sklearn_version: str | None = None
    config_hash: str | None = None


@dataclass
class AnomalyModelRecord:
    """Registry record for a trained anomaly model using composition.

    Composed of 4 focused components, each under the 16-attribute limit:
    - identity: Core model identification (5 fields)
    - training: Training configuration and metrics (6 fields)
    - features: Feature engineering metadata (5 fields)
    - segmentation: Segmentation configuration (5 fields)

    Stored as nested structs in Delta tables (no flattening needed).
    """

    identity: ModelIdentity
    training: TrainingMetadata
    features: FeatureEngineering
    segmentation: SegmentationConfig


def compute_config_hash(columns: list[str], segment_by: list[str] | None) -> str:
    """Generate stable hash of model configuration.

    Args:
        columns: List of column names used for training
        segment_by: List of columns used for segmentation, or None

    Returns:
        16-character hex string (first 16 chars of SHA256 hash)

    Note:
        This hash uniquely identifies a model configuration based on:
        - Sorted list of columns (order-independent)
        - Sorted list of segment_by columns (order-independent)
        Used for collision detection when same model_name is reused with different configs.
    """
    config = {
        "columns": sorted(columns),
        "segment_by": sorted(segment_by) if segment_by else None,
    }
    config_str = json.dumps(config, sort_keys=True)
    return hashlib.sha256(config_str.encode()).hexdigest()[:16]
