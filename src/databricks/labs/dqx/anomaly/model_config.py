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
    """How a model relates to groups in the data (6 fields).

    ``baseline_by`` is duplicated here from the feature metadata on purpose. It also lives inside the
    ``features.feature_metadata`` JSON blob, which is where scoring reads it, but a JSON blob is not
    queryable: answering "is this model conditioned, and on what" meant parsing it. It is the first
    question worth asking when detection looks wrong, so it belongs in a column next to
    ``training.columns``.
    """

    segment_by: list[str] | None = None
    segment_values: dict[str, str] | None = None
    baseline_by: list[str] | None = None
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
    - segmentation: Grouping configuration (6 fields)

    Stored as nested structs in Delta tables (no flattening needed).
    """

    identity: ModelIdentity
    training: TrainingMetadata
    features: FeatureEngineering
    segmentation: SegmentationConfig


def compute_config_hash(columns: list[str], segment_by: list[str] | None, baseline_by: list[str] | None = None) -> str:
    """Generate stable hash of model configuration.

    Args:
        columns: List of column names used for training
        segment_by: List of columns used for segmentation, or None
        baseline_by: Columns the metrics are judged against, or None

    Returns:
        16-character hex string (first 16 chars of SHA256 hash)

    Note:
        This hash uniquely identifies a model configuration based on the sorted, order-independent
        lists of feature columns, segment columns and baseline columns. It is used for collision
        detection when the same model_name is reused with a different configuration.

        **Breaking change.** *baseline_by* joined the hash inputs in 0.17.0, which changes the hash of
        every configuration -- including ones with no grouping, since the key is present either way.
        A model registered before that therefore fails the configuration check in
        :func:`~databricks.labs.dqx.anomaly.scoring_run.score_global_model` and must be retrained.
        That is deliberate: without *baseline_by* in the hash, retraining under the same name with a
        different grouping produced an identical hash, so the one thing this hash exists to catch --
        same name, different configuration -- was invisible for the grouping. Row anomaly detection
        was Experimental through 0.16.0, which carries no backward-compatibility or on-disk format
        guarantee. See https://github.com/databrickslabs/dqx/issues/1484.
    """
    config = {
        "columns": sorted(columns),
        "segment_by": sorted(segment_by) if segment_by else None,
        "baseline_by": sorted(baseline_by) if baseline_by else None,
    }
    config_str = json.dumps(config, sort_keys=True)
    return hashlib.sha256(config_str.encode()).hexdigest()[:16]
