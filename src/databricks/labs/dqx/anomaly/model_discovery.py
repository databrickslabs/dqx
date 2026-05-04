"""Discover model columns, segments, and quantile points from the anomaly registry."""

from datetime import datetime

from pyspark.sql import DataFrame

from databricks.labs.dqx.anomaly.model_registry import AnomalyModelRecord, AnomalyModelRegistry
from databricks.labs.dqx.anomaly.scoring_config import SEVERITY_QUANTILE_KEYS
from databricks.labs.dqx.errors import InvalidParameterError


def get_record_for_discovery(
    registry_client: AnomalyModelRegistry,
    registry_table: str,
    model_name_local: str,
) -> AnomalyModelRecord:
    """Get model record for auto-discovery, checking global and segmented models."""
    record = registry_client.get_active_model(registry_table, model_name_local)

    if record:
        return record

    all_segments = registry_client.get_all_segment_models(registry_table, model_name_local)
    if all_segments:
        return select_segment_record(all_segments)

    raise InvalidParameterError(
        f"Model '{model_name_local}' not found in '{registry_table}'. " "Train first using anomaly.train(...)."
    )


def select_segment_record(all_segments: list[AnomalyModelRecord]) -> AnomalyModelRecord:
    """Select a deterministic segment record (latest training_time, tie-breaker by model_name)."""
    return max(
        all_segments,
        key=lambda record: (
            record.training.training_time or datetime.min,
            record.identity.model_name,
        ),
    )


def get_quantile_points_for_severity(record: AnomalyModelRecord) -> list[tuple[float, float]]:
    """Extract percentile->score points for severity mapping.

    Used internally for scoring and exposed for testing and advanced use.
    """
    return extract_quantile_points(record)


def extract_quantile_points(record: AnomalyModelRecord) -> list[tuple[float, float]]:
    """Extract percentile->score points for severity mapping."""
    quantiles = record.training.score_quantiles
    if not quantiles:
        raise InvalidParameterError(
            f"Model '{record.identity.model_name}' is missing score quantiles required for severity mapping. "
            "Retrain the model to compute severity percentiles."
        )

    points: list[tuple[float, float]] = []
    for percentile, key in SEVERITY_QUANTILE_KEYS:
        value = quantiles.get(key)
        if value is None:
            raise InvalidParameterError(
                f"Model '{record.identity.model_name}' is missing quantile '{key}'. "
                "Retrain the model to compute severity percentiles."
            )
        points.append((percentile, float(value)))
    return points


def fetch_model_columns_and_segments(
    df: DataFrame,
    model_name: str,
    registry_table: str,
) -> tuple[list[str], list[str] | None]:
    """Auto-discover columns and segmentation from the model registry.

    Returns:
        Tuple of (columns, segment_by).
    """
    registry_client = AnomalyModelRegistry(df.sparkSession)
    record = get_record_for_discovery(registry_client, registry_table, model_name)

    columns = list(record.training.columns)
    segment_by = record.segmentation.segment_by

    missing_columns = [c for c in columns if c not in df.columns]
    if missing_columns:
        raise InvalidParameterError(
            f"Input DataFrame is missing required columns for model '{model_name}': {missing_columns}. "
            f"Available columns: {df.columns}."
        )

    return columns, segment_by
