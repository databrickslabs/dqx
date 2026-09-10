"""Discover model columns and quantile points from the anomaly registry."""

from pyspark.sql import DataFrame

from databricks.labs.dqx.anomaly.model_registry import AnomalyModelRecord, AnomalyModelRegistry
from databricks.labs.dqx.anomaly.scoring_config import SEVERITY_QUANTILE_KEYS
from databricks.labs.dqx.errors import InvalidParameterError


def get_record_for_discovery(
    registry_client: AnomalyModelRegistry,
    registry_table: str,
    model_name_local: str,
) -> AnomalyModelRecord:
    """Get the active model record for auto-discovery."""
    record = registry_client.get_active_model(registry_table, model_name_local)
    if record:
        return record

    raise InvalidParameterError(
        f"Model '{model_name_local}' not found in '{registry_table}'. " "Train first using anomaly.train(...)."
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


def fetch_model_columns(
    df: DataFrame,
    model_name: str,
    registry_table: str,
) -> list[str]:
    """Auto-discover the feature columns a model was trained on, from the registry."""
    registry_client = AnomalyModelRegistry(df.sparkSession)
    record = get_record_for_discovery(registry_client, registry_table, model_name)

    columns = list(record.training.columns)
    missing_columns = [c for c in columns if c not in df.columns]
    if missing_columns:
        raise InvalidParameterError(
            f"Input DataFrame is missing required columns for model '{model_name}': {missing_columns}. "
            f"Available columns: {df.columns}."
        )

    return columns
