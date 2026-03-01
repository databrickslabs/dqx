"""Orchestrates anomaly scoring: route global vs segmented, run global model pipeline."""

from pyspark.sql import DataFrame

from databricks.labs.dqx.anomaly.model_discovery import select_segment_record
from databricks.labs.dqx.anomaly.model_registry import AnomalyModelRegistry
from databricks.labs.dqx.anomaly.scoring_config import ScoringConfig
from databricks.labs.dqx.anomaly.scoring_strategies import resolve_scoring_strategy
from databricks.labs.dqx.anomaly.scoring_run import load_segment_models
from databricks.labs.dqx.errors import InvalidParameterError


def run_anomaly_scoring(
    df_to_score: DataFrame,
    config: ScoringConfig,
    registry_table: str,
    model_name: str,
) -> DataFrame:
    """Route to segmented or global scoring and return scored DataFrame (caller drops row_id_col)."""
    registry_client = AnomalyModelRegistry(df_to_score.sparkSession)
    if config.segment_by:
        all_segments = load_segment_models(registry_client, config)
        strategy = resolve_scoring_strategy(all_segments[0].identity.algorithm)
        return strategy.score_segmented(df_to_score, config, registry_client, all_segments)

    record = registry_client.get_active_model(registry_table, model_name)
    if not record:
        fallback = try_segmented_scoring_fallback(df_to_score, config, registry_client)
        if fallback is not None:
            return fallback
        raise InvalidParameterError(
            f"Model '{model_name}' not found in '{registry_table}'. Train first using anomaly.train(...)."
        )
    strategy = resolve_scoring_strategy(record.identity.algorithm)
    return strategy.score_global(df_to_score, record, config)


def try_segmented_scoring_fallback(
    df: DataFrame,
    config: ScoringConfig,
    registry_client: AnomalyModelRegistry,
) -> DataFrame | None:
    """Try to score using segmented models as fallback. Returns None if no segments found."""
    all_segments = registry_client.get_all_segment_models(config.registry_table, config.model_name)
    if not all_segments:
        return None

    first_segment = select_segment_record(all_segments)
    if first_segment.segmentation.segment_by is None:
        raise InvalidParameterError("Segment model must have segment_by")
    strategy = resolve_scoring_strategy(first_segment.identity.algorithm)
    return strategy.score_segmented(df, config, registry_client, all_segments)
