"""Orchestrates anomaly scoring: route global vs segmented, run global model pipeline."""

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from databricks.labs.dqx.anomaly.discovery import extract_quantile_points, select_segment_record
from databricks.labs.dqx.anomaly.drift_checks import check_and_warn_drift
from databricks.labs.dqx.anomaly.ensemble_scorer import (
    score_ensemble_models,
    score_ensemble_models_local,
)
from databricks.labs.dqx.anomaly.model_config import compute_config_hash
from databricks.labs.dqx.anomaly.model_loader import check_model_staleness
from databricks.labs.dqx.anomaly.model_registry import AnomalyModelRecord, AnomalyModelRegistry
from databricks.labs.dqx.anomaly.scoring import (
    add_info_column,
    add_severity_percentile_column,
)
from databricks.labs.dqx.anomaly.scoring_config import ScoringConfig
from databricks.labs.dqx.anomaly.scoring_helpers import (
    apply_row_filter,
    join_filtered_results_back,
)
from databricks.labs.dqx.anomaly.scoring_strategies import resolve_scoring_strategy
from databricks.labs.dqx.anomaly.segment_scoring import load_segment_models
from databricks.labs.dqx.anomaly.single_model_scorer import (
    score_with_sklearn_model,
    score_with_sklearn_model_local,
)
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
    assert first_segment.segmentation.segment_by is not None, "Segment model must have segment_by"
    strategy = resolve_scoring_strategy(first_segment.identity.algorithm)
    return strategy.score_segmented(df, config, registry_client, all_segments)


def score_global_model(
    df: DataFrame,
    record: AnomalyModelRecord,
    config: ScoringConfig,
) -> DataFrame:
    """Score using a global (non-segmented) model."""
    expected_hash = compute_config_hash(config.columns, config.segment_by)

    if expected_hash != record.segmentation.config_hash:
        raise InvalidParameterError(
            f"Configuration mismatch for model '{config.model_name}':\n"
            f"  Trained columns: {record.training.columns}\n"
            f"  Provided columns: {config.columns}\n"
            f"  Trained segment_by: {record.segmentation.segment_by}\n"
            f"  Provided segment_by: {config.segment_by}\n\n"
            f"This model was trained with a different configuration. Either:\n"
            f"  1. Use the correct columns/segments that match the trained model\n"
            f"  2. Retrain the model with the new configuration"
        )

    check_model_staleness(record, config.model_name)

    df_filtered = apply_row_filter(df, config.row_filter)
    check_and_warn_drift(
        df_filtered,
        config.columns,
        record,
        config.model_name,
        config.drift_threshold,
        config.drift_threshold_value,
    )

    model_uris = record.identity.model_uri.split(",")
    assert record.features.feature_metadata is not None, f"Model {record.identity.model_name} missing feature_metadata"

    if config.driver_only:
        scored_df = (
            score_ensemble_models_local(
                model_uris,
                df_filtered,
                config.columns,
                record.features.feature_metadata,
                config.merge_columns,
                config.include_contributions,
                model_record=record,
            )
            if len(model_uris) > 1
            else score_with_sklearn_model_local(
                record.identity.model_uri,
                df_filtered,
                config.columns,
                record.features.feature_metadata,
                config.merge_columns,
                include_contributions=config.include_contributions,
                model_record=record,
            ).withColumn("anomaly_score_std", F.lit(0.0))
        )
    else:
        scored_df = (
            score_ensemble_models(
                model_uris,
                df_filtered,
                config.columns,
                record.features.feature_metadata,
                config.merge_columns,
                config.include_contributions,
                model_record=record,
            )
            if len(model_uris) > 1
            else score_with_sklearn_model(
                record.identity.model_uri,
                df_filtered,
                config.columns,
                record.features.feature_metadata,
                config.merge_columns,
                include_contributions=config.include_contributions,
                model_record=record,
            ).withColumn("anomaly_score_std", F.lit(0.0))
        )

    scored_df = scored_df.withColumnRenamed("anomaly_score", config.score_col)
    scored_df = scored_df.withColumnRenamed("anomaly_score_std", config.score_std_col)
    if config.include_contributions and "anomaly_contributions" in scored_df.columns:
        scored_df = scored_df.withColumnRenamed("anomaly_contributions", config.contributions_col)

    quantile_points = extract_quantile_points(record)
    scored_df = add_severity_percentile_column(
        scored_df,
        score_col=config.score_col,
        severity_col=config.severity_col,
        quantile_points=quantile_points,
    )

    scored_df = add_info_column(
        scored_df,
        config.model_name,
        config.threshold,
        info_col_name=config.info_col,
        segment_values=None,
        include_contributions=config.include_contributions,
        include_confidence=config.include_confidence,
        score_col=config.score_col,
        score_std_col=config.score_std_col,
        contributions_col=config.contributions_col,
        severity_col=config.severity_col,
    )

    internal_to_remove = [config.score_std_col, config.severity_col]
    if config.include_contributions:
        internal_to_remove.append(config.contributions_col)

    if config.row_filter:
        columns_to_keep = [c for c in scored_df.columns if c not in internal_to_remove]
    else:
        internal_to_remove.append(config.score_col)
        columns_to_keep = [c for c in scored_df.columns if c not in internal_to_remove]
    scored_df = scored_df.select(*columns_to_keep)

    if config.row_filter:
        scored_df = join_filtered_results_back(df, scored_df, config.merge_columns, config.score_col, config.info_col)
        scored_df = scored_df.drop(config.score_col)

    return scored_df
