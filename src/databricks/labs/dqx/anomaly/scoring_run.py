"""Global and segmented anomaly model scoring.

Provides score_global_model, score_segmented, and load_segment_models.
Kept in one module to avoid over-fragmentation of the scoring layer.
"""

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from databricks.labs.dqx.anomaly.model_discovery import extract_quantile_points
from databricks.labs.dqx.anomaly.drift import check_and_warn_drift, check_segment_drift, format_drift_summary
from databricks.labs.dqx.anomaly.ensemble_scorer import (
    score_ensemble_models,
    score_ensemble_models_local,
)
from databricks.labs.dqx.anomaly.model_config import compute_config_hash
from databricks.labs.dqx.anomaly.model_loader import check_model_staleness
from databricks.labs.dqx.anomaly.model_registry import AnomalyModelRecord, AnomalyModelRegistry
from databricks.labs.dqx.anomaly.anomaly_llm_explainer import ExplanationContext, add_explanation_column
from databricks.labs.dqx.anomaly.scoring_utils import (
    add_info_column,
    add_severity_percentile_column,
    apply_row_filter,
    create_null_scored_dataframe,
    join_filtered_results_back,
)
from databricks.labs.dqx.anomaly.scoring_config import ScoringConfig
from databricks.labs.dqx.anomaly.segment_utils import build_segment_filter
from databricks.labs.dqx.anomaly.single_model_scorer import (
    score_with_sklearn_model,
    score_with_sklearn_model_local,
)
from databricks.labs.dqx.errors import InvalidParameterError


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
    drift_result = check_and_warn_drift(
        df_filtered,
        config.columns,
        record,
        config.model_name,
        config.drift_threshold,
        config.drift_threshold_value,
    )

    model_uris = record.identity.model_uris
    if record.features.feature_metadata is None:
        raise InvalidParameterError(f"Model {record.identity.model_name} missing feature_metadata")

    if config.driver_only:
        scored_df = (
            score_ensemble_models_local(
                model_uris,
                df_filtered,
                config.columns,
                record.features.feature_metadata,
                config.merge_columns,
                config.enable_contributions,
                model_record=record,
            )
            if record.identity.is_ensemble
            else score_with_sklearn_model_local(
                record.identity.model_uri,
                df_filtered,
                config.columns,
                record.features.feature_metadata,
                config.merge_columns,
                enable_contributions=config.enable_contributions,
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
                config.enable_contributions,
                model_record=record,
            )
            if record.identity.is_ensemble
            else score_with_sklearn_model(
                record.identity.model_uri,
                df_filtered,
                config.columns,
                record.features.feature_metadata,
                config.merge_columns,
                enable_contributions=config.enable_contributions,
                model_record=record,
            ).withColumn("anomaly_score_std", F.lit(0.0))
        )

    scored_df = scored_df.withColumnRenamed("anomaly_score", config.score_col)
    scored_df = scored_df.withColumnRenamed("anomaly_score_std", config.score_std_col)
    if config.enable_contributions and "anomaly_contributions" in scored_df.columns:
        scored_df = scored_df.withColumnRenamed("anomaly_contributions", config.contributions_col)

    quantile_points = extract_quantile_points(record)
    scored_df = add_severity_percentile_column(
        scored_df,
        score_col=config.score_col,
        severity_col=config.severity_col,
        quantile_points=quantile_points,
    )

    if config.enable_ai_explanation:
        scored_df = add_explanation_column(
            scored_df,
            ExplanationContext.from_scoring_config(config),
            segment_values=None,
            is_ensemble=record.identity.is_ensemble,
            drift_summary=format_drift_summary(drift_result),
        )

    scored_df = add_info_column(
        scored_df,
        config.model_name,
        config.threshold,
        info_col_name=config.info_col,
        segment_values=None,
        enable_contributions=config.enable_contributions,
        enable_confidence_std=config.enable_confidence_std,
        ai_explanation_col=config.ai_explanation_col if config.enable_ai_explanation else None,
        score_col=config.score_col,
        score_std_col=config.score_std_col,
        contributions_col=config.contributions_col,
        severity_col=config.severity_col,
    )

    internal_to_remove = [config.score_std_col, config.severity_col]
    if config.enable_contributions:
        internal_to_remove.append(config.contributions_col)
    if config.enable_ai_explanation:
        internal_to_remove.append(config.ai_explanation_col)

    if config.row_filter:
        columns_to_keep = [col for col in scored_df.columns if col not in internal_to_remove]
    else:
        internal_to_remove.append(config.score_col)
        columns_to_keep = [col for col in scored_df.columns if col not in internal_to_remove]
    scored_df = scored_df.select(*columns_to_keep)

    if config.row_filter:
        scored_df = join_filtered_results_back(df, scored_df, config.merge_columns, config.score_col, config.info_col)
        scored_df = scored_df.drop(config.score_col)

    return scored_df


def load_segment_models(
    registry_client: AnomalyModelRegistry,
    config: ScoringConfig,
) -> list[AnomalyModelRecord]:
    """Load all segment models for a base model from the registry."""
    all_segments = registry_client.get_all_segment_models(config.registry_table, config.model_name)
    if not all_segments:
        raise InvalidParameterError(
            f"No segment models found for base model '{config.model_name}'. "
            "Train segmented models first using anomaly.train(...)."
        )
    return all_segments


def score_single_segment(
    segment_df: DataFrame,
    segment_model: AnomalyModelRecord,
    config: ScoringConfig,
) -> DataFrame:
    """Score a single segment with its specific model."""
    drift_result = check_segment_drift(
        segment_df,
        config.columns,
        segment_model,
        config.drift_threshold,
        config.drift_threshold_value,
    )

    if segment_model.features.feature_metadata is None:
        raise InvalidParameterError(
            f"Model '{segment_model.identity.model_name}' is missing feature_metadata required for scoring."
        )

    if config.driver_only:
        segment_scored = score_with_sklearn_model_local(
            segment_model.identity.model_uri,
            segment_df,
            config.columns,
            segment_model.features.feature_metadata,
            config.merge_columns,
            enable_contributions=config.enable_contributions,
            model_record=segment_model,
        )
    else:
        segment_scored = score_with_sklearn_model(
            segment_model.identity.model_uri,
            segment_df,
            config.columns,
            segment_model.features.feature_metadata,
            config.merge_columns,
            enable_contributions=config.enable_contributions,
            model_record=segment_model,
        )

    segment_scored = segment_scored.withColumn("anomaly_score_std", F.lit(0.0))
    segment_scored = segment_scored.withColumnRenamed("anomaly_score", config.score_col)
    segment_scored = segment_scored.withColumnRenamed("anomaly_score_std", config.score_std_col)

    if config.enable_contributions and "anomaly_contributions" in segment_scored.columns:
        segment_scored = segment_scored.withColumnRenamed("anomaly_contributions", config.contributions_col)

    quantile_points = extract_quantile_points(segment_model)
    segment_scored = add_severity_percentile_column(
        segment_scored,
        score_col=config.score_col,
        severity_col=config.severity_col,
        quantile_points=quantile_points,
    )

    if config.enable_ai_explanation:
        segment_scored = add_explanation_column(
            segment_scored,
            ExplanationContext.from_scoring_config(config),
            segment_model.segmentation.segment_values,
            segment_model.identity.is_ensemble,
            drift_summary=format_drift_summary(drift_result),
        )

    segment_scored = add_info_column(
        segment_scored,
        config.model_name,
        config.threshold,
        info_col_name=config.info_col,
        segment_values=segment_model.segmentation.segment_values,
        enable_contributions=config.enable_contributions,
        enable_confidence_std=config.enable_confidence_std,
        ai_explanation_col=config.ai_explanation_col if config.enable_ai_explanation else None,
        score_col=config.score_col,
        score_std_col=config.score_std_col,
        contributions_col=config.contributions_col,
        severity_col=config.severity_col,
    )

    return segment_scored


def score_segmented(
    df: DataFrame,
    config: ScoringConfig,
    registry_client: AnomalyModelRegistry,
    all_segments: list[AnomalyModelRecord] | None = None,
) -> DataFrame:
    """Score DataFrame using segment-specific models."""
    all_segments = all_segments if all_segments is not None else load_segment_models(registry_client, config)

    if not all_segments:
        raise InvalidParameterError(
            f"No segment models found for base model '{config.model_name}'. "
            "Train segmented models first using anomaly.train(...)."
        )

    df_to_score = apply_row_filter(df, config.row_filter)

    scored_dfs: list[DataFrame] = []

    for segment_model in all_segments:
        segment_filter = build_segment_filter(segment_model.segmentation.segment_values)
        if segment_filter is None:
            continue

        segment_df = df_to_score.filter(segment_filter)
        if segment_df.limit(1).count() == 0:
            continue
        segment_scored = score_single_segment(segment_df, segment_model, config)
        scored_dfs.append(segment_scored)

    if not scored_dfs:
        result = create_null_scored_dataframe(
            df_to_score,
            config.enable_contributions,
            config.enable_confidence_std,
            score_col=config.score_col,
            score_std_col=config.score_std_col,
            contributions_col=config.contributions_col,
            severity_col=config.severity_col,
            info_col_name=config.info_col,
        )
    else:
        result = scored_dfs[0]
        for sdf in scored_dfs[1:]:
            result = result.union(sdf)

    internal_to_remove = [config.score_std_col, config.severity_col]
    if config.enable_contributions:
        internal_to_remove.append(config.contributions_col)
    if config.enable_ai_explanation:
        internal_to_remove.append(config.ai_explanation_col)
    columns_to_keep = [c for c in result.columns if c not in internal_to_remove]
    result = result.select(*columns_to_keep)

    df_to_join = df if config.row_filter else df_to_score
    result = join_filtered_results_back(df_to_join, result, config.merge_columns, config.score_col, config.info_col)

    result = result.drop(config.score_col)
    return result
