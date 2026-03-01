"""Segmented (segment-based) anomaly scoring."""

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from databricks.labs.dqx.anomaly.model_discovery import extract_quantile_points
from databricks.labs.dqx.anomaly.drift import check_segment_drift
from databricks.labs.dqx.anomaly.model_registry import AnomalyModelRecord, AnomalyModelRegistry
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
    check_segment_drift(
        segment_df,
        config.columns,
        segment_model,
        config.drift_threshold,
        config.drift_threshold_value,
    )

    assert (
        segment_model.features.feature_metadata is not None
    ), f"Model {segment_model.identity.model_name} missing feature_metadata"

    if config.driver_only:
        segment_scored = score_with_sklearn_model_local(
            segment_model.identity.model_uri,
            segment_df,
            config.columns,
            segment_model.features.feature_metadata,
            config.merge_columns,
            include_contributions=config.include_contributions,
            model_record=segment_model,
        )
    else:
        segment_scored = score_with_sklearn_model(
            segment_model.identity.model_uri,
            segment_df,
            config.columns,
            segment_model.features.feature_metadata,
            config.merge_columns,
            include_contributions=config.include_contributions,
            model_record=segment_model,
        )

    segment_scored = segment_scored.withColumn("anomaly_score_std", F.lit(0.0))
    segment_scored = segment_scored.withColumnRenamed("anomaly_score", config.score_col)
    segment_scored = segment_scored.withColumnRenamed("anomaly_score_std", config.score_std_col)

    if config.include_contributions and "anomaly_contributions" in segment_scored.columns:
        segment_scored = segment_scored.withColumnRenamed("anomaly_contributions", config.contributions_col)

    quantile_points = extract_quantile_points(segment_model)
    segment_scored = add_severity_percentile_column(
        segment_scored,
        score_col=config.score_col,
        severity_col=config.severity_col,
        quantile_points=quantile_points,
    )

    segment_scored = add_info_column(
        segment_scored,
        config.model_name,
        config.threshold,
        info_col_name=config.info_col,
        segment_values=segment_model.segmentation.segment_values,
        include_contributions=config.include_contributions,
        include_confidence=config.include_confidence,
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
            config.include_contributions,
            config.include_confidence,
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
    if config.include_contributions:
        internal_to_remove.append(config.contributions_col)
    columns_to_keep = [c for c in result.columns if c not in internal_to_remove]
    result = result.select(*columns_to_keep)

    df_to_join = df if config.row_filter else df_to_score
    result = join_filtered_results_back(df_to_join, result, config.merge_columns, config.score_col, config.info_col)

    result = result.drop(config.score_col)
    return result
