"""Anomaly model scoring.

One model per registered name, conditioned on a baseline grouping when it has one. Kept in one
module to avoid over-fragmentation of the scoring layer.
"""

import logging

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from databricks.labs.dqx.anomaly.model_discovery import extract_quantile_points
from databricks.labs.dqx.anomaly.drift import check_and_warn_drift, format_drift_summary
from databricks.labs.dqx.anomaly.ensemble_scorer import (
    score_ensemble_models,
    score_ensemble_models_local,
)
from databricks.labs.dqx.anomaly.model_config import compute_config_hash
from databricks.labs.dqx.anomaly.model_loader import check_model_staleness
from databricks.labs.dqx.anomaly.model_registry import AnomalyModelRecord
from databricks.labs.dqx.anomaly.anomaly_llm_explainer import (
    ExplanationContext,
    add_explanation_column,
)
from databricks.labs.dqx.anomaly.scoring_utils import (
    add_baseline_severity_percentile_column,
    add_info_column,
    add_severity_percentile_column,
    apply_row_filter,
    join_filtered_results_back,
    mark_unseen_baselines,
    null_out_unseen_baseline_scores,
    permissive_quantile_points,
    UnseenGroupContext,
)
from databricks.labs.dqx.anomaly.scoring_config import SEVERITY_QUANTILE_KEYS, ScoringConfig
from databricks.labs.dqx.anomaly.transformers import SparkFeatureMetadata
from databricks.labs.dqx.anomaly.single_model_scorer import (
    score_with_sklearn_model,
    score_with_sklearn_model_local,
)
from databricks.labs.dqx.errors import InvalidParameterError


logger = logging.getLogger(__name__)


def _known_group_keys(parsed_metadata: SparkFeatureMetadata) -> list[str]:
    """Group keys the model actually saw in training.

    Read from the persisted baselines rather than from the per-group quantiles, because the
    baselines exist for every grouped model while the quantiles are dropped for groups whose
    calibration was incomplete — a group with a baseline was seen, whatever its calibration.
    """
    keys: set[str] = set()
    for baselines in parsed_metadata.baseline_medians.values():
        keys.update(baselines)
    return sorted(keys)


def _group_quantile_points(
    parsed_metadata: SparkFeatureMetadata,
) -> dict[str, list[tuple[float, float]]]:
    """Reshape persisted per-group quantiles into interpolation points, per group.

    A group missing any quantile key is dropped rather than partially interpolated: it falls back
    to the global calibration, which is a defined answer, where a gap in the points would not be.
    """
    return {
        key: [(percentile, quantiles[quantile_key]) for percentile, quantile_key in SEVERITY_QUANTILE_KEYS]
        for key, quantiles in parsed_metadata.baseline_score_quantiles.items()
        if all(quantile_key in quantiles for _, quantile_key in SEVERITY_QUANTILE_KEYS)
    }


def _add_severity(
    scored_df: DataFrame,
    config: ScoringConfig,
    parsed_metadata: SparkFeatureMetadata,
    group_quantile_points: dict[str, list[tuple[float, float]]],
    global_quantile_points: list[tuple[float, float]],
) -> DataFrame:
    """Calibrate severity per group where the model has per-group quantiles, globally otherwise."""
    if parsed_metadata.baseline_by and group_quantile_points:
        return add_baseline_severity_percentile_column(
            scored_df,
            score_col=config.score_col,
            severity_col=config.severity_col,
            baseline_by=parsed_metadata.baseline_by,
            group_quantile_points=group_quantile_points,
            fallback_quantile_points=global_quantile_points,
        )
    return add_severity_percentile_column(
        scored_df,
        score_col=config.score_col,
        severity_col=config.severity_col,
        quantile_points=global_quantile_points,
    )


def score_global_model(
    df: DataFrame,
    record: AnomalyModelRecord,
    config: ScoringConfig,
) -> DataFrame:
    """Score using the trained model, conditioned on its baseline grouping if it has one."""
    # baseline_by is a property of the trained model rather than something the caller supplies, so it
    # is read back from the persisted metadata. That makes the recomputed hash match for any model
    # trained by this version, and mismatch for one trained before baseline_by joined the hash --
    # which is the intended loud failure rather than an accident. See compute_config_hash.
    # A record with no persisted feature metadata cannot have been trained with a grouping, so it
    # hashes as ungrouped -- and will still mismatch, because the hash formula itself changed.
    trained_baseline_by = (
        SparkFeatureMetadata.from_json(record.features.feature_metadata).baseline_by
        if record.features.feature_metadata
        else None
    )
    expected_hash = compute_config_hash(config.columns, trained_baseline_by)

    if expected_hash != record.grouping.config_hash:
        raise InvalidParameterError(
            f"Configuration mismatch for model '{config.model_name}':\n"
            f"  Trained columns: {record.training.columns}\n"
            f"  Provided columns: {config.columns}\n"
            f"  Trained baseline_by: {trained_baseline_by or None}\n\n"
            f"This model was trained with a different configuration, or by a DQX version before\n"
            f"baseline_by became part of the configuration hash (0.17.0). Either:\n"
            f"  1. Use the columns that match the trained model\n"
            f"  2. Retrain the model — required for any model registered before 0.17.0"
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

    global_quantile_points = extract_quantile_points(record)
    parsed_metadata = SparkFeatureMetadata.from_json(record.features.feature_metadata)
    group_quantile_points = _group_quantile_points(parsed_metadata)
    # The scorers use these only to decide which rows are worth a SHAP call. With per-group
    # calibration the bound differs by group, so the gate takes the per-percentile minimum:
    # permissive, and add_info_column re-masks contributions on the real severity anyway.
    quantile_points = permissive_quantile_points(group_quantile_points, global_quantile_points)
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
                quantile_points=quantile_points,
                threshold=config.threshold,
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
                quantile_points=quantile_points,
                threshold=config.threshold,
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
                quantile_points=quantile_points,
                threshold=config.threshold,
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
                quantile_points=quantile_points,
                threshold=config.threshold,
            ).withColumn("anomaly_score_std", F.lit(0.0))
        )

    scored_df = scored_df.withColumnRenamed("anomaly_score", config.score_col)
    scored_df = scored_df.withColumnRenamed("anomaly_score_std", config.score_std_col)
    if config.enable_contributions and "anomaly_contributions" in scored_df.columns:
        scored_df = scored_df.withColumnRenamed("anomaly_contributions", config.contributions_col)

    # Mark unseen groups before severity, then null the score in place afterwards: severity is
    # interpolated from the score, so nulling first would leave severity computed from nothing.
    unseen_col = "__dqx_is_new_group"
    group_key_col = "__dqx_row_group_key"
    scored_df = mark_unseen_baselines(
        scored_df,
        parsed_metadata.baseline_by,
        _known_group_keys(parsed_metadata),
        unseen_col=unseen_col,
        group_key_col=group_key_col,
    )

    scored_df = _add_severity(scored_df, config, parsed_metadata, group_quantile_points, global_quantile_points)

    scored_df = null_out_unseen_baseline_scores(
        scored_df,
        unseen_col=unseen_col,
        score_col=config.score_col,
        severity_col=config.severity_col,
        contributions_col=config.contributions_col if config.enable_contributions else None,
        score_std_col=config.score_std_col,
    )

    if config.enable_ai_explanation:
        scored_df = add_explanation_column(
            scored_df,
            ExplanationContext.from_scoring_config(config, parsed_metadata, record.identity.algorithm),
            is_ensemble=record.identity.is_ensemble,
            drift_summary=format_drift_summary(drift_result, config.redact_columns),
        )

    scored_df = add_info_column(
        scored_df,
        config.model_name,
        config.threshold,
        output_columns=config.output_columns,
        info_col_name=config.info_col,
        enable_contributions=config.enable_contributions,
        enable_confidence_std=config.enable_confidence_std,
        ai_explanation_col=config.ai_explanation_col if config.enable_ai_explanation else None,
        unseen=UnseenGroupContext(
            unseen_col=unseen_col,
            group_key_col=group_key_col,
            flag_as_violation=config.flag_unseen_baseline_as_violation,
        ),
    )

    internal_to_remove = [config.score_std_col, config.severity_col, unseen_col, group_key_col]
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
