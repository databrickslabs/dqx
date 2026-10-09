"""Anomaly model scoring.

One model per registered name, conditioned on a baseline grouping when it has one. Kept in one
module to avoid over-fragmentation of the scoring layer.
"""

import logging
import uuid

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from databricks.labs.dqx.anomaly.model_discovery import extract_quantile_points
from databricks.labs.dqx.anomaly.drift import check_and_warn_drift, format_drift_summary
from databricks.labs.dqx.anomaly.explainability import AttributionGate
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
    redaction_set,
)
from databricks.labs.dqx.anomaly.scoring_utils import (
    add_baseline_severity_percentile_column,
    add_info_column,
    add_severity_percentile_column,
    apply_row_filter,
    join_filtered_results_back,
    mark_stale_baselines,
    mark_unseen_baselines,
    null_out_unseen_baseline_scores,
    StaleBaselineContext,
    UnseenGroupContext,
)
from databricks.labs.dqx.anomaly.scoring_config import SEVERITY_QUANTILE_KEYS, ScoringConfig
from databricks.labs.dqx.anomaly.transformers import SparkFeatureMetadata
from databricks.labs.dqx.anomaly.single_model_scorer import (
    score_with_sklearn_model,
    score_with_sklearn_model_local,
)
from databricks.labs.dqx.errors import InvalidParameterError
from databricks.labs.dqx.utils import quote_column_name


logger = logging.getLogger(__name__)


def _known_group_keys(parsed_metadata: SparkFeatureMetadata) -> list[str]:
    """Group keys the model actually saw in training.

    Membership is independent of numeric features and quantile completeness. A categorical-only
    group, or a group whose calibration is incomplete, was still seen during training.
    """
    return parsed_metadata.baseline_group_keys


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
    trained_metadata = (
        SparkFeatureMetadata.from_json(record.features.feature_metadata) if record.features.feature_metadata else None
    )
    trained_baseline_by = trained_metadata.baseline_by if trained_metadata else None
    trained_baseline_over_time = trained_metadata.baseline_over_time if trained_metadata else None
    expected_hash = compute_config_hash(config.columns, trained_baseline_by, trained_baseline_over_time)

    if expected_hash != record.grouping.config_hash:
        raise InvalidParameterError(
            f"Configuration mismatch for model '{config.model_name}':\n"
            f"  Trained columns: {record.training.columns}\n"
            f"  Provided columns: {config.columns}\n"
            f"  Trained baseline_by: {trained_baseline_by or None}\n"
            f"  Trained baseline_over_time: {trained_baseline_over_time or None}\n\n"
            f"This model was trained with a different configuration, or by a DQX version before\n"
            f"baseline_by became part of the configuration hash. Either:\n"
            f"  1. Use the columns that match the trained model\n"
            f"  2. Retrain the model — required for any model registered before that change"
        )

    check_model_staleness(record, config.model_name)

    df_filtered = apply_row_filter(df, config.row_filter)
    # Marked here rather than on the scored frame: by then the projection has dropped the caller's time
    # column, which is correct (a time axis is not a feature) but leaves nothing to compare against. These
    # two flags then ride through feature engineering as passthrough columns, the way the row id does.
    #
    # Extrapolation is reported and never corrected. Unlike an unseen group, the score is still produced:
    # measured, it is still good one window past the boundary, so nulling it would discard a usable verdict.
    stale_col = f"__dqx_is_stale_baseline_{uuid.uuid4().hex}"
    horizon_col = f"__dqx_stale_horizon_{uuid.uuid4().hex}"
    df_filtered = mark_stale_baselines(
        df_filtered,
        trained_baseline_over_time or "",
        trained_metadata.temporal_window if trained_metadata else {},
        stale_col=stale_col,
        horizon_col=horizon_col,
    )
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
    # Admit any row that a real comparison curve would flag. Public severity and the final
    # contribution mask still use the row's own calibration, not this permissive gate.
    gate = AttributionGate.from_calibrations(group_quantile_points, global_quantile_points)
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
                quantile_points=global_quantile_points,
                threshold=config.threshold,
                output_columns=config.output_columns,
                gate=gate,
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
                quantile_points=global_quantile_points,
                threshold=config.threshold,
                output_columns=config.output_columns,
                gate=gate,
            ).withColumn(config.score_std_col, F.lit(0.0))
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
                quantile_points=global_quantile_points,
                threshold=config.threshold,
                output_columns=config.output_columns,
                gate=gate,
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
                quantile_points=global_quantile_points,
                threshold=config.threshold,
                output_columns=config.output_columns,
                gate=gate,
            ).withColumn(config.score_std_col, F.lit(0.0))
        )

    # Mark unseen groups before severity, then null the score in place afterwards: severity is
    # interpolated from the score, so nulling first would leave severity computed from nothing.
    unseen_col = f"__dqx_is_new_group_{uuid.uuid4().hex}"
    group_key_col = f"__dqx_row_group_key_{uuid.uuid4().hex}"
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
            drift_summary=format_drift_summary(
                drift_result, redaction_set(tuple(config.redact_columns), parsed_metadata)
            ),
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
        stale=StaleBaselineContext(stale_col=stale_col, horizon_col=horizon_col),
    )

    internal_to_remove = [
        config.score_std_col,
        config.severity_col,
        unseen_col,
        group_key_col,
        stale_col,
        horizon_col,
    ]
    if config.enable_contributions:
        internal_to_remove.extend([config.contributions_col, config.output_columns.basis_contributions])
    if config.enable_ai_explanation:
        internal_to_remove.append(config.ai_explanation_col)

    if config.row_filter:
        columns_to_keep = [col for col in scored_df.columns if col not in internal_to_remove]
    else:
        internal_to_remove.append(config.score_col)
        columns_to_keep = [col for col in scored_df.columns if col not in internal_to_remove]
    scored_df = scored_df.select(*[F.col(quote_column_name(c)) for c in columns_to_keep])

    if config.row_filter:
        scored_df = join_filtered_results_back(df, scored_df, config.merge_columns, config.score_col, config.info_col)
        scored_df = scored_df.drop(config.score_col)

    return scored_df
