"""Global (non-segmented) anomaly model scoring. Separate module to avoid circular import with scoring_strategies."""

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from databricks.labs.dqx.anomaly.model_discovery import extract_quantile_points
from databricks.labs.dqx.anomaly.drift import check_and_warn_drift
from databricks.labs.dqx.anomaly.ensemble_scorer import (
    score_ensemble_models,
    score_ensemble_models_local,
)
from databricks.labs.dqx.anomaly.model_config import compute_config_hash
from databricks.labs.dqx.anomaly.model_loader import check_model_staleness
from databricks.labs.dqx.anomaly.model_registry import AnomalyModelRecord
from databricks.labs.dqx.anomaly.scoring_utils import (
    add_info_column,
    add_severity_percentile_column,
    apply_row_filter,
    join_filtered_results_back,
)
from databricks.labs.dqx.anomaly.scoring_config import ScoringConfig
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
    check_and_warn_drift(
        df_filtered,
        config.columns,
        record,
        config.model_name,
        config.drift_threshold,
        config.drift_threshold_value,
    )

    model_uris = record.identity.model_uri.split(",")
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
        columns_to_keep = [col for col in scored_df.columns if col not in internal_to_remove]
    else:
        internal_to_remove.append(config.score_col)
        columns_to_keep = [col for col in scored_df.columns if col not in internal_to_remove]
    scored_df = scored_df.select(*columns_to_keep)

    if config.row_filter:
        scored_df = join_filtered_results_back(df, scored_df, config.merge_columns, config.score_col, config.info_col)
        scored_df = scored_df.drop(config.score_col)

    return scored_df
