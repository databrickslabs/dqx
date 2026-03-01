"""Drift detection and warnings for anomaly scoring."""

import warnings

from pyspark.sql import DataFrame

from databricks.labs.dqx.anomaly.drift_detector import compute_drift_score
from databricks.labs.dqx.anomaly.feature_prep import (
    apply_feature_engineering_for_scoring,
    prepare_feature_metadata,
)
from databricks.labs.dqx.anomaly.model_registry import AnomalyModelRecord
from databricks.labs.dqx.anomaly.segment_utils import build_segment_name


def check_segment_drift(
    segment_df: DataFrame,
    columns: list[str],
    segment_model: AnomalyModelRecord,
    drift_threshold: float | None,
    drift_threshold_value: float,
) -> None:
    """Check and warn about data drift in a segment."""
    if drift_threshold is not None and segment_model.training.baseline_stats:
        drift_df, drift_columns = prepare_drift_df(segment_df, columns, segment_model)
        drift_result = compute_drift_score(
            drift_df,
            drift_columns,
            segment_model.training.baseline_stats,
            drift_threshold_value,
        )

        if drift_result.drift_detected:
            drifted_cols_str = ", ".join(drift_result.drifted_columns)
            segment_name = build_segment_name(segment_model.segmentation.segment_values) or "unknown"
            warnings.warn(
                f"Data drift detected in segment '{segment_name}', columns: {drifted_cols_str} "
                f"(drift score: {drift_result.drift_score:.2f}). "
                f"Consider retraining the segmented anomaly model.",
                UserWarning,
                stacklevel=5,
            )


def check_and_warn_drift(
    df: DataFrame,
    columns: list[str],
    record: AnomalyModelRecord,
    model_name: str,
    drift_threshold: float | None,
    drift_threshold_value: float,
) -> None:
    """Check for data drift and issue warning if detected."""
    if drift_threshold is not None and record.training.baseline_stats:
        drift_df, drift_columns = prepare_drift_df(df, columns, record)
        drift_result = compute_drift_score(
            drift_df,
            drift_columns,
            record.training.baseline_stats,
            drift_threshold_value,
        )

        if drift_result.drift_detected:
            drifted_cols_str = ", ".join(drift_result.drifted_columns)
            retrain_cmd = f"anomaly_engine.train(df=your_dataframe, columns={columns}, model_name='{model_name}')"
            warnings.warn(
                f"DISTRIBUTION DRIFT DETECTED in columns: {drifted_cols_str} "
                f"(drift score: {drift_result.drift_score:.2f}, sample size: {drift_result.sample_size:,} rows). "
                f"Input data distribution differs significantly from training baseline. "
                f"This may indicate data quality issues or changing patterns. "
                f"Consider retraining: {retrain_cmd}",
                UserWarning,
                stacklevel=3,
            )


def prepare_drift_df(
    df: DataFrame,
    columns: list[str],
    record: AnomalyModelRecord,
) -> tuple[DataFrame, list[str]]:
    """Prepare drift DataFrame and columns aligned to training baseline stats."""
    feature_metadata_json = record.features.feature_metadata
    if not feature_metadata_json:
        return df.select(*columns), columns

    column_infos, feature_metadata = prepare_feature_metadata(feature_metadata_json)
    engineered_df = apply_feature_engineering_for_scoring(
        df,
        columns,
        merge_columns=[],
        column_infos=column_infos,
        feature_metadata=feature_metadata,
    )
    return engineered_df, feature_metadata.engineered_feature_names
