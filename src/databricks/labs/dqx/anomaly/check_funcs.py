"""
Check functions for anomaly detection.
"""

from __future__ import annotations

import warnings
from uuid import uuid4
from datetime import datetime
from typing import Any

import mlflow.spark
from pyspark.sql import Column, DataFrame
import pyspark.sql.functions as F

from databricks.labs.dqx.anomaly.model_registry import AnomalyModelRegistry
from databricks.labs.dqx.anomaly.trainer import _derive_model_name, _derive_registry_table
from databricks.labs.dqx.anomaly.drift_detector import compute_drift_score
from databricks.labs.dqx.anomaly.explainer import compute_feature_contributions
from databricks.labs.dqx.errors import InvalidParameterError
from databricks.labs.dqx.rule import register_rule
from databricks.labs.dqx.check_funcs import make_condition
from databricks.labs.dqx.utils import get_column_name_or_alias


@register_rule("dataset")
def has_no_anomalies(
    columns: list[str | Column],
    model: str | None = None,
    registry_table: str | None = None,
    score_threshold: float = 0.5,
    row_filter: str | None = None,
    drift_threshold: float | None = None,
    include_contributions: bool = False,
    include_confidence: bool = False,
) -> tuple[Column, Any]:
    """
    Check that records are not anomalous according to a trained model.

    Args:
        columns: Columns to check for anomalies.
        model: Model name (auto-derived if omitted).
        registry_table: Registry table (auto-derived if omitted).
        score_threshold: Anomaly score threshold (default 0.5).
        row_filter: Optional SQL expression to filter rows.
        drift_threshold: Drift detection threshold (default 3.0, None to disable).
        include_contributions: Include anomaly_contributions column (default False).
        include_confidence: Include anomaly_score_std column for ensembles (default False).

    Returns:
        Tuple of condition expression and apply function.
    """

    normalized_columns = [get_column_name_or_alias(c) for c in columns]
    condition_col = f"__anomaly_condition_{uuid4().hex}"
    drift_threshold_value = drift_threshold if drift_threshold is not None else 3.0

    def apply(df: DataFrame) -> DataFrame:
        model_name = model or _derive_model_name(df, normalized_columns)
        registry = registry_table or _derive_registry_table(df)

        registry_client = AnomalyModelRegistry(df.sparkSession)
        record = registry_client.get_active_model(registry, model_name)
        if not record:
            raise InvalidParameterError(
                f"Model '{model_name}' not found in '{registry}'. Train first using anomaly.train(...)."
            )

        if set(normalized_columns) != set(record.columns):
            raise InvalidParameterError(
                f"Columns {normalized_columns} don't match trained model columns {record.columns}"
            )

        # Check model staleness
        if record.training_time:
            age_days = (datetime.utcnow() - record.training_time).days
            if age_days > 30:
                warnings.warn(
                    f"Model '{model_name}' is {age_days} days old. Consider retraining.",
                    UserWarning,
                    stacklevel=3,
                )

        # Check for data drift
        if drift_threshold is not False and record.baseline_stats:
            drift_result = compute_drift_score(
                df.select(normalized_columns),
                normalized_columns,
                record.baseline_stats,
                drift_threshold_value,
            )

            if drift_result.drift_detected:
                drifted_cols_str = ", ".join(drift_result.drifted_columns)
                warnings.warn(
                    f"Data drift detected in columns: {drifted_cols_str} "
                    f"(drift score: {drift_result.drift_score:.2f}). "
                    f"Model may be stale. Retrain using: "
                    f"anomaly.train(df=spark.table('{record.input_table}'), "
                    f"columns={normalized_columns}, model_name='{model_name}')",
                    UserWarning,
                    stacklevel=3,
                )

        if row_filter:
            df_filtered = df.filter(F.expr(row_filter))
        else:
            df_filtered = df

        # Check if ensemble model (multiple URIs separated by comma)
        model_uris = record.model_uri.split(",")
        
        if len(model_uris) > 1:
            # Ensemble: load all models and average scores
            scored_dfs = []
            for i, uri in enumerate(model_uris):
                model = mlflow.spark.load_model(uri.strip())
                temp_scored = model.transform(df_filtered)
                temp_scored = temp_scored.withColumn(f"_score_{i}", F.col("anomaly_score"))
                scored_dfs.append(temp_scored.select("*", f"_score_{i}").drop("anomaly_score", "prediction"))
            
            # Merge all scored DataFrames
            scored_df = scored_dfs[0]
            for i in range(1, len(scored_dfs)):
                # Join on all original columns
                join_cols = [c for c in df_filtered.columns]
                scored_df = scored_df.join(
                    scored_dfs[i].select(join_cols + [f"_score_{i}"]),
                    on=join_cols,
                    how="inner"
                )
            
            # Compute mean and std
            score_cols = [f"_score_{i}" for i in range(len(model_uris))]
            scored_df = scored_df.withColumn(
                "anomaly_score",
                sum([F.col(col) for col in score_cols]) / F.lit(len(model_uris))
            )
            
            # Standard deviation (confidence)
            mean_col = F.col("anomaly_score")
            variance = sum([(F.col(col) - mean_col) ** 2 for col in score_cols]) / F.lit(len(model_uris) - 1)
            scored_df = scored_df.withColumn("anomaly_score_std", F.sqrt(variance))
            
            # Drop intermediate columns
            for col in score_cols:
                scored_df = scored_df.drop(col)
        else:
            # Single model
            loaded_model = mlflow.spark.load_model(record.model_uri)
            scored_df = loaded_model.transform(df_filtered)
            scored_df = scored_df.withColumn("anomaly_score_std", F.lit(0.0))

        # Add feature contributions if requested
        if include_contributions:
            if len(model_uris) > 1:
                # Use first model for contributions
                first_model = mlflow.spark.load_model(model_uris[0].strip())
                scored_df = compute_feature_contributions(first_model, scored_df, normalized_columns)
            else:
                scored_df = compute_feature_contributions(loaded_model, scored_df, normalized_columns)

        # Drop confidence column if not requested
        if not include_confidence:
            scored_df = scored_df.drop("anomaly_score_std")

        # Note: Anomaly rate can be computed from the output DataFrame:
        # anomaly_rate = scored_df.filter(F.col("anomaly_score") > threshold).count() / scored_df.count()
        # For tracking over time, use DQX metrics observer with dataset-level aggregations

        condition = F.when(F.col("anomaly_score").isNull(), F.lit(False)).otherwise(
            F.col("anomaly_score") > F.lit(score_threshold)
        )

        return scored_df.withColumn(condition_col, condition)

    message = F.lit(f"Anomaly score exceeded threshold {score_threshold}")
    condition_expr = F.col(condition_col)
    return make_condition(condition_expr, message, "has_anomalies"), apply

