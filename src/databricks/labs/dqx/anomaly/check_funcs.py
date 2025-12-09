"""
Check functions for anomaly detection.
"""

from __future__ import annotations

import warnings
from uuid import uuid4
from datetime import datetime
from typing import Any

import mlflow.sklearn
from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import pandas_udf, PandasUDFType, struct, col
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, IntegerType, StructType, StructField

from databricks.labs.dqx.anomaly.model_registry import AnomalyModelRegistry
from databricks.labs.dqx.anomaly.trainer import _derive_model_name, _derive_registry_table
from databricks.labs.dqx.anomaly.drift_detector import compute_drift_score
from databricks.labs.dqx.anomaly.explainer import compute_feature_contributions
from databricks.labs.dqx.errors import InvalidParameterError
from databricks.labs.dqx.rule import register_rule
from databricks.labs.dqx.check_funcs import make_condition
from databricks.labs.dqx.utils import get_column_name_or_alias


def _score_with_sklearn_model(model_uri: str, df: DataFrame, feature_cols: list[str]) -> DataFrame:
    """
    Score DataFrame using scikit-learn model with distributed pandas UDF.
    
    Args:
        model_uri: MLflow model URI
        df: DataFrame to score
        feature_cols: List of feature column names
    
    Returns:
        DataFrame with anomaly_score and prediction columns added
    """
    import cloudpickle
    import pandas as pd
    import numpy as np
    
    # Load and serialize model (will be captured in UDF closure - Spark Connect compatible)
    sklearn_model = mlflow.sklearn.load_model(model_uri)
    model_bytes = cloudpickle.dumps(sklearn_model)
    
    # Define schema for UDF output (nullable=True to match pandas behavior)
    schema = StructType([
        StructField("anomaly_score", DoubleType(), True),
        StructField("prediction", IntegerType(), True),
    ])
    
    @pandas_udf(schema, PandasUDFType.SCALAR)
    def predict_udf(*cols):
        """Pandas UDF for distributed scoring (Spark Connect compatible)."""
        import cloudpickle
        import pandas as pd
        import numpy as np
        
        # Deserialize model from closure (works with Spark Connect)
        model_local = cloudpickle.loads(model_bytes)
        
        # Convert input columns to numpy array
        X = pd.concat(cols, axis=1).values
        
        # Score samples (negative scores, higher = more anomalous)
        scores = -model_local.score_samples(X)  # Negate to make higher = more anomalous
        
        # Predict labels (1 = anomaly, 0 = normal)
        predictions = model_local.predict(X)
        predictions = np.where(predictions == -1, 1, 0)  # Convert sklearn -1/1 to 0/1
        
        return pd.DataFrame({
            "anomaly_score": scores,
            "prediction": predictions
        })
    
    # Apply UDF to all feature columns
    result = df.withColumn("_scores", predict_udf(*[col(c) for c in feature_cols]))
    result = result.select("*", "_scores.anomaly_score", "_scores.prediction").drop("_scores")
    
    return result


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
    merge_columns: list[str] | None = None,
) -> tuple[Column, Any]:
    """
    Check that records are not anomalous according to a trained model.

    Args:
        columns: Columns to check for anomalies.
        model: Model name (auto-derived if omitted).
        registry_table: Registry table (auto-derived if omitted).
        score_threshold: Anomaly score threshold (default 0.5).
        row_filter: Optional SQL expression to filter rows before scoring.
        drift_threshold: Drift detection threshold (default 3.0, None to disable).
        include_contributions: Include anomaly_contributions column (default False).
        include_confidence: Include anomaly_score_std column for ensembles (default False).
        merge_columns: Columns to use for joining scored results back to original DataFrame.
            If None, uses all original columns (can be slow for wide DataFrames).
            Recommend providing primary key columns for better performance.

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
        if drift_threshold is not None and record.baseline_stats:
            drift_result = compute_drift_score(
                df.select(normalized_columns),
                normalized_columns,
                record.baseline_stats,
                drift_threshold_value,
            )

            if drift_result.drift_detected:
                drifted_cols_str = ", ".join(drift_result.drifted_columns)
                retrain_cmd = (
                    f"anomaly.train(df=your_dataframe, "
                    f"columns={normalized_columns}, model_name='{model_name}')"
                )
                warnings.warn(
                    f"Data drift detected in columns: {drifted_cols_str} "
                    f"(drift score: {drift_result.drift_score:.2f}). "
                    f"Model may be stale. Consider retraining: {retrain_cmd}",
                    UserWarning,
                    stacklevel=3,
                )

        # Filter rows if row_filter is provided
        # Determine join columns for merging results back
        # If merge_columns is None, use all original DataFrame columns (default behavior)
        # If row_filter is used, we need to join scored results back to preserve all rows
        if row_filter:
            df_filtered = df.filter(F.expr(row_filter))
        else:
            df_filtered = df

        # Check if ensemble model (multiple URIs separated by comma)
        model_uris = record.model_uri.split(",")
        
        if len(model_uris) > 1:
            # Ensemble: score with all models (distributed) and average scores
            scored_dfs = []
            for i, uri in enumerate(model_uris):
                temp_scored = _score_with_sklearn_model(uri.strip(), df_filtered, normalized_columns)
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
            
            # Compute mean and std (distributed on Spark)
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
            # Single model (distributed scoring via pandas UDF)
            scored_df = _score_with_sklearn_model(record.model_uri, df_filtered, normalized_columns)
            scored_df = scored_df.withColumn("anomaly_score_std", F.lit(0.0))

        # Add feature contributions if requested (distributed computation)
        if include_contributions:
            sklearn_model_uri = model_uris[0].strip() if len(model_uris) > 1 else record.model_uri
            sklearn_model = mlflow.sklearn.load_model(sklearn_model_uri)
            scored_df = compute_feature_contributions(sklearn_model, scored_df, normalized_columns)

        # Drop confidence column if not requested
        if not include_confidence:
            scored_df = scored_df.drop("anomaly_score_std")

        # If row_filter was used, join scored results back to original DataFrame
        # This preserves all rows (non-filtered rows will have null anomaly_score)
        if row_filter:
            # Determine which columns to use for joining
            # Default to merge_columns if provided, otherwise use all original columns
            join_cols = merge_columns if merge_columns else df.columns
            
            # Get score columns to join
            score_cols_to_join = ["anomaly_score"]
            if include_confidence:
                score_cols_to_join.append("anomaly_score_std")
            if include_contributions:
                score_cols_to_join.append("anomaly_contributions")
            
            # Select only join columns + score columns from scored DataFrame
            scored_subset = scored_df.select(*join_cols, *score_cols_to_join)
            
            # Take distinct rows to avoid duplicates (similar to sql_query pattern)
            # In case of duplicates, take max anomaly_score (most conservative)
            agg_exprs = []
            for col in score_cols_to_join:
                if col == "anomaly_contributions":
                    # For map columns, use first() instead of max()
                    agg_exprs.append(F.first(col).alias(col))
                else:
                    # For numeric columns, use max() (most conservative for anomaly scores)
                    agg_exprs.append(F.max(col).alias(col))
            scored_subset_unique = scored_subset.groupBy(*join_cols).agg(*agg_exprs)
            
            # Left join back to original DataFrame (preserves all rows)
            scored_df = df.join(scored_subset_unique, on=join_cols, how="left")
        
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

