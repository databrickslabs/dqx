"""
Check functions for anomaly detection.
"""

from __future__ import annotations

import warnings
from uuid import uuid4
from datetime import datetime
from typing import Any

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import pandas_udf, PandasUDFType, struct, col
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, IntegerType, StructType, StructField

from databricks.labs.dqx.anomaly.model_registry import AnomalyModelRegistry
from databricks.labs.dqx.anomaly.trainer import _derive_registry_table, _ensure_full_model_name
from databricks.labs.dqx.anomaly.drift_detector import compute_drift_score
from databricks.labs.dqx.anomaly.explainer import compute_feature_contributions
from databricks.labs.dqx.errors import InvalidParameterError, MissingParameterError
from databricks.labs.dqx.rule import register_rule
from databricks.labs.dqx.check_funcs import make_condition
from databricks.labs.dqx.utils import get_column_name_or_alias


def _score_segmented(
    df: DataFrame,
    segment_by: list[str],
    columns: list[str],
    base_model_name: str,
    registry_table: str,
    score_threshold: float,
    row_filter: str | None,
    drift_threshold: float | None,
    drift_threshold_value: float,
    include_contributions: bool,
    include_confidence: bool,
    merge_columns: list[str],
    condition_col: str,
    registry_client: AnomalyModelRegistry,
) -> DataFrame:
    """Score DataFrame using segment-specific models.
    
    Args:
        merge_columns: Required primary key columns for joining results back.
    """
    # Get all segment models
    all_segments = registry_client.get_all_segment_models(registry_table, base_model_name)
    
    if not all_segments:
        raise InvalidParameterError(
            f"No segment models found for base model '{base_model_name}'. "
            "Train segmented models first using anomaly.train(...)."
        )
    
    # Filter rows if row_filter is provided
    if row_filter:
        df_to_score = df.filter(F.expr(row_filter))
    else:
        df_to_score = df
    
    scored_dfs = []
    
    # Score each segment separately
    for segment_model in all_segments:
        # Build filter expression for this segment
        segment_filter_exprs = []
        for k, v in segment_model.segment_values.items():
            segment_filter_exprs.append(F.col(k) == F.lit(v))
        
        if not segment_filter_exprs:
            continue
        
        # Combine segment filters
        segment_filter = segment_filter_exprs[0]
        for expr in segment_filter_exprs[1:]:
            segment_filter = segment_filter & expr
        
        # Filter to this segment
        segment_df = df_to_score.filter(segment_filter)
        
        # Skip empty segments
        if segment_df.count() == 0:
            continue
        
        # Check for drift in this segment
        if drift_threshold is not None and segment_model.baseline_stats:
            drift_result = compute_drift_score(
                segment_df.select(columns),
                columns,
                segment_model.baseline_stats,
                drift_threshold_value,
            )
            
            if drift_result.drift_detected:
                drifted_cols_str = ", ".join(drift_result.drifted_columns)
                segment_name = "_".join(f"{k}={v}" for k, v in segment_model.segment_values.items())
                warnings.warn(
                    f"Data drift detected in segment '{segment_name}', columns: {drifted_cols_str} "
                    f"(drift score: {drift_result.drift_score:.2f}). "
                    f"Consider retraining the segmented model.",
                    UserWarning,
                    stacklevel=4,
                )
        
        # Score this segment with optional SHAP (computed in single UDF pass)
        segment_scored = _score_with_sklearn_model(
            segment_model.model_uri, 
            segment_df, 
            columns, 
            segment_model.feature_metadata,
            merge_columns,
            include_contributions=include_contributions  # SHAP computed in same UDF pass
        )
        segment_scored = segment_scored.withColumn("anomaly_score_std", F.lit(0.0))
        
        scored_dfs.append(segment_scored)
    
    # Union all scored segments
    if not scored_dfs:
        # No segments had data - return original DataFrame with null scores
        result = df_to_score.withColumn("anomaly_score", F.lit(None).cast(DoubleType()))
        result = result.withColumn("anomaly_score_std", F.lit(None).cast(DoubleType()))
        if include_contributions:
            from pyspark.sql.types import MapType, StringType
            result = result.withColumn("anomaly_contributions", F.lit(None).cast(MapType(StringType(), DoubleType())))
    else:
        result = scored_dfs[0]
        for sdf in scored_dfs[1:]:
            result = result.union(sdf)
    
    # Drop confidence column if not requested
    if not include_confidence:
        result = result.drop("anomaly_score_std")
    
    # If row_filter was used, join scored results back to original DataFrame
    if row_filter:
        # Get score columns to join
        score_cols_to_join = ["anomaly_score"]
        if include_confidence:
            score_cols_to_join.append("anomaly_score_std")
        if include_contributions:
            score_cols_to_join.append("anomaly_contributions")
        
        # Select only merge columns + score columns from scored DataFrame
        scored_subset = result.select(*merge_columns, *score_cols_to_join)
        
        # Take distinct rows to avoid duplicates
        agg_exprs = []
        for col in score_cols_to_join:
            if col == "anomaly_contributions":
                agg_exprs.append(F.first(col).alias(col))
            else:
                agg_exprs.append(F.max(col).alias(col))
        scored_subset_unique = scored_subset.groupBy(*merge_columns).agg(*agg_exprs)
        
        # Left join back to original DataFrame using merge columns
        result = df.join(scored_subset_unique, on=merge_columns, how="left")
    
    # Add condition column
    condition = F.when(F.col("anomaly_score").isNull(), F.lit(False)).otherwise(
        F.col("anomaly_score") > F.lit(score_threshold)
    )
    
    return result.withColumn(condition_col, condition)


def _score_with_sklearn_model(
    model_uri: str, 
    df: DataFrame, 
    feature_cols: list[str], 
    feature_metadata_json: str,
    merge_columns: list[str],
    include_contributions: bool = False
) -> DataFrame:
    """
    Score DataFrame using scikit-learn model with distributed pandas UDF.
    
    Feature engineering is applied in Spark before the pandas UDF.
    The pandas UDF handles sklearn components (RobustScaler + IsolationForest) and optionally SHAP.
    
    Args:
        model_uri: MLflow model URI
        df: DataFrame to score (with original columns)
        feature_cols: List of original feature column names (before engineering)
        feature_metadata_json: JSON string with feature engineering metadata (from registry)
        merge_columns: Columns to use for joining results back (e.g., primary keys or row IDs).
            Must be provided by caller to ensure correct row alignment.
        include_contributions: If True, compute SHAP feature contributions in the same UDF pass.
    
    Returns:
        DataFrame with anomaly_score, prediction, and optionally anomaly_contributions columns added
    """
    import cloudpickle
    import pandas as pd
    import numpy as np
    from pyspark.sql import types as T
    from databricks.labs.dqx.anomaly.transformers import ColumnTypeInfo, SparkFeatureMetadata, apply_feature_engineering
    
    # Lazy import to avoid circular import issues with MLflow
    import mlflow.sklearn
    
    # Load model
    sklearn_model = mlflow.sklearn.load_model(model_uri)
    
    # Load feature metadata from the provided JSON (from model registry)
    feature_metadata = SparkFeatureMetadata.from_json(feature_metadata_json)
    
    # Reconstruct column_infos from metadata
    column_infos = [
        ColumnTypeInfo(
            name=info["name"],
            spark_type=T.StringType(),  # Placeholder, not needed for scoring
            category=info["category"],
            cardinality=info.get("cardinality"),
            null_count=info.get("null_count"),
        )
        for info in feature_metadata.column_infos
    ]
    
    # Use provided merge columns for joining
    join_cols = merge_columns
    cols_to_preserve = merge_columns
    
    # Deduplicate columns to avoid duplicate column errors if merge_columns overlap with feature_cols
    # Preserve order: feature_cols first, then any merge_columns not in feature_cols
    cols_to_select = list(dict.fromkeys([*feature_cols, *cols_to_preserve]))
    
    # Apply feature engineering in Spark (distributed), preserving join columns
    # Use pre-computed frequency maps and OneHot categories from training
    engineered_df, _ = apply_feature_engineering(
        df.select(*cols_to_select),
        column_infos,
        categorical_cardinality_threshold=20,  # Use same threshold as training
        frequency_maps=feature_metadata.categorical_frequency_maps,
        onehot_categories=feature_metadata.onehot_categories,
    )
    
    # Get engineered feature names
    engineered_feature_cols = feature_metadata.engineered_feature_names
    
    # Serialize model (will be captured in UDF closure)
    # Model contains only standard sklearn components (no custom transformers)
    model_bytes = cloudpickle.dumps(sklearn_model)
    
    # Define schema for UDF output (nullable=True to match pandas behavior)
    from pyspark.sql.types import MapType, StringType
    schema_fields = [
        StructField("anomaly_score", DoubleType(), True),
        StructField("prediction", IntegerType(), True),
    ]
    if include_contributions:
        schema_fields.append(StructField("anomaly_contributions", MapType(StringType(), DoubleType()), True))
    schema = StructType(schema_fields)
    
    @pandas_udf(schema, PandasUDFType.SCALAR)
    def predict_with_shap_udf(*cols):
        """Pandas UDF for distributed scoring with optional SHAP (Spark Connect compatible)."""
        import pandas as pd
        import numpy as np
        import cloudpickle
        
        # Deserialize model (only standard sklearn components)
        model_local = cloudpickle.loads(model_bytes)
        
        # Extract components for SHAP (if needed)
        if include_contributions:
            try:
                import shap
                from sklearn.pipeline import Pipeline
            except ImportError as e:
                raise ImportError(
                    "To use feature contributions (include_contributions=True), install 'shap>=0.42.0,<0.46' on your cluster.\n"
                    "Cluster -> Libraries -> Install New -> PyPI -> 'shap>=0.42.0,<0.46'"
                ) from e
            
            # Extract scaler and tree model from pipeline
            if isinstance(model_local, Pipeline):
                scaler = model_local.named_steps.get('scaler')
                tree_model = model_local.named_steps['model']
                needs_scaling = scaler is not None
            else:
                scaler = None
                tree_model = model_local
                needs_scaling = False
            
            # Create TreeExplainer once per batch
            explainer = shap.TreeExplainer(tree_model)
        
        # Convert input columns to DataFrame
        X = pd.concat(cols, axis=1)
        X.columns = engineered_feature_cols
        
        # 1. SCORE (using full pipeline)
        predictions = model_local.predict(X)
        scores = -model_local.score_samples(X)  # Negate to make higher = more anomalous
        predictions = np.where(predictions == -1, 1, 0)
        
        # 2. SHAP (if requested)
        contributions_list = []
        if include_contributions:
            # Prepare data for SHAP (scale if needed)
            X_for_shap = scaler.transform(X) if needs_scaling else X.values
            
            # Compute SHAP for each row
            for i in range(len(X_for_shap)):
                # Check for NaN values
                if pd.isna(X_for_shap[i]).any():
                    contributions_list.append({col: None for col in engineered_feature_cols})
                else:
                    # Compute SHAP values (one per engineered feature)
                    shap_values = explainer.shap_values(X_for_shap[i:i+1])[0]
                    
                    # Convert to absolute contributions normalized to sum to 1.0
                    abs_shap = np.abs(shap_values)
                    total = abs_shap.sum()
                    
                    if total > 0:
                        normalized = abs_shap / total
                        # Map SHAP values to engineered feature names (not original features)
                        contributions = {col: float(normalized[j]) for j, col in enumerate(engineered_feature_cols)}
                    else:
                        # Equal contributions if all SHAP values are 0
                        contributions = {col: 1.0 / len(engineered_feature_cols) for col in engineered_feature_cols}
                    
                    contributions_list.append(contributions)
        
        # Build result DataFrame
        result = {
            "anomaly_score": scores,
            "prediction": predictions,
        }
        if include_contributions:
            result["anomaly_contributions"] = contributions_list
        
        return pd.DataFrame(result)
    
    # Apply UDF to all engineered feature columns
    # The join columns are preserved in engineered_df
    scored_df = engineered_df.withColumn("_scores", predict_with_shap_udf(*[col(c) for c in engineered_feature_cols]))
    
    # Select columns to join back
    # Note: Include join_cols + new anomaly columns for the join
    # Spark's join(on=...) automatically deduplicates the join key columns
    cols_to_select = [*join_cols, "_scores.anomaly_score", "_scores.prediction"]
    if include_contributions:
        cols_to_select.append("_scores.anomaly_contributions")
    
    # Join scores back to original DataFrame using ONLY the merge columns
    # Result will have all columns from df + anomaly columns, no duplicates
    result = df.join(
        scored_df.select(*cols_to_select),
        on=join_cols,
        how="left"
    )
    
    return result


@register_rule("dataset")
def has_no_anomalies(
    columns: list[str | Column] | None = None,
    segment_by: list[str] | None = None,
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
    Check that records are not anomalous according to a trained model(s).

    Auto-discovery:
    - columns=None: Inferred from model registry
    - segment_by=None: Inferred from model registry (checks if model is segmented)

    Args:
        columns: Columns to check for anomalies (auto-inferred if omitted).
        segment_by: Segment columns (auto-inferred from model if omitted).
        model: Model name (REQUIRED). Provide the base model name returned from train().
        registry_table: Registry table (auto-derived if omitted).
        score_threshold: Anomaly score threshold (default 0.5).
        row_filter: Optional SQL expression to filter rows before scoring.
        drift_threshold: Drift detection threshold (default 3.0, None to disable).
        include_contributions: Include anomaly_contributions column (default False).
        include_confidence: Include anomaly_score_std column for ensembles (default False).
        merge_columns: Primary key columns (e.g., ["activity_id"]) for joining scored results 
            back to original DataFrame (REQUIRED). Must uniquely identify rows.

    Returns:
        Tuple of condition expression and apply function.
    """

    condition_col = f"__anomaly_condition_{uuid4().hex}"
    drift_threshold_value = drift_threshold if drift_threshold is not None else 3.0

    def apply(df: DataFrame) -> DataFrame:
        # Validate merge_columns is provided (required for joining results back)
        if not merge_columns:
            raise MissingParameterError("merge_columns is not provided.")
        
        registry_client = AnomalyModelRegistry(df.sparkSession)
        
        # Auto-infer columns and segment_by from registry if not provided
        nonlocal columns, segment_by
        if columns is None or segment_by is None:
            # Derive model name from DataFrame (need columns for this, but we might not have them yet)
            # For auto-discovery, we'll derive model name from table, then lookup columns
            model_name_local = model
            registry_local = registry_table or _derive_registry_table(df)
            
            # If model name not provided, we cannot auto-discover without columns
            # So we'll require at least model name OR columns for now
            if model_name_local is None and columns is None:
                raise InvalidParameterError(
                    "Either 'model' or 'columns' must be provided for auto-discovery. "
                    "Provide at least one to infer the other from the model registry."
                )
            
            # If model name provided, normalize it to full catalog.schema.model format
            if model_name_local:
                model_name_local = _ensure_full_model_name(model_name_local, registry_local)
                record_for_discovery = registry_client.get_active_model(registry_local, model_name_local)
                
                # If exact match not found, check for segmented models with this base name
                if not record_for_discovery:
                    # Check if there are segmented models matching this base name
                    all_segments = registry_client.get_all_segment_models(registry_local, model_name_local)
                    if all_segments:
                        # Use the first segment to get configuration
                        record_for_discovery = all_segments[0]
                    else:
                        raise InvalidParameterError(
                            f"Model '{model_name_local}' not found in '{registry_local}'. "
                            "Train first using anomaly.train(...)."
                        )
                
                if columns is None:
                    columns = record_for_discovery.columns
                if segment_by is None:
                    segment_by = record_for_discovery.segment_by
        
        # Now normalize columns (we know columns is not None at this point)
        normalized_columns = [get_column_name_or_alias(c) for c in columns]
        
        registry = registry_table or _derive_registry_table(df)
        
        # Validate model is provided
        if not model:
            raise InvalidParameterError(
                "model parameter is required. Provide the model name returned from train(). "
                "Example: has_no_anomalies(model='my_model', ...)"
            )
        
        # Normalize model_name to full catalog.schema.model format
        model_name = _ensure_full_model_name(model, registry)

        # Check if segmented model (either explicit segment_by or detected from registry)
        if segment_by:
            # Segment-aware scoring
            return _score_segmented(
                df, segment_by, normalized_columns, model_name, registry,
                score_threshold, row_filter, drift_threshold, drift_threshold_value,
                include_contributions, include_confidence, merge_columns,
                condition_col, registry_client
            )
        
        # Try global model scoring first
        record = registry_client.get_active_model(registry, model_name)
        
        # If not found, check if it's a segmented model (base name provided)
        if not record:
            all_segments = registry_client.get_all_segment_models(registry, model_name)
            if all_segments:
                # Auto-detect segmentation and use segmented scoring
                first_segment = all_segments[0]
                return _score_segmented(
                    df, first_segment.segment_by, normalized_columns, model_name, registry,
                    score_threshold, row_filter, drift_threshold, drift_threshold_value,
                    include_contributions, include_confidence, merge_columns,
                    condition_col, registry_client
                )
            else:
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
                    stacklevel=2
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
                    f"DATA DRIFT DETECTED in columns: {drifted_cols_str} "
                    f"(drift score: {drift_result.drift_score:.2f}). "
                    f"Model may be stale. Consider retraining: {retrain_cmd}",
                    UserWarning,
                    stacklevel=2
                )

        # Use merge_columns for joining (required parameter)
        join_cols_for_scoring = merge_columns
        
        # Filter rows if row_filter is provided
        if row_filter:
            df_filtered = df.filter(F.expr(row_filter))
        else:
            df_filtered = df

        # Check if ensemble model (multiple URIs separated by comma)
        model_uris = record.model_uri.split(",")
        
        if len(model_uris) > 1:
            # Ensemble: score with all models (distributed) and average scores
            # For ensembles, we can optionally compute SHAP on the first model only
            scored_dfs = []
            for i, uri in enumerate(model_uris):
                # Only compute SHAP for first model in ensemble (if requested)
                compute_shap_for_this = include_contributions and i == 0
                temp_scored = _score_with_sklearn_model(
                    uri, df_filtered, normalized_columns, record.feature_metadata, 
                    join_cols_for_scoring, 
                    include_contributions=compute_shap_for_this
                )
                temp_scored = temp_scored.withColumn(f"_score_{i}", F.col("anomaly_score"))
                
                # Keep SHAP from first model, drop from others
                if compute_shap_for_this:
                    scored_dfs.append(temp_scored.select("*", f"_score_{i}").drop("anomaly_score", "prediction"))
                else:
                    scored_dfs.append(temp_scored.select("*", f"_score_{i}").drop("anomaly_score", "prediction", "anomaly_contributions"))
            
            # Merge all scored DataFrames using join columns
            scored_df = scored_dfs[0]
            for i in range(1, len(scored_dfs)):
                scored_df = scored_df.join(
                    scored_dfs[i].select(*join_cols_for_scoring, f"_score_{i}"),
                    on=join_cols_for_scoring,
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
            # Single model (distributed scoring via pandas UDF with optional SHAP)
            scored_df = _score_with_sklearn_model(
                record.model_uri, df_filtered, normalized_columns, record.feature_metadata, 
                join_cols_for_scoring,
                include_contributions=include_contributions  # SHAP computed in same UDF pass
            )
            scored_df = scored_df.withColumn("anomaly_score_std", F.lit(0.0))

        # Drop confidence column if not requested
        if not include_confidence:
            scored_df = scored_df.drop("anomaly_score_std")
        
        # If row_filter was used, join scored results back to original DataFrame
        # This preserves all rows (non-filtered rows will have null anomaly_score)
        if row_filter:
            # Get score columns to join
            score_cols_to_join = ["anomaly_score"]
            if include_confidence:
                score_cols_to_join.append("anomaly_score_std")
            if include_contributions:
                score_cols_to_join.append("anomaly_contributions")
            
            # Select only join columns + score columns from scored DataFrame
            scored_subset = scored_df.select(*join_cols_for_scoring, *score_cols_to_join)
            
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
            scored_subset_unique = scored_subset.groupBy(*join_cols_for_scoring).agg(*agg_exprs)
            
            # Left join back to original DataFrame (preserves all rows)
            scored_df = df.join(scored_subset_unique, on=join_cols_for_scoring, how="left")
        
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

