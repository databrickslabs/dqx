"""
Check functions for anomaly detection.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass
from uuid import uuid4
from datetime import datetime
from typing import Any

import cloudpickle
import mlflow.sklearn
import numpy as np
import pandas as pd
from pyspark.sql import Column, DataFrame
from pyspark.sql import types as T
from pyspark.sql.functions import pandas_udf, PandasUDFType, col
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, IntegerType, StructType, StructField, MapType, StringType

# Optional dependencies for SHAP explainability
try:
    import shap
    from sklearn.pipeline import Pipeline

    SHAP_AVAILABLE = True
except ImportError:
    shap = None  # type: ignore
    Pipeline = None  # type: ignore
    SHAP_AVAILABLE = False

from databricks.labs.dqx.anomaly.model_registry import AnomalyModelRegistry, AnomalyModelRecord
from databricks.labs.dqx.anomaly.trainer import _derive_registry_table, _ensure_full_model_name
from databricks.labs.dqx.anomaly.drift_detector import compute_drift_score
from databricks.labs.dqx.anomaly.transformers import ColumnTypeInfo, SparkFeatureMetadata, apply_feature_engineering
from databricks.labs.dqx.errors import InvalidParameterError, MissingParameterError
from databricks.labs.dqx.rule import register_rule
from databricks.labs.dqx.check_funcs import make_condition
from databricks.labs.dqx.utils import get_column_name_or_alias


@dataclass
class ScoringConfig:
    """Configuration for anomaly scoring."""

    columns: list[str]
    model_name: str
    registry_table: str
    score_threshold: float
    merge_columns: list[str]
    row_filter: str | None = None
    drift_threshold: float | None = None
    drift_threshold_value: float = 3.0
    include_contributions: bool = False
    include_confidence: bool = False


def _build_segment_filter(segment_values: dict[str, str] | None) -> Column | None:
    """Build Spark filter expression for a segment's values."""
    if not segment_values:
        return None

    filter_exprs = [F.col(key) == F.lit(value) for key, value in segment_values.items()]

    segment_filter = filter_exprs[0]
    for expr in filter_exprs[1:]:
        segment_filter = segment_filter & expr

    return segment_filter


def _check_segment_drift(
    segment_df: DataFrame,
    columns: list[str],
    segment_model: "AnomalyModelRecord",
    drift_threshold: float | None,
    drift_threshold_value: float,
) -> None:
    """Check and warn about data drift in a segment."""
    if drift_threshold is not None and segment_model.baseline_stats:
        drift_result = compute_drift_score(
            segment_df.select(columns),
            columns,
            segment_model.baseline_stats,
            drift_threshold_value,
        )

        if drift_result.drift_detected:
            drifted_cols_str = ", ".join(drift_result.drifted_columns)
            segment_name = (
                "_".join(f"{k}={v}" for k, v in segment_model.segment_values.items())
                if segment_model.segment_values
                else "unknown"
            )
            warnings.warn(
                f"Data drift detected in segment '{segment_name}', columns: {drifted_cols_str} "
                f"(drift score: {drift_result.drift_score:.2f}). "
                f"Consider retraining the segmented model.",
                UserWarning,
                stacklevel=5,
            )


def _join_filtered_results_back(
    df: DataFrame,
    result: DataFrame,
    merge_columns: list[str],
    include_confidence: bool,
    include_contributions: bool,
) -> DataFrame:
    """Join scored results back to original DataFrame (for row_filter case)."""
    # Get score columns to join
    score_cols_to_join = ["anomaly_score"]
    if include_confidence:
        score_cols_to_join.append("anomaly_score_std")
    if include_contributions:
        score_cols_to_join.append("anomaly_contributions")

    # Select only merge columns + score columns
    scored_subset = result.select(*merge_columns, *score_cols_to_join)

    # Take distinct rows to avoid duplicates
    agg_exprs = []
    for score_col in score_cols_to_join:
        if score_col == "anomaly_contributions":
            agg_exprs.append(F.first(score_col).alias(score_col))
        else:
            agg_exprs.append(F.max(score_col).alias(score_col))
    scored_subset_unique = scored_subset.groupBy(*merge_columns).agg(*agg_exprs)

    # Left join back to original DataFrame
    return df.join(scored_subset_unique, on=merge_columns, how="left")


def _prepare_scoring_dataframe(df: DataFrame, row_filter: str | None) -> DataFrame:
    """Prepare DataFrame for scoring by applying optional row filter."""
    return df.filter(F.expr(row_filter)) if row_filter else df


def _create_null_scored_dataframe(df: DataFrame, include_contributions: bool) -> DataFrame:
    """Create a DataFrame with null anomaly scores (for empty segments)."""
    result = df.withColumn("anomaly_score", F.lit(None).cast(DoubleType()))
    result = result.withColumn("anomaly_score_std", F.lit(None).cast(DoubleType()))
    if include_contributions:
        result = result.withColumn("anomaly_contributions", F.lit(None).cast(MapType(StringType(), DoubleType())))
    return result


def _score_single_segment(
    segment_df: DataFrame,
    segment_model: AnomalyModelRecord,
    config: ScoringConfig,
) -> DataFrame:
    """Score a single segment with its specific model."""
    # Check for drift in this segment
    _check_segment_drift(
        segment_df,
        config.columns,
        segment_model,
        config.drift_threshold,
        config.drift_threshold_value,
    )

    # Score this segment with optional SHAP (computed in single UDF pass)
    assert segment_model.feature_metadata is not None, f"Model {segment_model.model_name} missing feature_metadata"
    segment_scored = _score_with_sklearn_model(
        segment_model.model_uri,
        segment_df,
        config.columns,
        segment_model.feature_metadata,
        config.merge_columns,
        include_contributions=config.include_contributions,
    )
    return segment_scored.withColumn("anomaly_score_std", F.lit(0.0))


def _score_segmented(
    df: DataFrame,
    config: ScoringConfig,
    condition_col: str,
    registry_client: AnomalyModelRegistry,
) -> DataFrame:
    """Score DataFrame using segment-specific models.

    Args:
        df: Input DataFrame to score.
        config: Scoring configuration with all parameters.
        condition_col: Name of output condition column.
        registry_client: Registry client for loading model metadata.

    Returns:
        Scored DataFrame with anomaly scores and condition column.
    """
    # Get all segment models
    all_segments = registry_client.get_all_segment_models(config.registry_table, config.model_name)

    if not all_segments:
        raise InvalidParameterError(
            f"No segment models found for base model '{config.model_name}'. "
            "Train segmented models first using anomaly.train(...)."
        )

    # Filter rows if row_filter is provided
    df_to_score = _prepare_scoring_dataframe(df, config.row_filter)

    scored_dfs: list[DataFrame] = []

    # Score each segment separately
    for segment_model in all_segments:
        segment_filter = _build_segment_filter(segment_model.segment_values)
        if segment_filter is None:
            continue

        segment_df = df_to_score.filter(segment_filter)
        if segment_df.count() == 0:
            continue

        segment_scored = _score_single_segment(segment_df, segment_model, config)
        scored_dfs.append(segment_scored)

    # Union all scored segments
    if not scored_dfs:
        result = _create_null_scored_dataframe(df_to_score, config.include_contributions)
    else:
        result = scored_dfs[0]
        for sdf in scored_dfs[1:]:
            result = result.union(sdf)

    # Drop confidence column if not requested
    if not config.include_confidence:
        result = result.drop("anomaly_score_std")

    # If row_filter was used, join scored results back to original DataFrame
    if config.row_filter:
        result = _join_filtered_results_back(
            df, result, config.merge_columns, config.include_confidence, config.include_contributions
        )

    # Add condition column
    condition = F.when(F.col("anomaly_score").isNull(), F.lit(False)).otherwise(
        F.col("anomaly_score") > F.lit(config.score_threshold)
    )

    return result.withColumn(condition_col, condition)


def _prepare_feature_metadata(feature_metadata_json: str) -> tuple[list[ColumnTypeInfo], SparkFeatureMetadata]:
    """Load and prepare feature metadata from JSON."""
    feature_metadata = SparkFeatureMetadata.from_json(feature_metadata_json)

    column_infos = [
        ColumnTypeInfo(
            name=info["name"],
            spark_type=T.StringType(),  # Type not used in scoring
            category=info["category"],
            cardinality=info.get("cardinality"),
            null_count=info.get("null_count"),
        )
        for info in feature_metadata.column_infos
    ]

    return column_infos, feature_metadata


def _apply_feature_engineering_for_scoring(
    df: DataFrame,
    feature_cols: list[str],
    merge_columns: list[str],
    column_infos: list[ColumnTypeInfo],
    feature_metadata: SparkFeatureMetadata,
) -> DataFrame:
    """Apply feature engineering to DataFrame for scoring."""
    # Deduplicate columns to avoid duplicate column errors
    cols_to_select = list(dict.fromkeys([*feature_cols, *merge_columns]))

    # Apply feature engineering (distributed)
    engineered_df, _ = apply_feature_engineering(
        df.select(*cols_to_select),
        column_infos,
        categorical_cardinality_threshold=20,
        frequency_maps=feature_metadata.categorical_frequency_maps,
        onehot_categories=feature_metadata.onehot_categories,
    )

    return engineered_df


def _create_udf_schema(include_contributions: bool) -> StructType:
    """Create schema for scoring UDF output."""
    schema_fields = [
        StructField("anomaly_score", DoubleType(), True),
        StructField("prediction", IntegerType(), True),
    ]
    if include_contributions:
        schema_fields.append(StructField("anomaly_contributions", MapType(StringType(), DoubleType()), True))
    return StructType(schema_fields)


def _score_with_sklearn_model(
    model_uri: str,
    df: DataFrame,
    feature_cols: list[str],
    feature_metadata_json: str,
    merge_columns: list[str],
    include_contributions: bool = False,
) -> DataFrame:
    """Score DataFrame using scikit-learn model with distributed pandas UDF.

    Feature engineering is applied in Spark before the pandas UDF.
    The pandas UDF handles sklearn components (RobustScaler + IsolationForest) and optionally SHAP.

    Args:
        model_uri: MLflow model URI
        df: DataFrame to score (with original columns)
        feature_cols: List of original feature column names (before engineering)
        feature_metadata_json: JSON string with feature engineering metadata (from registry)
        merge_columns: Columns to use for joining results back (e.g., primary keys or row IDs).
        include_contributions: If True, compute SHAP feature contributions in the same UDF pass.

    Returns:
        DataFrame with anomaly_score, prediction, and optionally anomaly_contributions columns added
    """
    # Load model
    sklearn_model = mlflow.sklearn.load_model(model_uri)

    # Prepare feature metadata
    column_infos, feature_metadata = _prepare_feature_metadata(feature_metadata_json)

    # Apply feature engineering
    engineered_df = _apply_feature_engineering_for_scoring(
        df, feature_cols, merge_columns, column_infos, feature_metadata
    )

    # Get engineered feature names and serialize model
    engineered_feature_cols = feature_metadata.engineered_feature_names
    model_bytes = cloudpickle.dumps(sklearn_model)

    # Create UDF schema
    schema = _create_udf_schema(include_contributions)

    @pandas_udf(schema, PandasUDFType.SCALAR)
    def predict_with_shap_udf(*cols):
        """Pandas UDF for distributed scoring with optional SHAP (Spark Connect compatible).

        Note: All imports are at module-level since DQX is installed as a wheel on all cluster nodes.
        """
        # Deserialize model
        model_local = cloudpickle.loads(model_bytes)

        # Convert input to DataFrame
        feature_matrix = pd.concat(cols, axis=1)
        feature_matrix.columns = engineered_feature_cols

        # Score using full pipeline
        predictions = np.where(model_local.predict(feature_matrix) == -1, 1, 0)
        scores = -model_local.score_samples(feature_matrix)

        # Compute SHAP if requested
        contributions_list = []
        if include_contributions:
            if not SHAP_AVAILABLE:
                raise ImportError(
                    "To use feature contributions (include_contributions=True), install 'shap>=0.42.0,<0.46' on your cluster.\n"
                    "Cluster -> Libraries -> Install New -> PyPI -> 'shap>=0.42.0,<0.46'"
                )

            # Extract components
            if isinstance(model_local, Pipeline):
                scaler = model_local.named_steps.get('scaler')
                tree_model = model_local.named_steps['model']
            else:
                scaler = None
                tree_model = model_local

            explainer = shap.TreeExplainer(tree_model)
            shap_data = scaler.transform(feature_matrix) if scaler else feature_matrix.values

            # Compute SHAP for each row
            for i, row_vals in enumerate(shap_data):
                if pd.isna(row_vals).any():
                    contributions_list.append({c: None for c in engineered_feature_cols})
                    continue

                shap_vals = explainer.shap_values(shap_data[i : i + 1])[0]
                abs_vals = np.abs(shap_vals)
                total = abs_vals.sum()

                if total > 0:
                    normalized = abs_vals / total
                    contributions = {c: float(normalized[j]) for j, c in enumerate(engineered_feature_cols)}
                else:
                    equal_contrib = 1.0 / len(engineered_feature_cols)
                    contributions = {c: equal_contrib for c in engineered_feature_cols}

                contributions_list.append(contributions)

        # Build result
        result = {"anomaly_score": scores, "prediction": predictions}
        if include_contributions:
            result["anomaly_contributions"] = contributions_list

        return pd.DataFrame(result)

    # Apply UDF to all engineered feature columns
    # The join columns are preserved in engineered_df
    scored_df = engineered_df.withColumn("_scores", predict_with_shap_udf(*[col(c) for c in engineered_feature_cols]))

    # Select columns to join back
    # Note: Include merge_columns + new anomaly columns for the join
    # Spark's join(on=...) automatically deduplicates the join key columns
    cols_to_select = [*merge_columns, "_scores.anomaly_score", "_scores.prediction"]
    if include_contributions:
        cols_to_select.append("_scores.anomaly_contributions")

    # Join scores back to original DataFrame using ONLY the merge columns
    # Result will have all columns from df + anomaly columns, no duplicates
    result = df.join(scored_df.select(*cols_to_select), on=merge_columns, how="left")

    return result


def _get_record_for_discovery(
    registry_client: AnomalyModelRegistry,
    registry: str,
    model_name_local: str,
) -> AnomalyModelRecord:
    """Get model record for auto-discovery, checking global and segmented models."""
    record = registry_client.get_active_model(registry, model_name_local)

    if record:
        return record

    # Try segmented models
    all_segments = registry_client.get_all_segment_models(registry, model_name_local)
    if all_segments:
        return all_segments[0]

    raise InvalidParameterError(
        f"Model '{model_name_local}' not found in '{registry}'. " "Train first using anomaly.train(...)."
    )


@register_rule("dataset")
def _discover_model_and_config(
    df: DataFrame,
    model: str | None,
    registry_table: str | None,
    columns_param: list[str | Column] | None,
    segment_by_param: list[str] | None,
) -> tuple[list[str], list[str] | None, str, str]:
    """Auto-discover columns and segment_by from model registry if not provided.

    Returns:
        Tuple of (columns, segment_by, model_name, registry)
    """
    registry_client = AnomalyModelRegistry(df.sparkSession)
    registry = registry_table or _derive_registry_table(df)

    columns = columns_param
    segment_by = segment_by_param

    # Auto-discover from model if needed
    if columns is None or segment_by is None:
        if not model:
            raise InvalidParameterError("Either 'model' or 'columns' must be provided for auto-discovery.")

        model_name_local = _ensure_full_model_name(model, registry)
        record = _get_record_for_discovery(registry_client, registry, model_name_local)

        if columns is None:
            columns = list(record.columns)
        if segment_by is None:
            segment_by = record.segment_by

    # Validate and normalize
    assert columns is not None, "columns should be set by now"
    normalized_columns: list[str] = [get_column_name_or_alias(c) for c in columns]

    if not model:
        raise InvalidParameterError("model parameter is required. Example: has_no_anomalies(model='my_model', ...)")

    model_name = _ensure_full_model_name(model, registry)
    return normalized_columns, segment_by, model_name, registry


def _get_and_validate_model_record(
    registry_client: "AnomalyModelRegistry",
    registry: str,
    model_name: str,
    columns: list[str],
) -> "AnomalyModelRecord":
    """
    Retrieve and validate model record from registry.

    Raises InvalidParameterError if model not found or columns don't match.
    """
    record = registry_client.get_active_model(registry, model_name)

    if not record:
        raise InvalidParameterError(
            f"Model '{model_name}' not found in '{registry}'. Train first using anomaly.train(...)."
        )

    if set(columns) != set(record.columns):
        raise InvalidParameterError(f"Columns {columns} don't match trained model columns {record.columns}")

    return record


def _check_model_staleness(record: "AnomalyModelRecord", model_name: str) -> None:
    """Check model training age and issue warning if stale (>30 days)."""
    if record.training_time:
        age_days = (datetime.utcnow() - record.training_time).days
        if age_days > 30:
            warnings.warn(
                f"Model '{model_name}' is {age_days} days old. Consider retraining.",
                UserWarning,
                stacklevel=3,
            )


def _check_and_warn_drift(
    df: DataFrame,
    columns: list[str],
    record: "AnomalyModelRecord",
    model_name: str,
    drift_threshold: float | None,
    drift_threshold_value: float,
) -> None:
    """Check for data drift and issue warning if detected."""
    if drift_threshold is not None and record.baseline_stats:
        drift_result = compute_drift_score(
            df.select(columns),
            columns,
            record.baseline_stats,
            drift_threshold_value,
        )

        if drift_result.drift_detected:
            drifted_cols_str = ", ".join(drift_result.drifted_columns)
            retrain_cmd = f"anomaly.train(df=your_dataframe, columns={columns}, model_name='{model_name}')"
            warnings.warn(
                f"DATA DRIFT DETECTED in columns: {drifted_cols_str} "
                f"(drift score: {drift_result.drift_score:.2f}). "
                f"Model may be stale. Consider retraining: {retrain_cmd}",
                UserWarning,
                stacklevel=3,
            )


def _score_ensemble_models(
    model_uris: list[str],
    df_filtered: DataFrame,
    columns: list[str],
    feature_metadata: str,
    merge_columns: list[str],
    include_contributions: bool,
) -> DataFrame:
    """Score DataFrame with ensemble of models and compute mean/std."""
    scored_dfs = []
    for i, uri in enumerate(model_uris):
        # Only compute SHAP for first model in ensemble (if requested)
        compute_shap_for_this = include_contributions and i == 0
        temp_scored = _score_with_sklearn_model(
            uri,
            df_filtered,
            columns,
            feature_metadata,
            merge_columns,
            include_contributions=compute_shap_for_this,
        )
        temp_scored = temp_scored.withColumn(f"_score_{i}", F.col("anomaly_score"))

        # Drop standard model output columns
        scored_dfs.append(temp_scored.select("*", f"_score_{i}").drop("anomaly_score", "prediction"))

    # Merge all scored DataFrames
    scored_df = scored_dfs[0]
    for i in range(1, len(scored_dfs)):
        scored_df = scored_df.join(scored_dfs[i].select(*merge_columns, f"_score_{i}"), on=merge_columns, how="inner")

    # Compute mean and std
    score_cols = [f"_score_{i}" for i in range(len(model_uris))]
    scored_df = scored_df.withColumn("anomaly_score", sum(F.col(col) for col in score_cols) / F.lit(len(model_uris)))

    # Standard deviation (confidence)
    mean_col = F.col("anomaly_score")
    variance = sum((F.col(col) - mean_col) ** 2 for col in score_cols) / F.lit(len(model_uris) - 1)
    scored_df = scored_df.withColumn("anomaly_score_std", F.sqrt(variance))

    # Drop intermediate columns
    for score_col in score_cols:
        scored_df = scored_df.drop(score_col)

    return scored_df


def _try_segmented_scoring_fallback(
    df: DataFrame,
    config: ScoringConfig,
    condition_col: str,
    registry_client: AnomalyModelRegistry,
) -> DataFrame | None:
    """Try to score using segmented models as fallback. Returns None if no segments found."""
    all_segments = registry_client.get_all_segment_models(config.registry_table, config.model_name)
    if not all_segments:
        return None

    # Auto-detect segmentation
    first_segment = all_segments[0]
    assert first_segment.segment_by is not None, "Segment model must have segment_by"
    return _score_segmented(df, config, condition_col, registry_client)


def _score_global_model(
    df: DataFrame,
    record: AnomalyModelRecord,
    config: ScoringConfig,
    condition_col: str,
) -> DataFrame:
    """Score using a global (non-segmented) model."""
    # Validate, check staleness, and drift
    if set(config.columns) != set(record.columns):
        raise InvalidParameterError(f"Columns {config.columns} don't match trained model columns {record.columns}")

    _check_model_staleness(record, config.model_name)
    _check_and_warn_drift(
        df, config.columns, record, config.model_name, config.drift_threshold, config.drift_threshold_value
    )

    # Prepare data
    df_filtered = _prepare_scoring_dataframe(df, config.row_filter)

    # Score (ensemble or single model)
    model_uris = record.model_uri.split(",")
    assert record.feature_metadata is not None, f"Model {record.model_name} missing feature_metadata"

    scored_df = (
        _score_ensemble_models(
            model_uris,
            df_filtered,
            config.columns,
            record.feature_metadata,
            config.merge_columns,
            config.include_contributions,
        )
        if len(model_uris) > 1
        else _score_with_sklearn_model(
            record.model_uri,
            df_filtered,
            config.columns,
            record.feature_metadata,
            config.merge_columns,
            include_contributions=config.include_contributions,
        ).withColumn("anomaly_score_std", F.lit(0.0))
    )

    # Post-process
    if not config.include_confidence:
        scored_df = scored_df.drop("anomaly_score_std")

    if config.row_filter:
        scored_df = _join_filtered_results_back(
            df, scored_df, config.merge_columns, config.include_confidence, config.include_contributions
        )

    # Add condition
    condition = F.when(F.col("anomaly_score").isNull(), F.lit(False)).otherwise(
        F.col("anomaly_score") > F.lit(config.score_threshold)
    )
    return scored_df.withColumn(condition_col, condition)


def has_no_anomalies(
    merge_columns: list[str],
    columns: list[str | Column] | None = None,
    segment_by: list[str] | None = None,
    model: str | None = None,
    registry_table: str | None = None,
    score_threshold: float = 0.5,
    row_filter: str | None = None,
    drift_threshold: float | None = None,
    include_contributions: bool = False,
    include_confidence: bool = False,
) -> tuple[Column, Any]:
    """Check that records are not anomalous according to a trained model(s).

    Auto-discovery:
    - columns=None: Inferred from model registry
    - segment_by=None: Inferred from model registry (checks if model is segmented)

    Args:
        merge_columns: Primary key columns (e.g., ["activity_id"]) for joining results.
        columns: Columns to check for anomalies (auto-inferred if omitted).
        segment_by: Segment columns (auto-inferred from model if omitted).
        model: Model name (REQUIRED). Provide the base model name returned from train().
        registry_table: Registry table (auto-derived if omitted).
        score_threshold: Anomaly score threshold (default 0.5).
        row_filter: Optional SQL expression to filter rows before scoring.
        drift_threshold: Drift detection threshold (default 3.0, None to disable).
        include_contributions: Include anomaly_contributions column (default False).
        include_confidence: Include anomaly_score_std column for ensembles (default False).

    Returns:
        Tuple of condition expression and apply function.
    """
    condition_col = f"__anomaly_condition_{uuid4().hex}"

    def apply(df: DataFrame) -> DataFrame:
        if not merge_columns:
            raise MissingParameterError("merge_columns is not provided.")

        # Auto-discover configuration
        nonlocal columns, segment_by
        normalized_columns, segment_by, model_name, registry = _discover_model_and_config(
            df, model, registry_table, columns, segment_by
        )

        # Create scoring configuration
        config = ScoringConfig(
            columns=normalized_columns,
            model_name=model_name,
            registry_table=registry,
            score_threshold=score_threshold,
            merge_columns=merge_columns,
            row_filter=row_filter,
            drift_threshold=drift_threshold,
            drift_threshold_value=drift_threshold if drift_threshold is not None else 3.0,
            include_contributions=include_contributions,
            include_confidence=include_confidence,
        )

        registry_client = AnomalyModelRegistry(df.sparkSession)

        # Route to segmented or global scoring
        if segment_by:
            return _score_segmented(df, config, condition_col, registry_client)

        # Try global model first
        record = registry_client.get_active_model(registry, model_name)
        if not record:
            # Fallback to segmented scoring
            result = _try_segmented_scoring_fallback(df, config, condition_col, registry_client)
            if result is not None:
                return result
            raise InvalidParameterError(
                f"Model '{model_name}' not found in '{registry}'. Train first using anomaly.train(...)."
            )

        return _score_global_model(df, record, config, condition_col)

    message = F.lit(f"Anomaly score exceeded threshold {score_threshold}")
    condition_expr = F.col(condition_col)
    return make_condition(condition_expr, message, "has_anomalies"), apply
