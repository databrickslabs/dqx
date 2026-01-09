"""
Check functions for anomaly detection.
"""

from __future__ import annotations

import logging
import sys
import warnings
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import cloudpickle
import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import pandas_udf, col
import pyspark.sql.functions as F
from pyspark.sql.types import (
    DoubleType,
    StructType,
    StructField,
    MapType,
    StringType,
    BooleanType,
)
import sklearn
from sklearn.pipeline import Pipeline
from databricks.labs.dqx.anomaly.model_registry import AnomalyModelRegistry, AnomalyModelRecord, compute_config_hash
from databricks.labs.dqx.anomaly.trainer import (
    _derive_registry_table,
    _ensure_mlflow_registry_uri,
    ensure_full_model_name,
)
from databricks.labs.dqx.anomaly.drift_detector import compute_drift_score
from databricks.labs.dqx.anomaly.explainer import create_optimal_tree_explainer
from databricks.labs.dqx.anomaly.transformers import (
    ColumnTypeInfo,
    SparkFeatureMetadata,
    apply_feature_engineering,
    reconstruct_column_infos,
)
from databricks.labs.dqx.errors import InvalidParameterError, MissingParameterError
from databricks.labs.dqx.rule import register_rule
from databricks.labs.dqx.check_funcs import make_condition
from databricks.labs.dqx.utils import get_column_name_or_alias, missing_required_packages

# Check if SHAP is available (required for feature contributions)
# sklearn is always available when this module loads (required dependency for anomaly extras)
SHAP_AVAILABLE = not missing_required_packages(["shap"])

logger = logging.getLogger(__name__)


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
    segment_by: list[str] | None = None


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
    if drift_threshold is not None and segment_model.training.baseline_stats:
        drift_result = compute_drift_score(
            segment_df.select(columns),
            columns,
            segment_model.training.baseline_stats,
            drift_threshold_value,
        )

        if drift_result.drift_detected:
            drifted_cols_str = ", ".join(drift_result.drifted_columns)
            segment_name = (
                "_".join(f"{k}={v}" for k, v in segment_model.segmentation.segment_values.items())
                if segment_model.segmentation.segment_values
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
) -> DataFrame:
    """Join scored results back to original DataFrame (for row_filter case)."""
    # Get score columns to join (only anomaly_score and _info now)
    score_cols_to_join = ["anomaly_score", "_info"]

    # Select only merge columns + score columns
    scored_subset = result.select(*merge_columns, *score_cols_to_join)

    # Take distinct rows to avoid duplicates
    agg_exprs = [
        F.max("anomaly_score").alias("anomaly_score"),
        F.first("_info").alias("_info"),  # Struct, take first
    ]
    scored_subset_unique = scored_subset.groupBy(*merge_columns).agg(*agg_exprs)

    # Left join back to original DataFrame
    return df.join(scored_subset_unique, on=merge_columns, how="left")


def _prepare_scoring_dataframe(df: DataFrame, row_filter: str | None) -> DataFrame:
    """Prepare DataFrame for scoring by applying optional row filter."""
    return df.filter(F.expr(row_filter)) if row_filter else df


def _create_null_scored_dataframe(
    df: DataFrame, include_contributions: bool, include_confidence: bool = False
) -> DataFrame:
    """Create a DataFrame with null anomaly scores (for empty segments)."""
    result = df.withColumn("anomaly_score", F.lit(None).cast(DoubleType()))
    if include_confidence:
        result = result.withColumn("anomaly_score_std", F.lit(None).cast(DoubleType()))
    if include_contributions:
        result = result.withColumn("anomaly_contributions", F.lit(None).cast(MapType(StringType(), DoubleType())))

    # Add null _info column with proper schema (direct struct, not array)
    null_anomaly_info = F.lit(None).cast(
        StructType(
            [
                StructField("check_name", StringType(), True),
                StructField("score", DoubleType(), True),
                StructField("is_anomaly", BooleanType(), True),
                StructField("threshold", DoubleType(), True),
                StructField("model", StringType(), True),
                StructField("segment", MapType(StringType(), StringType()), True),
                StructField("contributions", MapType(StringType(), DoubleType()), True),
                StructField("confidence_std", DoubleType(), True),
            ]
        )
    )

    info_struct = F.struct(null_anomaly_info.alias("anomaly"))
    result = result.withColumn("_info", info_struct)

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
    assert (
        segment_model.features.feature_metadata is not None
    ), f"Model {segment_model.identity.model_name} missing feature_metadata"
    segment_scored = _score_with_sklearn_model(
        segment_model.identity.model_uri,
        segment_df,
        config.columns,
        segment_model.features.feature_metadata,
        config.merge_columns,
        include_contributions=config.include_contributions,
        model_record=segment_model,  # Pass model record for version validation
    )
    segment_scored = segment_scored.withColumn("anomaly_score_std", F.lit(0.0))

    # Add _info column with segment values
    segment_scored = _add_info_column(
        segment_scored,
        config.model_name,
        config.score_threshold,
        segment_values=segment_model.segmentation.segment_values,
        include_contributions=config.include_contributions,
        include_confidence=config.include_confidence,
    )

    return segment_scored


def _score_segmented(
    df: DataFrame,
    config: ScoringConfig,
    registry_client: AnomalyModelRegistry,
) -> DataFrame:
    """Score DataFrame using segment-specific models.

    Args:
        df: Input DataFrame to score.
        config: Scoring configuration with all parameters.
        registry_client: Registry client for loading model metadata.

    Returns:
        Scored DataFrame with anomaly scores and _info column.
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
    # Note: This loops over segments and filters each time, which means O(n*m) row reads
    # where n=rows, m=segments. For large datasets with many segments, consider:
    # 1. User-side caching: df.cache() before calling apply_checks()
    # 2. Saving scored results to tables (as shown in demos)
    # 3. Using fewer segments or global models for massive datasets
    for segment_model in all_segments:
        segment_filter = _build_segment_filter(segment_model.segmentation.segment_values)
        if segment_filter is None:
            continue

        segment_df = df_to_score.filter(segment_filter)
        # Note: We don't check if segment_df is empty here to avoid triggering
        # a count() action. Empty segments result in empty DataFrames which
        # union() handles gracefully.

        segment_scored = _score_single_segment(segment_df, segment_model, config)
        scored_dfs.append(segment_scored)

    # Union all scored segments
    if not scored_dfs:
        result = _create_null_scored_dataframe(df_to_score, config.include_contributions, config.include_confidence)
    else:
        result = scored_dfs[0]
        for sdf in scored_dfs[1:]:
            result = result.union(sdf)

    # Drop internal columns that are now in _info (after union, before join)
    result = result.drop("anomaly_score_std")  # Always drop (null if not ensemble)
    if config.include_contributions:
        result = result.drop("anomaly_contributions")  # Drop top-level, use _info instead

    # Always join back to original DataFrame to include unscored rows
    # (rows with segment combinations not seen during training will have null scores)
    if config.row_filter:
        result = _join_filtered_results_back(df, result, config.merge_columns)
    else:
        # Even without row_filter, join back to include all rows (including unscored segments)
        result = _join_filtered_results_back(df_to_score, result, config.merge_columns)

    # Drop internal anomaly_score column (use _info.anomaly.score instead)
    result = result.drop("anomaly_score")

    return result


def _prepare_feature_metadata(feature_metadata_json: str) -> tuple[list[ColumnTypeInfo], SparkFeatureMetadata]:
    """Load and prepare feature metadata from JSON."""
    feature_metadata = SparkFeatureMetadata.from_json(feature_metadata_json)
    column_infos = reconstruct_column_infos(feature_metadata)
    return column_infos, feature_metadata


def _apply_feature_engineering_for_scoring(
    df: DataFrame,
    feature_cols: list[str],
    merge_columns: list[str],
    column_infos: list[ColumnTypeInfo],
    feature_metadata: SparkFeatureMetadata,
) -> DataFrame:
    """Apply feature engineering to DataFrame for scoring.

    Note: merge_columns must exist in the DataFrame as they are required for
    joining results back in row_filter cases.
    """
    # Validate that all merge_columns exist in the DataFrame
    missing_cols = [c for c in merge_columns if c not in df.columns]
    if missing_cols:
        raise InvalidParameterError(
            f"merge_columns {missing_cols} not found in DataFrame. "
            f"Available columns: {df.columns}. "
            f"merge_columns must reference actual columns (e.g., primary keys or row identifiers)."
        )

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
    """Create schema for scoring UDF output.

    The anomaly_score is used internally for populating _info.anomaly.score.
    Users should check _info.anomaly.is_anomaly for anomaly status.
    """
    schema_fields = [
        StructField("anomaly_score", DoubleType(), True),
    ]
    if include_contributions:
        schema_fields.append(StructField("anomaly_contributions", MapType(StringType(), DoubleType()), True))
    return StructType(schema_fields)


def _parse_version_tuple(version_str: str) -> tuple[int, int]:
    """Parse version string into (major, minor) tuple."""
    parts = version_str.split(".")
    major = int(parts[0])
    minor = int(parts[1]) if len(parts) > 1 else 0
    return major, minor


def _validate_sklearn_compatibility(model_record: AnomalyModelRecord) -> None:
    """Validate sklearn version compatibility between training and inference.

    Args:
        model_record: Model record containing sklearn_version from training

    Raises:
        Warning if minor version mismatch detected (e.g., 1.2.x vs 1.3.x)
    """
    if not model_record.segmentation.sklearn_version:
        # Old models without version tracking - can't validate
        return

    trained_version = model_record.segmentation.sklearn_version
    current_version = sklearn.__version__

    if trained_version == current_version:
        return  # Perfect match

    # Parse versions
    try:
        trained_major, trained_minor = _parse_version_tuple(trained_version)
        current_major, current_minor = _parse_version_tuple(current_version)

        # Check for version mismatches
        if trained_major != current_major or trained_minor != current_minor:
            warnings.warn(
                f"\nSKLEARN VERSION MISMATCH DETECTED\n"
                f"Model Information:\n"
                f"  - Model: {model_record.identity.model_name}\n"
                f"  - Trained with: sklearn {trained_version}\n"
                f"  - Current environment: sklearn {current_version}\n"
                f"\n"
                f"Minor version changes (e.g., 1.2 -> 1.3) can cause pickle incompatibility errors.\n"
                f"\n"
                f"RECOMMENDED ACTION:\n"
                f"Retrain the model with your current sklearn version:\n"
                f"  anomaly_engine.train(\n"
                f"      df=df_train,\n"
                f"      model_name=\"{model_record.identity.model_name}\",\n"
                f"      columns={model_record.training.columns}\n"
                f"  )\n",
                UserWarning,
                stacklevel=3,
            )
    except (ValueError, IndexError):
        # Couldn't parse version - skip validation
        pass


def _load_sklearn_model_with_error_handling(model_uri: str, model_record: AnomalyModelRecord) -> Any:
    """Load sklearn model from MLflow with graceful error handling.

    Args:
        model_uri: MLflow model URI
        model_record: Model record with metadata for error messages

    Returns:
        Loaded sklearn model

    Raises:
        RuntimeError with actionable error message if loading fails
    """
    # Ensure MLflow registry URI is configured (shared helper from trainer module)
    _ensure_mlflow_registry_uri()

    try:
        return mlflow.sklearn.load_model(model_uri)
    except (ValueError, AttributeError, TypeError) as e:
        # These are typical pickle incompatibility errors
        error_msg = str(e)

        trained_version = model_record.segmentation.sklearn_version or "unknown"
        current_version = sklearn.__version__
        python_version = f"{sys.version_info.major}.{sys.version_info.minor}"

        raise RuntimeError(
            f"\nFAILED TO LOAD ANOMALY DETECTION MODEL\n"
            f"\n"
            f"Model Information:\n"
            f"  - Model: {model_record.identity.model_name}\n"
            f"  - Trained with: sklearn {trained_version}\n"
            f"  - Current environment: sklearn {current_version}, Python {python_version}\n"
            f"  - Model URI: {model_uri}\n"
            f"\n"
            f"Error Details:\n"
            f"  {type(e).__name__}: {error_msg}\n"
            f"\n"
            f"This typically happens when scikit-learn's internal pickle format changes between versions.\n"
            f"Common causes:\n"
            f"  - Major/minor sklearn version mismatch (e.g., 1.2.x vs 1.3.x)\n"
            f"  - Python version changes (e.g., 3.11 vs 3.12)\n"
            f"  - Databricks runtime upgrades\n"
            f"\n"
            f"SOLUTIONS:\n"
            f"\n"
            f"1. Retrain the model (RECOMMENDED):\n"
            f"   anomaly_engine.train(\n"
            f"       df=df_train,\n"
            f"       model_name=\"{model_record.identity.model_name}\",\n"
            f"       columns={model_record.training.columns},\n"
            f"       registry_table=\"<your_registry_table>\"\n"
            f"   )\n"
            f"\n"
            f"2. Downgrade sklearn (temporary workaround):\n"
            f"   %pip install scikit-learn=={trained_version}\n"
            f"   # Then restart Python kernel\n"
            f"\n"
            f"3. Clear registry and retrain all models:\n"
            f"   spark.sql(\"DROP TABLE IF EXISTS <your_registry_table>\")\n"
            f"   # Then retrain all models\n"
        ) from e


def _load_and_validate_model(
    model_uri: str,
    model_record: AnomalyModelRecord,
) -> Any:
    """Load model with validation and error handling.

    Args:
        model_uri: MLflow model URI
        model_record: Model record for version validation

    Returns:
        Loaded sklearn model
    """
    _validate_sklearn_compatibility(model_record)
    return _load_sklearn_model_with_error_handling(model_uri, model_record)


def _create_scoring_udf(
    model_bytes: bytes,
    engineered_feature_cols: list[str],
    include_contributions: bool,
    schema: StructType,
):
    """Create pandas UDF for distributed scoring with optional SHAP.

    Args:
        model_bytes: Serialized sklearn model
        engineered_feature_cols: List of engineered feature column names
        include_contributions: Whether to compute SHAP contributions
        schema: UDF output schema

    Returns:
        Pandas UDF for scoring
    """

    @pandas_udf(schema)  # type: ignore[call-overload]  # StructType is valid but mypy has incomplete stubs
    def predict_with_shap_udf(*cols: pd.Series) -> pd.DataFrame:
        """Pandas UDF for distributed scoring with optional SHAP (Spark Connect compatible).

        Note: All imports are at module-level since DQX is installed as a wheel on all cluster nodes.
        """
        # Deserialize model
        model_local = cloudpickle.loads(model_bytes)

        # Convert input to DataFrame
        feature_matrix = pd.concat(cols, axis=1)
        feature_matrix.columns = engineered_feature_cols

        # Score using full pipeline
        scores = -model_local.score_samples(feature_matrix)

        # Compute SHAP if requested
        contributions_list = []
        if include_contributions:
            contributions_list = _compute_shap_contributions_in_udf(
                model_local, feature_matrix, engineered_feature_cols
            )

        # Build result
        result = {"anomaly_score": scores}
        if include_contributions:
            result["anomaly_contributions"] = contributions_list

        return pd.DataFrame(result)

    return predict_with_shap_udf


def _compute_shap_contributions_in_udf(
    model_local: Any,
    feature_matrix: pd.DataFrame,
    engineered_feature_cols: list[str],
) -> list[dict[str, float | None]]:
    """Compute SHAP contributions using batch processing for optimal performance.

    Optimizations implemented:
    - Single SHAP call for all valid rows (not N calls) - 3-5x faster
    - Vectorized normalization via NumPy
    - Efficient NaN handling
    - Uses SHAP's TreeExplainer with optimized C++ implementation

    Args:
        model_local: Loaded sklearn model (Pipeline or IsolationForest)
        feature_matrix: DataFrame with engineered features
        engineered_feature_cols: List of feature column names

    Returns:
        List of contribution dictionaries, one per row (values can be float or None for rows with NaN)
    """
    if not SHAP_AVAILABLE:
        raise ImportError(
            "SHAP library not available in pandas UDF worker. This should have been caught earlier.\n\n"
            "To fix:\n"
            "  1. Install DQX with anomaly extras: %pip install 'databricks-labs-dqx[anomaly]'\n"
            "  2. Or install SHAP separately: %pip install 'shap>=0.42.0,<0.46'\n"
            "  3. Restart Python: dbutils.library.restartPython()\n"
            "  4. Re-run the scoring operation\n\n"
            "If this error persists after installation, the cluster may need to be restarted."
        )

    # Extract components from Pipeline if present
    if isinstance(model_local, Pipeline):
        scaler = model_local.named_steps.get('scaler')
        tree_model = model_local.named_steps['model']
    else:
        scaler = None
        tree_model = model_local

    # Create TreeSHAP explainer for feature contribution computation
    explainer = create_optimal_tree_explainer(tree_model)

    # Prepare data (scaling if needed)
    shap_data = scaler.transform(feature_matrix) if scaler else feature_matrix.values

    # Identify rows with NaN values
    has_nan = pd.isna(shap_data).any(axis=1)
    valid_indices = ~has_nan

    # Initialize contributions list (None for all rows initially)
    num_rows = len(shap_data)
    num_features = len(engineered_feature_cols)
    contributions_list: list[dict[str, float | None]] = [
        {c: None for c in engineered_feature_cols} for _ in range(num_rows)
    ]

    # Early exit if no valid rows
    if valid_indices.sum() == 0:
        return contributions_list

    # Compute SHAP values for all valid rows in a single batch
    # TreeExplainer.shap_values() processes batches efficiently
    valid_data = shap_data[valid_indices]
    shap_values_batch = explainer.shap_values(valid_data)

    # Normalize absolute SHAP values to sum to 1.0 per row
    # This makes contributions interpretable as relative importance percentages
    abs_shap = np.abs(shap_values_batch)
    totals = abs_shap.sum(axis=1, keepdims=True)

    # Assign equal contributions when all SHAP values are zero
    # This occurs for data points perfectly aligned with training distribution
    normalized = np.where(totals > 0, abs_shap / totals, 1.0 / num_features)

    # Build contribution dictionaries for valid rows
    valid_row_idx = 0
    for i in range(num_rows):
        if valid_indices[i]:
            contributions_list[i] = {
                engineered_feature_cols[j]: round(float(normalized[valid_row_idx, j]), 3) for j in range(num_features)
            }
            valid_row_idx += 1

    return contributions_list


def _score_with_sklearn_model(
    model_uri: str,
    df: DataFrame,
    feature_cols: list[str],
    feature_metadata_json: str,
    merge_columns: list[str],
    include_contributions: bool = False,
    *,
    model_record: AnomalyModelRecord,
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
        model_record: Model record for version validation and error handling.

    Returns:
        DataFrame with anomaly_score, prediction, and optionally anomaly_contributions columns added
    """
    # Load and validate model
    sklearn_model = _load_and_validate_model(model_uri, model_record)

    # Prepare feature metadata
    column_infos, feature_metadata = _prepare_feature_metadata(feature_metadata_json)

    # Apply feature engineering
    engineered_df = _apply_feature_engineering_for_scoring(
        df, feature_cols, merge_columns, column_infos, feature_metadata
    )

    # Get engineered feature names and serialize model
    engineered_feature_cols = feature_metadata.engineered_feature_names
    model_bytes = cloudpickle.dumps(sklearn_model)

    # Create UDF schema and scoring UDF
    schema = _create_udf_schema(include_contributions)
    predict_with_shap_udf = _create_scoring_udf(model_bytes, engineered_feature_cols, include_contributions, schema)

    # Apply UDF to all engineered feature columns
    scored_df = engineered_df.withColumn("_scores", predict_with_shap_udf(*[col(c) for c in engineered_feature_cols]))

    # Select columns to join back
    cols_to_select = [*merge_columns, "_scores.anomaly_score"]
    if include_contributions:
        cols_to_select.append("_scores.anomaly_contributions")

    # Join scores back to original DataFrame using ONLY the merge columns
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
    registry = registry_table or _derive_registry_table(df.sparkSession)

    columns = columns_param
    segment_by = segment_by_param

    # Auto-discover from model if needed
    if columns is None or segment_by is None:
        if not model:
            raise InvalidParameterError("Either 'model' or 'columns' must be provided for auto-discovery.")

        model_name_local = ensure_full_model_name(model, registry)
        record = _get_record_for_discovery(registry_client, registry, model_name_local)

        if columns is None:
            columns = list(record.training.columns)
        if segment_by is None:
            segment_by = record.segmentation.segment_by

    # Validate and normalize
    assert columns is not None, "columns should be set by now"
    normalized_columns: list[str] = [get_column_name_or_alias(c) for c in columns]

    if not model:
        raise InvalidParameterError("model parameter is required. Example: has_no_anomalies(model='my_model', ...)")

    model_name = ensure_full_model_name(model, registry)
    return normalized_columns, segment_by, model_name, registry


def _get_and_validate_model_record(
    registry_client: "AnomalyModelRegistry",
    registry: str,
    model_name: str,
    columns: list[str],
    segment_by: list[str] | None = None,
) -> "AnomalyModelRecord":
    """
    Retrieve and validate model record from registry.

    Validates that the configuration (columns + segment_by) matches the trained model.

    Args:
        registry_client: Registry client instance
        registry: Registry table name
        model_name: Model name to retrieve
        columns: Columns to use for scoring
        segment_by: Segmentation columns (optional)

    Returns:
        Validated AnomalyModelRecord

    Raises:
        InvalidParameterError: If model not found or config doesn't match
    """
    record = registry_client.get_active_model(registry, model_name)

    if not record:
        raise InvalidParameterError(
            f"Model '{model_name}' not found in '{registry}'. Train first using anomaly.train(...)."
        )

    # Compute expected config hash and compare with stored hash
    expected_hash = compute_config_hash(columns, segment_by)

    if expected_hash != record.segmentation.config_hash:
        raise InvalidParameterError(
            f"Configuration mismatch for model '{model_name}':\n"
            f"  Trained columns: {record.training.columns}\n"
            f"  Provided columns: {columns}\n"
            f"  Trained segment_by: {record.segmentation.segment_by}\n"
            f"  Provided segment_by: {segment_by}\n\n"
            f"This model was trained with a different configuration. Either:\n"
            f"  1. Use the correct columns/segments that match the trained model\n"
            f"  2. Retrain the model with the new configuration"
        )

    return record


def _check_model_staleness(record: "AnomalyModelRecord", model_name: str) -> None:
    """Check model training age and issue warning if stale (>30 days)."""
    if record.training.training_time:
        age_days = (datetime.utcnow() - record.training.training_time).days
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
    if drift_threshold is not None and record.training.baseline_stats:
        drift_result = compute_drift_score(
            df.select(columns),
            columns,
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


def _score_ensemble_models(
    model_uris: list[str],
    df_filtered: DataFrame,
    columns: list[str],
    feature_metadata_json: str,
    merge_columns: list[str],
    include_contributions: bool,
    *,
    model_record: AnomalyModelRecord,
) -> DataFrame:
    """Score DataFrame with multiple ensemble models and compute statistics.

    Loads all models upfront and scores them in a single pandas UDF to avoid
    repeated model loading and multiple DataFrame operations. SHAP contributions
    are computed only on the first model since contribution patterns are consistent
    across ensemble members.

    Args:
        model_uris: List of MLflow model URIs for ensemble members
        df_filtered: DataFrame to score
        columns: Original feature columns
        feature_metadata_json: JSON string with feature engineering metadata
        merge_columns: Columns to use for joining results back
        include_contributions: Whether to compute SHAP contributions
        model_record: Model record for validation

    Returns:
        DataFrame with anomaly_score (mean), anomaly_score_std (confidence),
        and optionally anomaly_contributions
    """
    # Load and validate all ensemble models before creating UDF
    # This ensures models are compatible and reduces UDF serialization size
    ensemble_models = []
    for uri in model_uris:
        model = _load_and_validate_model(uri, model_record)
        ensemble_models.append(model)

    # Serialize all models once for UDF
    models_bytes = [cloudpickle.dumps(model) for model in ensemble_models]

    # Prepare feature metadata
    column_infos, feature_metadata = _prepare_feature_metadata(feature_metadata_json)

    # Apply feature engineering
    engineered_df = _apply_feature_engineering_for_scoring(
        df_filtered, columns, merge_columns, column_infos, feature_metadata
    )

    engineered_feature_cols = feature_metadata.engineered_feature_names

    # Create ensemble UDF schema
    schema_fields = [
        StructField("anomaly_score", DoubleType(), True),
        StructField("anomaly_score_std", DoubleType(), True),
    ]
    if include_contributions:
        schema_fields.append(StructField("anomaly_contributions", MapType(StringType(), DoubleType()), True))
    schema = StructType(schema_fields)

    # Create ensemble UDF
    @pandas_udf(schema)  # type: ignore[call-overload]
    def ensemble_scoring_udf(*cols: pd.Series) -> pd.DataFrame:
        """Score with all ensemble models in single pass."""
        # Deserialize all models once per executor
        models = [cloudpickle.loads(mb) for mb in models_bytes]

        # Convert input to DataFrame
        feature_matrix = pd.concat(cols, axis=1)
        feature_matrix.columns = engineered_feature_cols

        # Score with all ensemble models
        # IsolationForest.score_samples returns negative anomaly scores (lower = more anomalous)
        # Negate to get positive anomaly scores (higher = more anomalous)
        scores_matrix = np.array([-model.score_samples(feature_matrix) for model in models])

        # Compute mean score (ensemble consensus) and std (confidence/uncertainty)
        mean_scores = scores_matrix.mean(axis=0)
        std_scores = scores_matrix.std(axis=0, ddof=1)

        # Compute SHAP contributions using first model
        # Contribution patterns are consistent across ensemble members
        contributions_list = []
        if include_contributions:
            contributions_list = _compute_shap_contributions_in_udf(models[0], feature_matrix, engineered_feature_cols)

        # Build result
        result = {
            "anomaly_score": mean_scores,
            "anomaly_score_std": std_scores,
        }
        if include_contributions:
            result["anomaly_contributions"] = contributions_list

        return pd.DataFrame(result)

    # Apply ensemble scoring UDF to engineered features
    scored_df = engineered_df.withColumn("_scores", ensemble_scoring_udf(*[col(c) for c in engineered_feature_cols]))

    # Select columns to join back
    cols_to_select = [*merge_columns, "_scores.anomaly_score", "_scores.anomaly_score_std"]
    if include_contributions:
        cols_to_select.append("_scores.anomaly_contributions")

    # Join scores back to original DataFrame
    result = df_filtered.join(scored_df.select(*cols_to_select), on=merge_columns, how="left")

    return result


def _try_segmented_scoring_fallback(
    df: DataFrame,
    config: ScoringConfig,
    registry_client: AnomalyModelRegistry,
) -> DataFrame | None:
    """Try to score using segmented models as fallback. Returns None if no segments found."""
    all_segments = registry_client.get_all_segment_models(config.registry_table, config.model_name)
    if not all_segments:
        return None

    # Auto-detect segmentation
    first_segment = all_segments[0]
    assert first_segment.segmentation.segment_by is not None, "Segment model must have segment_by"
    return _score_segmented(df, config, registry_client)


def _add_info_column(
    df: DataFrame,
    model_name: str,
    score_threshold: float,
    segment_values: dict[str, str] | None = None,
    include_contributions: bool = False,
    include_confidence: bool = False,
) -> DataFrame:
    """Add _info struct column with anomaly metadata.

    Args:
        df: Scored DataFrame with anomaly_score, prediction, etc.
        model_name: Name of the model used for scoring.
        score_threshold: Threshold used for anomaly detection.
        segment_values: Segment values if model is segmented (None for global models).
        include_contributions: Whether anomaly_contributions are available.
        include_confidence: Whether anomaly_score_std is available.

    Returns:
        DataFrame with _info column added.
    """
    # Build anomaly info struct
    anomaly_info_fields = {
        "check_name": F.lit("has_no_anomalies"),
        "score": F.col("anomaly_score"),
        "is_anomaly": F.col("anomaly_score") >= F.lit(score_threshold),
        "threshold": F.lit(score_threshold),
        "model": F.lit(model_name),
    }

    # Add segment as map (null for global models)
    if segment_values:
        anomaly_info_fields["segment"] = F.create_map(
            *[F.lit(item) for pair in segment_values.items() for item in pair]
        )
    else:
        anomaly_info_fields["segment"] = F.lit(None).cast(MapType(StringType(), StringType()))

    # Add contributions (null if not requested or not available)
    if include_contributions and "anomaly_contributions" in df.columns:
        anomaly_info_fields["contributions"] = F.col("anomaly_contributions")
    else:
        anomaly_info_fields["contributions"] = F.lit(None).cast(MapType(StringType(), DoubleType()))

    # Add confidence_std (null if not requested or not available)
    if include_confidence and "anomaly_score_std" in df.columns:
        anomaly_info_fields["confidence_std"] = F.col("anomaly_score_std")
    else:
        anomaly_info_fields["confidence_std"] = F.lit(None).cast(DoubleType())

    # Create anomaly info struct and wrap in array
    anomaly_info = F.struct(*[value.alias(key) for key, value in anomaly_info_fields.items()])

    # Create _info struct with anomaly (direct struct, not array - single result per row)
    info_struct = F.struct(anomaly_info.alias("anomaly"))

    return df.withColumn("_info", info_struct)


def _score_global_model(
    df: DataFrame,
    record: AnomalyModelRecord,
    config: ScoringConfig,
) -> DataFrame:
    """Score using a global (non-segmented) model."""
    # Validate config hash matches
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

    _check_model_staleness(record, config.model_name)
    _check_and_warn_drift(
        df, config.columns, record, config.model_name, config.drift_threshold, config.drift_threshold_value
    )

    # Prepare data
    df_filtered = _prepare_scoring_dataframe(df, config.row_filter)

    # Score (ensemble or single model)
    model_uris = record.identity.model_uri.split(",")
    assert record.features.feature_metadata is not None, f"Model {record.identity.model_name} missing feature_metadata"

    scored_df = (
        _score_ensemble_models(
            model_uris,
            df_filtered,
            config.columns,
            record.features.feature_metadata,
            config.merge_columns,
            config.include_contributions,
            model_record=record,  # Pass model record for version validation
        )
        if len(model_uris) > 1
        else _score_with_sklearn_model(
            record.identity.model_uri,
            df_filtered,
            config.columns,
            record.features.feature_metadata,
            config.merge_columns,
            include_contributions=config.include_contributions,
            model_record=record,  # Pass model record for version validation
        ).withColumn("anomaly_score_std", F.lit(0.0))
    )

    # Add _info column (before dropping internal columns)
    scored_df = _add_info_column(
        scored_df,
        config.model_name,
        config.score_threshold,
        segment_values=None,  # Global model has no segments
        include_contributions=config.include_contributions,
        include_confidence=config.include_confidence,
    )

    # Post-process: drop internal columns that are now in _info
    scored_df = scored_df.drop("anomaly_score_std")  # Always drop (null if not ensemble)
    if config.include_contributions:
        scored_df = scored_df.drop("anomaly_contributions")  # Drop top-level, use _info instead

    if config.row_filter:
        scored_df = _join_filtered_results_back(df, scored_df, config.merge_columns)

    # Drop internal anomaly_score column (use _info.anomaly.score instead)
    scored_df = scored_df.drop("anomaly_score")

    return scored_df


@register_rule("dataset")
def has_no_anomalies(
    merge_columns: list[str],
    columns: list[str | Column] | None = None,
    segment_by: list[str] | None = None,
    model: str | None = None,
    registry_table: str | None = None,
    score_threshold: float = 0.60,
    row_filter: str | None = None,
    drift_threshold: float | None = None,
    include_contributions: bool = True,
    include_confidence: bool = False,
) -> tuple[Column, Any]:
    """Check that records are not anomalous according to a trained model(s).

    Auto-discovery:
    - columns=None: Inferred from model registry
    - segment_by=None: Inferred from model registry (checks if model is segmented)

    Output columns:
    - _errors or _warnings: Standard DQX result column based on criticality setting
      (customizable via ExtraParams.result_column_names)
    - _info: Structured anomaly metadata
      - _info.anomaly.score: Anomaly score (0-1)
      - _info.anomaly.is_anomaly: Boolean flag
      - _info.anomaly.threshold: Threshold used
      - _info.anomaly.model: Model name
      - _info.anomaly.segment: Segment values (if segmented)
      - _info.anomaly.contributions: SHAP values (if requested)
      - _info.anomaly.confidence_std: Ensemble std (if requested)

    Args:
        merge_columns: Primary key columns (e.g., ["activity_id"]) for joining results.
        columns: Columns to check for anomalies (auto-inferred if omitted).
        segment_by: Segment columns (auto-inferred from model if omitted).
        model: Model name (REQUIRED). Provide the base model name returned from train().
        registry_table: Registry table (auto-derived if omitted).
        score_threshold: Anomaly score threshold (default 0.60). Records with score >= threshold
            are flagged as anomalous. Higher threshold = stricter detection (fewer anomalies).
        row_filter: Optional SQL expression to filter rows before scoring.
        drift_threshold: Drift detection threshold (default 3.0, None to disable).
        include_contributions: Include SHAP feature contributions for explainability (default True).
            Requires SHAP library. Performance-optimized with native batching.
        include_confidence: Include ensemble confidence scores in _info and top-level (default False).
            Automatically available when training with ensemble_size > 1 (default is 2).

    Returns:
        Tuple of condition expression and apply function.

    Example:
        Access anomaly metadata via _info column:
        >>> df_scored.select("_info.anomaly.score", "_info.anomaly.is_anomaly")
        >>> df_scored.filter(col("_info.anomaly.is_anomaly"))
    """

    def apply(df: DataFrame) -> DataFrame:
        if not merge_columns:
            raise MissingParameterError("merge_columns is not provided.")

        # Validate SHAP availability early (before Spark jobs)
        if include_contributions and not SHAP_AVAILABLE:
            raise ImportError(
                "Feature contributions require SHAP library (not included in base DQX installation).\n\n"
                "To install SHAP:\n"
                "  Option 1 - Install DQX with anomaly extras (recommended):\n"
                "    %pip install 'databricks-labs-dqx[anomaly]'\n"
                "    dbutils.library.restartPython()\n\n"
                "  Option 2 - Install SHAP separately:\n"
                "    %pip install 'shap>=0.42.0,<0.46'\n"
                "    dbutils.library.restartPython()\n\n"
                "  Option 3 - Use anomaly detection without explanations:\n"
                "    has_no_anomalies(..., include_contributions=False)"
            )

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
            return _score_segmented(df, config, registry_client)

        # Try global model first
        record = registry_client.get_active_model(registry, model_name)
        if not record:
            # Fallback to segmented scoring
            result = _try_segmented_scoring_fallback(df, config, registry_client)
            if result is not None:
                return result
            raise InvalidParameterError(
                f"Model '{model_name}' not found in '{registry}'. Train first using anomaly.train(...)."
            )

        return _score_global_model(df, record, config)

    # Create condition directly from _info.anomaly.is_anomaly (no intermediate column needed)
    message = F.concat_ws(
        "",
        F.lit("Anomaly score "),
        F.round(F.col("_info").anomaly.score, 3).cast("string"),
        F.lit(f" exceeded threshold {score_threshold}"),
    )
    condition_expr = F.col("_info").anomaly.is_anomaly
    return make_condition(condition_expr, message, "has_anomalies"), apply
