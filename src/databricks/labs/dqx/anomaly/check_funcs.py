"""
Check functions for anomaly detection.
"""

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
)
import sklearn
from databricks.labs.dqx.anomaly.model_registry import AnomalyModelRegistry, AnomalyModelRecord, compute_config_hash
from databricks.labs.dqx.anomaly.trainer import _ensure_mlflow_registry_uri
from databricks.labs.dqx.anomaly.drift_detector import compute_drift_score
from databricks.labs.dqx.anomaly.transformers import (
    ColumnTypeInfo,
    SparkFeatureMetadata,
    apply_feature_engineering,
    reconstruct_column_infos,
)
from databricks.labs.dqx.anomaly.utils import (
    build_segment_filter,
    create_null_scored_dataframe,
    add_info_column,
    create_udf_schema,
    validate_sklearn_compatibility,
)
from databricks.labs.dqx.errors import InvalidParameterError
from databricks.labs.dqx.rule import register_rule
from databricks.labs.dqx.check_funcs import make_condition
from databricks.labs.dqx.package_utils import missing_required_packages

# Check if SHAP is available (required for feature contributions)
# sklearn is always available when this module loads (required dependency for anomaly extras)
SHAP_AVAILABLE = not missing_required_packages(["shap"])
_DRIVER_ONLY = {"value": False}

logger = logging.getLogger(__name__)


class AnomalyScoringStrategy:
    """Scoring strategy interface for anomaly models."""

    def supports(self, algorithm: str) -> bool:
        raise NotImplementedError

    def score_global(self, df: DataFrame, record: AnomalyModelRecord, config: "ScoringConfig") -> DataFrame:
        raise NotImplementedError

    def score_segmented(
        self,
        df: DataFrame,
        config: "ScoringConfig",
        registry_client: AnomalyModelRegistry,
        all_segments: list[AnomalyModelRecord],
    ) -> DataFrame:
        raise NotImplementedError


class IsolationForestScoringStrategy(AnomalyScoringStrategy):
    """IsolationForest scoring strategy (default)."""

    def supports(self, algorithm: str) -> bool:
        return algorithm.startswith("IsolationForest")

    def score_global(self, df: DataFrame, record: AnomalyModelRecord, config: "ScoringConfig") -> DataFrame:
        return _score_global_model(df, record, config)

    def score_segmented(
        self,
        df: DataFrame,
        config: "ScoringConfig",
        registry_client: AnomalyModelRegistry,
        all_segments: list[AnomalyModelRecord],
    ) -> DataFrame:
        return _score_segmented(df, config, registry_client, all_segments=all_segments)


_SCORING_STRATEGIES: list[AnomalyScoringStrategy] = [IsolationForestScoringStrategy()]


def _resolve_scoring_strategy(algorithm: str) -> AnomalyScoringStrategy:
    for strategy in _SCORING_STRATEGIES:
        if strategy.supports(algorithm):
            return strategy
    raise InvalidParameterError(
        f"Unsupported model algorithm '{algorithm}'. Add a scoring strategy for this algorithm."
    )


@register_rule("dataset")
def has_no_anomalies(
    model: str,
    registry_table: str,
    threshold: float = 0.60,
    row_filter: str | None = None,
    drift_threshold: float | None = None,
    include_contributions: bool = True,
    include_confidence: bool = False,
) -> tuple[Column, Any]:
    """Check that records are not anomalous according to a trained model(s).

    Auto-discovery:
    - columns: Inferred from model registry
    - segmentation: Inferred from model registry (checks if model is segmented)

    Output columns:
    - _errors or _warnings: Standard DQX result column based on criticality setting
      (customizable via ExtraParams.result_column_names)
    - _dq_info: Structured anomaly metadata
      - _dq_info.anomaly.score: Anomaly score (0-1)
      - _dq_info.anomaly.is_anomaly: Boolean flag
      - _dq_info.anomaly.threshold: Threshold used
      - _dq_info.anomaly.model: Model name
      - _dq_info.anomaly.segment: Segment values (if segmented)
    - _dq_info.anomaly.contributions: SHAP values (if requested)
    - _dq_info.anomaly.confidence_std: Ensemble std (if requested)

    Notes:
        DQX always scores using the columns the model was trained on.
        DQX aligns scored rows back to the input using an internal row id and removes it before returning.
        Segmentation is inferred from the trained model configuration.

    Args:
        model: Model name (REQUIRED). Provide the fully qualified model name
            in catalog.schema.model format returned from train().
        registry_table: Registry table (REQUIRED). Provide the fully qualified table
            name in catalog.schema.table format.
        threshold: Anomaly score threshold (default 0.60). Records with score >= threshold
            are flagged as anomalous. Higher threshold = stricter detection (fewer anomalies).
        row_filter: Optional SQL expression to filter rows before scoring.
        drift_threshold: Drift detection threshold (default 3.0, None to disable).
        include_contributions: Include SHAP feature contributions for explainability (default True).
            Requires SHAP library. Performance-optimized with native batching.
        include_confidence: Include ensemble confidence scores in _dq_info and top-level (default False).
            Automatically available when training with ensemble_size > 1 (default is 2).

    Returns:
        Tuple of condition expression and apply function.

    Example:
        Access anomaly metadata via _dq_info column:
        >>> df_scored.select("_dq_info.anomaly.score", "_dq_info.anomaly.is_anomaly")
        >>> df_scored.filter(col("_dq_info.anomaly.is_anomaly"))
    """
    _validate_has_no_anomalies_args(
        model=model,
        registry_table=registry_table,
        threshold=threshold,
        row_filter=row_filter,
        drift_threshold=drift_threshold,
        include_contributions=include_contributions,
        include_confidence=include_confidence,
    )
    if include_contributions and not SHAP_AVAILABLE:
        raise ImportError(
            "Feature contributions require SHAP (not included in base DQX installation).\n\n"
            "Install DQX anomaly extras:\n"
            "  %pip install 'databricks-labs-dqx[anomaly]'\n"
            "  dbutils.library.restartPython()"
        )

    def apply(df: DataFrame) -> DataFrame:
        df_to_score, local_merge_columns, row_id_col = _ensure_merge_columns(df)

        driver_only_local = _DRIVER_ONLY["value"]
        score_col, score_std_col, contributions_col = _resolve_internal_columns(df_to_score)

        # Auto-discover configuration
        normalized_columns, segment_by, model_name, registry = _discover_model_and_config(
            df_to_score, model, registry_table
        )

        # Create scoring configuration
        config = ScoringConfig(
            columns=normalized_columns,
            model_name=model_name,
            registry_table=registry,
            threshold=threshold,
            merge_columns=local_merge_columns,
            row_filter=row_filter,
            drift_threshold=drift_threshold,
            drift_threshold_value=drift_threshold if drift_threshold is not None else 3.0,
            include_contributions=include_contributions,
            include_confidence=include_confidence,
            segment_by=segment_by,
            driver_only=driver_only_local,
            score_col=score_col,
            score_std_col=score_std_col,
            contributions_col=contributions_col,
        )

        registry_client = AnomalyModelRegistry(df_to_score.sparkSession)

        # Route to segmented or global scoring
        if segment_by:
            all_segments = _load_segment_models(registry_client, config)
            strategy = _resolve_scoring_strategy(all_segments[0].identity.algorithm)
            result = strategy.score_segmented(df_to_score, config, registry_client, all_segments)
            if row_id_col:
                result = result.drop(row_id_col)
            return result

        # Try global model first
        record = registry_client.get_active_model(registry, model_name)
        if not record:
            # Fallback to segmented scoring
            fallback = _try_segmented_scoring_fallback(df_to_score, config, registry_client)
            if fallback is not None:
                if row_id_col:
                    fallback = fallback.drop(row_id_col)
                return fallback
            raise InvalidParameterError(
                f"Model '{model_name}' not found in '{registry}'. Train first using anomaly.train(...)."
            )

        strategy = _resolve_scoring_strategy(record.identity.algorithm)
        result = strategy.score_global(df_to_score, record, config)
        if row_id_col:
            result = result.drop(row_id_col)
        return result

    # Create condition directly from _dq_info.anomaly.is_anomaly (no intermediate column needed)
    message = F.concat_ws(
        "",
        F.lit("Anomaly score "),
        F.round(F.col("_dq_info").anomaly.score, 3).cast("string"),
        F.lit(f" exceeded threshold {threshold}"),
    )
    condition_expr = F.col("_dq_info").anomaly.is_anomaly
    return make_condition(condition_expr, message, "has_anomalies"), apply


@dataclass
class ScoringConfig:
    """Configuration for anomaly scoring."""

    columns: list[str]
    model_name: str
    registry_table: str
    threshold: float
    merge_columns: list[str]
    row_filter: str | None = None
    drift_threshold: float | None = None
    drift_threshold_value: float = 3.0
    include_contributions: bool = False
    include_confidence: bool = False
    segment_by: list[str] | None = None
    driver_only: bool = False
    score_col: str = "anomaly_score"
    score_std_col: str = "anomaly_score_std"
    contributions_col: str = "anomaly_contributions"


_INTERNAL_ROW_ID = "_dqx_row_id"


def _ensure_merge_columns(df: DataFrame) -> tuple[DataFrame, list[str], str | None]:
    if _INTERNAL_ROW_ID in df.columns:
        raise InvalidParameterError(
            f"Input DataFrame already contains column '{_INTERNAL_ROW_ID}'. " "Rename the column to use anomaly checks."
        )
    return df.withColumn(_INTERNAL_ROW_ID, F.monotonically_increasing_id()), [_INTERNAL_ROW_ID], _INTERNAL_ROW_ID


def _resolve_internal_columns(df: DataFrame) -> tuple[str, str, str]:
    """Resolve internal scoring column names to avoid collisions with user columns."""
    score_col = "anomaly_score"
    score_std_col = "anomaly_score_std"
    contributions_col = "anomaly_contributions"

    if score_col in df.columns:
        score_col = "_dq_anomaly_score"
    if score_std_col in df.columns:
        score_std_col = "_dq_anomaly_score_std"
    if contributions_col in df.columns:
        contributions_col = "_dq_anomaly_contributions"

    return score_col, score_std_col, contributions_col


def set_driver_only_for_tests(enabled: bool) -> None:
    """Toggle driver-only scoring for tests."""
    _DRIVER_ONLY["value"] = enabled


def _check_segment_drift(
    segment_df: DataFrame,
    columns: list[str],
    segment_model: "AnomalyModelRecord",
    drift_threshold: float | None,
    drift_threshold_value: float,
) -> None:
    """Check and warn about data drift in a segment."""
    if drift_threshold is not None and segment_model.training.baseline_stats:
        drift_df, drift_columns = _prepare_drift_df(
            segment_df,
            columns,
            segment_model,
        )
        drift_result = compute_drift_score(
            drift_df,
            drift_columns,
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
    score_col: str,
) -> DataFrame:
    """Join scored results back to original DataFrame (for row_filter case)."""
    # Get score columns to join (only anomaly_score and _dq_info now)
    score_cols_to_join = [score_col, "_dq_info"]

    # Select only merge columns + score columns
    scored_subset = result.select(*merge_columns, *score_cols_to_join)

    # Take distinct rows to avoid duplicates
    agg_exprs = [
        F.max(score_col).alias(score_col),
        F.max_by("_dq_info", score_col).alias("_dq_info"),
    ]
    scored_subset_unique = scored_subset.groupBy(*merge_columns).agg(*agg_exprs)

    # Left join back to original DataFrame
    return df.join(scored_subset_unique, on=merge_columns, how="left")


def _prepare_scoring_dataframe(df: DataFrame, row_filter: str | None) -> DataFrame:
    """Prepare DataFrame for scoring by applying optional row filter."""
    return df.filter(F.expr(row_filter)) if row_filter else df


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
    if config.driver_only:
        segment_scored = _score_with_sklearn_model_local(
            segment_model.identity.model_uri,
            segment_df,
            config.columns,
            segment_model.features.feature_metadata,
            config.merge_columns,
            include_contributions=config.include_contributions,
            model_record=segment_model,  # Pass model record for version validation
        )
    else:
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
    segment_scored = segment_scored.withColumnRenamed("anomaly_score", config.score_col)
    segment_scored = segment_scored.withColumnRenamed("anomaly_score_std", config.score_std_col)
    if config.include_contributions and "anomaly_contributions" in segment_scored.columns:
        segment_scored = segment_scored.withColumnRenamed("anomaly_contributions", config.contributions_col)

    # Add _info column with segment values
    segment_scored = add_info_column(
        segment_scored,
        config.model_name,
        config.threshold,
        segment_values=segment_model.segmentation.segment_values,
        include_contributions=config.include_contributions,
        include_confidence=config.include_confidence,
        score_col=config.score_col,
        score_std_col=config.score_std_col,
        contributions_col=config.contributions_col,
    )

    return segment_scored


def _score_segmented(
    df: DataFrame,
    config: ScoringConfig,
    registry_client: AnomalyModelRegistry,
    all_segments: list[AnomalyModelRecord] | None = None,
) -> DataFrame:
    """Score DataFrame using segment-specific models.

    Args:
        df: Input DataFrame to score.
        config: Scoring configuration with all parameters.
        registry_client: Registry client for loading model metadata.
        all_segments: Optional preloaded segment records.

    Returns:
        Scored DataFrame with anomaly scores and _info column.
    """
    # Get all segment models
    all_segments = (
        all_segments
        if all_segments is not None
        else registry_client.get_all_segment_models(config.registry_table, config.model_name)
    )

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
        segment_filter = build_segment_filter(segment_model.segmentation.segment_values)
        if segment_filter is None:
            continue

        segment_df = df_to_score.filter(segment_filter)
        if segment_df.limit(1).count() == 0:
            continue
        segment_scored = _score_single_segment(segment_df, segment_model, config)
        scored_dfs.append(segment_scored)

    # Union all scored segments
    if not scored_dfs:
        result = create_null_scored_dataframe(
            df_to_score,
            config.include_contributions,
            config.include_confidence,
            score_col=config.score_col,
            score_std_col=config.score_std_col,
            contributions_col=config.contributions_col,
        )
    else:
        result = scored_dfs[0]
        for sdf in scored_dfs[1:]:
            result = result.union(sdf)

    # Drop internal columns that are now in _dq_info (after union, before join)
    result = result.drop(config.score_std_col)  # Always drop (null if not ensemble)
    if config.include_contributions:
        result = result.drop(config.contributions_col)  # Drop top-level, use _dq_info instead

    # Always join back to original DataFrame to include unscored rows
    # (rows with segment combinations not seen during training will have null scores)
    if config.row_filter:
        result = _join_filtered_results_back(df, result, config.merge_columns, config.score_col)
    else:
        # Even without row_filter, join back to include all rows (including unscored segments)
        result = _join_filtered_results_back(df_to_score, result, config.merge_columns, config.score_col)

    # Drop internal anomaly_score column (use _dq_info.anomaly.score instead)
    result = result.drop(config.score_col)

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

    Note: the internal row identifier must exist in the DataFrame as it is required for
    joining results back in row_filter cases.
    """
    # Validate that all merge_columns exist in the DataFrame
    missing_cols = [c for c in merge_columns if c not in df.columns]
    if missing_cols:
        raise InvalidParameterError(
            f"Internal row identifier {missing_cols} not found in DataFrame. "
            f"Available columns: {df.columns}. "
            "Ensure the anomaly check is applied to the same DataFrame instance."
        )

    # Deduplicate columns to avoid duplicate column errors
    cols_to_select = list(dict.fromkeys([*feature_cols, *merge_columns]))

    # Apply feature engineering (distributed)
    engineered_df, _ = apply_feature_engineering(
        df.select(*cols_to_select),
        column_infos,
        categorical_cardinality_threshold=feature_metadata.categorical_cardinality_threshold,
        frequency_maps=feature_metadata.categorical_frequency_maps,
        onehot_categories=feature_metadata.onehot_categories,
    )

    return engineered_df


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
    validate_sklearn_compatibility(model_record)
    return _load_sklearn_model_with_error_handling(model_uri, model_record)


def _create_scoring_udf(
    model_bytes: bytes,
    engineered_feature_cols: list[str],
    schema: StructType,
):
    """Create pandas UDF for distributed scoring."""

    @pandas_udf(schema)  # type: ignore[call-overload]
    def predict_udf(*cols: pd.Series) -> pd.DataFrame:
        """Pandas UDF for distributed scoring (Spark Connect compatible)."""
        import cloudpickle
        import pandas as pd

        # Deserialize model and prepare input
        model_local = cloudpickle.loads(model_bytes)
        feature_matrix = pd.concat(cols, axis=1)
        feature_matrix.columns = engineered_feature_cols

        # Score using full pipeline
        scores = -model_local.score_samples(feature_matrix)
        return pd.DataFrame({"anomaly_score": scores})

    return predict_udf


def _create_scoring_udf_with_contributions(
    model_bytes: bytes,
    engineered_feature_cols: list[str],
    schema: StructType,
):
    """Create pandas UDF for distributed scoring with SHAP contributions."""

    @pandas_udf(schema)  # type: ignore[call-overload]
    def predict_with_shap_udf(*cols: pd.Series) -> pd.DataFrame:
        """Pandas UDF for distributed scoring with optional SHAP (Spark Connect compatible)."""
        import cloudpickle
        import pandas as pd

        # Deserialize model and prepare input
        model_local = cloudpickle.loads(model_bytes)
        feature_matrix = pd.concat(cols, axis=1)
        feature_matrix.columns = engineered_feature_cols

        # Score using full pipeline
        scores = -model_local.score_samples(feature_matrix)

        shap_values, valid_indices = _compute_shap_values(
            model_local,
            feature_matrix,
            engineered_feature_cols,
            allow_missing=True,
        )
        contributions_list = _format_shap_contributions(
            shap_values, valid_indices, len(feature_matrix), engineered_feature_cols
        )

        return pd.DataFrame({"anomaly_score": scores, "anomaly_contributions": contributions_list})

    return predict_with_shap_udf


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
    schema = create_udf_schema(include_contributions)
    if include_contributions:
        predict_with_shap_udf = _create_scoring_udf_with_contributions(model_bytes, engineered_feature_cols, schema)
    else:
        predict_with_shap_udf = _create_scoring_udf(model_bytes, engineered_feature_cols, schema)

    # Apply UDF to all engineered feature columns
    scored_df = engineered_df.withColumn("_scores", predict_with_shap_udf(*[col(c) for c in engineered_feature_cols]))

    # Select columns to join back
    cols_to_select = [*merge_columns, "_scores.anomaly_score"]
    if include_contributions:
        cols_to_select.append("_scores.anomaly_contributions")

    # Join scores back to original DataFrame using ONLY the merge columns
    result = df.join(scored_df.select(*cols_to_select), on=merge_columns, how="left")

    return result


def _format_shap_contributions(
    shap_values: np.ndarray,
    valid_indices: np.ndarray,
    num_rows: int,
    engineered_feature_cols: list[str],
) -> list[dict[str, float | None]]:
    """Format SHAP values into contribution dictionaries."""
    import numpy as np

    num_features = len(engineered_feature_cols)
    contributions: list[dict[str, float | None]] = [{c: None for c in engineered_feature_cols} for _ in range(num_rows)]

    if shap_values.size == 0:
        return contributions

    # Normalize absolute SHAP values to sum to 1.0 per row
    abs_shap = np.abs(shap_values)
    totals = abs_shap.sum(axis=1, keepdims=True)
    normalized = np.divide(abs_shap, totals, out=np.zeros_like(abs_shap), where=totals > 0)
    if num_features > 0:
        normalized[totals.squeeze(axis=1) == 0] = 1.0 / num_features

    # Build contribution dictionaries for valid rows
    valid_row_idx = 0
    for i in range(num_rows):
        if valid_indices[i]:
            contributions[i] = {
                engineered_feature_cols[j]: round(float(normalized[valid_row_idx, j]), 3) for j in range(num_features)
            }
            valid_row_idx += 1

    return contributions


def _compute_shap_values(
    model_local: Any,
    feature_matrix: pd.DataFrame,
    engineered_feature_cols: list[str],
    *,
    allow_missing: bool,
) -> tuple[np.ndarray, np.ndarray]:
    """Compute SHAP values for a model and feature matrix."""
    import pandas as pd
    import numpy as np

    try:
        import shap
    except ImportError as e:
        if allow_missing:
            return np.array([]), np.zeros(len(feature_matrix), dtype=bool)
        raise ImportError("SHAP library not available for driver-only scoring.") from e

    scaler = getattr(model_local, "named_steps", {}).get("scaler")
    tree_model = getattr(model_local, "named_steps", {}).get("model", model_local)

    shap_data = scaler.transform(feature_matrix) if scaler else feature_matrix.values
    valid_indices = ~pd.isna(shap_data).any(axis=1)

    shap_values = np.array([])
    if valid_indices.any():
        if len(engineered_feature_cols) == 1:
            shap_values = np.ones((len(shap_data[valid_indices]), 1))
        else:
            explainer = shap.TreeExplainer(tree_model)
            shap_values = explainer.shap_values(shap_data[valid_indices])

    return shap_values, valid_indices


def _score_with_sklearn_model_local(
    model_uri: str,
    df: DataFrame,
    feature_cols: list[str],
    feature_metadata_json: str,
    merge_columns: list[str],
    include_contributions: bool = False,
    *,
    model_record: AnomalyModelRecord,
) -> DataFrame:
    """Score DataFrame using scikit-learn model locally on the driver."""
    import pandas as pd

    sklearn_model = _load_and_validate_model(model_uri, model_record)
    column_infos, feature_metadata = _prepare_feature_metadata(feature_metadata_json)
    engineered_df = _apply_feature_engineering_for_scoring(
        df, feature_cols, merge_columns, column_infos, feature_metadata
    )

    engineered_feature_cols = feature_metadata.engineered_feature_names
    local_pdf = engineered_df.select(*merge_columns, *engineered_feature_cols).toPandas()

    feature_matrix = local_pdf[engineered_feature_cols]
    scores = -sklearn_model.score_samples(feature_matrix)

    result = {col_name: local_pdf[col_name] for col_name in merge_columns}
    result["anomaly_score"] = scores

    if include_contributions:
        shap_values, valid_indices = _compute_shap_values(
            sklearn_model,
            feature_matrix,
            engineered_feature_cols,
            allow_missing=False,
        )
        result["anomaly_contributions"] = _format_shap_contributions(
            shap_values, valid_indices, len(local_pdf), engineered_feature_cols
        )

    result_pdf = pd.DataFrame(result)
    result_schema = StructType(
        [
            *[df.schema[c] for c in merge_columns],
            StructField("anomaly_score", DoubleType(), True),
            *(
                [StructField("anomaly_contributions", MapType(StringType(), DoubleType()), True)]
                if include_contributions
                else []
            ),
        ]
    )
    scored_df = df.sparkSession.createDataFrame(result_pdf, schema=result_schema)
    return df.join(scored_df, on=merge_columns, how="left")


def _score_ensemble_models_local(
    model_uris: list[str],
    df_filtered: DataFrame,
    columns: list[str],
    feature_metadata_json: str,
    merge_columns: list[str],
    include_contributions: bool,
    *,
    model_record: AnomalyModelRecord,
) -> DataFrame:
    """Score ensemble models locally on the driver."""
    import numpy as np
    import pandas as pd

    models = [_load_and_validate_model(uri, model_record) for uri in model_uris]
    column_infos, feature_metadata = _prepare_feature_metadata(feature_metadata_json)
    engineered_df = _apply_feature_engineering_for_scoring(
        df_filtered, columns, merge_columns, column_infos, feature_metadata
    )
    engineered_feature_cols = feature_metadata.engineered_feature_names
    local_pdf = engineered_df.select(*merge_columns, *engineered_feature_cols).toPandas()

    feature_matrix = local_pdf[engineered_feature_cols]
    scores_matrix = np.array([-model.score_samples(feature_matrix) for model in models])

    result = {col_name: local_pdf[col_name] for col_name in merge_columns}
    result["anomaly_score"] = scores_matrix.mean(axis=0)
    result["anomaly_score_std"] = scores_matrix.std(axis=0, ddof=1)

    if include_contributions:
        shap_values, valid_indices = _compute_shap_values(
            models[0],
            feature_matrix,
            engineered_feature_cols,
            allow_missing=False,
        )
        result["anomaly_contributions"] = _format_shap_contributions(
            shap_values, valid_indices, len(local_pdf), engineered_feature_cols
        )

    result_pdf = pd.DataFrame(result)
    result_schema = StructType(
        [
            *[df_filtered.schema[c] for c in merge_columns],
            StructField("anomaly_score", DoubleType(), True),
            StructField("anomaly_score_std", DoubleType(), True),
            *(
                [StructField("anomaly_contributions", MapType(StringType(), DoubleType()), True)]
                if include_contributions
                else []
            ),
        ]
    )
    scored_df = df_filtered.sparkSession.createDataFrame(result_pdf, schema=result_schema)
    return df_filtered.join(scored_df, on=merge_columns, how="left")


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
        return _select_segment_record(all_segments)

    raise InvalidParameterError(
        f"Model '{model_name_local}' not found in '{registry}'. " "Train first using anomaly.train(...)."
    )


def _load_segment_models(
    registry_client: AnomalyModelRegistry,
    config: "ScoringConfig",
) -> list[AnomalyModelRecord]:
    all_segments = registry_client.get_all_segment_models(config.registry_table, config.model_name)
    if not all_segments:
        raise InvalidParameterError(
            f"No segment models found for base model '{config.model_name}'. "
            "Train segmented models first using anomaly.train(...)."
        )
    return all_segments


def _validate_fully_qualified_name(value: str, *, label: str) -> None:
    """Validate that a name is in catalog.schema.name format."""
    if value.count(".") != 2:
        raise InvalidParameterError(f"{label} must be fully qualified as catalog.schema.name, got: {value!r}.")


def _validate_has_no_anomalies_args(
    *,
    model: str,
    registry_table: str,
    threshold: float,
    row_filter: str | None,
    drift_threshold: float | None,
    include_contributions: bool,
    include_confidence: bool,
) -> None:
    """Validate has_no_anomalies arguments that do not require Spark."""
    _validate_required_name(
        model,
        label="model",
        example="has_no_anomalies(model='catalog.schema.my_model', ...)",
    )
    _validate_required_name(
        registry_table,
        label="registry_table",
        example="registry_table='catalog.schema.dqx_anomaly_models'",
    )
    _validate_fully_qualified_name(model, label="model")
    _validate_fully_qualified_name(registry_table, label="registry_table")
    _validate_threshold(threshold)
    _validate_optional_sql_expression(row_filter)
    _validate_optional_positive_float(drift_threshold, label="drift_threshold")
    _validate_boolean(include_contributions, label="include_contributions")
    _validate_boolean(include_confidence, label="include_confidence")


def _validate_required_name(value: str, *, label: str, example: str) -> None:
    if not value:
        raise InvalidParameterError(f"{label} parameter is required. Example: {example}.")


def _validate_threshold(threshold: float) -> None:
    if isinstance(threshold, bool) or not isinstance(threshold, (int, float)):
        raise InvalidParameterError("threshold must be a float between 0.0 and 1.0.")
    if not 0.0 <= float(threshold) <= 1.0:
        raise InvalidParameterError("threshold must be between 0.0 and 1.0.")


def _validate_optional_sql_expression(row_filter: str | None) -> None:
    if row_filter is not None and not isinstance(row_filter, str):
        raise InvalidParameterError("row_filter must be a SQL expression string when provided.")


def _validate_optional_positive_float(value: float | None, *, label: str) -> None:
    if value is None:
        return
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise InvalidParameterError(f"{label} must be a float when provided.")
    if value <= 0:
        raise InvalidParameterError(f"{label} must be greater than 0 when provided.")


def _validate_boolean(value: bool, *, label: str) -> None:
    if not isinstance(value, bool):
        raise InvalidParameterError(f"{label} must be a boolean.")


def _select_segment_record(all_segments: list[AnomalyModelRecord]) -> AnomalyModelRecord:
    """Select a deterministic segment record (latest training_time, tie-breaker by model_name)."""
    return max(
        all_segments,
        key=lambda record: (
            record.training.training_time or datetime.min,
            record.identity.model_name,
        ),
    )


def _discover_model_and_config(
    df: DataFrame,
    model: str,
    registry_table: str,
) -> tuple[list[str], list[str] | None, str, str]:
    """Auto-discover columns and segmentation from the model registry.

    Returns:
        Tuple of (columns, segment_by, model_name, registry)
    """
    registry_client = AnomalyModelRegistry(df.sparkSession)
    registry = registry_table

    if not model:
        raise InvalidParameterError(
            "model parameter is required. Example: has_no_anomalies(model='catalog.schema.my_model', ...)"
        )

    _validate_fully_qualified_name(registry, label="registry_table")
    _validate_fully_qualified_name(model, label="model")
    model_name = model
    record = _get_record_for_discovery(registry_client, registry, model_name)

    normalized_columns = list(record.training.columns)
    segment_by = record.segmentation.segment_by

    missing_columns = [col for col in normalized_columns if col not in df.columns]
    if missing_columns:
        raise InvalidParameterError(
            f"Input DataFrame is missing required columns for model '{model_name}': {missing_columns}. "
            f"Available columns: {df.columns}."
        )

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
        drift_df, drift_columns = _prepare_drift_df(df, columns, record)
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


def _prepare_drift_df(
    df: DataFrame,
    columns: list[str],
    record: "AnomalyModelRecord",
) -> tuple[DataFrame, list[str]]:
    """Prepare drift DataFrame and columns aligned to training baseline stats."""
    feature_metadata_json = record.features.feature_metadata
    if not feature_metadata_json:
        return df.select(*columns), columns

    column_infos, feature_metadata = _prepare_feature_metadata(feature_metadata_json)
    engineered_df = _apply_feature_engineering_for_scoring(
        df,
        columns,
        merge_columns=[],
        column_infos=column_infos,
        feature_metadata=feature_metadata,
    )
    return engineered_df, feature_metadata.engineered_feature_names


def _serialize_ensemble_models(
    model_uris: list[str],
    model_record: AnomalyModelRecord,
) -> list[bytes]:
    """Load and serialize ensemble models for UDF."""
    models_bytes = []
    for uri in model_uris:
        model = _load_and_validate_model(uri, model_record)
        models_bytes.append(cloudpickle.dumps(model))
    return models_bytes


def _prepare_ensemble_scoring_schema(include_contributions: bool) -> StructType:
    """Prepare schema for ensemble scoring UDF."""
    schema_fields = [
        StructField("anomaly_score", DoubleType(), True),
        StructField("anomaly_score_std", DoubleType(), True),
    ]
    if include_contributions:
        schema_fields.append(StructField("anomaly_contributions", MapType(StringType(), DoubleType()), True))
    return StructType(schema_fields)


def _join_ensemble_scores(
    df_filtered: DataFrame,
    scored_df: DataFrame,
    merge_columns: list[str],
    include_contributions: bool,
) -> DataFrame:
    """Join scores back to original DataFrame."""
    cols_to_select = [*merge_columns, "_scores.anomaly_score", "_scores.anomaly_score_std"]
    if include_contributions:
        cols_to_select.append("_scores.anomaly_contributions")

    return df_filtered.join(scored_df.select(*cols_to_select), on=merge_columns, how="left")


def _create_ensemble_scoring_udf(
    models_bytes: list[bytes],
    engineered_feature_cols: list[str],
    schema: StructType,
):
    """Create ensemble scoring UDF."""

    @pandas_udf(schema)  # type: ignore[call-overload]
    def ensemble_scoring_udf(*cols: pd.Series) -> pd.DataFrame:
        """Score with all ensemble models in single pass (Spark Connect compatible)."""
        import cloudpickle
        import pandas as pd
        import numpy as np

        # Deserialize all models and prepare input
        models = [cloudpickle.loads(mb) for mb in models_bytes]
        feature_matrix = pd.concat(cols, axis=1)
        feature_matrix.columns = engineered_feature_cols

        # Score with all ensemble models and compute stats
        scores_matrix = np.array([-model.score_samples(feature_matrix) for model in models])
        mean_scores = scores_matrix.mean(axis=0)
        std_scores = scores_matrix.std(axis=0, ddof=1)

        return pd.DataFrame({"anomaly_score": mean_scores, "anomaly_score_std": std_scores})

    return ensemble_scoring_udf


def _create_ensemble_scoring_udf_with_contributions(
    models_bytes: list[bytes],
    engineered_feature_cols: list[str],
    schema: StructType,
):
    """Create ensemble scoring UDF with SHAP contributions."""

    @pandas_udf(schema)  # type: ignore[call-overload]
    def ensemble_scoring_udf(*cols: pd.Series) -> pd.DataFrame:
        """Score with all ensemble models in single pass (Spark Connect compatible)."""
        import cloudpickle
        import pandas as pd
        import numpy as np

        # Deserialize all models and prepare input
        models = [cloudpickle.loads(mb) for mb in models_bytes]
        feature_matrix = pd.concat(cols, axis=1)
        feature_matrix.columns = engineered_feature_cols

        # Score with all ensemble models and compute stats
        scores_matrix = np.array([-model.score_samples(feature_matrix) for model in models])
        mean_scores = scores_matrix.mean(axis=0)
        std_scores = scores_matrix.std(axis=0, ddof=1)

        # Build result
        result = {"anomaly_score": mean_scores, "anomaly_score_std": std_scores}

        # Compute SHAP contributions using first model
        model_local = models[0]
        shap_values, valid_indices = _compute_shap_values(
            model_local,
            feature_matrix,
            engineered_feature_cols,
            allow_missing=True,
        )
        result["anomaly_contributions"] = _format_shap_contributions(
            shap_values, valid_indices, len(feature_matrix), engineered_feature_cols
        )

        return pd.DataFrame(result)

    return ensemble_scoring_udf


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
    # Load and serialize models
    models_bytes = _serialize_ensemble_models(model_uris, model_record)

    # Prepare feature metadata and apply engineering
    column_infos, feature_metadata = _prepare_feature_metadata(feature_metadata_json)
    engineered_df = _apply_feature_engineering_for_scoring(
        df_filtered, columns, merge_columns, column_infos, feature_metadata
    )
    engineered_feature_cols = feature_metadata.engineered_feature_names

    # Create ensemble UDF
    schema = _prepare_ensemble_scoring_schema(include_contributions)
    if include_contributions:
        ensemble_scoring_udf = _create_ensemble_scoring_udf_with_contributions(
            models_bytes, engineered_feature_cols, schema
        )
    else:
        ensemble_scoring_udf = _create_ensemble_scoring_udf(models_bytes, engineered_feature_cols, schema)

    # Apply UDF and join results
    input_cols = [col(c) for c in engineered_feature_cols]
    scored_df = engineered_df.withColumn("_scores", ensemble_scoring_udf(*input_cols))

    return _join_ensemble_scores(df_filtered, scored_df, merge_columns, include_contributions)


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
    first_segment = _select_segment_record(all_segments)
    assert first_segment.segmentation.segment_by is not None, "Segment model must have segment_by"
    strategy = _resolve_scoring_strategy(first_segment.identity.algorithm)
    return strategy.score_segmented(df, config, registry_client, all_segments)


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

    if config.driver_only:
        scored_df = (
            _score_ensemble_models_local(
                model_uris,
                df_filtered,
                config.columns,
                record.features.feature_metadata,
                config.merge_columns,
                config.include_contributions,
                model_record=record,  # Pass model record for version validation
            )
            if len(model_uris) > 1
            else _score_with_sklearn_model_local(
                record.identity.model_uri,
                df_filtered,
                config.columns,
                record.features.feature_metadata,
                config.merge_columns,
                include_contributions=config.include_contributions,
                model_record=record,  # Pass model record for version validation
            ).withColumn("anomaly_score_std", F.lit(0.0))
        )
    else:
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

    scored_df = scored_df.withColumnRenamed("anomaly_score", config.score_col)
    scored_df = scored_df.withColumnRenamed("anomaly_score_std", config.score_std_col)
    if config.include_contributions and "anomaly_contributions" in scored_df.columns:
        scored_df = scored_df.withColumnRenamed("anomaly_contributions", config.contributions_col)

    # Add _dq_info column (before dropping internal columns)
    scored_df = add_info_column(
        scored_df,
        config.model_name,
        config.threshold,
        segment_values=None,  # Global model has no segments
        include_contributions=config.include_contributions,
        include_confidence=config.include_confidence,
        score_col=config.score_col,
        score_std_col=config.score_std_col,
        contributions_col=config.contributions_col,
    )

    # Post-process: drop internal columns that are now in _dq_info
    scored_df = scored_df.drop(config.score_std_col)  # Always drop (null if not ensemble)
    if config.include_contributions:
        scored_df = scored_df.drop(config.contributions_col)  # Drop top-level, use _dq_info instead

    if config.row_filter:
        scored_df = _join_filtered_results_back(df, scored_df, config.merge_columns, config.score_col)

    # Drop internal anomaly_score column (use _dq_info.anomaly.score instead)
    scored_df = scored_df.drop(config.score_col)

    return scored_df
