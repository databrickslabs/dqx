"""
Training pipeline for anomaly detection using scikit-learn IsolationForest.

Architecture:
- Training: scikit-learn on driver (efficient for sampled data ≤1M rows)
- Scoring: Distributed across Spark cluster via pandas UDFs
- Everything else: Distributed on Spark (sampling, splits, metrics, drift detection)

Note: All imports are at module-level since DQX is installed as a wheel on all cluster nodes.
"""

import collections.abc
import logging
import os
from abc import ABC, abstractmethod
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import cloudpickle
import mlflow
import pandas as pd
import pyspark.sql.functions as F
import sklearn
from mlflow.models import infer_signature
from mlflow.tracking import MlflowClient
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StructField,
    StructType,
)
from sklearn.ensemble import IsolationForest
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import RobustScaler

from databricks.labs.dqx.anomaly.model_registry import (
    AnomalyModelRecord,
    AnomalyModelRegistry,
    FeatureEngineering,
    ModelIdentity,
    SegmentationConfig,
    TrainingMetadata,
    compute_config_hash,
)
from databricks.labs.dqx.anomaly.profiler import auto_discover_columns
from databricks.labs.dqx.anomaly.transformers import (
    ColumnTypeClassifier,
    SparkFeatureMetadata,
    apply_feature_engineering,
    reconstruct_column_infos,
)
from databricks.labs.dqx.config import AnomalyParams, IsolationForestConfig
from databricks.labs.dqx.errors import InvalidParameterError

logger = logging.getLogger(__name__)

DEFAULT_SAMPLE_FRACTION = 0.3
DEFAULT_MAX_ROWS = 1_000_000
DEFAULT_TRAIN_RATIO = 0.8
SCORE_QUANTILE_PROBS = [0.0, 0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 1.0]
SCORE_QUANTILE_KEYS = ["p00", "p01", "p05", "p10", "p25", "p50", "p75", "p90", "p95", "p99", "p100"]


@dataclass(frozen=True)
class TrainingResult:
    model_uri: str
    run_id: str | None
    hyperparams: dict[str, Any]
    validation_metrics: dict[str, float]
    score_quantiles: dict[str, float]
    feature_metadata: SparkFeatureMetadata
    ensemble_size: int
    algorithm: str


class AnomalyTrainingStrategy(ABC):
    """Training strategy interface for anomaly models."""

    name: str

    @abstractmethod
    def train(
        self,
        train_df: DataFrame,
        val_df: DataFrame,
        columns: list[str],
        params: AnomalyParams,
        model_name: str,
        *,
        allow_ensemble: bool,
    ) -> TrainingResult:
        pass


class IsolationForestTrainingStrategy(AnomalyTrainingStrategy):
    """IsolationForest training strategy (default)."""

    name = "isolation_forest"

    def train(
        self,
        train_df: DataFrame,
        val_df: DataFrame,
        columns: list[str],
        params: AnomalyParams,
        model_name: str,
        *,
        allow_ensemble: bool,
    ) -> TrainingResult:
        ensemble_size = params.ensemble_size if allow_ensemble and params.ensemble_size else 1

        if ensemble_size > 1:
            model_uris, hyperparams, validation_metrics, score_quantiles, feature_metadata = _train_ensemble(
                train_df, val_df, columns, params, ensemble_size, model_name
            )
            model_uri = ",".join(model_uris)
            run_id = "ensemble"
        else:
            model, hyperparams, feature_metadata = _fit_isolation_forest(train_df, columns, params)
            validation_metrics = _compute_validation_metrics(model, val_df, columns, feature_metadata)
            score_quantiles = _compute_score_quantiles(model, train_df, columns, feature_metadata)
            model_uri, run_id = _register_single_model_to_mlflow(
                model, train_df, feature_metadata, model_name, hyperparams, validation_metrics
            )

        algorithm = f"IsolationForest_Ensemble_{ensemble_size}" if ensemble_size > 1 else "IsolationForest"
        return TrainingResult(
            model_uri=model_uri,
            run_id=run_id,
            hyperparams=hyperparams,
            validation_metrics=validation_metrics,
            score_quantiles=score_quantiles,
            feature_metadata=feature_metadata,
            ensemble_size=ensemble_size,
            algorithm=algorithm,
        )


@dataclass(frozen=True)
class AnomalyTrainingContext:
    spark: SparkSession
    df: DataFrame
    df_filtered: DataFrame
    model_name: str
    registry_table: str
    columns: list[str]
    segment_by: list[str] | None
    params: AnomalyParams
    expected_anomaly_rate: float
    exclude_columns: list[str] | None
    auto_discovery_used: bool


@dataclass(frozen=True)
class TrainingArtifacts:
    model_name: str
    model_uri: str
    run_id: str | None
    ensemble_size: int
    feature_metadata: SparkFeatureMetadata
    hyperparams: dict[str, Any]
    training_rows: int
    validation_metrics: dict[str, float]
    score_quantiles: dict[str, float]
    baseline_stats: dict[str, dict[str, float]]
    algorithm: str
    segment_values: dict[str, Any] | None = None


class AnomalyTrainingService:
    """Service for building training context and orchestrating model training.

    Extension point:
        If we add new algorithms/models in the future, introduce a TrainingStrategy
        interface and make this service delegate to it based on params/metadata.
    """

    def __init__(self, spark: SparkSession, strategy: AnomalyTrainingStrategy | None = None) -> None:
        self._spark = spark
        self._strategy = strategy or IsolationForestTrainingStrategy()

    def build_context(
        self,
        df: DataFrame,
        model_name: str,
        registry_table: str,
        *,
        columns: list[str] | None,
        segment_by: list[str] | None,
        params: AnomalyParams | None,
        exclude_columns: list[str] | None,
        expected_anomaly_rate: float,
    ) -> AnomalyTrainingContext:
        _validate_spark_version(self._spark)

        if not model_name:
            raise InvalidParameterError("model_name is required and must be fully qualified as 'catalog.schema.model'.")
        if not registry_table:
            raise InvalidParameterError(
                "registry_table is required and must be fully qualified as 'catalog.schema.table'."
            )

        exclude_list = exclude_columns or []
        if columns is not None and exclude_list:
            columns = [col for col in columns if col not in exclude_list]

        df_filtered = _process_exclude_columns(df, columns, exclude_columns)
        auto_discovery_used = columns is None

        if columns is None:
            columns, segment_by = _perform_auto_discovery(df_filtered, segment_by)

        if not columns:
            raise InvalidParameterError("No columns provided or auto-discovered. Provide columns explicitly.")

        params = AnomalyParams() if params is None else params
        validation_warnings = _validate_columns(df, columns, params)
        for warning in validation_warnings:
            logger.warning(warning)

        self._prepare_training_config(
            model_name=model_name,
            registry_table=registry_table,
            columns=columns,
            segment_by=segment_by,
        )

        params = apply_expected_anomaly_rate_if_default_contamination(params, expected_anomaly_rate)

        return AnomalyTrainingContext(
            spark=self._spark,
            df=df,
            df_filtered=df_filtered,
            model_name=model_name,
            registry_table=registry_table,
            columns=columns,
            segment_by=segment_by,
            params=params,
            expected_anomaly_rate=expected_anomaly_rate,
            exclude_columns=exclude_columns,
            auto_discovery_used=auto_discovery_used,
        )

    def train(self, context: AnomalyTrainingContext) -> str:
        if context.segment_by:
            return self._train_segmented(context)
        return self._train_global(context)

    def _prepare_training_config(
        self,
        *,
        model_name: str,
        registry_table: str,
        columns: list[str],
        segment_by: list[str] | None,
    ) -> None:
        _validate_fully_qualified_name(model_name, label="model_name")
        _validate_fully_qualified_name(registry_table, label="registry_table")

        if _model_exists_in_uc(model_name):
            logger.warning(
                f"Model '{model_name}' already exists. Creating a new version. Previous versions remain available."
            )

        registry = AnomalyModelRegistry(self._spark)
        existing = registry.get_active_model(registry_table, model_name)

        if existing:
            config_changed = (
                set(columns) != set(existing.training.columns) or segment_by != existing.segmentation.segment_by
            )

            if config_changed:
                logger.warning(
                    f"⚠️  Model '{model_name}' exists with different configuration:\n"
                    f"   Existing: columns={existing.training.columns}, segment_by={existing.segmentation.segment_by}\n"
                    f"   New: columns={columns}, segment_by={segment_by}\n"
                    f"   The old model will be archived. Consider using a different model_name "
                    f"if this is a different use case."
                )

    def _train_global(self, context: AnomalyTrainingContext) -> str:
        sampled_df, _, truncated = _sample_df(context.df_filtered, context.columns, context.params)
        if not sampled_df.head(1):
            raise InvalidParameterError(
                "Sampling produced 0 rows. Provide more data or adjust sampling parameters "
                "(sample_fraction/max_rows)."
            )

        train_df, val_df = _train_validation_split(sampled_df, context.params)

        result = self._strategy.train(
            train_df,
            val_df,
            context.columns,
            context.params,
            context.model_name,
            allow_ensemble=True,
        )

        baseline_stats = _compute_post_training_metadata(train_df, result.feature_metadata)

        if truncated:
            logger.warning(f"Sampling capped at {context.params.max_rows} rows; model trained on truncated sample.")

        artifacts = TrainingArtifacts(
            model_name=context.model_name,
            model_uri=result.model_uri,
            run_id=result.run_id,
            ensemble_size=result.ensemble_size,
            feature_metadata=result.feature_metadata,
            hyperparams=result.hyperparams,
            training_rows=train_df.count(),
            validation_metrics=result.validation_metrics,
            score_quantiles=result.score_quantiles,
            baseline_stats=baseline_stats,
            algorithm=result.algorithm,
        )
        self._save_training_record(context, artifacts, segment_by=None)

        logger.info(f"   Model trained: {context.model_name}")
        logger.info(f"   Model URI: {result.model_uri}")
        logger.info(f"   Registry: {context.registry_table}")

        return context.model_name

    def _train_segmented(self, context: AnomalyTrainingContext) -> str:
        if context.segment_by is None:
            raise InvalidParameterError(
                "segment_by must be provided for segmented training. Provide segment_by or use global training."
            )

        segments = _get_and_validate_segments(context.df_filtered, context.segment_by)

        model_uris: list[str] = []
        skipped_segments: list[str] = []
        failed_segments: list[tuple[str, str]] = []

        for i, segment_vals in enumerate(segments):
            segment_name = "_".join(f"{k}={v}" for k, v in segment_vals.items())
            logger.info(f"Training segment {i+1}/{len(segments)}: {segment_name}")

            try:
                model_uri = self._train_one_segment_with_validation(context, segment_vals)

                if model_uri is not None:
                    model_uris.append(model_uri)
                else:
                    skipped_segments.append(segment_name)

            except Exception as exc:
                error_msg = f"Segment {segment_name} training failed: {type(exc).__name__}: {exc}"
                logger.warning(error_msg)
                failed_segments.append((segment_name, str(exc)))

        _report_training_summary(
            model_uris,
            skipped_segments,
            failed_segments,
            len(segments),
            context.model_name,
            context.registry_table,
            context.params,
        )

        return context.model_name

    def _train_one_segment_with_validation(
        self, context: AnomalyTrainingContext, segment_vals: dict[str, Any]
    ) -> str | None:
        segment_name = "_".join(f"{k}={v}" for k, v in segment_vals.items())

        segment_df = context.df_filtered
        for col_name, val in segment_vals.items():
            segment_df = segment_df.filter(F.col(col_name) == val)

        segment_size = segment_df.count()
        if segment_size < 1000:
            logger.warning(f"Segment {segment_name} has only {segment_size} rows, model may be unreliable.")

        segment_model_name = f"{context.model_name}__seg_{segment_name}"

        return self._train_single_segment(context, segment_df, segment_vals, segment_model_name)

    def _train_single_segment(
        self,
        context: AnomalyTrainingContext,
        segment_df: DataFrame,
        segment_values: dict[str, Any],
        model_name: str,
    ) -> str | None:
        sampled_df, sampled_count, _ = _sample_df(segment_df, context.columns, context.params)
        if sampled_count == 0:
            logger.info(f"Segment {segment_values} has 0 rows after sampling. Skipping model training.")
            return None

        train_df, val_df = _train_validation_split(sampled_df, context.params)

        result = self._strategy.train(
            train_df,
            val_df,
            context.columns,
            context.params,
            model_name,
            allow_ensemble=False,
        )

        baseline_stats = _compute_post_training_metadata(train_df, result.feature_metadata)

        artifacts = TrainingArtifacts(
            model_name=model_name,
            model_uri=result.model_uri,
            run_id=result.run_id,
            ensemble_size=result.ensemble_size,
            feature_metadata=result.feature_metadata,
            hyperparams=result.hyperparams,
            training_rows=train_df.count(),
            validation_metrics=result.validation_metrics,
            score_quantiles=result.score_quantiles,
            baseline_stats=baseline_stats,
            algorithm=result.algorithm,
            segment_values=segment_values,
        )
        self._save_training_record(context, artifacts, segment_by=context.segment_by)

        return result.model_uri

    def _save_training_record(
        self,
        context: AnomalyTrainingContext,
        artifacts: TrainingArtifacts,
        *,
        segment_by: list[str] | None,
    ) -> None:
        record = AnomalyModelRecord(
            identity=ModelIdentity(
                model_name=artifacts.model_name,
                model_uri=artifacts.model_uri,
                algorithm=artifacts.algorithm,
                mlflow_run_id=artifacts.run_id or "unknown",
                status="active",
            ),
            training=TrainingMetadata(
                columns=context.columns,
                hyperparameters=_stringify_dict(artifacts.hyperparams),
                training_rows=artifacts.training_rows,
                training_time=datetime.utcnow(),
                metrics=artifacts.validation_metrics,
                score_quantiles=artifacts.score_quantiles,
                baseline_stats=artifacts.baseline_stats,
            ),
            features=FeatureEngineering(
                mode="spark",
                column_types=None,
                feature_metadata=artifacts.feature_metadata.to_json(),
                feature_importance=None,
                temporal_config=None,
            ),
            segmentation=SegmentationConfig(
                segment_by=segment_by,
                segment_values=_stringify_dict(artifacts.segment_values) if artifacts.segment_values else None,
                is_global_model=segment_by is None,
                sklearn_version=sklearn.__version__,
                config_hash=compute_config_hash(context.columns, segment_by),
            ),
        )
        registry = AnomalyModelRegistry(context.spark)
        registry.save_model(record, context.registry_table)


def _process_exclude_columns(
    df: DataFrame,
    columns: list[str] | None,
    exclude_columns: list[str] | None,
) -> DataFrame:
    """
    Process exclude_columns parameter and return filtered DataFrame.

    Returns:
        Filtered DataFrame
    """
    exclude_list = exclude_columns or []

    if not exclude_list:
        return df

    # Validate that exclude_columns exist in DataFrame
    df_columns = set(df.columns)
    invalid_excludes = [col for col in exclude_list if col not in df_columns]
    if invalid_excludes:
        raise InvalidParameterError(f"exclude_columns contains columns not in DataFrame: {invalid_excludes}")

    # Filter DataFrame for auto-discovery (exclude unwanted columns)
    if columns is None:
        remaining_columns = [col for col in df.columns if col not in exclude_list]
        df_filtered = df.select(*remaining_columns)
        logger.info(f"Excluding {len(exclude_list)} columns from auto-discovery: {exclude_list}")
        return df_filtered

    # No need to filter if columns explicitly provided
    return df


def _perform_auto_discovery(
    df_filtered: DataFrame,
    segment_by: list[str] | None,
) -> tuple[list[str], list[str] | None]:
    """
    Perform auto-discovery of columns and segments.

    Returns:
        Tuple of (columns, segment_by)
    """
    # Auto-discover columns
    profile = auto_discover_columns(df_filtered)
    discovered_columns = profile.recommended_columns
    discovered_segments = segment_by

    # Auto-detect segments ONLY if segment_by also not provided
    if segment_by is None:
        discovered_segments = profile.recommended_segments

    # Print what was discovered
    logger.info(f"Auto-selected {len(discovered_columns)} columns: {discovered_columns}")
    if discovered_segments:
        logger.info(
            f"Auto-detected {len(discovered_segments)} segment columns: {discovered_segments} "
            f"({profile.segment_count} total segments)"
        )

    for warning in profile.warnings:
        logger.warning(warning)

    return discovered_columns, discovered_segments


def apply_expected_anomaly_rate_if_default_contamination(
    params: AnomalyParams | None, expected_anomaly_rate: float
) -> AnomalyParams:
    """
    Apply expected_anomaly_rate to params if contamination is not explicitly set.

    Args:
        params: Existing AnomalyParams or None
        expected_anomaly_rate: Expected fraction of anomalies (e.g., 0.02 for 2%)

    Returns:
        AnomalyParams with contamination set appropriately
    """
    # Create default params if None
    if params is None:
        params = AnomalyParams()

    # Deep copy to avoid mutating caller's params
    params = deepcopy(params)

    # Only apply expected_anomaly_rate if contamination is unset
    # This allows explicit contamination settings to take precedence
    if params.algorithm_config.contamination is None:
        params.algorithm_config.contamination = expected_anomaly_rate
        logger.info(f"Using expected_anomaly_rate={expected_anomaly_rate:.2%} for model training")
    else:
        logger.info(
            f"Using explicitly set contamination={params.algorithm_config.contamination:.2%} "
            f"(expected_anomaly_rate={expected_anomaly_rate:.2%} ignored)"
        )

    return params


def _validate_fully_qualified_name(value: str, *, label: str) -> None:
    """Validate that a name is in catalog.schema.name format."""
    if value.count(".") != 2:
        raise InvalidParameterError(f"{label} must be fully qualified as catalog.schema.name, got: {value!r}.")


def validate_fully_qualified_name(value: str, *, label: str) -> None:
    """Public validator for fully qualified names."""
    _validate_fully_qualified_name(value, label=label)


def _register_single_model_to_mlflow(
    model: Any,
    train_df: DataFrame,
    feature_metadata: Any,
    model_name: str,
    hyperparams: dict[str, Any],
    validation_metrics: dict[str, float],
) -> tuple[str, str]:
    """
    Register a single sklearn model to MLflow/Unity Catalog.

    Returns:
        Tuple of (model_uri, run_id)
    """
    # Note: When running outside Databricks (e.g., local Spark Connect), you may see a warning:
    # "Unable to get model version source run's workspace ID from request headers"
    # This is expected and informational only - the model will register successfully
    _ensure_mlflow_registry_uri()

    with mlflow.start_run() as run:
        # Infer model signature for Unity Catalog (required)
        # Get engineered features for signature
        column_infos_reconstructed = reconstruct_column_infos(feature_metadata)

        engineered_train_df, _ = apply_feature_engineering(
            train_df,
            column_infos_reconstructed,
            categorical_cardinality_threshold=20,
            frequency_maps=feature_metadata.categorical_frequency_maps,
            onehot_categories=feature_metadata.onehot_categories,
        )
        train_pandas = engineered_train_df.toPandas()
        predictions = model.predict(train_pandas)
        signature = infer_signature(train_pandas, predictions)

        # Log scikit-learn model with signature
        model_info = mlflow.sklearn.log_model(
            sk_model=model,
            name="model",
            registered_model_name=model_name,
            signature=signature,
        )

        # Note: feature_metadata is saved in the model registry table, not as MLflow artifact
        mlflow.log_params(_flatten_hyperparams(hyperparams))
        mlflow.log_metrics(validation_metrics)

        # Use explicit version-based URI format
        # model_name already has full catalog.schema.model format from train() setup
        model_uri = f"models:/{model_name}/{model_info.registered_model_version}"
        return model_uri, run.info.run_id


def _compute_post_training_metadata(
    train_df: DataFrame,
    feature_metadata: Any,
) -> dict[str, dict[str, float]]:
    """
    Compute baseline statistics after training.

    Extracted helper to reduce local variables in training functions.

    Args:
        train_df: Training DataFrame with original columns
        feature_metadata: Feature engineering metadata from training

    Returns:
        Baseline statistics dictionary
    """
    # Apply feature engineering to compute baseline stats on the same features the model was trained on
    column_infos_for_stats = reconstruct_column_infos(feature_metadata)
    engineered_train_df, _ = apply_feature_engineering(
        train_df,
        column_infos_for_stats,
        categorical_cardinality_threshold=20,
        frequency_maps=feature_metadata.categorical_frequency_maps,
        onehot_categories=feature_metadata.onehot_categories,
    )

    # Compute baseline statistics for drift detection on engineered features
    baseline_stats = _compute_baseline_statistics(engineered_train_df, feature_metadata.engineered_feature_names)

    return baseline_stats


def _get_and_validate_segments(
    df: DataFrame,
    segment_by: list[str],
) -> list[dict[str, Any]]:
    """Get distinct segments and validate count.

    Args:
        df: Input DataFrame
        segment_by: Columns to segment by

    Returns:
        List of segment value dictionaries

    Raises:
        Warning if segment count is too high
    """
    segments_df = df.select(*segment_by).distinct()
    segments = [row.asDict() for row in segments_df.collect()]

    if len(segments) > 100:
        logger.warning(
            f"Training {len(segments)} segments may be slow. Consider coarser segmentation or explicit segment_by."
        )

    return segments


def _report_training_summary(
    model_uris: list[str],
    skipped_segments: list[str],
    failed_segments: list[tuple[str, str]],
    total_segments: int,
    base_model_name: str,
    registry_table: str,
    params: AnomalyParams,
) -> None:
    """Report training summary including skipped and failed segments.

    Args:
        model_uris: List of successfully trained model URIs
        skipped_segments: List of segment names that were skipped
        failed_segments: List of (segment_name, error_message) tuples
        total_segments: Total number of segments attempted
        base_model_name: Base model name
        registry_table: Registry table name
        params: Training parameters

    Raises:
        InvalidParameterError if no models were successfully trained
    """
    # Log skipped segments
    if skipped_segments:
        logger.info(
            f"Skipped {len(skipped_segments)}/{total_segments} segments due to insufficient data after sampling: "
            f"{', '.join(skipped_segments[:5])}"
            + (f" and {len(skipped_segments) - 5} more" if len(skipped_segments) > 5 else "")
        )

    # Log failed segments
    if failed_segments:
        logger.warning(f"\nWARNING: {len(failed_segments)}/{total_segments} segments failed during training:")
        for seg_name, error in failed_segments[:3]:
            logger.warning(f"  - {seg_name}: {error}")
        if len(failed_segments) > 3:
            logger.warning(f"  ... and {len(failed_segments) - 3} more")

    # Validate that at least one segment was successfully trained
    if not model_uris:
        raise InvalidParameterError(
            f"All {total_segments} segments failed ({len(skipped_segments)} skipped, "
            f"{len(failed_segments)} errors). Cannot train any models. "
            f"Consider increasing sample_fraction (current: {params.sample_fraction}) or checking segment definitions."
        )

    # Print success summary
    trained_count = len(model_uris)
    logger.info(f"   Trained {trained_count}/{total_segments} segment models for: {base_model_name}")
    logger.info(f"   Registry: {registry_table}")


def _validate_spark_version(spark: SparkSession) -> None:
    major, minor, *_ = spark.version.split(".")
    if int(major) < 3 or (int(major) == 3 and int(minor) < 4):
        raise InvalidParameterError(
            f"Anomaly detection requires Spark >= 3.4 for SynapseML compatibility. Found Spark {major}.{minor}."
        )


def _validate_columns(
    df: DataFrame, columns: collections.abc.Iterable[str], params: AnomalyParams | None = None
) -> list[str]:
    """
    Validate columns for anomaly detection with multi-type support.

    Returns:
        List of warnings to display to user.
    """
    params = params or AnomalyParams()
    fe_config = params.feature_engineering

    # Use ColumnTypeClassifier to analyze and validate
    classifier = ColumnTypeClassifier(
        categorical_cardinality_threshold=fe_config.categorical_cardinality_threshold,
        max_input_columns=fe_config.max_input_columns,
        max_engineered_features=fe_config.max_engineered_features,
    )

    # This will raise InvalidParameterError if limits are exceeded
    _column_infos, warnings_list = classifier.analyze_columns(df, list(columns))

    return warnings_list


def _model_exists_in_uc(model_name: str) -> bool:
    """
    Check if a model exists in Unity Catalog using MLflow API.

    Args:
        model_name: Full model name (catalog.schema.model)

    Returns:
        True if model exists, False otherwise
    """
    try:
        client = MlflowClient()
        # Use get_registered_model instead of deprecated get_latest_versions
        # This is Unity Catalog compatible and doesn't rely on deprecated stages
        client.get_registered_model(model_name)
        return True
    except Exception:
        # Model doesn't exist or MLflow API error
        return False


def _sample_df(df: DataFrame, columns: list[str], params: AnomalyParams) -> tuple[DataFrame, int, bool]:
    fraction = params.sample_fraction if params.sample_fraction is not None else DEFAULT_SAMPLE_FRACTION
    missing_cols = [col for col in columns if col not in df.columns]
    if missing_cols:
        raise InvalidParameterError(f"Columns not found in DataFrame: {missing_cols}")
    sampled = df.select(*columns).sample(withReplacement=False, fraction=fraction, seed=42)
    if params.max_rows:
        sampled = sampled.limit(params.max_rows)
    count = sampled.count()
    truncated = params.max_rows is not None and count == params.max_rows
    return sampled, count, truncated


def _train_validation_split(df: DataFrame, params: AnomalyParams) -> tuple[DataFrame, DataFrame]:
    train_ratio = params.train_ratio if params.train_ratio is not None else DEFAULT_TRAIN_RATIO
    train_df, val_df = df.randomSplit([train_ratio, 1 - train_ratio], seed=42)
    return train_df, val_df


def _prepare_training_features(
    train_df: DataFrame, feature_columns: list[str], params: AnomalyParams
) -> tuple[Any, Any]:
    """
    Prepare training features using Spark-based feature engineering.

    Analyzes column types, applies transformations (one-hot encoding, frequency maps, etc.),
    and collects the result to the driver as a pandas DataFrame.

    Returns:
        - train_pandas: pandas DataFrame with engineered numeric features
        - feature_metadata: Transformation metadata for distributed scoring
    """
    fe_config = params.feature_engineering

    classifier = ColumnTypeClassifier(
        categorical_cardinality_threshold=fe_config.categorical_cardinality_threshold,
        max_input_columns=fe_config.max_input_columns,
        max_engineered_features=fe_config.max_engineered_features,
    )

    feature_df = train_df.select(*feature_columns)
    column_infos, _ = classifier.analyze_columns(feature_df, feature_columns)

    engineered_df, feature_metadata = apply_feature_engineering(
        feature_df,
        column_infos,
        categorical_cardinality_threshold=fe_config.categorical_cardinality_threshold,
        frequency_maps=None,
    )

    train_pandas = engineered_df.toPandas()

    return train_pandas, feature_metadata


def _fit_sklearn_model(train_pandas: Any, params: AnomalyParams) -> tuple[Any, dict[str, Any]]:
    """
    Train sklearn IsolationForest pipeline on pre-engineered pandas DataFrame.

    Returns:
        - pipeline: sklearn Pipeline (RobustScaler + IsolationForest)
        - hyperparams: Model configuration for MLflow tracking
    """
    algo_cfg = params.algorithm_config or IsolationForestConfig()

    iso_forest = IsolationForest(
        contamination=algo_cfg.contamination,
        n_estimators=algo_cfg.num_trees,
        max_samples=algo_cfg.subsampling_rate if algo_cfg.subsampling_rate else 'auto',
        max_features=1.0,
        bootstrap=False,
        random_state=algo_cfg.random_seed,
        n_jobs=-1,
    )

    # RobustScaler uses median/IQR, which is resilient to outliers in training data
    pipeline = Pipeline([('scaler', RobustScaler()), ('model', iso_forest)])
    pipeline.fit(train_pandas)

    hyperparams: dict[str, Any] = {
        "contamination": algo_cfg.contamination,
        "num_trees": algo_cfg.num_trees,
        "max_samples": algo_cfg.subsampling_rate,
        "random_seed": algo_cfg.random_seed,
        "feature_scaling": "RobustScaler",
    }

    return pipeline, hyperparams


def _fit_isolation_forest(
    train_df: DataFrame, feature_columns: list[str], params: AnomalyParams
) -> tuple[Any, dict[str, Any], Any]:
    """
    Train IsolationForest model with distributed feature engineering and driver-based training.

    Feature engineering runs on Spark, then the model trains on the driver.

    Returns:
        - pipeline: sklearn Pipeline (RobustScaler + IsolationForest)
        - hyperparams: Model configuration for MLflow tracking
        - feature_metadata: Transformation metadata for distributed scoring
    """
    train_pandas, feature_metadata = _prepare_training_features(train_df, feature_columns, params)

    # Train sklearn model
    pipeline, hyperparams = _fit_sklearn_model(train_pandas, params)

    return pipeline, hyperparams, feature_metadata


def _score_with_model(model: Any, df: DataFrame, feature_cols: list[str], feature_metadata: Any) -> DataFrame:
    """
    Score DataFrame using scikit-learn model with distributed pandas UDF.

    Feature engineering is applied in Spark before the pandas UDF.
    The pandas UDF only handles standard sklearn components (RobustScaler + IsolationForest).

    This enables distributed inference across the Spark cluster.
    Works with both regular Spark and Spark Connect.

    Args:
        model: Trained sklearn Pipeline (RobustScaler + IsolationForest, NO custom transformers)
        df: Input DataFrame with original columns
        feature_cols: Original column names (before engineering)
        feature_metadata: SparkFeatureMetadata with transformation info

    Returns:
        DataFrame with anomaly_score and prediction columns
    """
    # Reconstruct column_infos from metadata
    column_infos = reconstruct_column_infos(feature_metadata)

    # Apply feature engineering in Spark (distributed)
    # Use pre-computed frequency maps from training
    engineered_df, updated_metadata = apply_feature_engineering(
        df.select(*feature_cols),
        column_infos,
        categorical_cardinality_threshold=feature_metadata.categorical_cardinality_threshold,
        frequency_maps=feature_metadata.categorical_frequency_maps,
        onehot_categories=feature_metadata.onehot_categories,
    )

    # Get engineered feature names from the updated metadata (not the passed-in metadata)
    # This ensures we only use features that actually exist in the engineered DataFrame
    engineered_feature_cols = updated_metadata.engineered_feature_names

    # Serialize model (will be captured in UDF closure)
    # Model contains only standard sklearn components (no custom classes)
    model_bytes = cloudpickle.dumps(model)

    # Define schema for UDF output (nullable=True to match pandas behavior)
    schema = StructType(
        [
            StructField("anomaly_score", DoubleType(), True),
            StructField("prediction", IntegerType(), True),
        ]
    )

    @pandas_udf(schema)  # type: ignore[call-overload]  # StructType is valid but mypy has incomplete stubs
    def predict_udf(*cols: pd.Series) -> pd.DataFrame:
        """Pandas UDF for distributed scoring (Spark Connect compatible).

        Note: All imports are local for Spark Connect compatibility when DQX
        is NOT installed as a wheel on all cluster nodes.
        """
        import cloudpickle
        import numpy as np
        import pandas as pd

        # Deserialize model (only standard sklearn components)
        model_local = cloudpickle.loads(model_bytes)

        # Convert input columns to DataFrame
        features_df = pd.concat(cols, axis=1)
        features_df.columns = engineered_feature_cols

        # Pipeline handles scaling and prediction (no custom transformers)
        predictions = model_local.predict(features_df)
        scores = -model_local.score_samples(features_df)  # Negate to make higher = more anomalous

        # Convert sklearn labels -1/1 to 0/1
        predictions = np.where(predictions == -1, 1, 0)

        return pd.DataFrame({"anomaly_score": scores, "prediction": predictions})

    # Apply UDF to all engineered feature columns and add scores to DataFrame
    result = engineered_df.withColumn("_scores", predict_udf(*[col(c) for c in engineered_feature_cols]))
    result = result.select("*", "_scores.anomaly_score", "_scores.prediction").drop("_scores")

    return result


def _score_with_ensemble_models(
    models: list[Any], df: DataFrame, feature_cols: list[str], feature_metadata: Any
) -> DataFrame:
    """
    Score DataFrame using an ensemble of models (distributed) and return mean scores.

    This mirrors the ensemble scoring behavior used at inference time.
    """
    column_infos = reconstruct_column_infos(feature_metadata)

    engineered_df, updated_metadata = apply_feature_engineering(
        df.select(*feature_cols),
        column_infos,
        categorical_cardinality_threshold=feature_metadata.categorical_cardinality_threshold,
        frequency_maps=feature_metadata.categorical_frequency_maps,
        onehot_categories=feature_metadata.onehot_categories,
    )

    engineered_feature_cols = updated_metadata.engineered_feature_names
    models_bytes = [cloudpickle.dumps(model) for model in models]

    schema = StructType([StructField("anomaly_score", DoubleType(), True)])

    @pandas_udf(schema)  # type: ignore[call-overload]
    def predict_udf(*cols: pd.Series) -> pd.DataFrame:
        import cloudpickle
        import numpy as np
        import pandas as pd

        features_df = pd.concat(cols, axis=1)
        features_df.columns = engineered_feature_cols

        scores_list = []
        for model_bytes in models_bytes:
            model_local = cloudpickle.loads(model_bytes)
            scores_list.append(-model_local.score_samples(features_df))

        mean_scores = np.mean(np.vstack(scores_list), axis=0)
        return pd.DataFrame({"anomaly_score": mean_scores})

    result = engineered_df.withColumn("_scores", predict_udf(*[col(c) for c in engineered_feature_cols]))
    result = result.select("*", "_scores.anomaly_score").drop("_scores")

    return result


def _compute_validation_metrics(
    model: Any, val_df: DataFrame, feature_cols: list[str], feature_metadata: Any
) -> dict[str, float]:
    """
    Compute validation metrics and distribution statistics.

    Uses distributed scoring via pandas UDF.
    """
    if val_df.count() == 0:
        return {"validation_rows": 0}

    scored = _score_with_model(model, val_df, feature_cols, feature_metadata)
    scores_df = scored.select(F.col("anomaly_score").alias("score"))

    # Basic stats
    val_count = scores_df.count()
    stats = scores_df.select(
        F.mean("score").alias("mean"),
        F.stddev("score").alias("std"),
        F.skewness("score").alias("skewness"),
    ).first()

    # Quantiles for distribution
    quantiles = scores_df.approxQuantile("score", [0.1, 0.25, 0.5, 0.75, 0.9], 0.01)

    assert stats is not None, "Failed to compute validation statistics"
    metrics = {
        "validation_rows": val_count,
        "score_mean": stats["mean"] or 0.0,
        "score_std": stats["std"] or 0.0,
        "score_skewness": stats["skewness"] or 0.0,
        "score_p10": quantiles[0],
        "score_p25": quantiles[1],
        "score_p50": quantiles[2],
        "score_p75": quantiles[3],
        "score_p90": quantiles[4],
    }

    # Filter out None values (MLflow doesn't accept them)
    metrics = {k: v for k, v in metrics.items() if v is not None}

    return metrics


def _compute_score_quantiles(
    model: Any, df: DataFrame, feature_cols: list[str], feature_metadata: Any
) -> dict[str, float]:
    """Compute score quantiles from the training score distribution."""
    if df.count() == 0:
        return {}

    scored = _score_with_model(model, df, feature_cols, feature_metadata)
    scores_df = scored.select(F.col("anomaly_score").alias("score"))
    quantiles = scores_df.approxQuantile("score", SCORE_QUANTILE_PROBS, 0.01)

    return dict(zip(SCORE_QUANTILE_KEYS, quantiles, strict=False))


def _compute_score_quantiles_ensemble(
    models: list[Any], df: DataFrame, feature_cols: list[str], feature_metadata: Any
) -> dict[str, float]:
    """Compute score quantiles using ensemble mean scores."""
    if df.count() == 0:
        return {}

    scored = _score_with_ensemble_models(models, df, feature_cols, feature_metadata)
    scores_df = scored.select(F.col("anomaly_score").alias("score"))
    quantiles = scores_df.approxQuantile("score", SCORE_QUANTILE_PROBS, 0.01)

    return dict(zip(SCORE_QUANTILE_KEYS, quantiles, strict=False))


def _compute_baseline_statistics(train_df: DataFrame, columns: list[str]) -> dict[str, dict[str, float]]:
    """
    Compute baseline distribution statistics for feature columns in training data.
    Used later for drift detection.

    Args:
        train_df: Training DataFrame
        columns: Feature columns to compute statistics for

    Returns:
        Dictionary mapping column names to their baseline statistics
    """
    baseline_stats = {}
    col_types = dict(train_df.dtypes)

    for col_name in columns:
        # Get column data type
        col_type = col_types.get(col_name)
        if not col_type:
            continue

        # Only compute stats for numeric-compatible types
        # Skip date, timestamp, and string types that can't be cast to numeric
        numeric_compatible_types = ["int", "long", "float", "double", "short", "byte", "boolean", "decimal"]
        if not any(t in col_type.lower() for t in numeric_compatible_types):
            continue

        # Cast boolean to double for statistics computation
        col_expr = F.col(col_name).cast("double") if col_type == "boolean" else F.col(col_name)

        col_stats = train_df.select(
            F.mean(col_expr).alias("mean"),
            F.stddev(col_expr).alias("std"),
            F.min(col_expr).alias("min"),
            F.max(col_expr).alias("max"),
        ).first()

        quantiles = train_df.select(col_expr.alias(col_name)).approxQuantile(col_name, [0.25, 0.5, 0.75], 0.01)

        assert col_stats is not None, f"Failed to compute stats for {col_name}"
        baseline_stats[col_name] = {
            "mean": col_stats["mean"],
            "std": col_stats["std"],
            "min": col_stats["min"],
            "max": col_stats["max"],
            "p25": quantiles[0],
            "p50": quantiles[1],
            "p75": quantiles[2],
        }

    return baseline_stats


def _ensure_mlflow_registry_uri() -> None:
    """Ensure MLflow registry URI is configured for Unity Catalog.

    Sets registry URI to 'databricks-uc' (or value from MLFLOW_REGISTRY_URI env var).
    Also sets tracking URI to ensure MLflow uses SDK auth in worker processes.
    In Databricks notebooks, MLflow is pre-configured, but setting it explicitly is idempotent and safe.
    """
    registry_uri = os.environ.get("MLFLOW_REGISTRY_URI", "databricks-uc")
    mlflow.set_registry_uri(registry_uri)

    # Also ensure tracking URI is set to use SDK auth (especially in worker processes)
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI")
    if tracking_uri:
        mlflow.set_tracking_uri(tracking_uri)


def _register_ensemble_member_to_mlflow(
    model: Any,
    model_name: str,
    ensemble_index: int,
    ensemble_size: int,
    hyperparams: dict[str, Any],
    metrics: dict[str, float],
    signature: Any,
) -> str:
    """Register a single ensemble member model to MLflow/Unity Catalog.

    Returns:
        Model URI in format: models:/<model_name>_ensemble_<index>/<version>
    """
    ensemble_model_name = f"{model_name}_ensemble_{ensemble_index}"
    run_name = f"{model_name}_ensemble_{ensemble_index}"

    with mlflow.start_run(run_name=run_name):
        model_info = mlflow.sklearn.log_model(
            sk_model=model,
            name="model",
            registered_model_name=ensemble_model_name,
            signature=signature,
        )
        mlflow.log_params(_flatten_hyperparams(hyperparams))
        mlflow.log_metrics(metrics)
        mlflow.log_param("ensemble_index", ensemble_index)
        mlflow.log_param("ensemble_size", ensemble_size)

        # Use explicit version-based URI format
        # ensemble_model_name inherits full catalog.schema.model format from base model_name
        return f"models:/{ensemble_model_name}/{model_info.registered_model_version}"


def _aggregate_ensemble_metrics(all_metrics: list[dict[str, float]]) -> dict[str, float]:
    """Aggregate metrics across ensemble members (mean and std).

    Args:
        all_metrics: List of metric dictionaries, one per ensemble member

    Returns:
        Dictionary with aggregated metrics (mean and std for each metric)
    """
    if not all_metrics:
        return {}

    aggregated = {}
    for key in all_metrics[0].keys():
        values = [m[key] for m in all_metrics if key in m]
        if values:
            mean_value = sum(values) / len(values)
            aggregated[key] = mean_value
            # Compute standard deviation
            if len(values) > 1:
                variance = sum((v - mean_value) ** 2 for v in values) / (len(values) - 1)
                aggregated[f"{key}_std"] = variance**0.5
            else:
                aggregated[f"{key}_std"] = 0.0

    return aggregated


def _train_ensemble(
    train_df: DataFrame,
    val_df: DataFrame,
    columns: list[str],
    params: AnomalyParams,
    ensemble_size: int,
    model_name: str,
) -> tuple[list[str], dict[str, Any], dict[str, float], dict[str, float], Any]:
    """
    Train ensemble of models with different random seeds.

    Feature engineering is performed once and reused across all ensemble members.
    Each member is trained with a different random seed (base_seed + index).

    Returns:
        Tuple of (model_uris, hyperparams, aggregated_metrics, score_quantiles, feature_metadata).
    """
    _ensure_mlflow_registry_uri()

    base_params = params or AnomalyParams()
    train_pandas, feature_metadata = _prepare_training_features(train_df, columns, base_params)

    model_uris = []
    all_metrics = []
    models = []
    hyperparams = None
    signature = None

    for i in range(ensemble_size):
        modified_params = deepcopy(base_params)
        modified_params.algorithm_config.random_seed += i

        model, model_hyperparams = _fit_sklearn_model(train_pandas, modified_params)
        models.append(model)

        if i == 0:
            hyperparams = model_hyperparams
            predictions = model.predict(train_pandas)
            signature = infer_signature(train_pandas, predictions)

        metrics = _compute_validation_metrics(model, val_df, columns, feature_metadata)
        all_metrics.append(metrics)

        model_uri = _register_ensemble_member_to_mlflow(
            model, model_name, i, ensemble_size, model_hyperparams, metrics, signature
        )
        model_uris.append(model_uri)

    aggregated_metrics = _aggregate_ensemble_metrics(all_metrics)

    assert hyperparams is not None, "Failed to capture hyperparams for ensemble training"
    score_quantiles = _compute_score_quantiles_ensemble(models, train_df, columns, feature_metadata)

    return model_uris, hyperparams, aggregated_metrics, score_quantiles, feature_metadata


def _flatten_hyperparams(hyperparams: dict[str, Any]) -> dict[str, Any]:
    return {f"hyperparam_{k}": v for k, v in hyperparams.items() if v is not None}


def _stringify_dict(data: dict[str, Any]) -> dict[str, str]:
    return {k: str(v) for k, v in data.items() if v is not None}
