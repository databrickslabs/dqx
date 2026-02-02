"""Anomaly training service - Main orchestration layer.

Provides the high-level API for training anomaly detection models, including
context building, validation, and both global and segmented training.

Contains both the service class and helper functions used during training.
"""

import collections.abc
import json
import logging
from copy import deepcopy
from datetime import datetime
from typing import Any

import sklearn
from mlflow.tracking import MlflowClient
from pyspark.sql import DataFrame, SparkSession

from databricks.labs.dqx.anomaly.core import (
    compute_baseline_statistics,
    sample_df,
    train_validation_split,
)
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
from databricks.labs.dqx.anomaly.strategies import AnomalyTrainingStrategy, IsolationForestTrainingStrategy
from databricks.labs.dqx.anomaly.transformers import (
    ColumnTypeClassifier,
    SparkFeatureMetadata,
    apply_feature_engineering,
    reconstruct_column_infos,
)
from databricks.labs.dqx.anomaly.types import AnomalyTrainingContext, TrainingArtifacts
from databricks.labs.dqx.config import AnomalyParams
from databricks.labs.dqx.errors import InvalidParameterError

logger = logging.getLogger(__name__)


# =============================================================================
# Validation Functions
# =============================================================================


def validate_spark_version(spark: SparkSession) -> None:
    """Validate Spark version is compatible with anomaly detection."""
    major, minor, *_ = spark.version.split(".")
    if int(major) < 3 or (int(major) == 3 and int(minor) < 4):
        raise InvalidParameterError(
            f"Anomaly detection requires Spark >= 3.4 for SynapseML compatibility. Found Spark {major}.{minor}."
        )


def validate_fully_qualified_name(value: str, *, label: str) -> None:
    """Validate that a name is in catalog.schema.name format."""
    if value.count(".") != 2:
        raise InvalidParameterError(f"{label} must be fully qualified as catalog.schema.name, got: {value!r}.")


def validate_columns(
    df: DataFrame, columns: collections.abc.Iterable[str], params: AnomalyParams | None = None
) -> list[str]:
    """Validate columns for anomaly detection with multi-type support."""
    params = params or AnomalyParams()
    fe_config = params.feature_engineering

    classifier = ColumnTypeClassifier(
        categorical_cardinality_threshold=fe_config.categorical_cardinality_threshold,
        max_input_columns=fe_config.max_input_columns,
        max_engineered_features=fe_config.max_engineered_features,
    )

    _column_infos, warnings_list = classifier.analyze_columns(df, list(columns))
    return warnings_list


# =============================================================================
# Processing Functions
# =============================================================================


def process_exclude_columns(
    df: DataFrame,
    columns: list[str] | None,
    exclude_columns: list[str] | None,
) -> DataFrame:
    """Process exclude_columns parameter and return filtered DataFrame."""
    exclude_list = exclude_columns or []

    if not exclude_list:
        return df

    df_columns = set(df.columns)
    invalid_excludes = [c for c in exclude_list if c not in df_columns]
    if invalid_excludes:
        raise InvalidParameterError(f"exclude_columns contains columns not in DataFrame: {invalid_excludes}")

    if columns is None:
        remaining_columns = [c for c in df.columns if c not in exclude_list]
        df_filtered = df.select(*remaining_columns)
        logger.info(f"Excluding {len(exclude_list)} columns from auto-discovery: {exclude_list}")
        return df_filtered

    return df


def perform_auto_discovery(
    df_filtered: DataFrame,
    segment_by: list[str] | None,
) -> tuple[list[str], list[str] | None]:
    """Perform auto-discovery of columns and segments."""
    profile = auto_discover_columns(df_filtered)
    discovered_columns = profile.recommended_columns
    discovered_segments = segment_by

    if segment_by is None:
        discovered_segments = profile.recommended_segments

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
    """Apply expected_anomaly_rate to params if contamination is not explicitly set."""
    if params is None:
        params = AnomalyParams()

    params = deepcopy(params)

    if params.algorithm_config.contamination is None:
        params.algorithm_config.contamination = expected_anomaly_rate
        logger.info(f"Using expected_anomaly_rate={expected_anomaly_rate:.2%} for model training")
    else:
        logger.info(
            f"Using explicitly set contamination={params.algorithm_config.contamination:.2%} "
            f"(expected_anomaly_rate={expected_anomaly_rate:.2%} ignored)"
        )

    return params


def get_and_validate_segments(df: DataFrame, segment_by: list[str]) -> list[dict[str, Any]]:
    """Get distinct segments and validate count."""
    segments_df = df.select(*segment_by).distinct()
    segments = [row.asDict() for row in segments_df.collect()]

    if len(segments) > 100:
        logger.warning(
            f"Training {len(segments)} segments may be slow. Consider coarser segmentation or explicit segment_by."
        )

    return segments


# =============================================================================
# Utility Functions
# =============================================================================


def model_exists_in_uc(model_name: str) -> bool:
    """Check if a model exists in Unity Catalog using MLflow API."""
    try:
        client = MlflowClient()
        client.get_registered_model(model_name)
        return True
    except Exception:
        return False


def compute_post_training_metadata(
    train_df: DataFrame,
    feature_metadata: SparkFeatureMetadata,
) -> dict[str, dict[str, float]]:
    """Compute baseline statistics after training for drift detection."""
    column_infos_for_stats = reconstruct_column_infos(feature_metadata)
    engineered_train_df, _ = apply_feature_engineering(
        train_df,
        column_infos_for_stats,
        categorical_cardinality_threshold=20,
        frequency_maps=feature_metadata.categorical_frequency_maps,
        onehot_categories=feature_metadata.onehot_categories,
    )

    baseline_stats = compute_baseline_statistics(engineered_train_df, feature_metadata.engineered_feature_names)
    return baseline_stats


def report_training_summary(
    model_uris: list[str],
    skipped_segments: list[str],
    failed_segments: list[tuple[str, str]],
    total_segments: int,
    base_model_name: str,
    registry_table: str,
    params: AnomalyParams,
) -> None:
    """Report training summary including skipped and failed segments."""
    if skipped_segments:
        logger.info(
            f"Skipped {len(skipped_segments)}/{total_segments} segments due to insufficient data after sampling: "
            f"{', '.join(skipped_segments[:5])}"
            + (f" and {len(skipped_segments) - 5} more" if len(skipped_segments) > 5 else "")
        )

    if failed_segments:
        logger.warning(f"\nWARNING: {len(failed_segments)}/{total_segments} segments failed during training:")
        for seg_name, error in failed_segments[:3]:
            logger.warning(f"  - {seg_name}: {error}")
        if len(failed_segments) > 3:
            logger.warning(f"  ... and {len(failed_segments) - 3} more")

    if not model_uris:
        raise InvalidParameterError(
            f"All {total_segments} segments failed ({len(skipped_segments)} skipped, "
            f"{len(failed_segments)} errors). Cannot train any models. "
            f"Consider increasing sample_fraction (current: {params.sample_fraction}) or checking segment definitions."
        )

    trained_count = len(model_uris)
    logger.info(f"   Trained {trained_count}/{total_segments} segment models for: {base_model_name}")
    logger.info(f"   Registry: {registry_table}")


def stringify_dict(data: dict[str, Any]) -> dict[str, str]:
    """Convert dict values to strings."""
    return {k: str(v) for k, v in data.items() if v is not None}


# =============================================================================
# Training Service
# =============================================================================


class AnomalyTrainingService:
    """Service for building training context and orchestrating model training.

    Provides the main entry point for training anomaly detection models.
    Supports both global models and segment-specific models.

    Extension point:
        To add new algorithms, implement AnomalyTrainingStrategy and pass to constructor.
    """

    def __init__(self, spark: SparkSession, strategy: AnomalyTrainingStrategy | None = None) -> None:
        """Initialize the training service."""
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
        """Build training context with all validated inputs."""
        validate_spark_version(self._spark)

        if not model_name:
            raise InvalidParameterError("model_name is required and must be fully qualified as 'catalog.schema.model'.")
        if not registry_table:
            raise InvalidParameterError(
                "registry_table is required and must be fully qualified as 'catalog.schema.table'."
            )

        exclude_list = exclude_columns or []
        if columns is not None and exclude_list:
            columns = [col for col in columns if col not in exclude_list]

        df_filtered = process_exclude_columns(df, columns, exclude_columns)
        auto_discovery_used = columns is None

        if columns is None:
            columns, segment_by = perform_auto_discovery(df_filtered, segment_by)

        if not columns:
            raise InvalidParameterError("No columns provided or auto-discovered. Provide columns explicitly.")

        params = AnomalyParams() if params is None else params
        validation_warnings = validate_columns(df, columns, params)
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
        """Train model(s) based on context."""
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
        """Validate and prepare training configuration."""
        validate_fully_qualified_name(model_name, label="model_name")
        validate_fully_qualified_name(registry_table, label="registry_table")

        if model_exists_in_uc(model_name):
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
        """Train a single global model."""
        sampled_df, _, truncated = sample_df(context.df_filtered, context.columns, context.params)
        if not sampled_df.head(1):
            raise InvalidParameterError(
                "Sampling produced 0 rows. Provide more data or adjust sampling parameters "
                "(sample_fraction/max_rows)."
            )

        train_df, val_df = train_validation_split(sampled_df, context.params)

        result = self._strategy.train(
            train_df,
            val_df,
            context.columns,
            context.params,
            context.model_name,
            allow_ensemble=True,
        )

        baseline_stats = compute_post_training_metadata(train_df, result.feature_metadata)

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

        return context.model_name

    def _train_segmented(self, context: AnomalyTrainingContext) -> str:
        """Train separate models for each segment."""
        assert context.segment_by is not None
        segments = get_and_validate_segments(context.df_filtered, context.segment_by)
        model_uris = []
        skipped_segments = []
        failed_segments: list[tuple[str, str]] = []

        for seg_values in segments:
            segment_name = "_".join(f"{k}={v}" for k, v in seg_values.items())
            model_name = f"{context.model_name}__seg_{segment_name}"

            segment_df = context.df_filtered
            for col_name, val in seg_values.items():
                segment_df = segment_df.filter(segment_df[col_name] == val)

            sampled_df, row_count, _ = sample_df(segment_df, context.columns, context.params)
            if row_count < 10:
                skipped_segments.append(segment_name)
                continue

            try:
                train_df, val_df = train_validation_split(sampled_df, context.params)

                result = self._strategy.train(
                    train_df,
                    val_df,
                    context.columns,
                    context.params,
                    model_name,
                    allow_ensemble=False,
                )

                baseline_stats = compute_post_training_metadata(train_df, result.feature_metadata)

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
                    segment_values=seg_values,
                )
                self._save_training_record(context, artifacts, segment_by=context.segment_by)

                model_uris.append(result.model_uri)

            except Exception as e:
                failed_segments.append((segment_name, str(e)))
                logger.error(f"Failed to train segment '{segment_name}': {e}")

        report_training_summary(
            model_uris,
            skipped_segments,
            failed_segments,
            len(segments),
            context.model_name,
            context.registry_table,
            context.params,
        )

        return context.model_name

    def _save_training_record(
        self,
        context: AnomalyTrainingContext,
        artifacts: TrainingArtifacts,
        segment_by: list[str] | None,
    ) -> None:
        """Save training record to registry table."""
        feature_metadata_json = json.dumps(
            {
                "column_infos": artifacts.feature_metadata.column_infos,
                "categorical_frequency_maps": artifacts.feature_metadata.categorical_frequency_maps,
                "onehot_categories": artifacts.feature_metadata.onehot_categories,
                "engineered_feature_names": artifacts.feature_metadata.engineered_feature_names,
                "categorical_cardinality_threshold": artifacts.feature_metadata.categorical_cardinality_threshold,
            }
        )

        record = AnomalyModelRecord(
            identity=ModelIdentity(
                model_name=artifacts.model_name,
                model_uri=artifacts.model_uri,
                algorithm=artifacts.algorithm,
                mlflow_run_id=artifacts.run_id or "unknown",
            ),
            training=TrainingMetadata(
                columns=context.columns,
                hyperparameters={k: str(v) for k, v in artifacts.hyperparams.items() if v is not None},
                training_rows=artifacts.training_rows,
                training_time=datetime.now(),
                metrics=artifacts.validation_metrics,
                score_quantiles=artifacts.score_quantiles,
                baseline_stats=artifacts.baseline_stats,
            ),
            features=FeatureEngineering(
                mode="spark",
                feature_metadata=feature_metadata_json,
            ),
            segmentation=SegmentationConfig(
                segment_by=segment_by,
                segment_values=stringify_dict(artifacts.segment_values) if artifacts.segment_values else None,
                is_global_model=segment_by is None,
                sklearn_version=sklearn.__version__,
                config_hash=compute_config_hash(context.columns, segment_by),
            ),
        )
        registry = AnomalyModelRegistry(context.spark)
        registry.save_model(record, context.registry_table)
