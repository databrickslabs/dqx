"""Anomaly training service - Main orchestration layer.

Provides the high-level API for training anomaly detection models, including
context building, validation, and both global and segmented training. All
training logic lives on AnomalyTrainingService (public and private methods).
"""

import collections.abc
import logging
from copy import deepcopy
from datetime import datetime
from typing import Any

import sklearn
from pyspark.sql import DataFrame, SparkSession
from mlflow.exceptions import MlflowException
from mlflow.tracking import MlflowClient

from databricks.labs.dqx.anomaly.core import (
    compute_baseline_statistics,
    sample_df,
    train_validation_split,
)
from databricks.labs.dqx.anomaly.model_config import compute_config_hash
from databricks.labs.dqx.anomaly.model_registry import (
    AnomalyModelRecord,
    AnomalyModelRegistry,
    FeatureEngineering,
    ModelIdentity,
    SegmentationConfig,
    TrainingMetadata,
)
from databricks.labs.dqx.anomaly.group_config import (
    MIN_ROWS_TO_TRAIN_SEGMENT,
    SEGMENT_COUNT_WARN_THRESHOLD,
)
from databricks.labs.dqx.anomaly.profiler import auto_discover_columns
from databricks.labs.dqx.anomaly.training_strategies import AnomalyTrainingStrategy, IsolationForestTrainingStrategy
from databricks.labs.dqx.anomaly.transformers import (
    SparkFeatureMetadata,
    apply_feature_engineering_from_metadata,
)
from databricks.labs.dqx.anomaly.segment_utils import build_segment_name
from databricks.labs.dqx.anomaly.types import AnomalyTrainingContext, TrainingArtifacts
from databricks.labs.dqx.anomaly.validation import (
    validate_columns,
    validate_fully_qualified_name,
    validate_baseline_columns,
    validate_spark_version,
    validate_training_params,
)
from databricks.labs.dqx.config import AnomalyParams
from databricks.labs.dqx.errors import InvalidParameterError

logger = logging.getLogger(__name__)


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

    @staticmethod
    def _perform_auto_discovery(
        df_filtered: DataFrame,
        segment_by: list[str] | None,
    ) -> tuple[list[str], list[str] | None]:
        """Perform auto-discovery of columns and segments.

        When the caller has not declared ``segment_by``, any grouping discovered here is routed to
        ``baseline_by``, so discovery is asked for a baseline-shaped grouping: finer, bounded by rows
        per group rather than by model count. Asking for the segmented shape was a real defect --
        it returned a single lowest-cardinality column and conditioning barely engaged.
        """
        profile = auto_discover_columns(df_filtered, for_baseline=segment_by is None)
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

    @staticmethod
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

    @staticmethod
    def _get_and_validate_segments(
        df: DataFrame, segment_by: list[str], params: AnomalyParams
    ) -> tuple[int, collections.abc.Iterator[dict[str, Any]]]:
        """Get distinct segments and validate count.

        Raises above ``params.max_segment_models``. One model is trained per segment and
        segmented training does not ensemble, so cost is linear in the segment count: 90
        segments measures roughly 88 minutes. Warning and proceeding meant a run could spend
        hours registering thousands of models behind a single log line, which is why this is
        an error rather than a warning.
        """
        segments_df = df.select(*segment_by).distinct()
        segment_count = segments_df.count()
        if segment_count > params.max_segment_models:
            raise InvalidParameterError(
                f"Segmenting by {segment_by} produces {segment_count} segments, above the limit of "
                f"{params.max_segment_models}. One model is trained per segment, so this run would train "
                f"{segment_count} models (roughly {segment_count} minutes). Either segment more coarsely, "
                f"or raise the limit with AnomalyParams(max_segment_models={segment_count})."
            )
        if segment_count > SEGMENT_COUNT_WARN_THRESHOLD:
            logger.warning(
                f"Training {segment_count} segments may be slow. Consider coarser segmentation or explicit segment_by."
            )
        return segment_count, (row.asDict() for row in segments_df.toLocalIterator())

    @staticmethod
    def _model_exists_in_uc(model_name: str) -> bool:
        """Check if a model exists in Unity Catalog using MLflow API."""
        try:
            client = MlflowClient()
            client.get_registered_model(model_name)
            return True
        except MlflowException:
            return False

    @staticmethod
    def _compute_post_training_metadata(
        train_df: DataFrame,
        feature_metadata: SparkFeatureMetadata,
    ) -> dict[str, dict[str, float]]:
        """Compute baseline statistics after training for drift detection."""
        engineered_train_df, _ = apply_feature_engineering_from_metadata(train_df, feature_metadata)
        baseline_stats = compute_baseline_statistics(engineered_train_df, feature_metadata.engineered_feature_names)
        return baseline_stats

    @staticmethod
    def _report_training_summary(
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

    @staticmethod
    def _stringify_dict(data: dict[str, Any]) -> dict[str, str]:
        """Convert dict values to strings."""
        return {str(k): str(v) for k, v in sorted(data.items(), key=lambda item: str(item[0])) if v is not None}

    def _resolve_columns_and_filtered_df(
        self,
        df: DataFrame,
        columns: list[str] | None,
        exclude_list: list[str],
    ) -> tuple[list[str] | None, DataFrame]:
        """Apply exclude_columns and return (columns, df_filtered)."""
        if columns is not None:
            if exclude_list:
                columns = [col for col in columns if col not in exclude_list]
            return columns, df
        if exclude_list:
            remaining = [col for col in df.columns if col not in exclude_list]
            logger.info(f"Excluding {len(exclude_list)} columns from auto-discovery: {exclude_list}")
            return None, df.select(*remaining)
        return None, df

    def _discover_columns_and_grouping(
        self,
        df_filtered: DataFrame,
        columns: list[str] | None,
        declared_baseline_by: list[str] | None,
        segment_by: list[str] | None,
    ) -> tuple[list[str], list[str] | None, list[str] | None]:
        """Fill in whichever of the feature columns and the grouping the caller left unspecified.

        Returns ``(columns, baseline_by, segment_by)``.
        """
        if columns is None:
            columns, discovered_segments = self._perform_auto_discovery(df_filtered, segment_by)
            if declared_baseline_by:
                # A declared baseline column is the basis metrics are compared against, not a
                # metric. Auto-discovery does not know that, so drop them here rather than making
                # the caller reconcile a list they never wrote.
                columns = [c for c in columns if c not in declared_baseline_by]
            elif segment_by is None and discovered_segments:
                # Route a discovered grouping to baseline_by, not segment_by. Assigning it to
                # segment_by would pin it to one model per group, which is how a discovered 90-way
                # grouping became an hours-long run — and would now hit the segment ceiling and fail
                # outright rather than using the mechanism that scales.
                return columns, discovered_segments, None
            else:
                segment_by = discovered_segments
            return columns, declared_baseline_by, segment_by

        if declared_baseline_by is None and segment_by is None:
            # Grouping discovery used to be reachable only when the columns were discovered too, so
            # naming your feature columns silently gave up any chance of conditioning. Those are
            # independent questions. Costs one extra profiling pass for callers who pass explicit
            # columns and no grouping.
            return columns, self._discover_baseline_columns(df_filtered, columns), None

        return columns, declared_baseline_by, segment_by

    @staticmethod
    def _discover_baseline_columns(df_filtered: DataFrame, columns: list[str]) -> list[str] | None:
        """Discover a baseline grouping when the caller named feature columns but no grouping.

        Kept separate from ``_perform_auto_discovery`` so that discovering a grouping does not
        require also discovering the feature columns.

        Anything the caller named as a feature is excluded. When discovery picks the columns itself
        the profiler already keeps the two lists disjoint, but here the feature list came from the
        caller: a column they asked to have measured must not silently become the basis it is
        measured against, which ``validate_baseline_columns`` would reject anyway.
        """
        profile = auto_discover_columns(df_filtered, for_baseline=True)
        discovered = [c for c in profile.recommended_segments if c not in set(columns)]
        if not discovered:
            return None
        logger.info(f"Auto-detected {len(discovered)} baseline columns: {discovered}")
        return discovered

    @staticmethod
    def _resolve_grouping(
        baseline_by: list[str] | None,
        segment_by: list[str] | None,
    ) -> tuple[list[str] | None, list[str] | None]:
        """Reconcile ``baseline_by`` against the legacy ``segment_by``.

        Returns ``(baseline_by, effective_segment_by)``. Exactly one of the two is ever populated:

        * ``segment_by`` — the legacy path, one model per segment. **Must** clear ``baseline_by``.
          Leaving both set would compute baseline-relative features *inside* each segment, where the
          baseline key is constant because ``_train_segmented`` has already filtered the frame, while
          ``compute_config_hash`` is built from ``segment_by`` alone and would not change. A model
          whose feature list moved but whose config hash did not is a silent train/score hazard, and
          it breaks the byte-identical-feature-list guarantee that makes already-trained models safe.
        * ``baseline_by`` — one pooled model fed each metric's deviation from its own group's
          baseline.

        There is no strategy to choose any more: on SMD, per-group models were the worst of three
        configurations (PR-AUC 0.1416 against 0.1499 pooled and 0.1536 relative) with one entity
        producing 15,963 false positives on 28,392 normal rows, so ``segment_by`` survives for
        compatibility rather than as a recommendation. See databrickslabs/dqx#1484.
        """
        if baseline_by is not None and segment_by is not None:
            raise InvalidParameterError(
                "Pass either baseline_by or segment_by, not both. segment_by is the legacy name for "
                "training one model per group; baseline_by judges each metric against its own "
                "group's baseline using a single model."
            )

        if segment_by is not None:
            return None, segment_by

        return baseline_by, None

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
        baseline_by: list[str] | None = None,
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
        if exclude_list:
            df_columns = set(df.columns)
            invalid = [col for col in exclude_list if col not in df_columns]
            if invalid:
                raise InvalidParameterError(f"exclude_columns contains columns not in DataFrame: {invalid}")

        params = AnomalyParams() if params is None else params
        validate_training_params(params, expected_anomaly_rate)
        declared_baseline_by = baseline_by if baseline_by is not None else params.baseline_by

        columns, df_filtered = self._resolve_columns_and_filtered_df(df, columns, exclude_list)
        auto_discovery_used = columns is None
        columns, declared_baseline_by, segment_by = self._discover_columns_and_grouping(
            df_filtered, columns, declared_baseline_by, segment_by
        )

        if not columns:
            raise InvalidParameterError("No columns provided or auto-discovered. Provide columns explicitly.")

        validation_warnings = validate_columns(df, columns, params)
        for warning in validation_warnings:
            logger.warning(warning)

        validate_baseline_columns(df, declared_baseline_by, columns)
        baseline_by, segment_by = self._resolve_grouping(declared_baseline_by, segment_by)
        if baseline_by:
            logger.info(f"Judging each metric against its own group's baseline, grouped by {baseline_by}")

        self._prepare_training_config(
            model_name=model_name,
            registry_table=registry_table,
            columns=columns,
            segment_by=segment_by,
        )

        params = self.apply_expected_anomaly_rate_if_default_contamination(params, expected_anomaly_rate)
        # Already a deepcopy, so recording the resolved grouping here cannot leak back to the
        # caller's params. Downstream feature engineering reads baseline_by off params, because
        # every narrowing select is already handed params and nothing else.
        params.baseline_by = baseline_by

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
            baseline_by=baseline_by,
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

        if self._model_exists_in_uc(model_name):
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

        baseline_stats = self._compute_post_training_metadata(train_df, result.feature_metadata)

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
        if context.segment_by is None:
            raise InvalidParameterError("segment_by is required for segmented training")
        segment_count, segment_iterator = self._get_and_validate_segments(
            context.df_filtered, context.segment_by, context.params
        )
        model_uris = []
        skipped_segments = []
        failed_segments: list[tuple[str, str]] = []

        for seg_values in segment_iterator:
            segment_name = build_segment_name(seg_values)
            model_name = f"{context.model_name}__seg_{segment_name}"

            segment_df = context.df_filtered
            for col_name, val in seg_values.items():
                segment_df = segment_df.filter(segment_df[col_name] == val)

            sampled_df, row_count, _ = sample_df(segment_df, context.columns, context.params)
            if row_count < MIN_ROWS_TO_TRAIN_SEGMENT:
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

                baseline_stats = self._compute_post_training_metadata(train_df, result.feature_metadata)

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

            except (InvalidParameterError, MlflowException, ValueError, RuntimeError, OSError) as e:
                failed_segments.append((segment_name, str(e)))
                logger.error(f"Failed to train segment '{segment_name}': {e}")

        self._report_training_summary(
            model_uris,
            skipped_segments,
            failed_segments,
            segment_count,
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
        # Must go through to_json() rather than hand-rolling the payload: it is the single
        # writer of this column, so any field added to SparkFeatureMetadata is persisted
        # here automatically instead of being silently dropped.
        feature_metadata_json = artifacts.feature_metadata.to_json()

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
                baseline_by=context.baseline_by,
                segment_values=self._stringify_dict(artifacts.segment_values) if artifacts.segment_values else None,
                is_global_model=segment_by is None,
                sklearn_version=sklearn.__version__,
                config_hash=compute_config_hash(context.columns, segment_by, context.baseline_by),
            ),
        )
        registry = AnomalyModelRegistry(context.spark)
        registry.save_model(record, context.registry_table)
