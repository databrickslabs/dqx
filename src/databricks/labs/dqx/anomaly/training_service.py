"""Anomaly training service - Main orchestration layer.

Provides the high-level API for training an anomaly detection model, including context building,
validation, and training. All training logic lives on AnomalyTrainingService (public and private
methods).
"""

import logging
from copy import deepcopy
from datetime import datetime

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
    GroupingConfig,
    ModelIdentity,
    TrainingMetadata,
)
from databricks.labs.dqx.anomaly.profiler import auto_discover_columns
from databricks.labs.dqx.anomaly.training_strategies import (
    PROFILE_AUTO,
    AnomalyTrainingStrategy,
    resolve_training_profile,
)
from databricks.labs.dqx.anomaly.transformers import (
    SparkFeatureMetadata,
    apply_feature_engineering_from_metadata,
)
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

    Provides the main entry point for training an anomaly detection model, conditioned on a baseline
    grouping when one is declared or discovered.

    Extension point:
        To add new algorithms, implement AnomalyTrainingStrategy and pass to constructor.
    """

    def __init__(self, spark: SparkSession, strategy: AnomalyTrainingStrategy | None = None) -> None:
        """Initialize the training service.

        Args:
            spark: Active Spark session.
            strategy: Explicit training strategy. When given it overrides the profile, which is what
                keeps an injected test double authoritative; when omitted the strategy is resolved from
                the context's *profile* at training time. Kept as None rather than defaulted here so
                those two cases stay distinguishable.
        """
        self._spark = spark
        self._strategy = strategy

    @staticmethod
    def _perform_auto_discovery(df_filtered: DataFrame) -> tuple[list[str], list[str] | None]:
        """Discover feature columns and a baseline grouping.

        The grouping is chosen for baseline conditioning: finer, bounded by rows per group rather
        than by model count, because there is one model however many groups result.
        """
        profile = auto_discover_columns(df_filtered)
        logger.info(f"Auto-selected {len(profile.recommended_columns)} columns: {profile.recommended_columns}")
        if profile.recommended_segments:
            logger.info(
                f"Auto-detected {len(profile.recommended_segments)} baseline columns: "
                f"{profile.recommended_segments} ({profile.segment_count} total groups)"
            )
        for warning in profile.warnings:
            logger.warning(warning)
        return profile.recommended_columns, profile.recommended_segments or None

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
    ) -> tuple[list[str], list[str] | None]:
        """Fill in whichever of the feature columns and the grouping the caller left unspecified.

        Returns ``(columns, baseline_by)``. A discovered grouping always becomes ``baseline_by``:
        there is one model regardless of group count, so discovery cannot turn into an hours-long
        run however many groups it finds.
        """
        if columns is None:
            columns, discovered = self._perform_auto_discovery(df_filtered)
            if declared_baseline_by:
                # A declared baseline column is the basis metrics are compared against, not a
                # metric. Auto-discovery does not know that, so drop them here rather than making
                # the caller reconcile a list they never wrote.
                return [c for c in columns if c not in declared_baseline_by], declared_baseline_by
            return columns, discovered

        if declared_baseline_by is None:
            # Grouping discovery used to be reachable only when the columns were discovered too, so
            # naming your feature columns silently gave up any chance of conditioning. Those are
            # independent questions. Costs one extra profiling pass for callers who pass explicit
            # columns and no grouping.
            return columns, self._discover_baseline_columns(df_filtered, columns)

        return columns, declared_baseline_by

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
        profile = auto_discover_columns(df_filtered)
        discovered = [c for c in profile.recommended_segments if c not in set(columns)]
        if not discovered:
            return None
        logger.info(f"Auto-detected {len(discovered)} baseline columns: {discovered}")
        return discovered

    def build_context(
        self,
        df: DataFrame,
        model_name: str,
        registry_table: str,
        *,
        columns: list[str] | None,
        params: AnomalyParams | None,
        exclude_columns: list[str] | None,
        expected_anomaly_rate: float,
        baseline_by: list[str] | None = None,
        profile: str | None = None,
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
        columns, baseline_by = self._discover_columns_and_grouping(df_filtered, columns, declared_baseline_by)

        if not columns:
            raise InvalidParameterError("No columns provided or auto-discovered. Provide columns explicitly.")

        validation_warnings = validate_columns(df, columns, params)
        for warning in validation_warnings:
            logger.warning(warning)

        validate_baseline_columns(df, baseline_by, columns)
        if baseline_by:
            logger.info(f"Judging each metric against its own group's baseline, grouped by {baseline_by}")

        self._prepare_training_config(
            model_name=model_name,
            registry_table=registry_table,
            columns=columns,
            baseline_by=baseline_by,
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
            params=params,
            expected_anomaly_rate=expected_anomaly_rate,
            exclude_columns=exclude_columns,
            auto_discovery_used=auto_discovery_used,
            baseline_by=baseline_by,
            profile=profile,
        )

    def train(self, context: AnomalyTrainingContext) -> str:
        """Train the model described by *context*.

        One model, always. The dispatch to per-segment training went with the ``segment_by`` path: a
        baseline grouping is expressed as features on a single model, so the group count no longer
        decides how many models are trained.
        """
        return self._train_global(context)

    def _prepare_training_config(
        self,
        *,
        model_name: str,
        registry_table: str,
        columns: list[str],
        baseline_by: list[str] | None,
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
            # The grouping is part of the configuration: it changes the feature list and the
            # persisted baselines, so a change in it is a change in the model.
            config_changed = set(columns) != set(existing.training.columns) or (baseline_by or None) != (
                existing.grouping.baseline_by or None
            )

            if config_changed:
                logger.warning(
                    f"⚠️  Model '{model_name}' exists with different configuration:\n"
                    f"   Existing: columns={existing.training.columns}, "
                    f"baseline_by={existing.grouping.baseline_by}\n"
                    f"   New: columns={columns}, baseline_by={baseline_by}\n"
                    f"   The old model will be archived. Consider using a different model_name "
                    f"if this is a different use case."
                )

    def _train_global(self, context: AnomalyTrainingContext) -> str:
        """Train a single model."""
        sampled_df, _, truncated = sample_df(context.df_filtered, context.columns, context.params)
        if not sampled_df.head(1):
            raise InvalidParameterError(
                "Sampling produced 0 rows. Provide more data or adjust sampling parameters "
                "(sample_fraction/max_rows)."
            )

        train_df, val_df = train_validation_split(sampled_df, context.params)

        # An explicitly injected strategy wins over the profile, so a test double is never silently
        # bypassed. Otherwise the profile decides, and may tighten parameters (the correlation-aware
        # detector collapses the ensemble to one model); for the tabular profiles the returned params
        # are the very same object, so nothing is perturbed.
        strategy, params = resolve_training_profile(context.profile, context.params, self._strategy)
        logger.info(f"profile={context.profile or PROFILE_AUTO} -> algorithm strategy '{strategy.name}'")

        result = strategy.train(
            train_df,
            val_df,
            context.columns,
            params,
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
        self._save_training_record(context, artifacts)

        return context.model_name

    def _save_training_record(
        self,
        context: AnomalyTrainingContext,
        artifacts: TrainingArtifacts,
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
            grouping=GroupingConfig(
                baseline_by=context.baseline_by,
                sklearn_version=sklearn.__version__,
                config_hash=compute_config_hash(context.columns, context.baseline_by),
            ),
        )
        registry = AnomalyModelRegistry(context.spark)
        registry.save_model(record, context.registry_table)
