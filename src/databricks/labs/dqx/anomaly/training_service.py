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
from pyspark.sql import types as T
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
from databricks.labs.dqx.anomaly.group_config import MAX_BASELINE_GROUPS
from databricks.labs.dqx.anomaly.profiler import auto_discover_columns, suggest_baseline_columns
from databricks.labs.dqx.anomaly.temporal_advisory import (
    TREND_STRENGTH_ADVISORY_FLOOR,
    measure_trend_strength,
)
from databricks.labs.dqx.anomaly.training_strategies import (
    DEFAULT_PROFILE,
    AnomalyTrainingStrategy,
    resolve_training_profile,
)
from databricks.labs.dqx.anomaly.transformers import (
    SparkFeatureMetadata,
    apply_feature_engineering_from_metadata,
)
from databricks.labs.dqx.anomaly.types import AnomalyTrainingContext, TrainingArtifacts
from databricks.labs.dqx.anomaly.validation import (
    validate_baseline_over_time,
    validate_columns,
    validate_fully_qualified_name,
    validate_baseline_columns,
    validate_generated_feature_names,
    validate_spark_version,
    validate_training_params,
)
from databricks.labs.dqx.config import AnomalyParams
from databricks.labs.dqx.errors import InvalidParameterError
from databricks.labs.dqx.utils import sanitize_for_logging

logger = logging.getLogger(__name__)

#: Spark types a metric can be, for deciding which named columns the trend advisory should measure.
_NUMERIC_SPARK_TYPES = (T.ByteType, T.ShortType, T.IntegerType, T.LongType, T.FloatType, T.DoubleType, T.DecimalType)
#: Spark types that get expanded into cyclical calendar features when they reach the feature list.
_TIME_SPARK_TYPES = (T.TimestampType, T.TimestampNTZType, T.DateType)


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

        Returns ``(columns, baseline_by)``.

        Discovery of a grouping is reachable only when the feature columns were discovered too, so
        naming *columns* keeps the whole-table comparison. A discovered grouping then becomes
        ``baseline_by``: there is one model regardless of group count, so discovery cannot turn into
        an hours-long run however many groups it finds.
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
            # Naming *columns* keeps the pooled comparison, as it did before baseline_by existed.
            # Explicit configuration wins: adding engineered baseline features the caller never
            # asked for would also make the feature set data-dependent, so a retrain after a
            # cardinality shift would silently move every score and drift their threshold.
            # Discovery still runs here, but only to advise.
            self._advise_baseline_columns(df_filtered, columns)
            return columns, None

        return columns, declared_baseline_by

    @staticmethod
    def _advise_baseline_columns(df_filtered: DataFrame, columns: list[str]) -> None:
        """Warn when the data looks grouped but the caller left the comparison pooled.

        Advisory only, and silent when there is nothing to act on, which is what keeps it worth
        reading: a warning that fires on every explicit-columns call is one nobody looks at.

        Anything the caller named as a feature is excluded from the suggestion. A column they asked
        to have measured must not be offered as the basis it is measured against, which
        ``validate_baseline_columns`` would reject anyway.
        """
        suggestion = suggest_baseline_columns(df_filtered, exclude=columns)
        if suggestion is None:
            return
        # Column names come from the caller's schema, so they are untrusted for logging (CWE-117).
        safe_columns = [sanitize_for_logging(name) for name in suggestion.columns]
        logger.warning(
            f"{safe_columns} looks like a grouping ({suggestion.group_count} groups, "
            f"~{suggestion.rows_per_group} rows/group), but metrics are being compared against the "
            f"whole table, so a value that is ordinary overall yet wrong for its own group will not "
            f"be flagged. Pass baseline_by={safe_columns} to compare each row against its own group, "
            f"or baseline_by=[] to keep the whole-table comparison and silence this."
        )

    @staticmethod
    def _advise_trend_strength(df_filtered: DataFrame, columns: list[str], time_column: str) -> None:
        """Warn when a time column was passed but the data shows almost nothing to expect over time.

        The parameter is not free. Measured on the Server Machine Dataset, whose metrics arrive already
        normalised and largely stationary, fitting a seasonal term over too few cycles cost Isolation
        Forest 15 points of event coverage. Where there is no temporal structure to remove, subtracting a
        fitted one removes signal instead and adds the fit's own error on top.

        The statistic separates the regimes sharply: SMD sits at a median 0.016 while synthetic trending
        data reaches 0.999. Advisory only -- the caller asked for this and may know something the training
        window does not show, so DQX says so and proceeds.
        """
        numeric = [
            field.name
            for field in df_filtered.schema.fields
            if field.name in set(columns) and isinstance(field.dataType, _NUMERIC_SPARK_TYPES)
        ]
        if not numeric:
            return
        try:
            strength = measure_trend_strength(df_filtered, time_column, numeric)
        # Broad on purpose: an advisory must never fail a training run, which is the same reason
        # telemetry.py and table_manager.py catch broadly. No suppression comment, because the rule that
        # would flag this is not enabled -- one was here and suppressed nothing.
        except Exception as exc:
            logger.debug(f"Could not measure trend strength: {exc}")
            return
        if strength >= TREND_STRENGTH_ADVISORY_FLOOR:
            return
        safe_column = sanitize_for_logging(time_column)
        logger.warning(
            f"baseline_over_time='{safe_column}' was requested, but the training window shows little "
            f"structure over time ({strength:.1%} of variance explained by trend and seasonality). The "
            f"time-relative feature is unlikely to help here and may cost accuracy: on a largely "
            f"stationary dataset the same transform measured worse than leaving it off. Consider omitting "
            f"baseline_over_time unless you know this metric trends."
        )

    @staticmethod
    def _advise_calendar_features(df: DataFrame, columns: list[str], time_column: str | None) -> None:
        """Warn when a datetime column will be expanded into calendar features as a side effect.

        Seven cyclical features per datetime column are derived automatically for anything that reaches
        the feature list. Measured, that is actively harmful in two of nine anomaly shapes: point-extreme
        detection fell from 100% to 81%, and group-contextual detection from 72% to *zero*, because five
        calendar features dilute a one-metric signal and the tree spends its splits on calendar noise.

        Nothing warned about this before, which is the actual defect: the inference is automatic and the
        caller had no way to know it had happened. The column named as *baseline_over_time* is excluded
        from the feature set already, so it is not the subject of this warning.
        """
        named = set(columns)
        calendar_columns = [
            field.name
            for field in df.schema.fields
            if field.name in named and field.name != time_column and isinstance(field.dataType, _TIME_SPARK_TYPES)
        ]
        if not calendar_columns:
            return
        safe = [sanitize_for_logging(name) for name in calendar_columns]
        logger.warning(
            f"{safe} will be expanded into seven cyclical calendar features each (hour, day of week, "
            f"month, weekend). That helps when an anomaly is contextual by calendar and hurts otherwise: "
            f"measured, it took group-contextual detection from 72% to 0% by diluting the metric it was "
            f"meant to support. Pass exclude_columns={safe} to leave them out, or "
            f"baseline_over_time='{safe[0]}' to use it as a time axis instead of as features."
        )

    @staticmethod
    def _reject_unbounded_grouping(df: DataFrame, baseline_by: list[str]) -> None:
        """Refuse a grouping whose group count exceeds what can be persisted and broadcast.

        Auto-discovery has always honoured ``MAX_BASELINE_GROUPS`` by skipping a column that would breach
        it. An explicit *baseline_by* bypassed that entirely, and the failure mode is not a slow run: the
        per-group medians and the per-group score quantiles are each collected to the driver, one row per
        group, and both are persisted into the feature metadata. An identifier-like grouping exhausts the
        driver before it can produce a model, with nothing in the error to say why.

        One ``countDistinct`` action, paid only when a grouping is declared. Reuses the ceiling
        auto-discovery already respects rather than introducing a second, so the two cannot drift.
        """
        group_count = df.select(*baseline_by).distinct().count()
        if group_count <= MAX_BASELINE_GROUPS:
            return
        safe_columns = [sanitize_for_logging(name) for name in baseline_by]
        raise InvalidParameterError(
            f"baseline_by={safe_columns} produces {group_count:,} groups, above the supported ceiling of "
            f"{MAX_BASELINE_GROUPS:,}. Every group's median and score quantiles are held in the model's "
            f"metadata and broadcast at scoring, so this would exhaust the driver rather than run slowly. "
            f"Group on a coarser dimension, or drop the finest column from baseline_by."
        )

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
        baseline_over_time: str | None = None,
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
            self._reject_unbounded_grouping(df_filtered, baseline_by)
            logger.info(f"Judging each metric against its own group's baseline, grouped by {baseline_by}")

        resolved_over_time = baseline_over_time if baseline_over_time is not None else params.baseline_over_time
        validate_baseline_over_time(df, resolved_over_time, columns)
        # After both bases are resolved, because which derived features exist depends on them.
        validate_generated_feature_names(df, columns, baseline_by, resolved_over_time)
        self._advise_calendar_features(df, columns, resolved_over_time)
        if resolved_over_time:
            self._advise_trend_strength(df_filtered, columns, resolved_over_time)
            safe_time_column = sanitize_for_logging(resolved_over_time)
            # Saying "within its group" when both are set matters: the expectation is then fitted on the
            # group-relative value rather than the raw metric, which is a different model of the data.
            within = ", within its own group" if baseline_by else ""
            logger.info(f"Judging each metric against its expected level over '{safe_time_column}'{within}")

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
        params.baseline_over_time = resolved_over_time

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
            baseline_over_time=resolved_over_time,
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
        logger.info(f"profile={context.profile or DEFAULT_PROFILE} -> algorithm strategy '{strategy.name}'")

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
                config_hash=compute_config_hash(context.columns, context.baseline_by, context.baseline_over_time),
            ),
        )
        registry = AnomalyModelRegistry(context.spark)
        registry.save_model(record, context.registry_table)
