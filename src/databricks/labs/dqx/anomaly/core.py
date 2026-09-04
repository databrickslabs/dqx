"""Core ML operations for row anomaly detection.

Contains the fundamental building blocks for training and scoring:
- Feature engineering (prepare_training_features, prepare_engineered_pandas)
- Model training (fit_sklearn_model, fit_isolation_forest)
- Scoring (score_with_model, score_with_ensemble_models)
- Metrics computation (compute_validation_metrics, compute_score_quantiles)
- Baseline statistics (compute_baseline_statistics)

All functions work with distributed Spark DataFrames and sklearn models.
MLflow model registration is handled by mlflow_registry.py.
"""

import logging
from typing import Any

import cloudpickle
import numpy as np
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import DoubleType, IntegerType, StructField, StructType
from sklearn.ensemble import IsolationForest
from sklearn.pipeline import Pipeline

from databricks.labs.dqx.anomaly.segment_utils import BASELINE_KEY_COLUMN, with_baseline_key
from databricks.labs.dqx.anomaly.transformers import (
    ColumnTypeClassifier,
    SparkFeatureMetadata,
    apply_feature_engineering,
    apply_feature_engineering_from_metadata,
)
from databricks.labs.dqx.config import AnomalyParams, IsolationForestConfig
from databricks.labs.dqx.errors import ComputationError, InvalidParameterError

logger = logging.getLogger(__name__)

DEFAULT_SAMPLE_FRACTION = 0.3
DEFAULT_TRAIN_RATIO = 0.8
SCORE_QUANTILE_PROBS = [0.0, 0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 1.0]
SCORE_QUANTILE_KEYS = ["p00", "p01", "p05", "p10", "p25", "p50", "p75", "p90", "p95", "p99", "p100"]


def _with_basis_columns(
    columns: list[str], baseline_by: list[str] | None, baseline_over_time: str | None = None
) -> list[str]:
    """Union *columns* with the comparison bases, preserving order and dropping duplicates.

    A basis column has to survive the narrowing select even though it is never a feature: the grouping
    columns build the group key, and the time column is the axis an expected level is fitted along. Both
    are projected away again by ``apply_feature_engineering``.

    Group columns are not features, but they must survive every narrowing ``select`` on the way
    to feature engineering, or the group-relative transform has nothing to compute a baseline
    from. Feature engineering drops them again before the sklearn pipeline sees anything.
    """
    extra = [*(baseline_by or []), *([baseline_over_time] if baseline_over_time else [])]
    if not extra:
        return columns
    return list(dict.fromkeys([*columns, *extra]))


def sample_df(df: DataFrame, columns: list[str], params: AnomalyParams) -> tuple[DataFrame, int, bool]:
    """Sample DataFrame for training.

    Args:
        df: Input DataFrame
        columns: Columns to include in sample
        params: Training parameters with sample_fraction and max_rows

    Returns:
        Tuple of (sampled DataFrame, row count, truncated flag)
    """
    fraction = params.sample_fraction if params.sample_fraction is not None else DEFAULT_SAMPLE_FRACTION
    columns = _with_basis_columns(columns, params.baseline_by, params.baseline_over_time)
    missing_cols = [c for c in columns if c not in df.columns]
    if missing_cols:
        raise InvalidParameterError(f"Columns not found in DataFrame: {missing_cols}")
    sampled = df.select(*columns).sample(withReplacement=False, fraction=fraction, seed=42)
    if params.max_rows:
        sampled = sampled.limit(params.max_rows)
    count = sampled.count()
    truncated = params.max_rows is not None and count == params.max_rows
    return sampled, count, truncated


def train_validation_split(df: DataFrame, params: AnomalyParams) -> tuple[DataFrame, DataFrame]:
    """Split DataFrame into training and validation sets."""
    train_ratio = params.train_ratio if params.train_ratio is not None else DEFAULT_TRAIN_RATIO
    train_df, val_df = df.randomSplit([train_ratio, 1 - train_ratio], seed=42)
    return train_df, val_df


def prepare_training_features(
    train_df: DataFrame, feature_columns: list[str], params: AnomalyParams
) -> tuple[pd.DataFrame, SparkFeatureMetadata]:
    """Prepare training features using Spark-based feature engineering.

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

    # Group columns ride along for the relative transform but are never classified as features:
    # analyze_columns sees only feature_columns.
    feature_df = train_df.select(*_with_basis_columns(feature_columns, params.baseline_by, params.baseline_over_time))
    column_infos, _ = classifier.analyze_columns(feature_df, feature_columns)

    engineered_df, feature_metadata = apply_feature_engineering(
        feature_df,
        column_infos,
        categorical_cardinality_threshold=fe_config.categorical_cardinality_threshold,
        frequency_maps=None,
        baseline_by=params.baseline_by,
        baseline_over_time=params.baseline_over_time,
    )

    # Project to the feature list explicitly. The engineered frame also carries the baseline key
    # column, which is a grouping record rather than a feature: ``fit_sklearn_model`` fits the
    # pipeline on every column of this frame, so a passthrough reaching here would be trained on
    # and would land in the inferred MLflow signature.
    train_pandas = engineered_df.select(*feature_metadata.engineered_feature_names).toPandas()
    return train_pandas, feature_metadata


def fit_sklearn_model(train_pandas: pd.DataFrame, params: AnomalyParams) -> tuple[Pipeline, dict[str, Any]]:
    """Train the IsolationForest pipeline on pre-engineered pandas features.

    No feature scaling. ``RobustScaler`` used to sit in front of the forest, and it could not have
    made any difference: it is an affine per-feature transform, and Isolation Forest splits on
    per-feature thresholds drawn uniformly between each feature's min and max, so the induced
    partitions are identical either way. Measured across five ADBench datasets, PR-AUC with and
    without it agreed to four decimal places -- covertype 0.0572/0.0572, mnist 0.2740/0.2740, cardio
    0.5766/0.5766, shuttle 0.9789/0.9789, fraud 0.1926/0.1926. It cost a fit and a transform on every
    training run and every scoring pass, and shipped inside every pickled artifact.

    The single-step ``Pipeline`` is kept deliberately: ``named_steps["model"]`` is how the SHAP
    explainer reaches the tree model, and it leaves somewhere for a transform that genuinely does
    something to go later.

    Returns:
        - pipeline: sklearn Pipeline wrapping the fitted IsolationForest
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

    pipeline = Pipeline([('model', iso_forest)])
    pipeline.fit(train_pandas)

    hyperparams: dict[str, Any] = {
        "contamination": algo_cfg.contamination,
        "num_trees": algo_cfg.num_trees,
        "max_samples": algo_cfg.subsampling_rate,
        "random_seed": algo_cfg.random_seed,
        "feature_scaling": "none",
    }

    return pipeline, hyperparams


def fit_isolation_forest(
    train_df: DataFrame, feature_columns: list[str], params: AnomalyParams
) -> tuple[Pipeline, dict[str, Any], SparkFeatureMetadata]:
    """Train IsolationForest model with distributed feature engineering.

    Feature engineering runs on Spark, then the model trains on the driver.

    Returns:
        - pipeline: sklearn Pipeline wrapping the fitted IsolationForest
        - hyperparams: Model configuration for MLflow tracking
        - feature_metadata: Transformation metadata for distributed scoring
    """
    train_pandas, feature_metadata = prepare_training_features(train_df, feature_columns, params)
    pipeline, hyperparams = fit_sklearn_model(train_pandas, params)
    return pipeline, hyperparams, feature_metadata


def score_with_model(
    model: Pipeline, df: DataFrame, feature_cols: list[str], feature_metadata: SparkFeatureMetadata
) -> DataFrame:
    """Score DataFrame using scikit-learn model with distributed pandas UDF.

    Feature engineering is applied in Spark before the pandas UDF.
    This enables distributed inference across the Spark cluster.
    """
    engineered_df, updated_metadata = apply_feature_engineering_from_metadata(
        df.select(
            *_with_basis_columns(feature_cols, feature_metadata.baseline_by, feature_metadata.baseline_over_time)
        ),
        feature_metadata,
    )

    engineered_feature_cols = updated_metadata.engineered_feature_names
    model_bytes = cloudpickle.dumps(model)

    schema = StructType(
        [
            StructField("anomaly_score", DoubleType(), True),
            StructField("prediction", IntegerType(), True),
        ]
    )

    @pandas_udf(schema)  # type: ignore[call-overload]
    def predict_udf(*cols: pd.Series) -> pd.DataFrame:
        """UDF for distributed scoring."""
        model_local = cloudpickle.loads(model_bytes)
        features_df = pd.concat(cols, axis=1)
        features_df.columns = engineered_feature_cols

        predictions = model_local.predict(features_df)
        scores = -model_local.score_samples(features_df)
        predictions = np.where(predictions == -1, 1, 0)

        return pd.DataFrame({"anomaly_score": scores, "prediction": predictions})

    result = engineered_df.withColumn("_scores", predict_udf(*[col(c) for c in engineered_feature_cols]))
    result = result.select("*", "_scores.anomaly_score", "_scores.prediction").drop("_scores")

    return result


def score_with_ensemble_models(
    models: list[Pipeline], df: DataFrame, feature_cols: list[str], feature_metadata: SparkFeatureMetadata
) -> DataFrame:
    """Score DataFrame using an ensemble of models and return mean scores."""
    engineered_df, updated_metadata = apply_feature_engineering_from_metadata(
        df.select(
            *_with_basis_columns(feature_cols, feature_metadata.baseline_by, feature_metadata.baseline_over_time)
        ),
        feature_metadata,
    )

    engineered_feature_cols = updated_metadata.engineered_feature_names
    models_bytes = [cloudpickle.dumps(mdl) for mdl in models]

    schema = StructType([StructField("anomaly_score", DoubleType(), True)])

    @pandas_udf(schema)  # type: ignore[call-overload]
    def predict_udf(*cols: pd.Series) -> pd.DataFrame:
        """UDF for distributed ensemble scoring."""
        features_df = pd.concat(cols, axis=1)
        features_df.columns = engineered_feature_cols

        scores_list = []
        for model_bytes_item in models_bytes:
            model_local = cloudpickle.loads(model_bytes_item)
            scores_list.append(-model_local.score_samples(features_df))

        mean_scores = np.mean(np.vstack(scores_list), axis=0)
        return pd.DataFrame({"anomaly_score": mean_scores})

    result = engineered_df.withColumn("_scores", predict_udf(*[col(c) for c in engineered_feature_cols]))
    result = result.select("*", "_scores.anomaly_score").drop("_scores")

    return result


def compute_validation_metrics(
    model: Pipeline, val_df: DataFrame, feature_cols: list[str], feature_metadata: SparkFeatureMetadata
) -> dict[str, float]:
    """Compute validation metrics and distribution statistics."""
    if not val_df.take(1):  # emptiness only — take(1) avoids a full-frame count scan
        return {"validation_rows": 0}

    scored = score_with_model(model, val_df, feature_cols, feature_metadata)
    scores_df = scored.select(F.col("anomaly_score").alias("score"))

    val_count = scores_df.count()
    stats = scores_df.select(
        F.mean("score").alias("mean"),
        F.stddev("score").alias("std"),
        F.skewness("score").alias("skewness"),
    ).first()

    quantiles = scores_df.approxQuantile("score", [0.1, 0.25, 0.5, 0.75, 0.9], 0.01)
    if stats is None:
        raise ComputationError("Failed to compute validation statistics")
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

    return {k: v for k, v in metrics.items() if v is not None}


def compute_score_quantiles(
    model: Pipeline, df: DataFrame, feature_cols: list[str], feature_metadata: SparkFeatureMetadata
) -> dict[str, float]:
    """Compute score quantiles from the training score distribution.

    Also populates ``feature_metadata.baseline_score_quantiles`` when the model is grouped, so
    scoring can calibrate severity against each group's own distribution.
    """
    if not df.take(1):  # emptiness only — take(1) avoids a full-frame count scan
        return {}

    scored = score_with_model(model, df, feature_cols, feature_metadata)
    return _quantiles_from_scored(scored, feature_metadata)


def compute_score_quantiles_ensemble(
    models: list[Pipeline], df: DataFrame, feature_cols: list[str], feature_metadata: SparkFeatureMetadata
) -> dict[str, float]:
    """Compute score quantiles using ensemble mean scores.

    Also populates ``feature_metadata.baseline_score_quantiles`` when the model is grouped.
    """
    if not df.take(1):  # emptiness only — take(1) avoids a full-frame count scan
        return {}

    scored = score_with_ensemble_models(models, df, feature_cols, feature_metadata)
    return _quantiles_from_scored(scored, feature_metadata)


#: How precisely the severity quantiles are estimated. Load-bearing since the severity tail takes its decay
#: rate from (q99 - q95): `approxQuantile` may return the value at any rank within this much of the one
#: requested, so at 0.01 a reported "p99" could be the true p100, and measured on an Isolation Forest
#: ensemble that moved the alert rate at threshold 98 from 2.25% to 0.65% against the 2% requested. At 0.001
#: the reported p99 is between ranks 98.9 and 99.1. Paid once, at training time, for one double column.
SEVERITY_QUANTILE_RELATIVE_ERROR = 0.001


def _quantiles_from_scored(scored: DataFrame, feature_metadata: SparkFeatureMetadata) -> dict[str, float]:
    """Derive the global score quantiles, and the per-group ones as a side effect.

    Note the cost: for a grouped model this walks the scored frame twice, and since ``.cache()``
    is unavailable on serverless the scoring UDF runs for each walk. Paid at training time only,
    and only when ``baseline_by`` is set — an ungrouped model behaves exactly as before.
    """
    scores_df = scored.select(F.col("anomaly_score").alias("score"))
    quantiles = scores_df.approxQuantile("score", SCORE_QUANTILE_PROBS, SEVERITY_QUANTILE_RELATIVE_ERROR)

    if feature_metadata.baseline_by:
        feature_metadata.baseline_score_quantiles = compute_baseline_score_quantiles(
            scored, feature_metadata.baseline_by
        )

    return dict(zip(SCORE_QUANTILE_KEYS, quantiles, strict=False))


def compute_baseline_score_quantiles(scored_df: DataFrame, baseline_by: list[str]) -> dict[str, dict[str, float]]:
    """Compute the score quantiles of each group from an already-scored DataFrame.

    Takes scores rather than a model so the training set is scored once and reused for both the
    global and the per-group calibration.

    ``percentile_approx`` in one ``groupBy`` rather than ``approxQuantile`` per group: the latter
    is a driver-side call and would mean one Spark job per group, which is the cost pattern this
    whole change exists to avoid.
    """
    if not baseline_by:
        return {}

    # Accuracy stated rather than left to the default, so the per-group calibration cannot drift away from
    # the global one above. `percentile_approx` expresses it as 1/relativeError.
    accuracy = int(round(1.0 / SEVERITY_QUANTILE_RELATIVE_ERROR))
    quantile_exprs = [
        F.percentile_approx(F.col("anomaly_score"), prob, accuracy).alias(key)
        for prob, key in zip(SCORE_QUANTILE_PROBS, SCORE_QUANTILE_KEYS, strict=True)
    ]
    # Read the key feature engineering already computed rather than rebuilding it from the raw
    # group columns, which this frame no longer has: it is the scored *engineered* frame, and
    # feature engineering drops the group columns before the model sees them.
    grouped = with_baseline_key(scored_df, baseline_by).groupBy(BASELINE_KEY_COLUMN).agg(*quantile_exprs)

    result: dict[str, dict[str, float]] = {}
    for row in grouped.collect():
        quantiles = {key: float(row[key]) for key in SCORE_QUANTILE_KEYS if row[key] is not None}
        # A group missing any quantile cannot be interpolated over, so it is left out entirely
        # and falls back to the global calibration rather than being half-calibrated.
        if len(quantiles) == len(SCORE_QUANTILE_KEYS):
            result[row[BASELINE_KEY_COLUMN]] = quantiles
    return result


def compute_baseline_statistics(train_df: DataFrame, columns: list[str]) -> dict[str, dict[str, float]]:
    """Compute baseline distribution statistics for drift detection.

    Args:
        train_df: Training DataFrame
        columns: Feature columns to compute statistics for

    Returns:
        Dictionary mapping column names to their baseline statistics
    """
    col_types = dict(train_df.dtypes)
    numeric_compatible_types = ["int", "long", "float", "double", "short", "byte", "boolean", "decimal"]
    eligible = [
        col_name
        for col_name in columns
        if col_types.get(col_name) and any(t in col_types[col_name].lower() for t in numeric_compatible_types)
    ]
    if not eligible:
        return {}

    # Two passes total regardless of column count: one combined aggregate select for
    # mean/std/min/max of every column, and one multi-column approxQuantile — instead of
    # two separate actions (full table scans) per column.
    def _col_expr(col_name: str) -> Column:
        return F.col(col_name).cast("double") if col_types[col_name] == "boolean" else F.col(col_name)

    agg_exprs = []
    for i, col_name in enumerate(eligible):
        expr = _col_expr(col_name)
        agg_exprs.extend(
            [
                F.mean(expr).alias(f"__mean_{i}"),
                F.stddev(expr).alias(f"__std_{i}"),
                F.min(expr).alias(f"__min_{i}"),
                F.max(expr).alias(f"__max_{i}"),
            ]
        )
    stats_row = train_df.select(*agg_exprs).first()
    if stats_row is None:
        raise ComputationError(f"Failed to compute stats for {eligible}")

    casted_df = train_df.select(*[_col_expr(c).alias(c) for c in eligible])
    all_quantiles = casted_df.approxQuantile(eligible, [0.25, 0.5, 0.75], 0.01)

    baseline_stats = {}
    for i, col_name in enumerate(eligible):
        quantiles = all_quantiles[i]
        if len(quantiles) != 3:
            raise ComputationError(f"Failed to compute quantiles for {col_name}")
        baseline_stats[col_name] = {
            "mean": stats_row[f"__mean_{i}"],
            "std": stats_row[f"__std_{i}"],
            "min": stats_row[f"__min_{i}"],
            "max": stats_row[f"__max_{i}"],
            "p25": quantiles[0],
            "p50": quantiles[1],
            "p75": quantiles[2],
        }

    return baseline_stats


def aggregate_ensemble_metrics(all_metrics: list[dict[str, float]]) -> dict[str, float]:
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
            if len(values) > 1:
                variance = sum((v - mean_value) ** 2 for v in values) / (len(values) - 1)
                aggregated[f"{key}_std"] = variance**0.5
            else:
                aggregated[f"{key}_std"] = 0.0

    return aggregated


def prepare_engineered_pandas(train_df: DataFrame, feature_metadata: SparkFeatureMetadata) -> pd.DataFrame:
    """Prepare engineered pandas DataFrame from Spark DataFrame.

    Applies feature engineering transformations, projects to the columns the model was fitted on, and
    collects to pandas. Used for MLflow signature inference.

    The projection is load-bearing. Feature engineering deliberately preserves columns it did not
    produce -- the group key among them, because the group-relative transform needs it at scoring time --
    so the engineered frame is wider than the feature list. Handing that frame to ``model.predict`` for
    signature inference passes the estimator a string column it was never fitted on, which fails for any
    grouped model on the single-model path (``profile="timeseries"``, or ``ensemble_size=1``). The default
    three-model ensemble registers by URI and never comes through here, which is why this was invisible.

    Args:
        train_df: Training Spark DataFrame
        feature_metadata: Feature engineering metadata from training

    Returns:
        Pandas DataFrame holding exactly the engineered feature columns, in their persisted order
    """
    engineered_train_df, _ = apply_feature_engineering_from_metadata(train_df, feature_metadata)
    return engineered_train_df.select(*feature_metadata.engineered_feature_names).toPandas()
