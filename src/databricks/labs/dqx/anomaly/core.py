"""Core ML operations for anomaly detection.

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
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import DoubleType, IntegerType, StructField, StructType
from sklearn.ensemble import IsolationForest
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import RobustScaler

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


def fit_sklearn_model(train_pandas: pd.DataFrame, params: AnomalyParams) -> tuple[Pipeline, dict[str, Any]]:
    """Train sklearn IsolationForest pipeline on pre-engineered pandas DataFrame.

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


def fit_isolation_forest(
    train_df: DataFrame, feature_columns: list[str], params: AnomalyParams
) -> tuple[Pipeline, dict[str, Any], SparkFeatureMetadata]:
    """Train IsolationForest model with distributed feature engineering.

    Feature engineering runs on Spark, then the model trains on the driver.

    Returns:
        - pipeline: sklearn Pipeline (RobustScaler + IsolationForest)
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
    column_infos = reconstruct_column_infos(feature_metadata)

    engineered_df, updated_metadata = apply_feature_engineering(
        df.select(*feature_cols),
        column_infos,
        categorical_cardinality_threshold=feature_metadata.categorical_cardinality_threshold,
        frequency_maps=feature_metadata.categorical_frequency_maps,
        onehot_categories=feature_metadata.onehot_categories,
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
    column_infos = reconstruct_column_infos(feature_metadata)

    engineered_df, updated_metadata = apply_feature_engineering(
        df.select(*feature_cols),
        column_infos,
        categorical_cardinality_threshold=feature_metadata.categorical_cardinality_threshold,
        frequency_maps=feature_metadata.categorical_frequency_maps,
        onehot_categories=feature_metadata.onehot_categories,
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
    if val_df.count() == 0:
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

    return {k: v for k, v in metrics.items() if v is not None}


def compute_score_quantiles(
    model: Pipeline, df: DataFrame, feature_cols: list[str], feature_metadata: SparkFeatureMetadata
) -> dict[str, float]:
    """Compute score quantiles from the training score distribution."""
    if df.count() == 0:
        return {}

    scored = score_with_model(model, df, feature_cols, feature_metadata)
    scores_df = scored.select(F.col("anomaly_score").alias("score"))
    quantiles = scores_df.approxQuantile("score", SCORE_QUANTILE_PROBS, 0.01)

    return dict(zip(SCORE_QUANTILE_KEYS, quantiles, strict=False))


def compute_score_quantiles_ensemble(
    models: list[Pipeline], df: DataFrame, feature_cols: list[str], feature_metadata: SparkFeatureMetadata
) -> dict[str, float]:
    """Compute score quantiles using ensemble mean scores."""
    if df.count() == 0:
        return {}

    scored = score_with_ensemble_models(models, df, feature_cols, feature_metadata)
    scores_df = scored.select(F.col("anomaly_score").alias("score"))
    quantiles = scores_df.approxQuantile("score", SCORE_QUANTILE_PROBS, 0.01)

    return dict(zip(SCORE_QUANTILE_KEYS, quantiles, strict=False))


def compute_baseline_statistics(train_df: DataFrame, columns: list[str]) -> dict[str, dict[str, float]]:
    """Compute baseline distribution statistics for drift detection.

    Args:
        train_df: Training DataFrame
        columns: Feature columns to compute statistics for

    Returns:
        Dictionary mapping column names to their baseline statistics
    """
    baseline_stats = {}
    col_types = dict(train_df.dtypes)

    for col_name in columns:
        col_type = col_types.get(col_name)
        if not col_type:
            continue

        numeric_compatible_types = ["int", "long", "float", "double", "short", "byte", "boolean", "decimal"]
        if not any(t in col_type.lower() for t in numeric_compatible_types):
            continue

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


# =============================================================================
# Feature Engineering Helpers
# =============================================================================


def prepare_engineered_pandas(train_df: DataFrame, feature_metadata: SparkFeatureMetadata) -> pd.DataFrame:
    """Prepare engineered pandas DataFrame from Spark DataFrame.

    Applies feature engineering transformations and collects to pandas.
    Used for MLflow signature inference.

    Args:
        train_df: Training Spark DataFrame
        feature_metadata: Feature engineering metadata from training

    Returns:
        Pandas DataFrame with engineered features
    """
    column_infos_reconstructed = reconstruct_column_infos(feature_metadata)
    engineered_train_df, _ = apply_feature_engineering(
        train_df,
        column_infos_reconstructed,
        categorical_cardinality_threshold=20,
        frequency_maps=feature_metadata.categorical_frequency_maps,
        onehot_categories=feature_metadata.onehot_categories,
    )
    return engineered_train_df.toPandas()
