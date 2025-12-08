"""
Training pipeline for anomaly detection (Spark ML IsolationForest).
"""

from __future__ import annotations

import hashlib
import warnings
from dataclasses import asdict
from datetime import datetime
from typing import Iterable

import mlflow
from pyspark.ml import Pipeline
from pyspark.ml.classification import IsolationForest
from pyspark.ml.pipeline import PipelineModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

from databricks.labs.dqx.config import AnomalyConfig, AnomalyParams, IsolationForestConfig
from databricks.labs.dqx.errors import InvalidParameterError
from databricks.labs.dqx.anomaly.model_registry import AnomalyModelRecord, AnomalyModelRegistry
from pyspark.sql import types as T


DEFAULT_SAMPLE_FRACTION = 0.3
DEFAULT_MAX_ROWS = 1_000_000
DEFAULT_TRAIN_RATIO = 0.8


def train(
    df: DataFrame,
    columns: list[str],
    model_name: str | None = None,
    registry_table: str | None = None,
    params: AnomalyParams | None = None,
) -> str:
    """
    Train an anomaly detection model using Spark ML IsolationForest.

    Args:
        df: Input DataFrame containing historical \"normal\" data.
        columns: Columns to use for anomaly detection.
        model_name: Optional model name; auto-derived if not provided.
        registry_table: Optional registry table; auto-derived if not provided.
        params: Optional tuning parameters; defaults applied if omitted.

    Returns:
        model_uri logged in MLflow.
    """
    spark = df.sparkSession
    _validate_spark_version(spark)

    cfg = _build_config(columns, model_name, registry_table, params)
    _validate_columns(df, cfg.columns)

    derived_model_name = cfg.model_name or _derive_model_name(df, cfg.columns)
    derived_registry_table = cfg.registry_table or _derive_registry_table(df)

    sampled_df, sampled_count, truncated = _sample_df(df, cfg.columns, cfg.params)
    if sampled_count == 0:
        raise InvalidParameterError("Sampling produced 0 rows; provide more data or adjust params.")

    train_df, val_df = _train_validation_split(sampled_df, cfg.params)

    # Check if ensemble training is requested
    ensemble_size = cfg.params.ensemble_size if cfg.params and cfg.params.ensemble_size else 1
    
    if ensemble_size > 1:
        # Train ensemble
        model_uris, hyperparams, validation_metrics = _train_ensemble(
            train_df, val_df, cfg, ensemble_size, derived_model_name
        )
        model_uri = ",".join(model_uris)  # Store as comma-separated string
    else:
        # Train single model
        model, hyperparams = _fit_isolation_forest(train_df, cfg.params)
        
        contamination = cfg.params.algorithm_config.contamination if cfg.params and cfg.params.algorithm_config else 0.1
        validation_metrics = _compute_validation_metrics(model, val_df, contamination)
        
        mlflow.set_registry_uri("databricks-uc")
        with mlflow.start_run() as run:
            model_info = mlflow.spark.log_model(
                spark_model=model,
                artifact_path="model",
                registered_model_name=derived_model_name,
            )
            mlflow.log_params(_flatten_hyperparams(hyperparams))
            mlflow.log_metrics(validation_metrics)
            model_uri = model_info.model_uri
    
    # Compute baseline statistics for drift detection
    baseline_stats = _compute_baseline_statistics(train_df)
    
    # Compute feature importance for explainability (use first model if ensemble)
    if ensemble_size > 1:
        first_model = mlflow.spark.load_model(model_uris[0])
        feature_importance = _compute_feature_importance(first_model, val_df, cfg.columns)
    else:
        feature_importance = _compute_feature_importance(model, val_df, cfg.columns)
    
    if truncated:
        warnings.warn(
            f"Sampling capped at {cfg.params.max_rows} rows; model trained on truncated sample.",
            UserWarning,
            stacklevel=2,
        )

    # Log feature importance to MLflow if single model
    if ensemble_size == 1:
        mlflow.set_registry_uri("databricks-uc")
        with mlflow.start_run(run_id=mlflow.active_run().info.run_id):
            for col, importance in feature_importance.items():
                mlflow.log_param(f"feature_importance_{col}", importance)

    registry = AnomalyModelRegistry(spark)
    
    # Get MLflow run ID (from active run or last run in ensemble)
    if ensemble_size == 1:
        run_id = run.info.run_id
    else:
        run_id = mlflow.active_run().info.run_id if mlflow.active_run() else "ensemble"
    
    record = AnomalyModelRecord(
        model_name=derived_model_name,
        model_uri=model_uri,
        input_table=_get_input_table(df),
        columns=cfg.columns,
        algorithm=f"isolation_forest_ensemble_{ensemble_size}" if ensemble_size > 1 else "isolation_forest",
        hyperparameters=_stringify_dict(hyperparams),
        training_rows=train_df.count(),
        training_time=datetime.utcnow(),
        mlflow_run_id=run_id,
        metrics=validation_metrics,
        mode="spark",
        baseline_stats=baseline_stats,
        feature_importance=feature_importance,
        temporal_config=None,
    )
    registry.save_model(record, derived_registry_table)

    return model_uri


def _validate_spark_version(spark: SparkSession) -> None:
    major, minor, *_ = spark.version.split(".")
    if int(major) < 3 or (int(major) == 3 and int(minor) < 5):
        raise InvalidParameterError("Anomaly detection requires Spark >= 3.5 / DBR >= 15.4")


def _build_config(
    columns: list[str],
    model_name: str | None,
    registry_table: str | None,
    params: AnomalyParams | None,
) -> AnomalyConfig:
    return AnomalyConfig(
        columns=columns,
        model_name=model_name,
        registry_table=registry_table,
        params=params or AnomalyParams(),
    )


def _validate_columns(df: DataFrame, columns: Iterable[str]) -> None:
    schema = {f.name: f.dataType for f in df.schema.fields}
    missing = [c for c in columns if c not in schema]
    if missing:
        raise InvalidParameterError(f"Columns not found: {missing}")
    non_numeric = [c for c in columns if not isinstance(schema[c], T.NumericType)]
    if non_numeric:
        raise InvalidParameterError(f"Columns must be numeric for anomaly detection: {non_numeric}")


def _derive_model_name(df: DataFrame, columns: list[str]) -> str:
    input_table = _get_input_table(df)
    base = input_table.split(".")[-1]
    col_hash = hashlib.md5(",".join(sorted(columns)).encode("utf-8")).hexdigest()[:8]
    return f"{base}__{col_hash}__anomaly"


def _derive_registry_table(df: DataFrame) -> str:
    input_table = _get_input_table(df)
    parts = input_table.split(".")
    if len(parts) == 3:
        catalog, schema, _ = parts
    elif len(parts) == 2:
        raise InvalidParameterError(
            "Cannot infer registry table without catalog. Provide registry_table explicitly."
        )
    else:
        raise InvalidParameterError(
            "Input table must be schema.table or catalog.schema.table to derive registry_table."
        )
    return f"{catalog}.{schema}.dqx_anomaly_models"


def _get_input_table(df: DataFrame) -> str:
    if df.isStreaming:
        raise InvalidParameterError("Streaming DataFrames are not supported for training.")
    source = df.sql_ctx.conf.get("spark.sql.sources.tableName", "")
    if source:
        return source
    # Fall back to table identifier if available via lineage
    raise InvalidParameterError("Input table name could not be inferred; please provide registry_table explicitly.")


def _sample_df(df: DataFrame, columns: list[str], params: AnomalyParams) -> tuple[DataFrame, int, bool]:
    fraction = params.sample_fraction if params.sample_fraction is not None else DEFAULT_SAMPLE_FRACTION
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


def _fit_isolation_forest(train_df: DataFrame, params: AnomalyParams) -> tuple[PipelineModel, dict[str, Any]]:
    algo_cfg = params.algorithm_config or IsolationForestConfig()
    iso_forest = IsolationForest(
        featuresCol="features",
        predictionCol="prediction",
        anomalyScoreCol="anomaly_score",
        contamination=algo_cfg.contamination,
        numEstimators=algo_cfg.num_trees,
        randomSeed=algo_cfg.random_seed,
    )
    if algo_cfg.max_depth is not None:
        iso_forest = iso_forest.setMaxDepth(algo_cfg.max_depth)
    if algo_cfg.subsampling_rate is not None:
        iso_forest = iso_forest.setMaxSamples(algo_cfg.subsampling_rate)

    pipeline = Pipeline(stages=[VectorAssembler(inputCols=train_df.columns, outputCol="features"), iso_forest])
    model = pipeline.fit(train_df)

    hyperparams: dict[str, Any] = {
        "contamination": algo_cfg.contamination,
        "num_trees": algo_cfg.num_trees,
        "max_depth": algo_cfg.max_depth,
        "subsampling_rate": algo_cfg.subsampling_rate,
        "random_seed": algo_cfg.random_seed,
    }
    return model, hyperparams


def _compute_validation_metrics(
    model: PipelineModel, val_df: DataFrame, contamination: float
) -> dict[str, float]:
    """
    Compute comprehensive validation metrics including precision, recall, F1,
    threshold recommendations, and distribution statistics.
    """
    if val_df.rdd.isEmpty():
        return {"validation_rows": 0}

    scored = model.transform(val_df)
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
    
    # Ground truth labels: top contamination% are anomalies
    threshold_for_labels = scores_df.approxQuantile("score", [1 - contamination], 0.01)[0]
    labeled_df = scores_df.withColumn(
        "true_label",
        F.when(F.col("score") >= F.lit(threshold_for_labels), 1).otherwise(0)
    )
    
    # Compute precision/recall/F1 for multiple thresholds
    test_thresholds = [0.3, 0.5, 0.7, 0.9]
    threshold_metrics = {}
    best_f1 = 0.0
    best_threshold = 0.5
    
    for threshold in test_thresholds:
        pred_df = labeled_df.withColumn(
            "pred_label",
            F.when(F.col("score") >= F.lit(threshold), 1).otherwise(0)
        )
        
        # Confusion matrix
        tp = pred_df.filter((F.col("pred_label") == 1) & (F.col("true_label") == 1)).count()
        fp = pred_df.filter((F.col("pred_label") == 1) & (F.col("true_label") == 0)).count()
        tn = pred_df.filter((F.col("pred_label") == 0) & (F.col("true_label") == 0)).count()
        fn = pred_df.filter((F.col("pred_label") == 0) & (F.col("true_label") == 1)).count()
        
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0
        
        threshold_metrics[f"threshold_{int(threshold*100)}_precision"] = precision
        threshold_metrics[f"threshold_{int(threshold*100)}_recall"] = recall
        threshold_metrics[f"threshold_{int(threshold*100)}_f1"] = f1
        
        if f1 > best_f1:
            best_f1 = f1
            best_threshold = threshold
    
    # Estimated contamination (percentage of scores above best threshold)
    estimated_contamination = labeled_df.filter(
        F.col("score") >= F.lit(best_threshold)
    ).count() / val_count
    
    metrics = {
        "validation_rows": val_count,
        "score_mean": stats["mean"],
        "score_std": stats["std"],
        "score_skewness": stats["skewness"],
        "score_p10": quantiles[0],
        "score_p25": quantiles[1],
        "score_p50": quantiles[2],
        "score_p75": quantiles[3],
        "score_p90": quantiles[4],
        "recommended_threshold": best_threshold,
        "recommended_threshold_f1": best_f1,
        "estimated_contamination": estimated_contamination,
    }
    
    # Add threshold-specific metrics
    metrics.update(threshold_metrics)
    
    return metrics


def _compute_baseline_statistics(train_df: DataFrame) -> dict[str, dict[str, float]]:
    """
    Compute baseline distribution statistics for each column in training data.
    Used later for drift detection.
    """
    baseline_stats = {}
    
    for col_name in train_df.columns:
        col_stats = train_df.select(
            F.mean(col_name).alias("mean"),
            F.stddev(col_name).alias("std"),
            F.min(col_name).alias("min"),
            F.max(col_name).alias("max"),
        ).first()
        
        quantiles = train_df.approxQuantile(col_name, [0.25, 0.5, 0.75], 0.01)
        
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


def _compute_feature_importance(
    model: PipelineModel, val_df: DataFrame, columns: list[str]
) -> dict[str, float]:
    """
    Compute global feature importance using permutation importance.
    Measures how much each feature contributes to anomaly detection.
    """
    if val_df.rdd.isEmpty():
        return {}
    
    # Baseline scores
    baseline_scored = model.transform(val_df)
    baseline_avg_score = baseline_scored.select(F.mean("anomaly_score")).first()[0]
    
    importance = {}
    
    for col in columns:
        # Shuffle this column's values
        shuffled_df = val_df.withColumn(
            col,
            F.expr(f"array_sort(collect_list({col}) over ())[cast(rand() * count(*) over () as int)]")
        )
        
        # Compute scores with shuffled column
        shuffled_scored = model.transform(shuffled_df)
        shuffled_avg_score = shuffled_scored.select(F.mean("anomaly_score")).first()[0]
        
        # Importance = increase in average score when feature is random
        importance[col] = abs(shuffled_avg_score - baseline_avg_score)
    
    # Normalize to sum to 1.0
    total = sum(importance.values())
    if total > 0:
        importance = {k: v / total for k, v in importance.items()}
    
    return importance


def _train_ensemble(
    train_df: DataFrame,
    val_df: DataFrame,
    cfg: AnomalyConfig,
    ensemble_size: int,
    model_name: str,
) -> tuple[list[str], dict[str, Any], dict[str, float]]:
    """
    Train ensemble of models with different random seeds.
    
    Returns:
        Tuple of (model_uris, hyperparams, aggregated_metrics).
    """
    model_uris = []
    all_metrics = []
    
    mlflow.set_registry_uri("databricks-uc")
    
    for i in range(ensemble_size):
        # Create modified params with different seed
        modified_params = cfg.params or AnomalyParams()
        algo_cfg = modified_params.algorithm_config or IsolationForestConfig()
        algo_cfg.random_seed = algo_cfg.random_seed + i
        
        # Train model
        model, hyperparams = _fit_isolation_forest(train_df, modified_params)
        
        # Compute metrics
        contamination = algo_cfg.contamination
        metrics = _compute_validation_metrics(model, val_df, contamination)
        all_metrics.append(metrics)
        
        # Log to MLflow
        with mlflow.start_run(run_name=f"{model_name}_ensemble_{i}"):
            model_info = mlflow.spark.log_model(
                spark_model=model,
                artifact_path="model",
                registered_model_name=f"{model_name}_ensemble_{i}",
            )
            mlflow.log_params(_flatten_hyperparams(hyperparams))
            mlflow.log_metrics(metrics)
            mlflow.log_param("ensemble_index", i)
            mlflow.log_param("ensemble_size", ensemble_size)
            
            model_uris.append(model_info.model_uri)
    
    # Aggregate metrics (average across ensemble)
    aggregated_metrics = {}
    if all_metrics:
        metric_keys = all_metrics[0].keys()
        for key in metric_keys:
            values = [m[key] for m in all_metrics if key in m]
            if values:
                aggregated_metrics[key] = sum(values) / len(values)
                aggregated_metrics[f"{key}_std"] = (
                    sum((v - aggregated_metrics[key]) ** 2 for v in values) / len(values)
                ) ** 0.5 if len(values) > 1 else 0.0
    
    return model_uris, hyperparams, aggregated_metrics


def _flatten_hyperparams(hyperparams: dict[str, Any]) -> dict[str, Any]:
    return {f"hyperparam_{k}": v for k, v in hyperparams.items() if v is not None}


def _stringify_dict(data: dict[str, Any]) -> dict[str, str]:
    return {k: str(v) for k, v in data.items() if v is not None}

