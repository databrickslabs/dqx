"""
Training pipeline for anomaly detection using scikit-learn IsolationForest.

Architecture:
- Training: scikit-learn on driver (efficient for sampled data ≤1M rows)
- Scoring: Distributed across Spark cluster via pandas UDFs
- Everything else: Distributed on Spark (sampling, splits, metrics, drift detection)
"""

from __future__ import annotations

import hashlib
import warnings
from dataclasses import asdict
from datetime import datetime
from typing import Any, Iterable

import mlflow
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import pandas_udf, PandasUDFType
import pyspark.sql.functions as F

from databricks.labs.dqx.config import AnomalyConfig, AnomalyParams, IsolationForestConfig
from databricks.labs.dqx.errors import InvalidParameterError
from databricks.labs.dqx.anomaly.model_registry import AnomalyModelRecord, AnomalyModelRegistry
from databricks.labs.dqx.anomaly.profiler import auto_discover
from pyspark.sql import types as T


DEFAULT_SAMPLE_FRACTION = 0.3
DEFAULT_MAX_ROWS = 1_000_000
DEFAULT_TRAIN_RATIO = 0.8


def train(
    df: DataFrame,
    columns: list[str] | None = None,
    segment_by: list[str] | None = None,
    model_name: str | None = None,
    registry_table: str | None = None,
    params: AnomalyParams | None = None,
    profiler_table: str | None = None,
) -> str:
    """
    Train anomaly detection model(s) with intelligent auto-discovery.

    Requires Spark >= 3.4 and the 'anomaly' extras installed:
        pip install 'databricks-labs-dqx[anomaly]'

    Auto-discovery behavior:
    - columns=None, segment_by=None: Auto-discovers both (simplest)
    - columns specified, segment_by=None: Uses columns, no segmentation
    - columns=None, segment_by specified: Auto-discovers columns, uses segments

    Args:
        df: Input DataFrame containing historical \"normal\" data.
        columns: Columns to use for anomaly detection (auto-discovered if omitted).
        segment_by: Segment columns (auto-discovered if both columns and segment_by omitted).
        model_name: Optional model name; auto-derived if not provided.
        registry_table: Optional registry table; auto-derived if not provided.
        params: Optional tuning parameters; defaults applied if omitted.
        profiler_table: DQX profiler output table for smarter auto-discovery.

    Returns:
        model_uri logged in MLflow (comma-separated for segmented models).
    """
    spark = df.sparkSession
    _validate_spark_version(spark)

    # Auto-discovery
    if columns is None:
        profile = auto_discover(df, profiler_output_table=profiler_table)
        columns = profile.recommended_columns
        
        # Auto-detect segments ONLY if segment_by also not provided
        if segment_by is None:
            segment_by = profile.recommended_segments
        
        # Print what was discovered
        print(f"Auto-selected {len(columns)} columns: {columns}")
        if segment_by:
            print(f"Auto-detected {len(segment_by)} segment columns: {segment_by} "
                  f"({profile.segment_count} total segments)")
        
        # Show warnings
        for warning in profile.warnings:
            warnings.warn(warning, UserWarning, stacklevel=2)
    
    # Validate columns
    if not columns:
        raise InvalidParameterError("No columns provided or auto-discovered. Provide columns explicitly.")

    _validate_columns(df, columns)

    derived_model_name = model_name or _derive_model_name(df, columns)
    derived_registry_table = registry_table or _derive_registry_table(df)

    # Segment-based training
    if segment_by:
        return _train_segmented(
            df, columns, segment_by, derived_model_name, derived_registry_table, params
        )
    else:
        return _train_global(
            df, columns, derived_model_name, derived_registry_table, params
        )


def _train_global(
    df: DataFrame,
    columns: list[str],
    model_name: str,
    registry_table: str,
    params: AnomalyParams | None,
) -> str:
    """Train a single global model (no segmentation)."""
    spark = df.sparkSession
    params = params or AnomalyParams()

    sampled_df, sampled_count, truncated = _sample_df(df, columns, params)
    if sampled_count == 0:
        raise InvalidParameterError("Sampling produced 0 rows; provide more data or adjust params.")

    train_df, val_df = _train_validation_split(sampled_df, params)

    # Check if ensemble training is requested
    ensemble_size = params.ensemble_size if params and params.ensemble_size else 1
    
    run_id = None  # Initialize to avoid NameError
    
    if ensemble_size > 1:
        # Train ensemble
        model_uris, hyperparams, validation_metrics = _train_ensemble(
            train_df, val_df, columns, params, ensemble_size, model_name
        )
        model_uri = ",".join(model_uris)  # Store as comma-separated string
        run_id = "ensemble"  # Placeholder for ensemble runs (multiple MLflow runs)
    else:
        # Train single model (sklearn IsolationForest on driver)
        model, hyperparams = _fit_isolation_forest(train_df, params)
        
        contamination = params.algorithm_config.contamination if params and params.algorithm_config else 0.1
        validation_metrics = _compute_validation_metrics(model, val_df, columns, contamination)
        
        # Register model to Unity Catalog
        # Note: When running outside Databricks (e.g., local Spark Connect), you may see a warning:
        # "Unable to get model version source run's workspace ID from request headers"
        # This is expected and informational only - the model will register successfully
        mlflow.set_registry_uri("databricks-uc")
        with mlflow.start_run() as run:
            # Infer model signature for Unity Catalog (required)
            import pandas as pd
            from mlflow.models import infer_signature
            train_pandas = train_df.toPandas()
            predictions = model.predict(train_pandas.values)
            signature = infer_signature(train_pandas, predictions)
            
            # Log scikit-learn model with signature
            model_info = mlflow.sklearn.log_model(
                sk_model=model,
                artifact_path="model",
                registered_model_name=model_name,
                signature=signature,
            )
            mlflow.log_params(_flatten_hyperparams(hyperparams))
            mlflow.log_metrics(validation_metrics)
            
            model_uri = model_info.model_uri
            run_id = run.info.run_id
    
    # Compute baseline statistics for drift detection (distributed on Spark)
    baseline_stats = _compute_baseline_statistics(train_df)
    
    # Compute feature importance for explainability (distributed on Spark, use first model if ensemble)
    if ensemble_size > 1:
        first_model = mlflow.sklearn.load_model(model_uris[0])
        feature_importance = _compute_feature_importance(first_model, val_df, columns)
    else:
        feature_importance = _compute_feature_importance(model, val_df, columns)
    
    if truncated:
        warnings.warn(
            f"Sampling capped at {params.max_rows} rows; model trained on truncated sample.",
            UserWarning,
            stacklevel=2,
        )

    registry = AnomalyModelRegistry(spark)
    
    # run_id was already saved during model logging
    
    record = AnomalyModelRecord(
        model_name=model_name,
        model_uri=model_uri,
        input_table=_get_input_table(df),
        columns=columns,
        algorithm=f"IsolationForest_Ensemble_{ensemble_size}" if ensemble_size > 1 else "IsolationForest",
        hyperparameters=_stringify_dict(hyperparams),
        training_rows=train_df.count(),
        training_time=datetime.utcnow(),
        mlflow_run_id=run_id,
        metrics=validation_metrics,
        mode="spark",
        baseline_stats=baseline_stats,
        feature_importance=feature_importance,
        temporal_config=None,
        segment_by=None,
        segment_values=None,
        is_global_model=True,
    )
    registry.save_model(record, registry_table)

    return model_uri


def _train_segmented(
    df: DataFrame,
    columns: list[str],
    segment_by: list[str],
    base_model_name: str,
    registry_table: str,
    params: AnomalyParams | None,
) -> str:
    """Train separate models for each segment."""
    params = params or AnomalyParams()
    spark = df.sparkSession
    
    # Get distinct segments
    segments_df = df.select(*segment_by).distinct()
    segments = [row.asDict() for row in segments_df.collect()]
    
    # Validate segment count
    if len(segments) > 100:
        warnings.warn(
            f"Training {len(segments)} segments may be slow. "
            "Consider coarser segmentation or explicit segment_by.",
            UserWarning,
            stacklevel=2,
        )
    
    model_uris = []
    
    # Train each segment
    for i, segment_vals in enumerate(segments):
        segment_name = "_".join(f"{k}={v}" for k, v in segment_vals.items())
        print(f"Training segment {i+1}/{len(segments)}: {segment_name}")
        
        # Filter to this segment
        segment_df = df
        for col, val in segment_vals.items():
            segment_df = segment_df.filter(F.col(col) == val)
        
        # Validate segment size
        segment_size = segment_df.count()
        if segment_size < 1000:
            warnings.warn(
                f"Segment {segment_name} has only {segment_size} rows, "
                "model may be unreliable.",
                UserWarning,
                stacklevel=2,
            )
        
        # Derive segment-specific model name
        segment_model_name = f"{base_model_name}__seg_{segment_name}"
        
        # Train model for this segment (reuse _train_global logic)
        model_uri = _train_single_segment(
            segment_df, columns, segment_model_name, segment_vals, segment_by, registry_table, params
        )
        model_uris.append(model_uri)
    
    # Return comma-separated model URIs
    return ",".join(model_uris)


def _train_single_segment(
    df: DataFrame,
    columns: list[str],
    model_name: str,
    segment_values: dict[str, Any],
    segment_by: list[str],
    registry_table: str,
    params: AnomalyParams,
) -> str:
    """Train a model for a single segment."""
    spark = df.sparkSession

    sampled_df, sampled_count, truncated = _sample_df(df, columns, params)
    if sampled_count == 0:
        warnings.warn(
            f"Segment {segment_values} produced 0 rows after sampling; skipping.",
            UserWarning,
            stacklevel=2,
        )
        return ""

    train_df, val_df = _train_validation_split(sampled_df, params)

    # Train single model (no ensemble for segments to reduce complexity)
    model, hyperparams = _fit_isolation_forest(train_df, params)
    
    contamination = params.algorithm_config.contamination if params and params.algorithm_config else 0.1
    validation_metrics = _compute_validation_metrics(model, val_df, columns, contamination)
    
    # Register model to Unity Catalog
    mlflow.set_registry_uri("databricks-uc")
    with mlflow.start_run() as run:
        # Infer model signature for Unity Catalog (required)
        import pandas as pd
        from mlflow.models import infer_signature
        train_pandas = train_df.toPandas()
        predictions = model.predict(train_pandas.values)
        signature = infer_signature(train_pandas, predictions)
        
        # Log scikit-learn model with signature
        model_info = mlflow.sklearn.log_model(
            sk_model=model,
            artifact_path="model",
            registered_model_name=model_name,
            signature=signature,
        )
        mlflow.log_params(_flatten_hyperparams(hyperparams))
        mlflow.log_metrics(validation_metrics)
        
        model_uri = model_info.model_uri
        run_id = run.info.run_id
    
    # Compute baseline statistics for drift detection
    baseline_stats = _compute_baseline_statistics(train_df)
    
    # Compute feature importance for explainability
    feature_importance = _compute_feature_importance(model, val_df, columns)
    
    registry = AnomalyModelRegistry(spark)
    
    record = AnomalyModelRecord(
        model_name=model_name,
        model_uri=model_uri,
        input_table=_get_input_table(df),
        columns=columns,
        algorithm="IsolationForest",
        hyperparameters=_stringify_dict(hyperparams),
        training_rows=train_df.count(),
        training_time=datetime.utcnow(),
        mlflow_run_id=run_id,
        metrics=validation_metrics,
        mode="spark",
        baseline_stats=baseline_stats,
        feature_importance=feature_importance,
        temporal_config=None,
        segment_by=segment_by,
        segment_values=_stringify_dict(segment_values),
        is_global_model=False,
    )
    registry.save_model(record, registry_table)

    return model_uri


def _validate_spark_version(spark: SparkSession) -> None:
    major, minor, *_ = spark.version.split(".")
    if int(major) < 3 or (int(major) == 3 and int(minor) < 4):
        raise InvalidParameterError(
            "Anomaly detection requires Spark >= 3.4 for SynapseML compatibility. "
            "Found Spark {}.{}.".format(major, minor)
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
    if input_table:
        base = input_table.split(".")[-1]
    else:
        # Fallback when input table name cannot be inferred
        base = "unknown_table"
    col_hash = hashlib.md5(",".join(sorted(columns)).encode("utf-8")).hexdigest()[:8]
    return f"{base}__{col_hash}__anomaly"


def _derive_registry_table(df: DataFrame) -> str:
    input_table = _get_input_table(df)
    if not input_table:
        # Fallback to default location when table name cannot be inferred
        # Use current catalog and schema
        spark = df.sparkSession
        try:
            current_catalog = spark.sql("SELECT current_catalog()").first()[0]
            current_schema = spark.sql("SELECT current_schema()").first()[0]
            return f"{current_catalog}.{current_schema}.dqx_anomaly_models"
        except Exception:
            raise InvalidParameterError(
                "Cannot infer registry table name from DataFrame and no current catalog/schema set. "
                "Provide registry_table explicitly (e.g., 'catalog.schema.dqx_anomaly_models')."
            )
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


def _get_input_table(df: DataFrame) -> str | None:
    """
    Attempt to get the input table name from DataFrame.
    
    Returns None if table name cannot be inferred (e.g., temporary DataFrames, Spark Connect).
    """
    if df.isStreaming:
        raise InvalidParameterError("Streaming DataFrames are not supported for training.")
    # Note: Spark Connect doesn't support sql_ctx, so we can't reliably get table name
    # Return None for DataFrames created programmatically
    return None


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


def _fit_isolation_forest(train_df: DataFrame, params: AnomalyParams) -> tuple[Any, dict[str, Any]]:
    """
    Train scikit-learn IsolationForest on driver, returning a model and hyperparameters.
    
    Training happens on the driver node with collected data (already sampled to <=1M rows).
    Scoring will be distributed via pandas UDF.
    """
    try:
        from sklearn.ensemble import IsolationForest
        import pandas as pd
        import numpy as np
    except ImportError as e:
        raise InvalidParameterError(
            "IsolationForest requires scikit-learn. Install with: pip install 'databricks-labs-dqx[anomaly]'"
        ) from e
    
    algo_cfg = params.algorithm_config or IsolationForestConfig()
    
    # Collect training data to driver (distributed sampling already done)
    # This is efficient because we've already capped at DEFAULT_MAX_ROWS (1M)
    train_pandas = train_df.toPandas()
    
    # Scikit-learn IsolationForest configuration
    iso_forest = IsolationForest(
        contamination=algo_cfg.contamination,
        n_estimators=algo_cfg.num_trees,
        max_samples=algo_cfg.subsampling_rate if algo_cfg.subsampling_rate else 'auto',
        max_features=1.0,  # Use all features by default
        bootstrap=False,  # Consistent with typical anomaly detection settings
        random_state=algo_cfg.random_seed,
        n_jobs=-1,  # Use all CPU cores on driver for parallel tree training
    )
    
    # Fit on training data
    iso_forest.fit(train_pandas.values)
    
    hyperparams: dict[str, Any] = {
        "contamination": algo_cfg.contamination,
        "num_trees": algo_cfg.num_trees,
        "max_samples": algo_cfg.subsampling_rate,
        "random_seed": algo_cfg.random_seed,
    }
    
    return iso_forest, hyperparams


def _score_with_model(model: Any, df: DataFrame, feature_cols: list[str]) -> DataFrame:
    """
    Score DataFrame using scikit-learn model with distributed pandas UDF.
    
    This enables distributed inference across the Spark cluster.
    Works with both regular Spark and Spark Connect.
    """
    import pandas as pd
    import cloudpickle
    from pyspark.sql.functions import pandas_udf, struct, col
    from pyspark.sql.types import DoubleType, IntegerType, StructType, StructField
    
    # Serialize model (will be captured in UDF closure)
    model_bytes = cloudpickle.dumps(model)
    
    # Define schema for UDF output (nullable=True to match pandas behavior)
    schema = StructType([
        StructField("anomaly_score", DoubleType(), True),
        StructField("prediction", IntegerType(), True),
    ])
    
    @pandas_udf(schema, PandasUDFType.SCALAR)
    def predict_udf(*cols):
        """Pandas UDF for distributed scoring (Spark Connect compatible)."""
        import cloudpickle
        import pandas as pd
        import numpy as np
        
        # Deserialize model from closure (works with Spark Connect)
        model_local = cloudpickle.loads(model_bytes)
        
        # Convert input columns to numpy array
        X = pd.concat(cols, axis=1).values
        
        # Score samples (negative scores, higher = more anomalous)
        scores = -model_local.score_samples(X)  # Negate to make higher = more anomalous
        
        # Predict labels (1 = anomaly, 0 = normal)
        predictions = model_local.predict(X)
        predictions = np.where(predictions == -1, 1, 0)  # Convert sklearn -1/1 to 0/1
        
        return pd.DataFrame({
            "anomaly_score": scores,
            "prediction": predictions
        })
    
    # Apply UDF to all feature columns
    result = df.withColumn("_scores", predict_udf(*[col(c) for c in feature_cols]))
    result = result.select("*", "_scores.anomaly_score", "_scores.prediction").drop("_scores")
    
    return result


def _compute_validation_metrics(
    model: Any, val_df: DataFrame, feature_cols: list[str], contamination: float
) -> dict[str, float]:
    """
    Compute comprehensive validation metrics including precision, recall, F1,
    threshold recommendations, and distribution statistics.
    
    Uses distributed scoring via pandas UDF.
    """
    if val_df.count() == 0:
        return {"validation_rows": 0}

    scored = _score_with_model(model, val_df, feature_cols)
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
        "score_mean": stats["mean"] or 0.0,
        "score_std": stats["std"] or 0.0,
        "score_skewness": stats["skewness"] or 0.0,
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
    
    # Filter out None values (MLflow doesn't accept them)
    metrics = {k: v for k, v in metrics.items() if v is not None}
    
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
    model: Any, val_df: DataFrame, columns: list[str]
) -> dict[str, float]:
    """
    Compute global feature importance using permutation importance.
    Measures how much each feature contributes to anomaly detection.
    
    Uses distributed scoring via pandas UDF for efficient computation across cluster.
    """
    if val_df.count() == 0:
        return {}
    
    # Baseline scores (distributed scoring)
    baseline_scored = _score_with_model(model, val_df, columns)
    baseline_avg_score = baseline_scored.select(F.mean("anomaly_score")).first()[0]
    
    importance = {}
    
    # Permutation importance: shuffle each column and measure impact (distributed on Spark)
    for col in columns:
        # Shuffle this column's values across rows
        # Use shuffle() to randomize and element_at() to access a random element
        shuffled_df = val_df.withColumn(
            col,
            F.expr(f"element_at(shuffle(collect_list({col}) over ()), 1)")
        )
        
        # Compute scores with shuffled column (distributed scoring)
        shuffled_scored = _score_with_model(model, shuffled_df, columns)
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
    columns: list[str],
    params: AnomalyParams,
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
    
    # Register models to Unity Catalog
    # Note: When running outside Databricks, you may see warnings about workspace ID headers
    # This is expected and informational only - models will register successfully
    mlflow.set_registry_uri("databricks-uc")
    
    for i in range(ensemble_size):
        # Create modified params with different seed (deep copy to avoid mutation)
        from copy import deepcopy
        base_params = params or AnomalyParams()
        modified_params = deepcopy(base_params)
        modified_params.algorithm_config.random_seed = base_params.algorithm_config.random_seed + i
        
        # Train model (sklearn IsolationForest on driver)
        model, hyperparams = _fit_isolation_forest(train_df, modified_params)
        
        # Compute metrics (distributed scoring on Spark)
        contamination = modified_params.algorithm_config.contamination
        metrics = _compute_validation_metrics(model, val_df, columns, contamination)
        all_metrics.append(metrics)
        
        # Log to MLflow
        with mlflow.start_run(run_name=f"{model_name}_ensemble_{i}"):
            # Infer model signature for Unity Catalog (required)
            import pandas as pd
            from mlflow.models import infer_signature
            train_pandas = train_df.toPandas()
            predictions = model.predict(train_pandas.values)
            signature = infer_signature(train_pandas, predictions)
            
            # Log scikit-learn model for this ensemble member
            model_info = mlflow.sklearn.log_model(
                sk_model=model,
                artifact_path="model",
                registered_model_name=f"{model_name}_ensemble_{i}",
                signature=signature,
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

