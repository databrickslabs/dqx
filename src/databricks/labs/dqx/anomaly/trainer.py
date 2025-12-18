"""
Training pipeline for anomaly detection using scikit-learn IsolationForest.

Architecture:
- Training: scikit-learn on driver (efficient for sampled data ≤1M rows)
- Scoring: Distributed across Spark cluster via pandas UDFs
- Everything else: Distributed on Spark (sampling, splits, metrics, drift detection)
"""

from __future__ import annotations

import warnings
from datetime import datetime
from typing import Any, Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import PandasUDFType
import pyspark.sql.functions as F

from databricks.labs.dqx.config import AnomalyParams, IsolationForestConfig
from databricks.labs.dqx.errors import InvalidParameterError
from databricks.labs.dqx.anomaly.model_registry import AnomalyModelRecord, AnomalyModelRegistry
from databricks.labs.dqx.telemetry import get_tables_from_spark_plan
from databricks.labs.dqx.anomaly.profiler import auto_discover


DEFAULT_SAMPLE_FRACTION = 0.3
DEFAULT_MAX_ROWS = 1_000_000
DEFAULT_TRAIN_RATIO = 0.8


def train(
    df: DataFrame,
    model_name: str,
    columns: list[str] | None = None,
    segment_by: list[str] | None = None,
    registry_table: str | None = None,
    params: AnomalyParams | None = None,
    profiler_table: str | None = None,
    exclude_columns: list[str] | None = None,
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
        model_name: Model name (REQUIRED). Provide a descriptive name like 'field_force_anomaly'.
                   Can be simple name ('my_model') or full path ('catalog.schema.my_model').
                   If simple name provided, catalog.schema will be derived from registry_table.
        columns: Columns to use for anomaly detection (auto-discovered if omitted).
        segment_by: Segment columns (auto-discovered if both columns and segment_by omitted).
        registry_table: Optional registry table; auto-derived if not provided.
        params: Optional tuning parameters; defaults applied if omitted.
        profiler_table: DQX profiler output table for smarter auto-discovery.
        exclude_columns: Columns to exclude from training (e.g., IDs, labels, ground truth).
                        Useful with auto-discovery to filter out unwanted columns without
                        specifying all desired columns manually.

    Returns:
        Base model name (e.g., 'catalog.schema.model_name'). For segmented models,
        individual segments are stored with suffixes like '__seg_region=APAC', but
        the base name is returned for simplified API usage.

    Examples:
        # Simple global model with auto-discovery
        model_name = train(df, model_name="my_anomaly_model")

        # Auto-discovery but exclude ID and ground truth columns
        model_name = train(df, model_name="my_model",
                          exclude_columns=["id", "label", "ground_truth"])

        # Segmented by region
        model_name = train(df, model_name="regional_model", segment_by=["region"])

        # With explicit columns
        model_name = train(df, model_name="sales_monitor",
                          columns=["revenue", "transactions", "avg_order_value"])
    """
    spark = df.sparkSession
    _validate_spark_version(spark)

    # Validate model_name is provided
    if not model_name:
        raise InvalidParameterError(
            "model_name is required. Provide a descriptive name like 'field_force_anomaly' or "
            "'sales_rep_monitor'. The full catalog.schema.model path will be constructed automatically."
        )

    # Process exclude_columns
    exclude_columns = exclude_columns or []
    if exclude_columns:
        # Validate that exclude_columns exist in DataFrame
        df_columns = set(df.columns)
        invalid_excludes = [col for col in exclude_columns if col not in df_columns]
        if invalid_excludes:
            raise InvalidParameterError(f"exclude_columns contains columns not in DataFrame: {invalid_excludes}")

        # If columns are explicitly provided, validate they don't overlap with exclude_columns
        if columns is not None:
            overlap = set(columns) & set(exclude_columns)
            if overlap:
                raise InvalidParameterError(
                    f"columns and exclude_columns overlap: {overlap}. " "Remove overlapping columns from either list."
                )

        # Filter DataFrame for auto-discovery (exclude unwanted columns)
        if columns is None:
            remaining_columns = [col for col in df.columns if col not in exclude_columns]
            df_filtered = df.select(*remaining_columns)
            print(f"Excluding {len(exclude_columns)} columns from auto-discovery: {exclude_columns}")
        else:
            df_filtered = df  # No need to filter if columns explicitly provided
    else:
        df_filtered = df

    # Auto-discovery
    if columns is None:
        profile = auto_discover(df_filtered, profiler_output_table=profiler_table)
        columns = profile.recommended_columns

        # Auto-detect segments ONLY if segment_by also not provided
        if segment_by is None:
            segment_by = profile.recommended_segments

        # Print what was discovered
        print(f"Auto-selected {len(columns)} columns: {columns}")
        if segment_by:
            print(
                f"Auto-detected {len(segment_by)} segment columns: {segment_by} "
                f"({profile.segment_count} total segments)"
            )

        # Show warnings
        for warning in profile.warnings:
            warnings.warn(warning, UserWarning, stacklevel=2)

    # Validate columns
    if not columns:
        raise InvalidParameterError("No columns provided or auto-discovered. Provide columns explicitly.")

    validation_warnings = _validate_columns(df, columns, params)

    # Show validation warnings
    for warning in validation_warnings:
        warnings.warn(warning, UserWarning, stacklevel=2)

    derived_registry_table = registry_table or _derive_registry_table(df)

    # Ensure model_name has full three-level catalog.schema.model format
    # by extracting catalog.schema from registry_table
    derived_model_name = _ensure_full_model_name(model_name, derived_registry_table)

    # Check if model already exists in Unity Catalog and warn
    if _model_exists_in_uc(spark, derived_model_name):
        warnings.warn(
            f"Model '{derived_model_name}' already exists. Creating a new version. "
            f"Previous versions remain available.",
            UserWarning,
            stacklevel=2,
        )

    # Segment-based training
    if segment_by:
        return _train_segmented(df, columns, segment_by, derived_model_name, derived_registry_table, params)
    else:
        return _train_global(df, columns, derived_model_name, derived_registry_table, params)


def _train_global(
    df: DataFrame,
    columns: list[str],
    model_name: str,
    registry_table: str,
    params: AnomalyParams | None,
) -> str:
    """Train a single global model (no segmentation)."""
    # Lazy import to avoid circular import issues with MLflow
    import mlflow

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
        model_uris, hyperparams, validation_metrics, feature_metadata = _train_ensemble(
            train_df, val_df, columns, params, ensemble_size, model_name
        )
        model_uri = ",".join(model_uris)  # Store as comma-separated string
        run_id = "ensemble"  # Placeholder for ensemble runs (multiple MLflow runs)
    else:
        # Train single model (sklearn IsolationForest on driver)
        model, hyperparams, feature_metadata = _fit_isolation_forest(train_df, params)

        contamination = params.algorithm_config.contamination if params and params.algorithm_config else 0.1
        validation_metrics = _compute_validation_metrics(model, val_df, columns, contamination, feature_metadata)

        # Register model to Unity Catalog
        # Note: When running outside Databricks (e.g., local Spark Connect), you may see a warning:
        # "Unable to get model version source run's workspace ID from request headers"
        # This is expected and informational only - the model will register successfully
        mlflow.set_registry_uri("databricks-uc")

        # Ensure any previous runs are closed (defensive cleanup)
        try:
            mlflow.end_run()
        except Exception:
            pass

        with mlflow.start_run() as run:
            # Infer model signature for Unity Catalog (required)
            from mlflow.models import infer_signature

            # Get engineered features for signature
            from databricks.labs.dqx.anomaly.transformers import ColumnTypeInfo, apply_feature_engineering
            from pyspark.sql import types as T

            column_infos_reconstructed = [
                ColumnTypeInfo(
                    name=info["name"],
                    spark_type=T.StringType(),  # Type not used in scoring
                    category=info["category"],
                    cardinality=info.get("cardinality"),
                    null_count=info.get("null_count"),
                )
                for info in feature_metadata.column_infos
            ]

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
                artifact_path="model",
                registered_model_name=model_name,
                signature=signature,
            )

            # Note: feature_metadata is saved in the model registry table, not as MLflow artifact
            mlflow.log_params(_flatten_hyperparams(hyperparams))
            mlflow.log_metrics(validation_metrics)

            # Use explicit version-based URI format
            # model_name already has full catalog.schema.model format from train() setup
            model_uri = f"models:/{model_name}/{model_info.registered_model_version}"
            run_id = run.info.run_id

    # Compute baseline statistics for drift detection (distributed on Spark)
    baseline_stats = _compute_baseline_statistics(train_df, columns)

    # Compute feature importance for explainability (distributed on Spark, use first model if ensemble)
    if ensemble_size > 1:
        first_model = mlflow.sklearn.load_model(model_uris[0])
        feature_importance = _compute_feature_importance(first_model, val_df, columns, feature_metadata)
    else:
        feature_importance = _compute_feature_importance(model, val_df, columns, feature_metadata)

    if truncated:
        warnings.warn(
            f"Sampling capped at {params.max_rows} rows; model trained on truncated sample.",
            UserWarning,
            stacklevel=2,
        )

    registry = AnomalyModelRegistry(spark)

    # run_id was already saved during model logging

    input_table = _get_input_table(df) or "unknown"

    record = AnomalyModelRecord(
        model_name=model_name,
        model_uri=model_uri,
        input_table=input_table,
        columns=columns,
        algorithm=f"IsolationForest_Ensemble_{ensemble_size}" if ensemble_size > 1 else "IsolationForest",
        hyperparameters=_stringify_dict(hyperparams),
        training_rows=train_df.count(),
        training_time=datetime.utcnow(),
        mlflow_run_id=run_id or "unknown",
        metrics=validation_metrics,
        mode="spark",
        baseline_stats=baseline_stats,
        feature_importance=feature_importance,
        temporal_config=None,
        segment_by=None,
        segment_values=None,
        is_global_model=True,
        feature_metadata=feature_metadata.to_json(),  # Save feature engineering metadata
    )
    registry.save_model(record, registry_table)

    print(f"   Model trained: {model_name}")
    print(f"   Model URI: {model_uri}")
    print(f"   Registry: {registry_table}")

    return model_name


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
    _spark = df.sparkSession

    # Get distinct segments
    segments_df = df.select(*segment_by).distinct()
    segments = [row.asDict() for row in segments_df.collect()]

    # Validate segment count
    if len(segments) > 100:
        warnings.warn(
            f"Training {len(segments)} segments may be slow. " "Consider coarser segmentation or explicit segment_by.",
            UserWarning,
            stacklevel=2,
        )

    model_uris = []

    # Train each segment
    skipped_segments = []

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
                f"Segment {segment_name} has only {segment_size} rows, " "model may be unreliable.",
                UserWarning,
                stacklevel=2,
            )

        # Derive segment-specific model name
        segment_model_name = f"{base_model_name}__seg_{segment_name}"

        # Train model for this segment
        model_uri = _train_single_segment(
            segment_df, columns, segment_model_name, segment_vals, segment_by, registry_table, params
        )

        if model_uri is not None:
            model_uris.append(model_uri)
        else:
            skipped_segments.append(segment_name)

    # Log summary of skipped segments
    if skipped_segments:
        print(
            f"Skipped {len(skipped_segments)}/{len(segments)} segments due to insufficient data after sampling: "
            f"{', '.join(skipped_segments[:5])}"
            + (f" and {len(skipped_segments) - 5} more" if len(skipped_segments) > 5 else "")
        )

    # Validate that at least one segment was successfully trained
    if not model_uris:
        raise InvalidParameterError(
            f"All {len(segments)} segments produced 0 rows after sampling. Cannot train any models. "
            f"Consider increasing sample_fraction (current: {params.sample_fraction}) or checking segment definitions."
        )

    trained_count = len(model_uris)
    print(f"   Trained {trained_count}/{len(segments)} segment models for: {base_model_name}")
    print(f"   Registry: {registry_table}")

    # Return base model name (user doesn't need to deal with individual segment URIs)
    return base_model_name


def _train_single_segment(
    df: DataFrame,
    columns: list[str],
    model_name: str,
    segment_values: dict[str, Any],
    segment_by: list[str],
    registry_table: str,
    params: AnomalyParams,
) -> str | None:
    """
    Train a model for a single segment.

    Returns:
        Model URI on success, None if segment has insufficient data after sampling.
    """
    # Lazy import to avoid circular import issues with MLflow
    import mlflow

    spark = df.sparkSession

    sampled_df, sampled_count, truncated = _sample_df(df, columns, params)
    if sampled_count == 0:
        print(f"Segment {segment_values} has 0 rows after sampling. Skipping model training.")
        return None

    train_df, val_df = _train_validation_split(sampled_df, params)

    # Train single model (no ensemble for segments to reduce complexity)
    model, hyperparams, feature_metadata = _fit_isolation_forest(train_df, params)

    contamination = params.algorithm_config.contamination if params and params.algorithm_config else 0.1
    validation_metrics = _compute_validation_metrics(model, val_df, columns, contamination, feature_metadata)

    # Register model to Unity Catalog
    mlflow.set_registry_uri("databricks-uc")

    # Ensure any previous runs are closed (defensive cleanup)
    try:
        mlflow.end_run()
    except Exception:
        pass

    with mlflow.start_run() as run:
        # Infer model signature for Unity Catalog (required)
        from mlflow.models import infer_signature
        from databricks.labs.dqx.anomaly.transformers import ColumnTypeInfo, apply_feature_engineering
        from pyspark.sql import types as T

        # Get engineered features for signature
        column_infos_reconstructed = [
            ColumnTypeInfo(
                name=info["name"],
                spark_type=T.StringType(),  # Type not used in scoring
                category=info["category"],
                cardinality=info.get("cardinality"),
                null_count=info.get("null_count"),
            )
            for info in feature_metadata.column_infos
        ]

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
            artifact_path="model",
            registered_model_name=model_name,
            signature=signature,
        )

        # Note: feature_metadata is saved in the model registry table, not as MLflow artifact
        mlflow.log_params(_flatten_hyperparams(hyperparams))
        mlflow.log_metrics(validation_metrics)

        # Use explicit version-based URI format
        # model_name already has full catalog.schema.model format from _train_segmented()
        model_uri = f"models:/{model_name}/{model_info.registered_model_version}"
        run_id = run.info.run_id

    # Compute baseline statistics for drift detection
    baseline_stats = _compute_baseline_statistics(train_df, columns)

    # Compute feature importance for explainability
    feature_importance = _compute_feature_importance(model, val_df, columns, feature_metadata)

    registry = AnomalyModelRegistry(spark)

    input_table = _get_input_table(df) or "unknown"

    record = AnomalyModelRecord(
        model_name=model_name,
        model_uri=model_uri,
        input_table=input_table,
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
        feature_metadata=feature_metadata.to_json(),  # Save feature engineering metadata
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


def _validate_columns(df: DataFrame, columns: Iterable[str], params: AnomalyParams | None = None) -> list[str]:
    """
    Validate columns for anomaly detection with multi-type support.

    Returns:
        List of warnings to display to user.
    """
    from databricks.labs.dqx.anomaly.transformers import ColumnTypeClassifier

    params = params or AnomalyParams()
    fe_config = params.feature_engineering

    # Use ColumnTypeClassifier to analyze and validate
    classifier = ColumnTypeClassifier(
        categorical_cardinality_threshold=fe_config.categorical_cardinality_threshold,
        max_input_columns=fe_config.max_input_columns,
        max_engineered_features=fe_config.max_engineered_features,
    )

    # This will raise InvalidParameterError if limits are exceeded
    column_infos, warnings_list = classifier.analyze_columns(df, list(columns))

    return warnings_list


def _derive_registry_table(df: DataFrame) -> str:
    input_table = _get_input_table(df)
    if not input_table:
        # Fallback to default location when table name cannot be inferred
        # Use current catalog and schema
        spark = df.sparkSession
        try:
            catalog_row = spark.sql("SELECT current_catalog()").first()
            schema_row = spark.sql("SELECT current_schema()").first()
            assert catalog_row is not None and schema_row is not None
            current_catalog = catalog_row[0]
            current_schema = schema_row[0]
            return f"{current_catalog}.{current_schema}.dqx_anomaly_models"
        except Exception as exc:
            raise InvalidParameterError(
                "Cannot infer registry table name from DataFrame and no current catalog/schema set. "
                "Provide registry_table explicitly (e.g., 'catalog.schema.dqx_anomaly_models')."
            ) from exc
    parts = input_table.split(".")
    if len(parts) == 3:
        catalog, schema, _ = parts
    elif len(parts) == 2:
        raise InvalidParameterError("Cannot infer registry table without catalog. Provide registry_table explicitly.")
    else:
        raise InvalidParameterError(
            "Input table must be schema.table or catalog.schema.table to derive registry_table."
        )
    return f"{catalog}.{schema}.dqx_anomaly_models"


def _get_input_table(df: DataFrame) -> str | None:
    """
    Attempt to get the input table name from DataFrame by analyzing the Spark execution plan.

    Returns None if table name cannot be inferred (e.g., in-memory DataFrames, programmatically created).
    """
    if df.isStreaming:
        raise InvalidParameterError("Streaming DataFrames are not supported for training.")

    try:
        # Get the execution plan and extract table names
        from io import StringIO
        import sys

        # Capture explain output
        old_stdout = sys.stdout
        sys.stdout = buffer = StringIO()
        df.explain(extended=True)
        sys.stdout = old_stdout
        plan_str = buffer.getvalue()

        # Extract tables from the plan
        tables = get_tables_from_spark_plan(plan_str)
        if tables:
            # Return the first table found (typically there's only one input table)
            return next(iter(tables))
    except Exception:
        # If anything fails, fall back to None
        pass

    return None


def _ensure_full_model_name(model_name: str, registry_table: str) -> str:
    """
    Ensure model name has the full three-level catalog.schema.model format required by Unity Catalog.
    Uses catalog and schema from registry_table.

    If model_name already has three levels (two dots), returns it as-is.
    Otherwise, prepends catalog and/or schema from registry_table.
    """
    if model_name.count('.') >= 2:
        # Already has catalog.schema.model format
        return model_name

    # Extract catalog.schema from registry_table
    parts = registry_table.split(".")
    if len(parts) >= 2:
        catalog, schema = parts[0], parts[1]
    else:
        raise InvalidParameterError(f"registry_table must have at least catalog.schema format, got: {registry_table}")

    if model_name.count('.') == 1:
        # Has schema.model, add catalog
        return f"{catalog}.{model_name}"

    # No dots, add catalog.schema.model
    return f"{catalog}.{schema}.{model_name}"


def _model_exists_in_uc(spark: SparkSession, model_name: str) -> bool:
    """
    Check if a model exists in Unity Catalog using MLflow API.

    Args:
        spark: SparkSession (unused, kept for consistency)
        model_name: Full model name (catalog.schema.model)

    Returns:
        True if model exists, False otherwise
    """
    try:
        from mlflow.tracking import MlflowClient

        client = MlflowClient()
        # Try to get the latest version - if model doesn't exist, this will raise
        client.get_latest_versions(model_name, stages=None)
        return True
    except Exception:
        # Model doesn't exist or MLflow API error
        return False


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


def _fit_isolation_forest(train_df: DataFrame, params: AnomalyParams) -> tuple[Any, dict[str, Any], Any]:
    """
    Train scikit-learn IsolationForest on driver, returning model, hyperparameters, and feature metadata.

    Training happens on the driver node with collected data (already sampled to <=1M rows).
    Feature engineering is applied in Spark (distributed) before training.
    Scoring will be distributed via pandas UDF with only standard sklearn components.

    Returns:
        - pipeline: sklearn Pipeline with RobustScaler + IsolationForest (NO custom transformers)
        - hyperparams: Model hyperparameters dict
        - feature_metadata: Spark transformation metadata for scoring
    """
    try:
        from sklearn.ensemble import IsolationForest
        from sklearn.preprocessing import RobustScaler
        from sklearn.pipeline import Pipeline

        # Dependency check - these are used by sklearn and pandas UDFs
        import pandas  # noqa: F401
        import numpy  # noqa: F401
    except ImportError as e:
        raise InvalidParameterError(
            "IsolationForest requires scikit-learn, pandas, and numpy. "
            "Install with: pip install 'databricks-labs-dqx[anomaly]'"
        ) from e

    from databricks.labs.dqx.anomaly.transformers import ColumnTypeClassifier, apply_feature_engineering

    algo_cfg = params.algorithm_config or IsolationForestConfig()
    fe_config = params.feature_engineering

    # Analyze columns to determine feature engineering strategy
    classifier = ColumnTypeClassifier(
        categorical_cardinality_threshold=fe_config.categorical_cardinality_threshold,
        max_input_columns=fe_config.max_input_columns,
        max_engineered_features=fe_config.max_engineered_features,
    )

    columns = train_df.columns
    column_infos, _ = classifier.analyze_columns(train_df, columns)

    # Apply feature engineering in Spark (distributed)
    # This transforms the DataFrame to numeric features only
    engineered_df, feature_metadata = apply_feature_engineering(
        train_df,
        column_infos,
        categorical_cardinality_threshold=fe_config.categorical_cardinality_threshold,
        frequency_maps=None,  # Training mode - compute frequency maps
    )

    # Collect engineered numeric data to driver (already sampled to <=1M rows)
    train_pandas = engineered_df.toPandas()

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

    # Create pipeline with ONLY standard sklearn components (no custom transformers)
    # RobustScaler uses median and IQR, making it robust to outliers and heavy tails
    pipeline = Pipeline([('scaler', RobustScaler()), ('model', iso_forest)])

    # Fit pipeline (scaling + model) on engineered training data
    pipeline.fit(train_pandas)

    hyperparams: dict[str, Any] = {
        "contamination": algo_cfg.contamination,
        "num_trees": algo_cfg.num_trees,
        "max_samples": algo_cfg.subsampling_rate,
        "random_seed": algo_cfg.random_seed,
        "feature_scaling": "RobustScaler",  # Document that we use robust scaling
    }

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
    import cloudpickle
    from pyspark.sql.functions import pandas_udf, col
    from pyspark.sql.types import DoubleType, IntegerType, StructType, StructField
    from pyspark.sql import types as T
    from databricks.labs.dqx.anomaly.transformers import ColumnTypeInfo, apply_feature_engineering

    # Reconstruct column_infos from metadata
    column_infos = [
        ColumnTypeInfo(
            name=info["name"],
            spark_type=T.StringType(),  # Type not used in scoring
            category=info["category"],
            cardinality=info.get("cardinality"),
            null_count=info.get("null_count"),
        )
        for info in feature_metadata.column_infos
    ]

    # Apply feature engineering in Spark (distributed)
    # Use pre-computed frequency maps from training
    _engineered_df, _ = apply_feature_engineering(
        df,
        column_infos,
        categorical_cardinality_threshold=20,  # Use same threshold as training
        frequency_maps=feature_metadata.categorical_frequency_maps,
        onehot_categories=feature_metadata.onehot_categories,
    )

    # Get engineered feature names
    engineered_feature_cols = feature_metadata.engineered_feature_names

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

    @pandas_udf(schema, PandasUDFType.SCALAR)
    def predict_udf(*cols):
        """Pandas UDF for distributed scoring (Spark Connect compatible)."""
        import numpy as np
        import pandas as pd
        import cloudpickle

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

    # Use temporary row ID to preserve row order during transformation and scoring
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number, monotonically_increasing_id

    window_spec = Window.orderBy(monotonically_increasing_id())
    df_with_id = df.withColumn("__dqx_row_id__", row_number().over(window_spec))

    # Apply feature engineering, preserving the row ID
    engineered_with_id, _ = apply_feature_engineering(
        df_with_id.select(*feature_cols, "__dqx_row_id__"),
        column_infos,
        categorical_cardinality_threshold=20,
        frequency_maps=feature_metadata.categorical_frequency_maps,
        onehot_categories=feature_metadata.onehot_categories,
    )

    # Apply UDF to all engineered feature columns
    scored_with_id = engineered_with_id.withColumn("_scores", predict_udf(*[col(c) for c in engineered_feature_cols]))

    # Join scores back to original DataFrame using the row ID
    result = df_with_id.join(
        scored_with_id.select("__dqx_row_id__", "_scores.anomaly_score", "_scores.prediction"),
        on="__dqx_row_id__",
        how="left",
    ).drop("__dqx_row_id__")

    return result


def _compute_validation_metrics(
    model: Any, val_df: DataFrame, feature_cols: list[str], contamination: float, feature_metadata: Any
) -> dict[str, float]:
    """
    Compute comprehensive validation metrics including precision, recall, F1,
    threshold recommendations, and distribution statistics.

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

    # Ground truth labels: top contamination% are anomalies
    threshold_for_labels = scores_df.approxQuantile("score", [1 - contamination], 0.01)[0]
    labeled_df = scores_df.withColumn(
        "true_label", F.when(F.col("score") >= F.lit(threshold_for_labels), 1).otherwise(0)
    )

    # Compute precision/recall/F1 for multiple thresholds
    test_thresholds = [0.3, 0.5, 0.7, 0.9]
    threshold_metrics = {}
    best_f1 = 0.0
    best_threshold = 0.5

    for threshold in test_thresholds:
        pred_df = labeled_df.withColumn("pred_label", F.when(F.col("score") >= F.lit(threshold), 1).otherwise(0))

        # Confusion matrix
        true_positives = pred_df.filter((F.col("pred_label") == 1) & (F.col("true_label") == 1)).count()
        false_positives = pred_df.filter((F.col("pred_label") == 1) & (F.col("true_label") == 0)).count()
        _true_negatives = pred_df.filter((F.col("pred_label") == 0) & (F.col("true_label") == 0)).count()
        false_negatives = pred_df.filter((F.col("pred_label") == 0) & (F.col("true_label") == 1)).count()

        precision = (
            true_positives / (true_positives + false_positives) if (true_positives + false_positives) > 0 else 0.0
        )
        recall = true_positives / (true_positives + false_negatives) if (true_positives + false_negatives) > 0 else 0.0
        f1_score = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0

        threshold_metrics[f"threshold_{int(threshold*100)}_precision"] = precision
        threshold_metrics[f"threshold_{int(threshold*100)}_recall"] = recall
        threshold_metrics[f"threshold_{int(threshold*100)}_f1"] = f1_score

        if f1_score > best_f1:
            best_f1 = f1_score
            best_threshold = threshold

    # Estimated contamination (percentage of scores above best threshold)
    estimated_contamination = labeled_df.filter(F.col("score") >= F.lit(best_threshold)).count() / val_count

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
        "recommended_threshold": best_threshold,
        "recommended_threshold_f1": best_f1,
        "estimated_contamination": estimated_contamination,
    }

    # Add threshold-specific metrics
    metrics.update(threshold_metrics)

    # Filter out None values (MLflow doesn't accept them)
    metrics = {k: v for k, v in metrics.items() if v is not None}

    return metrics


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


def _compute_feature_importance(
    model: Any, val_df: DataFrame, columns: list[str], feature_metadata: Any
) -> dict[str, float]:
    """
    Compute global feature importance using permutation importance.
    Measures how much each feature contributes to anomaly detection.

    Uses distributed scoring via pandas UDF for efficient computation across cluster.
    """
    if val_df.count() == 0:
        return {}

    # Baseline scores (distributed scoring)
    baseline_scored = _score_with_model(model, val_df, columns, feature_metadata)
    baseline_row = baseline_scored.select(F.mean("anomaly_score")).first()
    assert baseline_row is not None, "Failed to compute baseline score"
    baseline_avg_score = baseline_row[0]

    importance = {}

    # Permutation importance: shuffle each column and measure impact (distributed on Spark)
    for col in columns:
        # Shuffle this column's values across rows
        # Use shuffle() to randomize and element_at() to access a random element
        shuffled_df = val_df.withColumn(col, F.expr(f"element_at(shuffle(collect_list({col}) over ()), 1)"))

        # Compute scores with shuffled column (distributed scoring)
        shuffled_scored = _score_with_model(model, shuffled_df, columns, feature_metadata)
        shuffled_row = shuffled_scored.select(F.mean("anomaly_score")).first()
        assert shuffled_row is not None, f"Failed to compute shuffled score for {col}"
        shuffled_avg_score = shuffled_row[0]

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
) -> tuple[list[str], dict[str, Any], dict[str, float], Any]:
    """
    Train ensemble of models with different random seeds.

    Returns:
        Tuple of (model_uris, hyperparams, aggregated_metrics, feature_metadata).
        feature_metadata is from the first ensemble member (all members use same features).
    """
    # Lazy import to avoid circular import issues with MLflow
    import mlflow

    model_uris = []
    all_metrics = []
    first_feature_metadata = None  # Capture from first ensemble member

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
        model, hyperparams, feature_metadata = _fit_isolation_forest(train_df, modified_params)

        # Capture feature_metadata from first member (all use same feature engineering)
        if i == 0:
            first_feature_metadata = feature_metadata

        # Compute metrics (distributed scoring on Spark)
        contamination = modified_params.algorithm_config.contamination
        metrics = _compute_validation_metrics(model, val_df, columns, contamination, feature_metadata)
        all_metrics.append(metrics)

        # Log to MLflow
        with mlflow.start_run(run_name=f"{model_name}_ensemble_{i}"):
            # Infer model signature for Unity Catalog (required)
            from mlflow.models import infer_signature

            train_pandas = train_df.toPandas()
            predictions = model.predict(train_pandas.values)
            signature = infer_signature(train_pandas, predictions)

            # Log scikit-learn model for this ensemble member
            ensemble_model_name = f"{model_name}_ensemble_{i}"
            model_info = mlflow.sklearn.log_model(
                sk_model=model,
                artifact_path="model",
                registered_model_name=ensemble_model_name,
                signature=signature,
            )
            mlflow.log_params(_flatten_hyperparams(hyperparams))
            mlflow.log_metrics(metrics)
            mlflow.log_param("ensemble_index", i)
            mlflow.log_param("ensemble_size", ensemble_size)

            # Use explicit version-based URI format
            # ensemble_model_name inherits full catalog.schema.model format from base model_name
            model_uris.append(f"models:/{ensemble_model_name}/{model_info.registered_model_version}")

    # Aggregate metrics (average across ensemble)
    aggregated_metrics = {}
    if all_metrics:
        metric_keys = all_metrics[0].keys()
        for key in metric_keys:
            values = [m[key] for m in all_metrics if key in m]
            if values:
                aggregated_metrics[key] = sum(values) / len(values)
                aggregated_metrics[f"{key}_std"] = (
                    (sum((v - aggregated_metrics[key]) ** 2 for v in values) / (len(values) - 1)) ** 0.5
                    if len(values) > 1
                    else 0.0
                )

    return model_uris, hyperparams, aggregated_metrics, first_feature_metadata


def _flatten_hyperparams(hyperparams: dict[str, Any]) -> dict[str, Any]:
    return {f"hyperparam_{k}": v for k, v in hyperparams.items() if v is not None}


def _stringify_dict(data: dict[str, Any]) -> dict[str, str]:
    return {k: str(v) for k, v in data.items() if v is not None}
