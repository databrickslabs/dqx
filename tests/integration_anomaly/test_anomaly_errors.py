"""Integration tests for anomaly detection error cases.

Tests integration-level errors that occur with real Spark operations,
model training, and Delta table access. Pure parameter validation errors
are covered by unit tests (tests/unit/test_anomaly_validation.py).
"""

import warnings
from datetime import datetime

import pyspark.sql.functions as F
import pytest
from pyspark.sql import SparkSession

import databricks.labs.dqx.anomaly.check_funcs as anomaly_check_funcs
from databricks.labs.dqx.anomaly import has_no_anomalies
from databricks.labs.dqx.anomaly.model_registry import (
    AnomalyModelRegistry,
    AnomalyModelRecord,
    FeatureEngineering,
    ModelIdentity,
    SegmentationConfig,
    TrainingMetadata,
)
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.errors import InvalidParameterError
from tests.integration_anomaly.test_anomaly_constants import DEFAULT_SCORE_THRESHOLD
from tests.integration_anomaly.test_anomaly_utils import (
    create_anomaly_dataset_rule,
    qualify_model_name,
    train_simple_2d_model,
)


def test_missing_columns_error(
    ws, spark: SparkSession, make_random, anomaly_engine, test_df_factory, anomaly_registry_prefix
):
    """Test error when input DataFrame is missing model columns."""
    unique_id = make_random(8).lower()
    model_name = f"{anomaly_registry_prefix}.test_col_mismatch_{make_random(4).lower()}"
    registry_table = f"{anomaly_registry_prefix}.{unique_id}_registry"

    # Train on [amount, quantity] - use helper
    train_simple_2d_model(spark, anomaly_engine, model_name, registry_table)

    # Try to score with [amount, discount] (different columns) - use factory
    test_df = test_df_factory(
        spark,
        normal_rows=[(100.0, 0.1)],
        anomaly_rows=[],
        columns_schema="amount double, discount double",
    )

    # Should raise error about missing model columns in the input DataFrame
    with pytest.raises(InvalidParameterError, match="missing required columns"):
        _, apply_fn = has_no_anomalies(
            model=qualify_model_name(model_name, registry_table),
            registry_table=registry_table,
            score_threshold=DEFAULT_SCORE_THRESHOLD,
        )
        result_df = apply_fn(test_df)
        result_df.collect()


def test_column_order_independence(
    ws,
    spark: SparkSession,
    make_random,
    anomaly_engine,
    test_df_factory,
    anomaly_scorer,
    anomaly_registry_prefix,
):
    """Test that column order doesn't matter (set comparison)."""
    unique_id = make_random(8).lower()
    model_name = f"{anomaly_registry_prefix}.test_col_order_{make_random(4).lower()}"
    registry_table = f"{anomaly_registry_prefix}.{unique_id}_registry"

    # Train on [amount, quantity] - use helper
    train_simple_2d_model(spark, anomaly_engine, model_name, registry_table)

    # Score with [quantity, amount] (different order) - note DataFrame column order
    test_df = test_df_factory(
        spark,
        normal_rows=[(2.0, 100.0)],  # quantity, amount order
        anomaly_rows=[],
        columns_schema="quantity double, amount double",  # Different column order
    )

    # Should succeed - order doesn't matter for anomaly check
    result_df = anomaly_scorer(
        test_df,
        model_name=model_name,
        registry_table=registry_table,
        score_threshold=DEFAULT_SCORE_THRESHOLD,
        extract_score=False,
    )
    result_df.collect()
    assert True  # No error - order doesn't matter


def test_config_hash_mismatch_raises(
    ws,
    spark: SparkSession,
    make_random,
    anomaly_engine,
    test_df_factory,
    anomaly_registry_prefix,
):
    """Model config hash mismatch should raise a clear error."""
    model_name = f"{anomaly_registry_prefix}.test_config_mismatch_{make_random(4).lower()}"
    registry_table = f"{anomaly_registry_prefix}.{make_random(8).lower()}_registry"

    train_simple_2d_model(spark, anomaly_engine, model_name, registry_table)
    full_model_name = qualify_model_name(model_name, registry_table)

    spark.sql(
        f"UPDATE {registry_table} "
        f"SET segmentation.config_hash = 'bogus' "
        f"WHERE identity.model_name = '{full_model_name}'"
    )

    df = test_df_factory(
        spark,
        normal_rows=[(100.0, 2.0)],
        anomaly_rows=[],
        columns_schema="amount double, quantity double",
    )

    dq_engine = DQEngine(ws, spark)
    check = create_anomaly_dataset_rule(model_name, registry_table)

    with pytest.raises(InvalidParameterError, match="Configuration mismatch"):
        dq_engine.apply_checks(df, [check]).collect()


def test_empty_dataframe_error(spark: SparkSession, make_random, anomaly_engine, anomaly_registry_prefix):
    """Test error when training on empty DataFrame."""
    unique_id = make_random(8).lower()
    model_name = f"{anomaly_registry_prefix}.test_empty_{make_random(4).lower()}"
    registry_table = f"{anomaly_registry_prefix}.{unique_id}_registry"

    empty_df = spark.createDataFrame([], "amount double, quantity double")

    # Should raise clear error about empty data
    with pytest.raises(InvalidParameterError, match="Sampling produced 0 rows"):
        anomaly_engine.train(
            df=empty_df,
            columns=["amount", "quantity"],
            model_name=qualify_model_name(model_name, registry_table),
            registry_table=registry_table,
        )


def test_missing_registry_table_for_scoring_error(
    ws, spark: SparkSession, make_random, test_df_factory, anomaly_registry_prefix
):
    """Test error when registry table doesn't exist during scoring."""
    unique_id = make_random(8).lower()
    model_name = f"{anomaly_registry_prefix}.test_missing_registry_{make_random(4).lower()}"
    registry_table = f"{anomaly_registry_prefix}.{unique_id}_nonexistent_registry"

    # Use factory to create test DataFrame
    df = test_df_factory(
        spark,
        normal_rows=[(100.0, 2.0)],
        anomaly_rows=[],
        columns_schema="amount double, quantity double",
    )

    # Try to score with non-existent registry
    with pytest.raises((InvalidParameterError, Exception)):  # Delta/Spark exception
        _, apply_fn = has_no_anomalies(
            model=qualify_model_name(model_name, registry_table),
            registry_table=registry_table,
            score_threshold=DEFAULT_SCORE_THRESHOLD,
        )
        result_df = apply_fn(df)
        result_df.collect()


def test_model_not_found_error(
    ws, spark: SparkSession, make_random, test_df_factory, anomaly_engine, anomaly_registry_prefix
):
    """Test error when model doesn't exist (neither global nor segmented) during scoring."""
    unique_id = make_random(8).lower()
    model_name = f"{anomaly_registry_prefix}.test_nonexistent_model_{make_random(4).lower()}"
    registry_table = f"{anomaly_registry_prefix}.{unique_id}_registry"

    # Create an empty registry table (no models trained)
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {registry_table} (
            identity STRUCT<model_name: STRING, model_uri: STRING, algorithm: STRING, 
                           mlflow_run_id: STRING, status: STRING>,
            training STRUCT<columns: ARRAY<STRING>, hyperparameters: MAP<STRING, STRING>,
                          training_rows: BIGINT, training_time: TIMESTAMP,
                          metrics: MAP<STRING, DOUBLE>, baseline_stats: MAP<STRING, DOUBLE>>,
            features STRUCT<mode: STRING, column_types: MAP<STRING, STRING>,
                          feature_metadata: STRING, feature_importance: MAP<STRING, DOUBLE>,
                          temporal_config: STRING>,
            segmentation STRUCT<segment_by: ARRAY<STRING>, segment_values: MAP<STRING, STRING>,
                              is_global_model: BOOLEAN, sklearn_version: STRING, config_hash: STRING>
        ) USING DELTA
    """
    )

    # Use factory to create test DataFrame
    df = test_df_factory(
        spark,
        normal_rows=[(100.0, 2.0)],
        anomaly_rows=[],
        columns_schema="amount double, quantity double",
    )

    # Try to score with non-existent model (registry exists but model doesn't)
    with pytest.raises(InvalidParameterError, match="not found in.*Train first"):
        _, apply_fn = has_no_anomalies(
            model=qualify_model_name(model_name, registry_table),
            registry_table=registry_table,
            score_threshold=DEFAULT_SCORE_THRESHOLD,
        )
        result_df = apply_fn(df)
        result_df.collect()


def test_internal_row_id_collision(ws, spark: SparkSession, make_random, anomaly_engine, anomaly_registry_prefix):
    """Ensure scoring fails fast if _dqx_row_id already exists in the input."""
    model_name = f"{anomaly_registry_prefix}.test_row_id_collision_{make_random(4).lower()}"
    registry_table = f"{anomaly_registry_prefix}.{make_random(8).lower()}_registry"

    train_simple_2d_model(spark, anomaly_engine, model_name, registry_table)

    df = spark.createDataFrame(
        [(1, 100.0, 2.0, 42)],
        "transaction_id int, amount double, quantity double, _dqx_row_id int",
    )

    dq_engine = DQEngine(ws, spark)
    check = create_anomaly_dataset_rule(model_name, registry_table)

    with pytest.raises(InvalidParameterError) as exc:
        dq_engine.apply_checks(df, [check])
    assert "_dqx_row_id" in str(exc.value)


def test_internal_score_column_collision(ws, spark: SparkSession, make_random, anomaly_engine, anomaly_registry_prefix):
    """Ensure existing anomaly_score columns are preserved and _dq_info is still added."""
    model_name = f"{anomaly_registry_prefix}.test_score_collision_{make_random(4).lower()}"
    registry_table = f"{anomaly_registry_prefix}.{make_random(8).lower()}_registry"

    train_simple_2d_model(spark, anomaly_engine, model_name, registry_table)

    df = spark.createDataFrame(
        [(1, 100.0, 2.0, 0.42, 0.1, {"amount": 0.2})],
        "transaction_id int, amount double, quantity double, anomaly_score double, anomaly_score_std double, "
        "anomaly_contributions map<string,double>",
    )

    dq_engine = DQEngine(ws, spark)
    check = create_anomaly_dataset_rule(model_name, registry_table)
    with pytest.raises(Exception) as exc:
        dq_engine.apply_checks(df, [check])

    assert "ambiguous" in str(exc.value).lower()


def test_has_no_anomalies_requires_fully_qualified_model_name():
    """Ensure model name must be fully qualified."""
    with pytest.raises(InvalidParameterError):
        has_no_anomalies(
            model="schema.model",
            registry_table="catalog.schema.table",
        )


def test_has_no_anomalies_requires_shap_when_enabled():
    """Include_contributions should fail fast when SHAP is unavailable."""
    shap_available = anomaly_check_funcs.SHAP_AVAILABLE
    try:
        anomaly_check_funcs.SHAP_AVAILABLE = False
        with pytest.raises(ImportError, match="Feature contributions require SHAP"):
            has_no_anomalies(
                model="catalog.schema.model",
                registry_table="catalog.schema.table",
                include_contributions=True,
            )
    finally:
        anomaly_check_funcs.SHAP_AVAILABLE = shap_available


@pytest.mark.parametrize(
    "kwargs, match",
    [
        ({"score_threshold": "0.5"}, "score_threshold must be a float"),  # type: ignore[arg-type]
        ({"score_threshold": 1.5}, "score_threshold must be between"),
        ({"row_filter": 123}, "row_filter must be a SQL expression"),  # type: ignore[arg-type]
        ({"drift_threshold": "bad"}, "drift_threshold must be a float"),  # type: ignore[arg-type]
        ({"drift_threshold": -1.0}, "drift_threshold must be greater than 0"),
        ({"include_contributions": "yes"}, "include_contributions must be a boolean"),  # type: ignore[arg-type]
        ({"include_confidence": "no"}, "include_confidence must be a boolean"),  # type: ignore[arg-type]
    ],
)
def test_has_no_anomalies_invalid_inputs(kwargs, match):
    with pytest.raises(InvalidParameterError, match=match):
        has_no_anomalies(
            model="catalog.schema.model",
            registry_table="catalog.schema.table",
            **kwargs,
        )


def test_build_segment_filter_handles_none_and_multi_key():
    assert anomaly_check_funcs._build_segment_filter(None) is None
    expr = anomaly_check_funcs._build_segment_filter({"region": "US", "product": "A"})
    assert expr is not None


def test_row_filter_scores_only_matching_rows(
    spark: SparkSession, make_random, anomaly_engine, test_df_factory, anomaly_registry_prefix
):
    """Ensure row_filter only scores matching rows and joins results back."""
    model_name = f"{anomaly_registry_prefix}.test_row_filter_{make_random(4).lower()}"
    registry_table = f"{anomaly_registry_prefix}.{make_random(8).lower()}_registry"

    train_simple_2d_model(spark, anomaly_engine, model_name, registry_table)

    df = test_df_factory(
        spark,
        normal_rows=[(100.0, 2.0), (200.0, 2.0)],
        anomaly_rows=[],
        columns_schema="amount double, quantity double",
    )

    _, apply_fn = has_no_anomalies(
        model=qualify_model_name(model_name, registry_table),
        registry_table=registry_table,
        score_threshold=DEFAULT_SCORE_THRESHOLD,
        row_filter="amount > 150",
        include_contributions=False,
    )
    result = apply_fn(df).select("amount", F.col("_dq_info.anomaly.score").alias("score")).collect()

    scored = {row.amount: row.score for row in result}
    assert scored[100.0] is None
    assert scored[200.0] is not None


def test_create_null_scored_dataframe_updates_existing_info(spark: SparkSession):
    df = spark.createDataFrame(
        [(1, 0.1, {"existing": "value"})],
        "id int, amount double, _dq_info struct<existing:string>",
    )
    result = anomaly_check_funcs._create_null_scored_dataframe(
        df,
        include_contributions=True,
        include_confidence=True,
    )
    row = result.select(F.col("_dq_info.anomaly.score").alias("score")).first()
    assert row is not None
    assert row["score"] is None


def test_add_info_column_preserves_existing_info(spark: SparkSession):
    df = spark.createDataFrame(
        [(1, 0.5, {"existing": "value"})],
        "id int, anomaly_score double, _dq_info struct<existing:string>",
    )
    result = anomaly_check_funcs._add_info_column(
        df,
        model_name="catalog.schema.model",
        score_threshold=0.6,
        include_contributions=False,
        include_confidence=False,
    )
    row = result.select(F.col("_dq_info.anomaly.score").alias("score")).first()
    assert row is not None
    assert row["score"] == 0.5


def test_create_udf_schema_includes_contributions():
    schema_without = anomaly_check_funcs._create_udf_schema(include_contributions=False)
    schema_with = anomaly_check_funcs._create_udf_schema(include_contributions=True)
    assert [field.name for field in schema_without.fields] == ["anomaly_score"]
    assert [field.name for field in schema_with.fields] == ["anomaly_score", "anomaly_contributions"]


def test_prepare_drift_df_without_feature_metadata(spark: SparkSession):
    record = AnomalyModelRecord(
        identity=ModelIdentity(
            model_name="catalog.schema.model",
            model_uri="models:/dummy",
            algorithm="IsolationForest",
            mlflow_run_id="run",
        ),
        training=TrainingMetadata(
            columns=["amount", "quantity"],
            hyperparameters={},
            training_rows=10,
            training_time=datetime.utcnow(),
        ),
        features=FeatureEngineering(feature_metadata=None),
        segmentation=SegmentationConfig(),
    )
    df = spark.createDataFrame([(1.0, 2.0)], "amount double, quantity double")
    drift_df, drift_cols = anomaly_check_funcs._prepare_drift_df(df, ["amount", "quantity"], record)
    assert drift_cols == ["amount", "quantity"]
    assert drift_df.columns == ["amount", "quantity"]


def test_score_segmented_raises_with_no_segments(spark: SparkSession):
    df = spark.createDataFrame([(1, 1.0)], "id int, amount double")
    config = anomaly_check_funcs.ScoringConfig(
        columns=["amount"],
        model_name="catalog.schema.model",
        registry_table="catalog.schema.registry",
        score_threshold=0.6,
        merge_columns=["id"],
    )
    with pytest.raises(InvalidParameterError, match="No segment models found"):
        anomaly_check_funcs._score_segmented(df, config, AnomalyModelRegistry(df.sparkSession), all_segments=[])


def test_sklearn_version_mismatch_warns(
    ws,
    spark: SparkSession,
    make_random,
    anomaly_engine,
    test_df_factory,
    anomaly_registry_prefix,
):
    """A mismatched sklearn version should emit a warning."""
    model_name = f"{anomaly_registry_prefix}.test_sklearn_warn_{make_random(4).lower()}"
    registry_table = f"{anomaly_registry_prefix}.{make_random(8).lower()}_registry"

    train_simple_2d_model(spark, anomaly_engine, model_name, registry_table)
    full_model_name = qualify_model_name(model_name, registry_table)

    spark.sql(
        f"UPDATE {registry_table} "
        f"SET segmentation.sklearn_version = '0.0' "
        f"WHERE identity.model_name = '{full_model_name}'"
    )

    df = test_df_factory(
        spark,
        normal_rows=[(100.0, 2.0)],
        anomaly_rows=[],
        columns_schema="amount double, quantity double",
    )

    dq_engine = DQEngine(ws, spark)
    check = create_anomaly_dataset_rule(model_name, registry_table)

    with pytest.warns(UserWarning, match="SKLEARN VERSION MISMATCH"):
        dq_engine.apply_checks(df, [check]).collect()


def test_sklearn_version_parse_error_silently_skips(
    ws,
    spark: SparkSession,
    make_random,
    anomaly_engine,
    test_df_factory,
    anomaly_registry_prefix,
):
    """Unparseable sklearn version should not crash scoring."""
    model_name = f"{anomaly_registry_prefix}.test_sklearn_badver_{make_random(4).lower()}"
    registry_table = f"{anomaly_registry_prefix}.{make_random(8).lower()}_registry"

    train_simple_2d_model(spark, anomaly_engine, model_name, registry_table)
    full_model_name = qualify_model_name(model_name, registry_table)

    spark.sql(
        f"UPDATE {registry_table} "
        f"SET segmentation.sklearn_version = 'bad.version' "
        f"WHERE identity.model_name = '{full_model_name}'"
    )

    df = test_df_factory(
        spark,
        normal_rows=[(100.0, 2.0)],
        anomaly_rows=[],
        columns_schema="amount double, quantity double",
    )

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        dq_engine = DQEngine(ws, spark)
        check = create_anomaly_dataset_rule(model_name, registry_table)
        dq_engine.apply_checks(df, [check]).collect()

    assert not any("SKLEARN VERSION MISMATCH" in str(w.message) for w in caught)


def test_validate_sklearn_compatibility_skips_when_missing_version():
    record = AnomalyModelRecord(
        identity=ModelIdentity(
            model_name="catalog.schema.model",
            model_uri="models:/dummy",
            algorithm="IsolationForest",
            mlflow_run_id="run",
        ),
        training=TrainingMetadata(
            columns=["amount"],
            hyperparameters={},
            training_rows=10,
            training_time=datetime.utcnow(),
        ),
        features=FeatureEngineering(feature_metadata=None),
        segmentation=SegmentationConfig(sklearn_version=None),
    )
    anomaly_check_funcs._validate_sklearn_compatibility(record)


def test_unknown_algorithm_raises(
    ws,
    spark: SparkSession,
    make_random,
    anomaly_engine,
    test_df_factory,
    anomaly_registry_prefix,
):
    """Unknown model algorithm should raise a clear error."""
    model_name = f"{anomaly_registry_prefix}.test_unknown_algo_{make_random(4).lower()}"
    registry_table = f"{anomaly_registry_prefix}.{make_random(8).lower()}_registry"

    train_simple_2d_model(spark, anomaly_engine, model_name, registry_table)
    full_model_name = qualify_model_name(model_name, registry_table)

    spark.sql(
        f"UPDATE {registry_table} "
        f"SET identity.algorithm = 'UnknownAlgo' "
        f"WHERE identity.model_name = '{full_model_name}'"
    )

    df = test_df_factory(
        spark,
        normal_rows=[(100.0, 2.0)],
        anomaly_rows=[],
        columns_schema="amount double, quantity double",
    )

    dq_engine = DQEngine(ws, spark)
    check = create_anomaly_dataset_rule(model_name, registry_table)

    with pytest.raises(InvalidParameterError, match="Unsupported model algorithm"):
        dq_engine.apply_checks(df, [check]).collect()
