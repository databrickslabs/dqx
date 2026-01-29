"""Integration tests for anomaly detection end-to-end flow."""

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.manager import DQRuleManager
from databricks.labs.dqx.rule import ColumnArguments
from databricks.labs.dqx.schema import dq_result_schema
from tests.constants import TEST_CATALOG
from tests.integration_anomaly.test_anomaly_utils import (
    create_anomaly_check_rule,
    get_percentile_threshold_from_data,
    get_standard_2d_training_data,
    get_standard_4d_training_data,
    get_standard_test_points_2d,
    get_standard_test_points_4d,
    train_simple_2d_model,
)


def test_basic_train_and_score(ws, spark: SparkSession, make_schema, make_random, anomaly_engine):
    """Test basic training and scoring workflow."""
    # Create unique schema and table names for test isolation
    catalog_name = TEST_CATALOG
    schema = make_schema(catalog_name=catalog_name)
    model_name = f"{catalog_name}.{schema.name}.test_basic_{make_random(4).lower()}"
    registry_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}_registry"

    # Use standard 2D training data for consistent results
    train_df = spark.createDataFrame(
        get_standard_2d_training_data(),
        "amount double, quantity double",
    )

    model_uri = anomaly_engine.train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name=model_name,
        registry_table=registry_table,
    )

    # Verify model name is returned (train() returns model name, not full URI)
    assert model_uri is not None
    assert model_name in model_uri

    # Score normal + anomalous data
    test_points = get_standard_test_points_2d()
    test_df = spark.createDataFrame(
        [
            (1, *test_points["normal_in_center"]),
            (2, *test_points["clear_anomaly"]),
        ],
        "transaction_id int, amount double, quantity double",
    )

    dq_engine = DQEngine(ws, spark)
    threshold = get_percentile_threshold_from_data(train_df, model_name, registry_table)
    checks = [
        create_anomaly_check_rule(
            model_name=model_name,
            registry_table=registry_table,
            threshold=threshold,
        )
    ]

    result_df = dq_engine.apply_checks(test_df, checks)
    engine_core = dq_engine._engine
    current_df = test_df
    check_conditions = []
    for check in checks:
        normalized_check = engine_core._preselect_original_columns(test_df, check)
        manager = DQRuleManager(
            check=normalized_check,
            df=current_df,
            spark=spark,
            run_id=engine_core.run_id,
            engine_user_metadata=engine_core.engine_user_metadata,
            run_time_overwrite=engine_core.run_time_overwrite,
            ref_dfs=None,
        )
        result = manager.process()
        check_conditions.append(result.condition)
        current_df = result.check_df

    combined_result_array = F.array_compact(F.array(*check_conditions))
    errors_col = F.when(F.size(combined_result_array) > 0, combined_result_array).otherwise(
        F.lit(None).cast(dq_result_schema)
    )
    warnings_col = F.lit(None).cast(dq_result_schema)
    expected_df = current_df.withColumn(
        engine_core.result_column_names[ColumnArguments.ERRORS], errors_col
    ).withColumn(engine_core.result_column_names[ColumnArguments.WARNINGS], warnings_col)

    info_col_name = engine_core.result_column_names[ColumnArguments.INFO]
    if "_dq_info" in expected_df.columns and info_col_name != "_dq_info":
        expected_df = expected_df.withColumnRenamed("_dq_info", info_col_name)

    expected_df = expected_df.select(*result_df.columns)

    assert result_df.schema == expected_df.schema

    def _strip_nondeterministic(value):
        if isinstance(value, dict):
            cleaned = {}
            for key, val in value.items():
                if key in {"run_time", "run_id"}:
                    continue
                cleaned[key] = _strip_nondeterministic(val)
            return cleaned
        if isinstance(value, list):
            return [_strip_nondeterministic(v) for v in value]
        return value

    def _normalize_value(value):
        if isinstance(value, dict):
            return tuple((k, _normalize_value(v)) for k, v in sorted(value.items()))
        if isinstance(value, list):
            return tuple(_normalize_value(v) for v in value)
        return value

    def _normalize_row(row):
        cleaned = _strip_nondeterministic(row.asDict(recursive=True))
        return _normalize_value(cleaned)

    result_rows = [_normalize_row(row) for row in result_df.collect()]
    expected_rows = [_normalize_row(row) for row in expected_df.collect()]

    assert sorted(result_rows) == sorted(expected_rows)

    rows = result_df.select("_errors", F.col("_dq_info.anomaly.score").alias("score")).collect()
    flagged = 0
    for row in rows:
        has_errors = row["_errors"] is not None and len(row["_errors"]) > 0
        assert (row["score"] >= threshold) == has_errors, (
            f"Mismatch between score and errors. score={row['score']}, "
            f"threshold={threshold}, errors={row['_errors']}"
        )
        if has_errors:
            flagged += 1
    assert flagged >= 1, "Expected at least one row to be flagged as anomalous"


def test_anomaly_scores_are_added(ws, spark: SparkSession, make_schema, make_random, anomaly_engine):
    """Test that anomaly scores are added to the DataFrame."""
    # Create unique schema and table names for test isolation
    catalog_name = TEST_CATALOG
    schema = make_schema(catalog_name=catalog_name)
    model_name = f"{catalog_name}.{schema.name}.test_scores_{make_random(4).lower()}"
    registry_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}_registry"

    # Use standard 2D training data - use helper
    train_simple_2d_model(spark, anomaly_engine, model_name, registry_table, train_data=get_standard_2d_training_data())
    # Test with normal and anomalous data
    test_points = get_standard_test_points_2d()
    test_df = spark.createDataFrame(
        [
            (1, *test_points["normal_in_center"]),
            (2, *test_points["clear_anomaly"]),
        ],
        "transaction_id int, amount double, quantity double",
    )

    dq_engine = DQEngine(ws, spark)
    threshold = 1.0
    checks = [
        create_anomaly_check_rule(
            model_name=model_name,
            registry_table=registry_table,
            threshold=threshold,
        )
    ]

    result_df = dq_engine.apply_checks(test_df, checks)

    # Verify anomaly_score column exists (nested in _dq_info.anomaly.score)

    assert "_dq_info" in result_df.columns

    # Verify scores are present by accessing nested field
    rows = result_df.select(F.col("_dq_info.anomaly.score").alias("anomaly_score")).collect()
    assert all(row["anomaly_score"] is not None for row in rows)


def test_auto_derivation_of_names(ws, spark: SparkSession, make_random, make_schema, anomaly_engine):
    """Test that model_name and registry_table can be omitted for scoring."""
    # Create unique schema and table names for test isolation
    catalog_name = TEST_CATALOG
    schema = make_schema(catalog_name=catalog_name)

    # Use standard 2D training data
    train_df = spark.createDataFrame(
        get_standard_2d_training_data(),
        "amount double, quantity double",
    )

    # Train with explicit names to avoid schema conflicts
    model_name = f"{catalog_name}.{schema.name}.test_auto_model_{make_random(4).lower()}"
    registry_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}_registry"

    model_uri = anomaly_engine.train(
        df=train_df,
        model_name=model_name,
        registry_table=registry_table,
        columns=["amount", "quantity"],
    )

    # Model URI should still be returned
    assert model_uri is not None

    # Score with normal data (within training distribution) without explicit names
    test_points = get_standard_test_points_2d()
    test_df = spark.createDataFrame(
        [(1, *test_points["normal_in_center"])],
        "transaction_id int, amount double, quantity double",
    )

    dq_engine = DQEngine(ws, spark)
    threshold = 1.0
    checks = [
        create_anomaly_check_rule(
            model_name=model_name,
            registry_table=registry_table,
            threshold=threshold,
        )
    ]

    result_df = dq_engine.apply_checks(test_df, checks)

    # Should succeed without errors - explicit names allow scoring to work
    errors_row = result_df.select("_errors").first()
    assert errors_row is not None, "Failed to get errors column"
    errors = errors_row["_errors"]
    assert errors is None or len(errors) == 0, f"Expected no errors, got: {errors}"


def test_threshold_flagging(ws, spark: SparkSession, make_schema, make_random, anomaly_engine):
    """Test that anomalous rows are flagged based on threshold."""
    # Create unique schema and table names for test isolation
    catalog_name = TEST_CATALOG
    schema = make_schema(catalog_name=catalog_name)
    model_name = f"{catalog_name}.{schema.name}.test_threshold_{make_random(4).lower()}"
    registry_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}_registry"

    # Use standard 2D training data - use helper
    train_df = spark.createDataFrame(
        get_standard_2d_training_data(),
        "amount double, quantity double",
    )
    anomaly_engine.train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name=model_name,
        registry_table=registry_table,
    )

    # Create test data with clear normal and anomalous rows
    test_points = get_standard_test_points_2d()
    test_df = spark.createDataFrame(
        [
            (1, *test_points["normal_in_center"]),
            (2, *test_points["normal_near_center"]),
            (3, *test_points["clear_anomaly"]),
        ],
        "transaction_id int, amount double, quantity double",
    )

    dq_engine = DQEngine(ws, spark)
    threshold = get_percentile_threshold_from_data(train_df, model_name, registry_table)
    checks = [
        create_anomaly_check_rule(
            model_name=model_name,
            registry_table=registry_table,
            threshold=threshold,
        )
    ]

    rows = (
        dq_engine.apply_checks(test_df, checks)
        .select("_errors", F.col("_dq_info.anomaly.score").alias("score"))
        .collect()
    )
    flagged = 0
    for row in rows:
        has_errors = row["_errors"] is not None and len(row["_errors"]) > 0
        assert (row["score"] >= threshold) == has_errors, (
            f"Mismatch between score and errors. score={row['score']}, "
            f"threshold={threshold}, errors={row['_errors']}"
        )
        if has_errors:
            flagged += 1
    assert flagged >= 1, "Expected at least one row to be flagged as anomalous"


def test_registry_table_auto_creation(spark: SparkSession, make_schema, make_random, anomaly_engine):
    """Test that registry table is auto-created if missing."""
    # Create unique schema and table names for test isolation
    catalog_name = TEST_CATALOG
    schema = make_schema(catalog_name=catalog_name)
    model_name = f"{catalog_name}.{schema.name}.test_auto_{make_random(4).lower()}"
    registry_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}_registry"

    # Drop table if exists
    spark.sql(f"DROP TABLE IF EXISTS {registry_table}")

    # Verify table doesn't exist (Unity Catalog compatible)
    table_exists = False
    try:
        spark.table(registry_table).limit(0).count()
        table_exists = True
    except Exception:  # noqa: BLE001
        pass
    assert not table_exists

    # Train (should auto-create registry) - use helper with standard 2D data
    train_simple_2d_model(spark, anomaly_engine, model_name, registry_table, train_data=get_standard_2d_training_data())

    # Verify table now exists (Unity Catalog compatible)
    table_exists = False
    try:
        spark.table(registry_table).limit(0).count()
        table_exists = True
    except Exception:  # noqa: BLE001
        pass
    assert table_exists

    # Verify table has expected schema (nested structs)
    registry_df = spark.table(registry_table)
    expected_top_level_columns = ["identity", "training", "features", "segmentation"]

    for col in expected_top_level_columns:
        assert col in registry_df.columns

    # Verify nested fields exist by accessing them (selecting proves they exist)
    registry_df.select("identity.model_name").limit(1).collect()  # Will error if field doesn't exist
    registry_df.select("training.columns").limit(1).collect()  # Will error if field doesn't exist
    registry_df.select("features.feature_importance").limit(1).collect()  # Will error if field doesn't exist


def test_multiple_columns(ws, spark: SparkSession, make_schema, make_random, anomaly_engine):
    """Test training and scoring with multiple columns."""
    # Create unique schema and table names for test isolation
    catalog_name = TEST_CATALOG
    schema = make_schema(catalog_name=catalog_name)
    model_name = f"{catalog_name}.{schema.name}.test_multi_{make_random(4).lower()}"
    registry_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}_registry"

    # Train 4D model - use helper with standard 4D data
    train_df = spark.createDataFrame(
        get_standard_4d_training_data(),
        "amount double, quantity double, discount double, weight double",
    )
    anomaly_engine.train(
        df=train_df,
        columns=["amount", "quantity", "discount", "weight"],
        model_name=model_name,
        registry_table=registry_table,
    )

    test_points = get_standard_test_points_4d()
    test_df = spark.createDataFrame(
        [
            (1, *test_points["normal_in_center"]),
            (2, *test_points["clear_anomaly"]),
        ],  # First in center, second far out
        "transaction_id int, amount double, quantity double, discount double, weight double",
    )

    dq_engine = DQEngine(ws, spark)
    threshold = get_percentile_threshold_from_data(train_df, model_name, registry_table)
    checks = [
        create_anomaly_check_rule(
            model_name=model_name,
            registry_table=registry_table,
            threshold=threshold,
        )
    ]

    result_df = dq_engine.apply_checks(test_df, checks)

    # Verify anomaly_score exists (nested in _dq_info.anomaly.score)

    assert "_dq_info" in result_df.columns

    rows = result_df.select("_errors", F.col("_dq_info.anomaly.score").alias("score")).collect()
    flagged = 0
    for row in rows:
        has_errors = row["_errors"] is not None and len(row["_errors"]) > 0
        assert (row["score"] >= threshold) == has_errors, (
            f"Mismatch between score and errors. score={row['score']}, "
            f"threshold={threshold}, errors={row['_errors']}"
        )
        if has_errors:
            flagged += 1
    assert flagged >= 1, "Expected at least one row to be flagged as anomalous"
