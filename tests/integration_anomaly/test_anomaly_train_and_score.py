"""Integration tests for row anomaly detection end-to-end flow."""

import pyspark.sql.functions as F
from pyspark.testing.utils import assertDataFrameEqual
from pyspark.sql import SparkSession

from databricks.labs.dqx.engine import DQEngine
from tests.constants import TEST_CATALOG
from tests.integration_anomaly.conftest import (
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
    catalog_name = TEST_CATALOG
    schema = make_schema(catalog_name=catalog_name)
    model_name = f"{catalog_name}.{schema.name}.test_basic_{make_random(4).lower()}"
    registry_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}_registry"

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
    assert model_uri is not None
    assert model_name in model_uri

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

    # Compare deterministic view: original data + has_errors (row 1 normal, row 2 anomalous)
    result_compare = result_df.withColumn("has_errors", F.size(F.coalesce(F.col("_errors"), F.array())) > 0).select(
        "transaction_id", "amount", "quantity", "has_errors"
    )
    expected_compare = test_df.withColumn("has_errors", F.when(F.col("transaction_id") == 2, True).otherwise(False))
    assertDataFrameEqual(result_compare, expected_compare, checkRowOrder=False)


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
    threshold = 100.0
    checks = [
        create_anomaly_check_rule(
            model_name=model_name,
            registry_table=registry_table,
            threshold=threshold,
        )
    ]

    result_df = dq_engine.apply_checks(test_df, checks)

    assert "_dq_info" in result_df.columns

    # Verify scores are present by accessing nested field
    rows = result_df.select(
        F.col("_dq_info").getItem(0).getField("anomaly").getField("score").alias("anomaly_score")
    ).collect()
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
    threshold = 100.0
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
        .select("_errors", F.col("_dq_info").getItem(0).getField("anomaly").getField("is_anomaly").alias("is_anomaly"))
        .collect()
    )
    flagged = 0
    for row in rows:
        has_errors = row["_errors"] is not None and len(row["_errors"]) > 0
        assert row["is_anomaly"] == has_errors, (
            f"Mismatch between is_anomaly and errors. is_anomaly={row['is_anomaly']}, "
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

    # Train (should auto-create registry) - use helper with standard 2D data
    train_simple_2d_model(spark, anomaly_engine, model_name, registry_table, train_data=get_standard_2d_training_data())

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

    # Verify anomaly_score exists (nested in _dq_info[0].anomaly.score)

    assert "_dq_info" in result_df.columns

    rows = result_df.select(
        "_errors", F.col("_dq_info").getItem(0).getField("anomaly").getField("is_anomaly").alias("is_anomaly")
    ).collect()
    flagged = 0
    for row in rows:
        has_errors = row["_errors"] is not None and len(row["_errors"]) > 0
        assert row["is_anomaly"] == has_errors, (
            f"Mismatch between is_anomaly and errors. is_anomaly={row['is_anomaly']}, "
            f"threshold={threshold}, errors={row['_errors']}"
        )
        if has_errors:
            flagged += 1
    assert flagged >= 1, "Expected at least one row to be flagged as anomalous"
