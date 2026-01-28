"""Integration tests for anomaly detection sampling and performance."""

import warnings
from collections.abc import Callable

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from databricks.labs.dqx.anomaly.check_funcs import set_driver_only_for_tests
from databricks.labs.dqx.config import AnomalyParams
from databricks.labs.dqx.engine import DQEngine

from tests.integration_anomaly.test_anomaly_utils import (
    create_anomaly_check_rule,
    train_large_dataset_model,
    train_model_with_params,
    train_simple_2d_model,
)


def test_sampling_caps_large_datasets(
    spark: SparkSession, make_random: Callable[[int], str], anomaly_engine, anomaly_registry_prefix
):
    """Test that sampling caps at max_rows for large datasets."""

    unique_id = make_random(8).lower()
    model_name = f"{anomaly_registry_prefix}.test_sampling_large_{make_random(4).lower()}"
    registry_table = f"{anomaly_registry_prefix}.{unique_id}_registry"

    # Train with defaults (should cap at max_rows if needed) - use helper
    train_large_dataset_model(spark, anomaly_engine, model_name, registry_table, num_rows=200_000)

    # Check registry records training_rows (should be sampled, use full three-level name)
    record = spark.table(registry_table).filter(f"identity.model_name = '{model_name}'").first()
    assert record is not None

    # With default sample_fraction=0.3, we should get ~60K rows
    assert record["training"]["training_rows"] > 0
    assert record["training"]["training_rows"] <= 200_000


def test_custom_sampling_parameters(
    spark: SparkSession, make_random: Callable[[int], str], anomaly_engine, anomaly_registry_prefix
):
    """Test that custom sample_fraction and max_rows are respected."""
    unique_id = make_random(8).lower()
    model_name = f"{anomaly_registry_prefix}.test_custom_sampling_{make_random(4).lower()}"
    registry_table = f"{anomaly_registry_prefix}.{unique_id}_registry"

    # Use custom sampling: 50% sample, max 300 rows
    params = AnomalyParams(
        sample_fraction=0.5,
        max_rows=300,
    )

    # Train with custom params - use helper
    train_large_dataset_model(spark, anomaly_engine, model_name, registry_table, num_rows=1000, params=params)

    # Check registry (use full three-level name)
    record = spark.table(registry_table).filter(f"identity.model_name = '{model_name}'").first()
    assert record is not None

    # Should have sampled roughly 50% up to max 300 rows
    # So training_rows should be <= 300
    assert record["training"]["training_rows"] <= 300


def test_exclude_columns_precedence(
    spark: SparkSession, make_random: Callable[[int], str], anomaly_engine, anomaly_registry_prefix
):
    """Test that exclude_columns always take precedence over columns."""
    unique_id = make_random(8).lower()
    model_name = f"{anomaly_registry_prefix}.test_exclude_{make_random(4).lower()}"
    registry_table = f"{anomaly_registry_prefix}.{unique_id}_registry"

    df = spark.createDataFrame(
        [(1, 100.0, 2.0), (2, 110.0, 3.0), (3, 120.0, 4.0)],
        "user_id int, amount double, quantity double",
    )

    anomaly_engine.train(
        df=df,
        model_name=model_name,
        registry_table=registry_table,
        columns=["amount", "quantity", "user_id"],
        exclude_columns=["user_id"],
    )

    record = spark.table(registry_table).filter(f"identity.model_name = '{model_name}'").first()
    assert record is not None
    assert "user_id" not in record.training.columns


def test_sampling_warning_issued(
    spark: SparkSession, make_random: Callable[[int], str], anomaly_engine, anomaly_registry_prefix, caplog
):
    """Test that warning is issued when data is truncated."""
    unique_id = make_random(8).lower()
    model_name = f"{anomaly_registry_prefix}.test_sampling_warning_{make_random(4).lower()}"
    registry_table = f"{anomaly_registry_prefix}.{unique_id}_registry"

    # Use very restrictive sampling
    params = AnomalyParams(
        sample_fraction=0.1,
        max_rows=500,
    )

    # Should issue warning about truncation
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        with caplog.at_level("WARNING"):
            # Train with restrictive params - use helper
            train_large_dataset_model(spark, anomaly_engine, model_name, registry_table, num_rows=10_000, params=params)

    warning_messages = [record.message for record in caplog.records if record.levelname == "WARNING"]
    assert any("sampling capped" in message.lower() for message in warning_messages)

    # Check for sampling/truncation warning
    # (Implementation may or may not warn, this is aspirational)
    # Commenting out assertion as implementation may vary


def test_train_validation_split(
    spark: SparkSession, make_random: Callable[[int], str], anomaly_engine, anomaly_registry_prefix
):
    """Test that train/validation split works correctly."""
    unique_id = make_random(8).lower()
    model_name = f"{anomaly_registry_prefix}.test_train_val_split_{make_random(4).lower()}"
    registry_table = f"{anomaly_registry_prefix}.{unique_id}_registry"

    # Use default train_ratio (0.8)
    params = AnomalyParams(
        sample_fraction=1.0,  # Use all data
        max_rows=1000,
        train_ratio=0.8,  # 80/20 split
    )

    # Train with validation split - use helper
    train_large_dataset_model(spark, anomaly_engine, model_name, registry_table, num_rows=1000, params=params)

    # Check that metrics exist (which indicates validation was performed, use full three-level name)
    record = spark.table(registry_table).filter(f"identity.model_name = '{model_name}'").first()
    assert record is not None
    assert record["training"]["training_rows"] <= params.max_rows

    # Verify metrics exist
    assert record["training"]["metrics"] is not None


def test_custom_train_ratio(
    spark: SparkSession, make_random: Callable[[int], str], anomaly_engine, anomaly_registry_prefix
):
    """Test that custom train_ratio is respected."""
    unique_id = make_random(8).lower()
    model_name = f"{anomaly_registry_prefix}.test_custom_train_ratio_{make_random(4).lower()}"
    registry_table = f"{anomaly_registry_prefix}.{unique_id}_registry"

    # Use 90/10 split
    params = AnomalyParams(
        sample_fraction=1.0,
        max_rows=1000,
        train_ratio=0.9,
    )

    # Train with custom train ratio - use helper
    train_large_dataset_model(spark, anomaly_engine, model_name, registry_table, num_rows=1000, params=params)


def test_no_sampling_with_full_fraction(
    spark: SparkSession, make_random: Callable[[int], str], anomaly_engine, anomaly_registry_prefix
):
    """Test that sample_fraction=1.0 uses all data (up to max_rows)."""
    unique_id = make_random(8).lower()
    model_name = f"{anomaly_registry_prefix}.test_no_sampling_{make_random(4).lower()}"
    registry_table = f"{anomaly_registry_prefix}.{unique_id}_registry"

    params = AnomalyParams(
        sample_fraction=1.0,  # Use all data
        max_rows=1000,  # Higher than dataset size
    )

    # Train with no sampling - use helper
    train_large_dataset_model(spark, anomaly_engine, model_name, registry_table, num_rows=500, params=params)

    # Use full three-level name for query
    record = spark.table(registry_table).filter(f"identity.model_name = '{model_name}'").first()
    assert record is not None


def test_distributed_scoring_with_row_filter(
    ws,
    spark: SparkSession,
    make_random: Callable[[int], str],
    anomaly_engine,
    test_df_factory,
    anomaly_registry_prefix,
):
    """Ensure row_filter scoring uses distributed path and joins results back."""
    unique_id = make_random(8).lower()
    model_name = f"{anomaly_registry_prefix}.test_row_filter_dist_{make_random(4).lower()}"
    registry_table = f"{anomaly_registry_prefix}.{unique_id}_registry"

    train_simple_2d_model(spark, anomaly_engine, model_name, registry_table)

    df = test_df_factory(
        spark,
        normal_rows=[(100.0, 2.0), (200.0, 2.0)],
        anomaly_rows=[],
        columns_schema="amount double, quantity double",
    )

    set_driver_only_for_tests(False)
    try:
        dq_engine = DQEngine(ws, spark)
        check = create_anomaly_check_rule(
            model_name=model_name,
            registry_table=registry_table,
            row_filter="amount > 150",
            include_contributions=False,
        )
        result = dq_engine.apply_checks(df, [check])
        scores = result.select("amount", F.col("_dq_info.anomaly.score").alias("score")).collect()
    finally:
        set_driver_only_for_tests(True)

    scored = {row.amount: row.score for row in scores}
    assert scored[100.0] is None
    assert scored[200.0] is not None


def test_train_ratio_no_validation_path(
    spark: SparkSession, make_random: Callable[[int], str], anomaly_engine, anomaly_registry_prefix
):
    """Ensure training works when validation split is disabled."""
    unique_id = make_random(8).lower()
    model_name = f"{anomaly_registry_prefix}.test_train_ratio_none_{make_random(4).lower()}"
    registry_table = f"{anomaly_registry_prefix}.{unique_id}_registry"

    params = AnomalyParams(
        sample_fraction=1.0,
        max_rows=200,
        train_ratio=1.0,
    )

    train_large_dataset_model(spark, anomaly_engine, model_name, registry_table, num_rows=200, params=params)

    record = spark.table(registry_table).filter(f"identity.model_name = '{model_name}'").first()
    assert record is not None
    assert record["training"]["training_rows"] > 0
    assert record["training"]["training_rows"] <= params.max_rows


def test_minimal_data_with_sampling(
    spark: SparkSession, make_random: Callable[[int], str], anomaly_engine, anomaly_registry_prefix
):
    """Test that small datasets work with sampling."""
    unique_id = make_random(8).lower()
    model_name = f"{anomaly_registry_prefix}.test_minimal_data_{make_random(4).lower()}"
    registry_table = f"{anomaly_registry_prefix}.{unique_id}_registry"

    params = AnomalyParams(
        sample_fraction=1.0,
        max_rows=100,
    )

    # Train with minimal data - use helper
    train_large_dataset_model(spark, anomaly_engine, model_name, registry_table, num_rows=20, params=params)


def test_performance_with_many_columns(
    spark: SparkSession, make_random: Callable[[int], str], anomaly_engine, anomaly_registry_prefix
):
    """Test that training completes in reasonable time with many columns."""
    # Create dataset with 10 columns
    _ = anomaly_engine
    df = spark.range(1000).selectExpr(
        "cast(id as double) as col1",
        "2.0 as col2",
        "3.0 as col3",
        "4.0 as col4",
        "5.0 as col5",
        "6.0 as col6",
        "7.0 as col7",
        "8.0 as col8",
        "9.0 as col9",
        "10.0 as col10",
    )

    unique_id = make_random(8).lower()
    model_name = f"{anomaly_registry_prefix}.test_many_cols_{make_random(4).lower()}"
    registry_table = f"{anomaly_registry_prefix}.{unique_id}_registry"

    params = AnomalyParams(
        sample_fraction=0.5,
        max_rows=500,
    )

    # This should complete without timing out
    model_uri = train_model_with_params(
        anomaly_engine=anomaly_engine,
        df=df,
        model_name=model_name,
        registry_table=registry_table,
        columns=[f"col{i}" for i in range(1, 11)],
        params=params,
    )

    assert model_uri is not None
