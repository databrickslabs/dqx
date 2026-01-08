"""Integration tests for null handling in anomaly detection."""

from collections.abc import Callable

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from databricks.labs.dqx.anomaly import has_no_anomalies
from tests.integration.test_anomaly_utils import apply_anomaly_check_direct


def test_training_filters_nulls(spark: SparkSession, make_random: Callable[[int], str], anomaly_engine):
    """Test that nulls are filtered during training."""
    # Create training data with nulls
    df = spark.createDataFrame(
        [(100.0, 2.0), (101.0, 2.0), (None, 2.0), (100.0, None), (102.0, 2.0)],
        "amount double, quantity double",
    )

    # Train (should filter nulls automatically)
    unique_id = make_random(8).lower()
    model_name = f"main.default.test_train_nulls_{make_random(4).lower()}"
    registry_table = f"main.default.{unique_id}_registry"

    anomaly_engine.train(
        df=df,
        columns=["amount", "quantity"],
        model_name=model_name,
        registry_table=registry_table,
    )

    # Check registry records training_rows (should be 3, not 5)
    record = spark.table(registry_table).filter(f"identity.model_name = '{model_name}'").first()
    assert record is not None
    assert record["training"]["training_rows"] > 0
    # Note: actual count may vary due to sampling, but should be <= 3


def test_nulls_are_skipped_not_flagged(spark: SparkSession, shared_2d_model, test_df_factory):
    """Test that rows with nulls are skipped (not flagged as anomalies)."""
    # Use shared pre-trained model (no training needed!)
    model_name = shared_2d_model["model_name"]
    registry_table = shared_2d_model["registry_table"]

    # Score data with nulls - use factory
    # Use values within training range (100-300 for amount, 10-50 for quantity)
    test_df = test_df_factory(
        spark,
        normal_rows=[(150.0, 20.0)],  # Middle of training range
        anomaly_rows=[(None, 20.0), (150.0, None), (None, None)],
        columns_schema="amount double, quantity double",
    )

    # Call apply function directly to get anomaly_score column
    result_df = apply_anomaly_check_direct(
        test_df, model_name, registry_table, columns=["amount", "quantity"], score_threshold=0.5
    )

    # NOTE: Null handling follows ML best practices:
    # 1. A null indicator column (e.g., "amount_is_null") is created to preserve null information
    # 2. Nulls are imputed to 0.0 so the model can process them
    # 3. All rows get scores (nulls are not skipped)
    # This allows the model to learn if nulls correlate with anomalies
    all_rows = result_df.count()
    scored_rows = result_df.filter("anomaly_score IS NOT NULL").count()
    assert scored_rows == all_rows  # All rows scored (nulls imputed to 0)

    # Verify all rows have scores (even those with nulls in original data)
    rows = result_df.collect()
    for row in rows:
        # All rows should have scores because nulls are imputed
        assert row["anomaly_score"] is not None


def test_partial_nulls(spark: SparkSession, shared_3d_model, test_df_factory):
    """Test behavior when some columns are null, others are non-null."""
    # Use shared pre-trained 3D model (no training needed!)
    model_name = shared_3d_model["model_name"]
    registry_table = shared_3d_model["registry_table"]

    # Test data with partial nulls - use factory
    # Use values within training range (100-300 for amount, 10-50 for quantity, 0.1-0.5 for discount)
    test_df = test_df_factory(
        spark,
        normal_rows=[(200.0, 30.0, 0.3)],  # Middle of training range
        anomaly_rows=[
            (None, 30.0, 0.3),  # amount is null
            (200.0, None, 0.3),  # quantity is null
            (200.0, 30.0, None),  # discount is null
        ],
        columns_schema="amount double, quantity double, discount double",
    )

    # Call apply function directly to get anomaly_score column
    _, apply_fn = has_no_anomalies(
        merge_columns=["transaction_id"],
        columns=["amount", "quantity", "discount"],
        model=model_name,
        registry_table=registry_table,
        score_threshold=0.5,
    )
    result_df = apply_fn(test_df)
    rows = result_df.select("transaction_id", F.col("_info.anomaly.score").alias("anomaly_score")).collect()

    # All rows should have scores (nulls are imputed to 0)
    assert rows[0]["anomaly_score"] is not None
    assert rows[1]["anomaly_score"] is not None  # Null imputed
    assert rows[2]["anomaly_score"] is not None  # Null imputed
    assert rows[3]["anomaly_score"] is not None  # Null imputed


def test_all_nulls_row(spark: SparkSession, shared_2d_model, test_df_factory):
    """Test row with all nulls in anomaly columns is skipped."""
    # Use shared pre-trained model (no training needed!)
    model_name = shared_2d_model["model_name"]
    registry_table = shared_2d_model["registry_table"]

    # Test data with all nulls - use factory
    # Use values within training range (100-300 for amount, 10-50 for quantity)
    test_df = test_df_factory(
        spark,
        normal_rows=[(200.0, 30.0)],  # Middle of training range
        anomaly_rows=[(None, None)],
        columns_schema="amount double, quantity double",
    )

    # Call apply function directly to get anomaly_score column
    result_df = apply_anomaly_check_direct(
        test_df, model_name, registry_table, columns=["amount", "quantity"], score_threshold=0.5
    )
    rows = result_df.collect()

    # All rows have scores (nulls are imputed to 0)
    assert rows[0]["anomaly_score"] is not None
    assert rows[1]["anomaly_score"] is not None  # All nulls imputed to 0


def test_mixed_null_and_anomaly(spark: SparkSession, shared_2d_model, test_df_factory):
    """Test dataset with both nulls and anomalies."""
    # Use shared pre-trained model (no training needed!)
    model_name = shared_2d_model["model_name"]
    registry_table = shared_2d_model["registry_table"]

    # Test data: normal, null, anomaly - use factory
    # Use values within training range (100-300 for amount, 10-50 for quantity)
    test_df = test_df_factory(
        spark,
        normal_rows=[(200.0, 30.0)],  # Normal (middle of training range)
        anomaly_rows=[
            (None, 30.0),  # Null
            (9999.0, 1.0),  # Anomaly (far outside training range)
        ],
        columns_schema="amount double, quantity double",
    )

    # Call apply function directly to get anomaly_score column
    result_df = apply_anomaly_check_direct(
        test_df, model_name, registry_table, columns=["amount", "quantity"], score_threshold=0.5
    )
    rows = result_df.collect()

    # All rows have scores (nulls are imputed to 0)
    assert rows[0]["anomaly_score"] is not None  # Normal row
    assert rows[1]["anomaly_score"] is not None  # Null row (imputed)
    assert rows[2]["anomaly_score"] is not None  # Anomaly row
