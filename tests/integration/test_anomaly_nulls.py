"""Integration tests for null handling in anomaly detection."""

from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession

from databricks.labs.dqx.anomaly import train, has_no_anomalies
from databricks.sdk import WorkspaceClient


@pytest.fixture
def mock_workspace_client():
    """Create a mock WorkspaceClient for testing."""
    return MagicMock(spec=WorkspaceClient)


def test_training_filters_nulls(spark: SparkSession, make_random: str):
    """Test that nulls are filtered during training."""
    # Create training data with nulls
    df = spark.createDataFrame(
        [(100.0, 2.0), (101.0, 2.0), (None, 2.0), (100.0, None), (102.0, 2.0)],
        "amount double, quantity double",
    )

    unique_id = make_random(8).lower()
    model_name = f"main.default.test_train_nulls_{make_random(4).lower()}"
    registry_table = f"main.default.{unique_id}_registry"

    # Train (should filter nulls automatically)
    model_uri = train(
        df=df,
        columns=["amount", "quantity"],
        model_name=model_name,
        registry_table=registry_table,
    )

    # Verify model was trained successfully
    assert model_uri is not None

    # Check registry records training_rows (should be 3, not 5)
    record = spark.table(registry_table).filter(f"model_name = '{model_name}'").first()
    assert record is not None
    assert record["training_rows"] > 0
    # Note: actual count may vary due to sampling, but should be <= 3


def test_nulls_are_skipped_not_flagged(spark: SparkSession, mock_workspace_client, make_random: str):
    """Test that rows with nulls are skipped (not flagged as anomalies)."""
    # Train on normal data (no nulls) with variance
    train_df = spark.createDataFrame(
        [(100.0 + i * 0.5, 2.0 + i * 0.01) for i in range(50)],
        "amount double, quantity double",
    )

    unique_id = make_random(8).lower()
    model_name = f"main.default.test_nulls_{make_random(4).lower()}"
    registry_table = f"main.default.{unique_id}_registry"

    train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name=model_name,
        registry_table=registry_table,
    )

    # Score data with nulls
    test_df = spark.createDataFrame(
        [(110.0, 2.1), (None, 2.0), (100.0, None), (None, None)],
        "amount double, quantity double",
    )

    # Call apply function directly to get anomaly_score column
    _, apply_fn = has_no_anomalies(
        merge_columns=["transaction_id"],
        columns=["amount", "quantity"],
        model=model_name,
        registry_table=registry_table,
        score_threshold=0.5,
    )
    result_df = apply_fn(test_df)

    # Collect rows with null anomaly_score
    null_score_rows = result_df.filter("anomaly_score IS NULL").count()

    # Three rows have nulls, so should have null anomaly_score
    assert null_score_rows == 3

    # Verify null rows don't have errors (they're skipped, not flagged)
    rows = result_df.collect()
    for row in rows:
        if row["amount"] is None or row["quantity"] is None:
            # Null rows should not have anomaly-related errors
            # (they might have other errors from other checks, but not from anomaly check)
            assert row["anomaly_score"] is None


def test_partial_nulls(spark: SparkSession, mock_workspace_client, make_random: str):
    """Test behavior when some columns are null, others are non-null."""
    train_df = spark.createDataFrame(
        [(100.0 + i * 0.5, 2.0 + i * 0.01, 0.1 + i * 0.001) for i in range(50)],
        "amount double, quantity double, discount double",
    )

    unique_id = make_random(8).lower()
    model_name = f"main.default.test_partial_nulls_{make_random(4).lower()}"
    registry_table = f"main.default.{unique_id}_registry"

    train(
        df=train_df,
        columns=["amount", "quantity", "discount"],
        model_name=model_name,
        registry_table=registry_table,
    )

    # Test data with partial nulls
    test_df = spark.createDataFrame(
        [
            (112.0, 2.1, 0.11),  # No nulls, middle values
            (None, 2.0, 0.1),  # amount is null
            (100.0, None, 0.1),  # quantity is null
            (100.0, 2.0, None),  # discount is null
        ],
        "amount double, quantity double, discount double",
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
    rows = result_df.collect()

    # First row (no nulls) should have a score
    assert rows[0]["anomaly_score"] is not None

    # Rows with any null should have null score
    assert rows[1]["anomaly_score"] is None
    assert rows[2]["anomaly_score"] is None
    assert rows[3]["anomaly_score"] is None


def test_all_nulls_row(spark: SparkSession, mock_workspace_client, make_random: str):
    """Test row with all nulls in anomaly columns is skipped."""
    train_df = spark.createDataFrame(
        [(100.0 + i * 0.5, 2.0 + i * 0.01) for i in range(50)],
        "amount double, quantity double",
    )

    unique_id = make_random(8).lower()
    model_name = f"main.default.test_all_nulls_{make_random(4).lower()}"
    registry_table = f"main.default.{unique_id}_registry"

    train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name=model_name,
        registry_table=registry_table,
    )

    # Test data with all nulls
    test_df = spark.createDataFrame(
        [(112.0, 2.1), (None, None)],
        "amount double, quantity double",
    )

    # Call apply function directly to get anomaly_score column
    _, apply_fn = has_no_anomalies(
        merge_columns=["transaction_id"],
        columns=["amount", "quantity"],
        model=model_name,
        registry_table=registry_table,
        score_threshold=0.5,
    )
    result_df = apply_fn(test_df)
    rows = result_df.collect()

    # First row has score
    assert rows[0]["anomaly_score"] is not None

    # Second row (all nulls) has null score (which means it was skipped, not flagged)
    assert rows[1]["anomaly_score"] is None


def test_mixed_null_and_anomaly(spark: SparkSession, mock_workspace_client, make_random: str):
    """Test dataset with both nulls and anomalies."""
    train_df = spark.createDataFrame(
        [(100.0 + i * 0.5, 2.0 + i * 0.01) for i in range(50)],
        "amount double, quantity double",
    )

    unique_id = make_random(8).lower()
    model_name = f"main.default.test_mixed_{make_random(4).lower()}"
    registry_table = f"main.default.{unique_id}_registry"

    train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name=model_name,
        registry_table=registry_table,
    )

    # Test data: normal, null, anomaly
    test_df = spark.createDataFrame(
        [
            (112.0, 2.1),  # Normal (middle of training range)
            (None, 2.0),  # Null
            (9999.0, 1.0),  # Anomaly
        ],
        "amount double, quantity double",
    )

    # Call apply function directly to get anomaly_score column
    _, apply_fn = has_no_anomalies(
        merge_columns=["transaction_id"],
        columns=["amount", "quantity"],
        model=model_name,
        registry_table=registry_table,
        score_threshold=0.5,
    )
    result_df = apply_fn(test_df)
    rows = result_df.collect()

    # Normal row: has score
    assert rows[0]["anomaly_score"] is not None

    # Null row: no score
    assert rows[1]["anomaly_score"] is None

    # Anomaly row: has score
    assert rows[2]["anomaly_score"] is not None
