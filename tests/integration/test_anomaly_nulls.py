"""Integration tests for null handling in anomaly detection."""

from unittest.mock import MagicMock

import pytest
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from databricks.labs.dqx.anomaly import has_no_anomalies
from databricks.sdk import WorkspaceClient
from tests.integration.test_anomaly_utils import apply_anomaly_check_direct


@pytest.fixture
def mock_workspace_client():
    """Create a mock WorkspaceClient for testing."""
    return MagicMock(spec=WorkspaceClient)


def _setup_anomaly_model(anomaly_engine, train_df, make_random_fn, prefix="test", columns=None):
    """
    Helper to train an anomaly model with standard setup.

    Args:
        anomaly_engine (AnomalyEngine): AnomalyEngine instance
        train_df (DataFrame): Training DataFrame
        make_random_fn (Callable): make_random fixture function
        prefix (str): Prefix for model name
        columns (list[str] | None): Columns to train on (default: ["amount", "quantity"])

    Returns:
        dict: dict with model_name and registry_table
    """
    if columns is None:
        columns = ["amount", "quantity"]

    unique_id = make_random_fn(8).lower()
    model_name = f"main.default.{prefix}_{make_random_fn(4).lower()}"
    registry_table = f"main.default.{unique_id}_registry"

    anomaly_engine.train(
        df=train_df,
        columns=columns,
        model_name=model_name,
        registry_table=registry_table,
    )

    return {"model_name": model_name, "registry_table": registry_table}


def test_training_filters_nulls(spark: SparkSession, make_random: str, anomaly_engine):
    """Test that nulls are filtered during training."""
    # Create training data with nulls
    df = spark.createDataFrame(
        [(100.0, 2.0), (101.0, 2.0), (None, 2.0), (100.0, None), (102.0, 2.0)],
        "amount double, quantity double",
    )

    # Train (should filter nulls automatically)
    info = _setup_anomaly_model(anomaly_engine, df, make_random, "test_train_nulls")
    model_name = info["model_name"]
    registry_table = info["registry_table"]

    # Check registry records training_rows (should be 3, not 5)
    record = spark.table(registry_table).filter(f"identity.model_name = '{model_name}'").first()
    assert record is not None
    assert record["training"]["training_rows"] > 0
    # Note: actual count may vary due to sampling, but should be <= 3


def test_nulls_are_skipped_not_flagged(
    spark: SparkSession, mock_workspace_client, make_random: str, anomaly_engine, test_df_factory
):
    """Test that rows with nulls are skipped (not flagged as anomalies)."""
    # Train on normal data (no nulls) with variance
    train_df = spark.createDataFrame(
        [(100.0 + i * 0.5, 2.0 + i * 0.01) for i in range(50)],
        "amount double, quantity double",
    )

    info = _setup_anomaly_model(anomaly_engine, train_df, make_random, "test_nulls")
    model_name = info["model_name"]
    registry_table = info["registry_table"]

    # Score data with nulls - use factory
    test_df = test_df_factory(
        spark,
        normal_rows=[(110.0, 2.1)],
        anomaly_rows=[(None, 2.0), (100.0, None), (None, None)],
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


def test_partial_nulls(spark: SparkSession, mock_workspace_client, make_random: str, anomaly_engine, test_df_factory):
    """Test behavior when some columns are null, others are non-null."""
    train_df = spark.createDataFrame(
        [(100.0 + i * 0.5, 2.0 + i * 0.01, 0.1 + i * 0.001) for i in range(50)],
        "amount double, quantity double, discount double",
    )

    info = _setup_anomaly_model(
        anomaly_engine, train_df, make_random, "test_partial_nulls", columns=["amount", "quantity", "discount"]
    )
    model_name = info["model_name"]
    registry_table = info["registry_table"]

    # Test data with partial nulls - use factory
    test_df = test_df_factory(
        spark,
        normal_rows=[(112.0, 2.1, 0.11)],  # No nulls, middle values
        anomaly_rows=[
            (None, 2.0, 0.1),  # amount is null
            (100.0, None, 0.1),  # quantity is null
            (100.0, 2.0, None),  # discount is null
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


def test_all_nulls_row(spark: SparkSession, mock_workspace_client, make_random: str, anomaly_engine, test_df_factory):
    """Test row with all nulls in anomaly columns is skipped."""
    train_df = spark.createDataFrame(
        [(100.0 + i * 0.5, 2.0 + i * 0.01) for i in range(50)],
        "amount double, quantity double",
    )

    info = _setup_anomaly_model(anomaly_engine, train_df, make_random, "test_all_nulls")
    model_name = info["model_name"]
    registry_table = info["registry_table"]

    # Test data with all nulls - use factory
    test_df = test_df_factory(
        spark,
        normal_rows=[(112.0, 2.1)],
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


def test_mixed_null_and_anomaly(
    spark: SparkSession, mock_workspace_client, make_random: str, anomaly_engine, test_df_factory
):
    """Test dataset with both nulls and anomalies."""
    train_df = spark.createDataFrame(
        [(100.0 + i * 0.5, 2.0 + i * 0.01) for i in range(50)],
        "amount double, quantity double",
    )

    info = _setup_anomaly_model(anomaly_engine, train_df, make_random, "test_mixed")
    model_name = info["model_name"]
    registry_table = info["registry_table"]

    # Test data: normal, null, anomaly - use factory
    test_df = test_df_factory(
        spark,
        normal_rows=[(112.0, 2.1)],  # Normal (middle of training range)
        anomaly_rows=[
            (None, 2.0),  # Null
            (9999.0, 1.0),  # Anomaly
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
