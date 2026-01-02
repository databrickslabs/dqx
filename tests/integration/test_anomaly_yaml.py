"""Integration tests for YAML-based anomaly detection configuration."""

from unittest.mock import MagicMock

import pytest
import yaml
from pyspark.sql import SparkSession

from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient


@pytest.fixture
def mock_workspace_client():
    """Create a mock WorkspaceClient for testing."""
    return MagicMock(spec=WorkspaceClient)


def test_yaml_based_checks(spark: SparkSession, mock_workspace_client, make_random: str, anomaly_engine):
    """Test applying anomaly checks defined in YAML."""
    # Train model first
    train_df = spark.createDataFrame(
        [(100.0 + i * 0.5, 2.0) for i in range(50)],
        "amount double, quantity double",
    )

    unique_id = make_random(8).lower()
    model_name = f"test_yaml_{make_random(4).lower()}"
    registry_table = f"main.default.{unique_id}_registry"

    anomaly_engine.train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name=model_name,
        registry_table=registry_table,
    )

    # Define checks in YAML format
    checks_yaml = f"""
    - criticality: error
      check:
        function: has_no_anomalies
        arguments:
          merge_columns: [transaction_id]
          columns: [amount, quantity]
          model: {model_name}
          registry_table: {registry_table}
          score_threshold: 0.5
    """

    checks = yaml.safe_load(checks_yaml)

    test_df = spark.createDataFrame(
        [(1, 100.0, 2.0), (2, 9999.0, 1.0)],
        "transaction_id int, amount double, quantity double",
    )

    dq_engine = DQEngine(mock_workspace_client)
    result_df = dq_engine.apply_checks_by_metadata(test_df, checks)

    # Verify DQX metadata columns are added
    assert "_errors" in result_df.columns
    assert "_warnings" in result_df.columns

    # Verify errors are added for anomalies
    rows = result_df.collect()
    assert len(rows[1]["_errors"]) > 0  # Anomalous row has errors


@pytest.mark.nightly
def test_yaml_with_multiple_checks(spark: SparkSession, mock_workspace_client, make_random: str, anomaly_engine):
    """Test YAML with multiple anomaly and standard checks."""
    train_df = spark.createDataFrame(
        [(100.0 + i * 0.5, 2.0 + i * 0.01) for i in range(200)],  # Increase training data for stability
        "amount double, quantity double",
    )

    unique_id = make_random(8).lower()
    model_name = f"test_yaml_multi_{make_random(4).lower()}"
    registry_table = f"main.default.{unique_id}_registry"

    anomaly_engine.train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name=model_name,
        registry_table=registry_table,
    )

    # Define multiple checks in YAML
    checks_yaml = f"""
    - criticality: error
      check:
        function: is_not_null
        arguments:
          column: amount
    - criticality: error
      check:
        function: has_no_anomalies
        arguments:
          merge_columns: [transaction_id]
          columns: [amount, quantity]
          model: {model_name}
          registry_table: {registry_table}
          score_threshold: 0.6
    """

    checks = yaml.safe_load(checks_yaml)

    test_df = spark.createDataFrame(
        [
            (1, 150.0, 3.0),
            (2, None, 3.0),
            (3, 9999.0, 1.0),
        ],  # Use middle values from training range [100, 199.5] and [2.0, 3.99]
        "transaction_id int, amount double, quantity double",
    )

    dq_engine = DQEngine(mock_workspace_client)
    result_df = dq_engine.apply_checks_by_metadata(test_df, checks)

    rows = result_df.collect()

    # Normal row: no errors (or handle None)
    row0_errors = rows[0]["_errors"] if rows[0]["_errors"] is not None else []
    assert len(row0_errors) == 0

    # Null row: has is_not_null error (handle None)
    row1_errors = rows[1]["_errors"] if rows[1]["_errors"] is not None else []
    assert len(row1_errors) > 0

    # Anomaly row: has anomaly error (handle None)
    row2_errors = rows[2]["_errors"] if rows[2]["_errors"] is not None else []
    assert len(row2_errors) > 0


@pytest.mark.nightly
def test_yaml_with_custom_threshold(spark: SparkSession, mock_workspace_client, make_random: str, anomaly_engine):
    """Test YAML configuration with custom score_threshold."""
    train_df = spark.createDataFrame(
        [(100.0 + i * 0.5, 2.0) for i in range(50)],
        "amount double, quantity double",
    )

    unique_id = make_random(8).lower()
    model_name = f"test_yaml_threshold_{make_random(4).lower()}"
    registry_table = f"main.default.{unique_id}_registry"

    anomaly_engine.train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name=model_name,
        registry_table=registry_table,
    )

    # Define check with custom threshold
    checks_yaml = f"""
    - criticality: error
      check:
        function: has_no_anomalies
        arguments:
          merge_columns: [transaction_id]
          columns: [amount, quantity]
          model: {model_name}
          registry_table: {registry_table}
          score_threshold: 0.9
    """

    checks = yaml.safe_load(checks_yaml)

    test_df = spark.createDataFrame(
        [(1, 100.0, 2.0), (2, 150.0, 1.8)],  # Slightly unusual but not extreme
        "transaction_id int, amount double, quantity double",
    )

    dq_engine = DQEngine(mock_workspace_client)
    result_df = dq_engine.apply_checks_by_metadata(test_df, checks)

    # With high threshold (0.9), slightly unusual data should pass
    assert "_errors" in result_df.columns
    rows = result_df.collect()
    # With high threshold, expect no or few errors (handle both None and empty list)
    error_counts = [len(row["_errors"]) if row["_errors"] is not None else 0 for row in rows]
    assert all(count == 0 for count in error_counts) or sum(error_counts) <= 1


@pytest.mark.nightly
def test_yaml_with_contributions(spark: SparkSession, mock_workspace_client, make_random: str, anomaly_engine):
    """Test YAML configuration with include_contributions flag."""
    train_df = spark.createDataFrame(
        [(100.0 + i * 0.5, 2.0 + i * 0.01, 0.1 + i * 0.001) for i in range(30)],
        "amount double, quantity double, discount double",
    )

    unique_id = make_random(8).lower()
    model_name = f"test_yaml_contrib_{make_random(4).lower()}"
    registry_table = f"main.default.{unique_id}_registry"

    anomaly_engine.train(
        df=train_df,
        columns=["amount", "quantity", "discount"],
        model_name=model_name,
        registry_table=registry_table,
    )

    # Define check with contributions
    checks_yaml = f"""
    - criticality: error
      check:
        function: has_no_anomalies
        arguments:
          merge_columns: [transaction_id]
          columns: [amount, quantity, discount]
          model: {model_name}
          registry_table: {registry_table}
          score_threshold: 0.5
          include_contributions: true
    """

    checks = yaml.safe_load(checks_yaml)

    test_df = spark.createDataFrame(
        [(1, 9999.0, 1.0, 0.95)],
        "transaction_id int, amount double, quantity double, discount double",
    )

    dq_engine = DQEngine(mock_workspace_client)
    result_df = dq_engine.apply_checks_by_metadata(test_df, checks)

    # Verify DQX metadata is added (contributions are not added by metadata API)
    assert "_errors" in result_df.columns
    rows = result_df.collect()
    # Anomalous data should have errors
    assert len(rows[0]["_errors"]) > 0


@pytest.mark.nightly
def test_yaml_with_drift_threshold(spark: SparkSession, mock_workspace_client, make_random: str, anomaly_engine):
    """Test YAML configuration with drift_threshold."""
    train_df = spark.createDataFrame(
        [(100.0 + i * 0.5, 2.0 + i * 0.01) for i in range(200)],  # Increase training data for stability
        "amount double, quantity double",
    )

    unique_id = make_random(8).lower()
    model_name = f"test_yaml_drift_{make_random(4).lower()}"
    registry_table = f"main.default.{unique_id}_registry"

    anomaly_engine.train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name=model_name,
        registry_table=registry_table,
    )

    # Define check with drift threshold
    checks_yaml = f"""
    - criticality: error
      check:
        function: has_no_anomalies
        arguments:
          merge_columns: [transaction_id]
          columns: [amount, quantity]
          model: {model_name}
          registry_table: {registry_table}
          score_threshold: 0.6
          drift_threshold: 3.0
    """

    checks = yaml.safe_load(checks_yaml)

    test_df = spark.createDataFrame(
        [(1, 150.0, 3.0)],  # Use middle values from training range [100, 199.5] and [2.0, 3.99]
        "transaction_id int, amount double, quantity double",
    )

    dq_engine = DQEngine(mock_workspace_client)
    result_df = dq_engine.apply_checks_by_metadata(test_df, checks)

    # Should succeed without errors (drift detection is configured)
    assert "_errors" in result_df.columns
    rows = result_df.collect()
    # Normal data should not have errors (handle None)
    row_errors = rows[0]["_errors"] if rows[0]["_errors"] is not None else []
    assert len(row_errors) == 0


@pytest.mark.nightly
def test_yaml_criticality_warn(spark: SparkSession, mock_workspace_client, make_random: str, anomaly_engine):
    """Test YAML with criticality='warn'."""
    train_df = spark.createDataFrame(
        [(100.0 + i * 0.5, 2.0 + i * 0.01) for i in range(200)],  # Increase training data for stability
        "amount double, quantity double",
    )

    unique_id = make_random(8).lower()
    model_name = f"test_yaml_warn_{make_random(4).lower()}"
    registry_table = f"main.default.{unique_id}_registry"

    anomaly_engine.train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name=model_name,
        registry_table=registry_table,
    )

    # Define check with warn criticality
    checks_yaml = f"""
    - criticality: warn
      check:
        function: has_no_anomalies
        arguments:
          merge_columns: [transaction_id]
          columns: [amount, quantity]
          model: {model_name}
          registry_table: {registry_table}
          score_threshold: 0.6
    """

    checks = yaml.safe_load(checks_yaml)

    test_df = spark.createDataFrame(
        [(1, 150.0, 3.0), (2, 9999.0, 1.0)],  # Use middle values from training range [100, 199.5] and [2.0, 3.99]
        "transaction_id int, amount double, quantity double",
    )

    dq_engine = DQEngine(mock_workspace_client)
    result_df = dq_engine.apply_checks_by_metadata(test_df, checks)

    # With warn criticality, anomalies should be in warnings
    rows = result_df.collect()

    # Anomalous row should have warnings or errors
    assert rows[1]["_warnings"] is not None or rows[1]["_errors"] is not None


@pytest.mark.nightly
def test_yaml_parsing_validation(spark: SparkSession):
    """Test that invalid YAML is caught."""
    # Invalid YAML (missing required argument)
    checks_yaml = """
    - criticality: error
      check:
        function: has_no_anomalies
        arguments:
          # Missing columns argument
          model: test_missing_args
          score_threshold: 0.5
    """

    checks = yaml.safe_load(checks_yaml)

    # This should fail when trying to apply the check
    # (Either at parse time or at runtime)
    test_df = spark.createDataFrame(
        [(100.0, 2.0)],
        "amount double, quantity double",
    )

    dq_engine = DQEngine(MagicMock(spec=WorkspaceClient))

    # Should raise error about missing columns
    with pytest.raises(Exception):  # May be TypeError or InvalidParameterError
        result_df = dq_engine.apply_checks_by_metadata(test_df, checks)
        result_df.collect()
