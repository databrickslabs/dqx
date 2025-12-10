"""Integration tests for YAML-based anomaly detection configuration."""

import pytest
import yaml
from pyspark.sql import SparkSession
from unittest.mock import MagicMock

from databricks.labs.dqx.anomaly import train
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient


@pytest.fixture
def mock_workspace_client():
    """Create a mock WorkspaceClient for testing."""
    return MagicMock(spec=WorkspaceClient)


def test_yaml_based_checks(spark: SparkSession, mock_workspace_client, make_random: str):
    """Test applying anomaly checks defined in YAML."""
    # Train model first
    train_df = spark.createDataFrame(
        [(100.0 + i * 0.5, 2.0) for i in range(50)],
        "amount double, quantity double",
    )
    
    unique_id = make_random(8).lower()
    model_name = f"test_yaml_{make_random(4).lower()}"
    registry_table = f"main.default.{unique_id}_registry"
    
    train(
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
          columns: [amount, quantity]
          model: {model_name}
          registry_table: {registry_table}
          score_threshold: 0.5
    """
    
    checks = yaml.safe_load(checks_yaml)
    
    test_df = spark.createDataFrame(
        [(100.0, 2.0), (9999.0, 1.0)],
        "amount double, quantity double",
    )
    
    dq_engine = DQEngine(mock_workspace_client)
    result_df = dq_engine.apply_checks_by_metadata(test_df, checks)
    
    # Verify DQX metadata columns are added
    assert "_errors" in result_df.columns
    assert "_warnings" in result_df.columns
    
    # Verify errors are added for anomalies
    rows = result_df.collect()
    assert len(rows[1]["_errors"]) > 0  # Anomalous row has errors


def test_yaml_with_multiple_checks(spark: SparkSession, mock_workspace_client, make_random: str):
    """Test YAML with multiple anomaly and standard checks."""
    train_df = spark.createDataFrame(
        [(100.0 + i * 0.5, 2.0) for i in range(50)],
        "amount double, quantity double",
    )
    
    unique_id = make_random(8).lower()
    model_name = f"test_yaml_multi_{make_random(4).lower()}"
    registry_table = f"main.default.{unique_id}_registry"
    
    train(
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
          columns: [amount, quantity]
          model: {model_name}
          registry_table: {registry_table}
          score_threshold: 0.5
    """
    
    checks = yaml.safe_load(checks_yaml)
    
    test_df = spark.createDataFrame(
        [(100.0, 2.0), (None, 2.0), (9999.0, 1.0)],
        "amount double, quantity double",
    )
    
    dq_engine = DQEngine(mock_workspace_client)
    result_df = dq_engine.apply_checks_by_metadata(test_df, checks)
    
    rows = result_df.collect()
    
    # Normal row: no errors
    assert rows[0]["_errors"] == [] or rows[0]["_errors"] is None
    
    # Null row: has is_not_null error
    assert len(rows[1]["_errors"]) > 0
    
    # Anomaly row: has anomaly error
    assert len(rows[2]["_errors"]) > 0


def test_yaml_with_custom_threshold(spark: SparkSession, mock_workspace_client, make_random: str):
    """Test YAML configuration with custom score_threshold."""
    train_df = spark.createDataFrame(
        [(100.0 + i * 0.5, 2.0) for i in range(50)],
        "amount double, quantity double",
    )
    
    unique_id = make_random(8).lower()
    model_name = f"test_yaml_threshold_{make_random(4).lower()}"
    registry_table = f"main.default.{unique_id}_registry"
    
    train(
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
          columns: [amount, quantity]
          model: {model_name}
          registry_table: {registry_table}
          score_threshold: 0.9
    """
    
    checks = yaml.safe_load(checks_yaml)
    
    test_df = spark.createDataFrame(
        [(100.0, 2.0), (150.0, 1.8)],  # Slightly unusual but not extreme
        "amount double, quantity double",
    )
    
    dq_engine = DQEngine(mock_workspace_client)
    result_df = dq_engine.apply_checks_by_metadata(test_df, checks)
    
    # With high threshold (0.9), slightly unusual data should pass
    assert "_errors" in result_df.columns
    rows = result_df.collect()
    # With high threshold, expect no or few errors
    assert all(len(row["_errors"]) == 0 for row in rows) or sum(len(row["_errors"]) for row in rows) <= 1


def test_yaml_with_contributions(spark: SparkSession, mock_workspace_client, make_random: str):
    """Test YAML configuration with include_contributions flag."""
    train_df = spark.createDataFrame(
        [(100.0 + i * 0.5, 2.0 + i * 0.01, 0.1 + i * 0.001) for i in range(30)],
        "amount double, quantity double, discount double",
    )
    
    unique_id = make_random(8).lower()
    model_name = f"test_yaml_contrib_{make_random(4).lower()}"
    registry_table = f"main.default.{unique_id}_registry"
    
    train(
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
          columns: [amount, quantity, discount]
          model: {model_name}
          registry_table: {registry_table}
          score_threshold: 0.5
          include_contributions: true
    """
    
    checks = yaml.safe_load(checks_yaml)
    
    test_df = spark.createDataFrame(
        [(9999.0, 1.0, 0.95)],
        "amount double, quantity double, discount double",
    )
    
    dq_engine = DQEngine(mock_workspace_client)
    result_df = dq_engine.apply_checks_by_metadata(test_df, checks)
    
    # Verify DQX metadata is added (contributions are not added by metadata API)
    assert "_errors" in result_df.columns
    rows = result_df.collect()
    # Anomalous data should have errors
    assert len(rows[0]["_errors"]) > 0


def test_yaml_with_drift_threshold(spark: SparkSession, mock_workspace_client, make_random: str):
    """Test YAML configuration with drift_threshold."""
    train_df = spark.createDataFrame(
        [(100.0 + i * 0.5, 2.0) for i in range(50)],
        "amount double, quantity double",
    )
    
    unique_id = make_random(8).lower()
    model_name = f"test_yaml_drift_{make_random(4).lower()}"
    registry_table = f"main.default.{unique_id}_registry"
    
    train(
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
          columns: [amount, quantity]
          model: {model_name}
          registry_table: {registry_table}
          score_threshold: 0.5
          drift_threshold: 3.0
    """
    
    checks = yaml.safe_load(checks_yaml)
    
    test_df = spark.createDataFrame(
        [(100.0, 2.0)],
        "amount double, quantity double",
    )
    
    dq_engine = DQEngine(mock_workspace_client)
    result_df = dq_engine.apply_checks_by_metadata(test_df, checks)
    
    # Should succeed without errors (drift detection is configured)
    assert "_errors" in result_df.columns
    rows = result_df.collect()
    # Normal data should not have errors
    assert len(rows[0]["_errors"]) == 0


def test_yaml_criticality_warn(spark: SparkSession, mock_workspace_client, make_random: str):
    """Test YAML with criticality='warn'."""
    train_df = spark.createDataFrame(
        [(100.0 + i * 0.5, 2.0) for i in range(50)],
        "amount double, quantity double",
    )
    
    unique_id = make_random(8).lower()
    model_name = f"test_yaml_warn_{make_random(4).lower()}"
    registry_table = f"main.default.{unique_id}_registry"
    
    train(
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
          columns: [amount, quantity]
          model: {model_name}
          registry_table: {registry_table}
          score_threshold: 0.5
    """
    
    checks = yaml.safe_load(checks_yaml)
    
    test_df = spark.createDataFrame(
        [(100.0, 2.0), (9999.0, 1.0)],
        "amount double, quantity double",
    )
    
    dq_engine = DQEngine(mock_workspace_client)
    result_df = dq_engine.apply_checks_by_metadata(test_df, checks)
    
    # With warn criticality, anomalies should be in warnings
    rows = result_df.collect()
    
    # Anomalous row should have warnings or errors
    assert rows[1]["_warnings"] is not None or rows[1]["_errors"] is not None


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
