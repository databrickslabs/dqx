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


def test_yaml_based_checks(spark: SparkSession, mock_workspace_client):
    """Test applying anomaly checks defined in YAML."""
    # Train model first
    train_df = spark.createDataFrame(
        [(100.0, 2.0) for i in range(50)],
        "amount double, quantity double",
    )
    
    train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name="test_yaml",
        registry_table="main.default.test_yaml_registry",
    )
    
    # Define checks in YAML format
    checks_yaml = """
    - criticality: error
      check:
        function: has_no_anomalies
        arguments:
          columns: [amount, quantity]
          model: test_yaml
          registry_table: main.default.test_yaml_registry
          score_threshold: 0.5
    """
    
    checks = yaml.safe_load(checks_yaml)
    
    test_df = spark.createDataFrame(
        [(100.0, 2.0), (9999.0, 1.0)],
        "amount double, quantity double",
    )
    
    dq_engine = DQEngine(mock_workspace_client)
    result_df = dq_engine.apply_checks_by_metadata(test_df, checks)
    
    # Verify anomaly scoring works
    assert "anomaly_score" in result_df.columns
    
    # Verify errors are added for anomalies
    rows = result_df.collect()
    assert len(rows[1]["_errors"]) > 0  # Anomalous row has errors


def test_yaml_with_multiple_checks(spark: SparkSession, mock_workspace_client):
    """Test YAML with multiple anomaly and standard checks."""
    train_df = spark.createDataFrame(
        [(100.0, 2.0) for i in range(50)],
        "amount double, quantity double",
    )
    
    train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name="test_yaml_multi",
        registry_table="main.default.test_yaml_multi_registry",
    )
    
    # Define multiple checks in YAML
    checks_yaml = """
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
          model: test_yaml_multi
          registry_table: main.default.test_yaml_multi_registry
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


def test_yaml_with_custom_threshold(spark: SparkSession, mock_workspace_client):
    """Test YAML configuration with custom score_threshold."""
    train_df = spark.createDataFrame(
        [(100.0, 2.0) for i in range(50)],
        "amount double, quantity double",
    )
    
    train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name="test_yaml_threshold",
        registry_table="main.default.test_yaml_threshold_registry",
    )
    
    # Define check with custom threshold
    checks_yaml = """
    - criticality: error
      check:
        function: has_no_anomalies
        arguments:
          columns: [amount, quantity]
          model: test_yaml_threshold
          registry_table: main.default.test_yaml_threshold_registry
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
    assert "anomaly_score" in result_df.columns


def test_yaml_with_contributions(spark: SparkSession, mock_workspace_client):
    """Test YAML configuration with include_contributions flag."""
    train_df = spark.createDataFrame(
        [(100.0, 2.0, 0.1) for i in range(30)],
        "amount double, quantity double, discount double",
    )
    
    train(
        df=train_df,
        columns=["amount", "quantity", "discount"],
        model_name="test_yaml_contrib",
        registry_table="main.default.test_yaml_contrib_registry",
    )
    
    # Define check with contributions
    checks_yaml = """
    - criticality: error
      check:
        function: has_no_anomalies
        arguments:
          columns: [amount, quantity, discount]
          model: test_yaml_contrib
          registry_table: main.default.test_yaml_contrib_registry
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
    
    # Verify contributions column is added
    assert "anomaly_contributions" in result_df.columns


def test_yaml_with_drift_threshold(spark: SparkSession, mock_workspace_client):
    """Test YAML configuration with drift_threshold."""
    train_df = spark.createDataFrame(
        [(100.0, 2.0) for i in range(50)],
        "amount double, quantity double",
    )
    
    train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name="test_yaml_drift",
        registry_table="main.default.test_yaml_drift_registry",
    )
    
    # Define check with drift threshold
    checks_yaml = """
    - criticality: error
      check:
        function: has_no_anomalies
        arguments:
          columns: [amount, quantity]
          model: test_yaml_drift
          registry_table: main.default.test_yaml_drift_registry
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
    assert "anomaly_score" in result_df.columns


def test_yaml_criticality_warn(spark: SparkSession, mock_workspace_client):
    """Test YAML with criticality='warn'."""
    train_df = spark.createDataFrame(
        [(100.0, 2.0) for i in range(50)],
        "amount double, quantity double",
    )
    
    train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name="test_yaml_warn",
        registry_table="main.default.test_yaml_warn_registry",
    )
    
    # Define check with warn criticality
    checks_yaml = """
    - criticality: warn
      check:
        function: has_no_anomalies
        arguments:
          columns: [amount, quantity]
          model: test_yaml_warn
          registry_table: main.default.test_yaml_warn_registry
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

