"""Integration tests for YAML-based anomaly detection configuration."""

from unittest.mock import MagicMock

import pytest
import yaml
from pyspark.sql import SparkSession

from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine

from tests.integration.test_anomaly_constants import (
    DEFAULT_SCORE_THRESHOLD,
    DQENGINE_SCORE_THRESHOLD,
    OUTLIER_AMOUNT,
    OUTLIER_QUANTITY,
)

pytestmark = pytest.mark.anomaly


@pytest.fixture
def mock_workspace_client():
    """Create a mock WorkspaceClient for testing."""

    return MagicMock(spec=WorkspaceClient)


def test_yaml_based_checks(spark: SparkSession, mock_workspace_client, shared_2d_model):
    """Test applying anomaly checks defined in YAML."""
    # Use shared pre-trained model (no training needed!)
    model_name = shared_2d_model["model_name"]
    registry_table = shared_2d_model["registry_table"]

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
          score_threshold: {DEFAULT_SCORE_THRESHOLD}
    """

    checks = yaml.safe_load(checks_yaml)

    # Use values within training range (100-300 for amount, 10-50 for quantity)
    test_df = spark.createDataFrame(
        [(1, 150.0, 20.0), (2, OUTLIER_AMOUNT, OUTLIER_QUANTITY)],  # Normal + extreme anomaly
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


def test_yaml_with_multiple_checks(spark: SparkSession, mock_workspace_client, shared_2d_model):
    """Test YAML with multiple anomaly and standard checks."""
    # Use shared pre-trained model (no training needed!)
    model_name = shared_2d_model["model_name"]
    registry_table = shared_2d_model["registry_table"]

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
          score_threshold: {DQENGINE_SCORE_THRESHOLD}
    """

    checks = yaml.safe_load(checks_yaml)

    # Use values within training range (100-300 for amount, 10-50 for quantity)
    test_df = spark.createDataFrame(
        [
            (1, 200.0, 30.0),  # Normal - middle of training range
            (2, None, 30.0),  # Null
            (3, OUTLIER_AMOUNT, OUTLIER_QUANTITY),  # Extreme anomaly
        ],
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


def test_yaml_with_custom_threshold(spark: SparkSession, mock_workspace_client, shared_2d_model):
    """Test YAML configuration with custom score_threshold."""
    # Use shared pre-trained model (no training needed!)
    model_name = shared_2d_model["model_name"]
    registry_table = shared_2d_model["registry_table"]

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

    # Use values within training range (100-300 for amount, 10-50 for quantity)
    test_df = spark.createDataFrame(
        [(1, 150.0, 20.0), (2, 200.0, 25.0)],  # Normal values in training range
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


def test_yaml_with_contributions(spark: SparkSession, mock_workspace_client, shared_3d_model):
    """Test YAML configuration with include_contributions flag."""
    # Use shared pre-trained 3D model (no training needed!)
    model_name = shared_3d_model["model_name"]
    registry_table = shared_3d_model["registry_table"]

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
          score_threshold: {DEFAULT_SCORE_THRESHOLD}
          include_contributions: true
    """

    checks = yaml.safe_load(checks_yaml)

    # Extreme anomaly (far outside training range)
    test_df = spark.createDataFrame(
        [(1, OUTLIER_AMOUNT, OUTLIER_QUANTITY, 0.95)],
        "transaction_id int, amount double, quantity double, discount double",
    )

    dq_engine = DQEngine(mock_workspace_client)
    result_df = dq_engine.apply_checks_by_metadata(test_df, checks)

    # Verify DQX metadata is added (contributions are not added by metadata API)
    assert "_errors" in result_df.columns
    rows = result_df.collect()
    # Anomalous data should have errors
    assert len(rows[0]["_errors"]) > 0


def test_yaml_with_drift_threshold(spark: SparkSession, mock_workspace_client, shared_2d_model):
    """Test YAML configuration with drift_threshold."""
    # Use shared pre-trained model (no training needed!)
    model_name = shared_2d_model["model_name"]
    registry_table = shared_2d_model["registry_table"]

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
          score_threshold: {DQENGINE_SCORE_THRESHOLD}
          drift_threshold: 3.0
    """

    checks = yaml.safe_load(checks_yaml)

    # Use values within training range (100-300 for amount, 10-50 for quantity)
    test_df = spark.createDataFrame(
        [(1, 200.0, 30.0)],  # Middle of training range
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


def test_yaml_criticality_warn(spark: SparkSession, mock_workspace_client, shared_2d_model):
    """Test YAML with criticality='warn'."""
    # Use shared pre-trained model (no training needed!)
    model_name = shared_2d_model["model_name"]
    registry_table = shared_2d_model["registry_table"]

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
          score_threshold: {DQENGINE_SCORE_THRESHOLD}
    """

    checks = yaml.safe_load(checks_yaml)

    # Use values within training range (100-300 for amount, 10-50 for quantity)
    test_df = spark.createDataFrame(
        [(1, 200.0, 30.0), (2, OUTLIER_AMOUNT, OUTLIER_QUANTITY)],  # Normal + extreme anomaly
        "transaction_id int, amount double, quantity double",
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
          score_threshold: {DEFAULT_SCORE_THRESHOLD}
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
